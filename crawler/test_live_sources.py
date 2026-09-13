# 用途：来源刷新、重访、种子热加载和加速时间测试。
# 分类：离线测试；使用：测试使用合成数据或模拟对象，不参与生产下载。
# 相关文件与阅读顺序：见同目录 README.md。

"""Mock network + accelerated clock tests for unattended source freshness."""
from __future__ import annotations

import asyncio
import dataclasses
import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from .campaign import fresh_campaign, readonly_status
from .config import CrawlConfig, validate_config
from .crawler import Crawler
from .live_sources import cached_source_is_fresh, parse_source_page, source_url, seed_snapshot
from .parsers import parse_battles
from .selftest import MockFetcher, _fast_cfg
from .test_discovery import page_html


class Clock:
    def __init__(self):
        self.now = 1_800_000_000.0
    def __call__(self):
        return self.now
    def advance(self, seconds):
        self.now += seconds


class LiveSourceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.root = Path(self.temp.name)
        self.resources = []
        self.clock = Clock()

    async def asyncTearDown(self):
        for value in reversed(self.resources):
            value.close()
        self.temp.cleanup()

    def crawler(self, routes, cfg=None):
        if cfg is None:
            cfg = _fast_cfg(str(self.root))
            cfg.base_url = 'http://offline'
            cfg.adaptive_discovery = True
            cfg.save_lists = False
            cfg.list_pause_free_memory_gb = 0
            cfg.list_resume_free_memory_gb = 0.01
            cfg.source_refresh.enabled = True
            cfg.source_refresh.urls = ['/players/leaderboard']
            cfg.source_refresh.refresh_interval = 100
            cfg.source_refresh.max_interval = 800
            cfg.source_refresh.error_retry_delay = 5
            cfg.source_refresh.poll_interval = 0.1
            cfg.source_refresh.player_revisit_interval = 10
            cfg.source_refresh.player_revisit_max_interval = 80
        c = Crawler(cfg, MockFetcher(routes))
        c.store._database._now = self.clock
        c._live_sources.clock = self.clock
        self.resources.append(c)
        return c

    async def source_once(self, c):
        rows = await c.store.pending(1)
        self.assertEqual(rows[0]['kind'], 'source')
        await c._process(rows[0])

    def visit_args(self, c, tag):
        return dict(player_key='player:'+tag, player_tag=tag,
                    url='http://offline/player/'+tag+'/battles',
                    interval=c.cfg.source_refresh.player_revisit_interval,
                    max_interval=c.cfg.source_refresh.player_revisit_max_interval)

    async def test_source_ttl_new_players_and_no_duplicate_poll(self):
        url = 'http://offline/players/leaderboard'
        c = self.crawler({url: [(200, '<a href="/player/AAA">a</a>'),
                                (200, '<a href="/player/AAA">a</a><a href="/player/BBB">b</a>')]})
        await c._live_sources.initialize()
        await self.source_once(c)
        first = c.store.conn.execute('SELECT * FROM seed_sources').fetchone()
        self.clock.advance(50)
        await c._live_sources.tick()
        self.assertEqual(len(c.fetcher.calls), 1)
        self.assertEqual(c.store.conn.execute("SELECT generation FROM seed_sources").fetchone()[0], 1)
        self.clock.advance(50)
        await c._live_sources.tick()
        await self.source_once(c)
        second = c.store.conn.execute('SELECT * FROM seed_sources').fetchone()
        self.assertGreater(second['fetched_at'], first['fetched_at'])
        self.assertEqual(second['generation'], 2)
        self.assertEqual(second['last_new_players'], 1)
        self.assertEqual(json.loads(second['cached_tags']), ['AAA', 'BBB'])
        self.assertEqual(c.store.conn.execute("SELECT COUNT(*) FROM tasks WHERE kind='list'").fetchone()[0], 2)

    async def test_zero_yield_sources_back_off(self):
        url = 'http://offline/players/leaderboard'
        c = self.crawler({url: [(200, '<a href="/player/AAA">a</a>')]})
        await c._live_sources.initialize()
        await self.source_once(c)
        self.clock.advance(100)
        await c._live_sources.tick()
        await self.source_once(c)
        row = c.store.conn.execute('SELECT * FROM seed_sources').fetchone()
        self.assertEqual(row['zero_new_streak'], 1)
        self.assertEqual(row['next_refresh_at'] - self.clock(), 200)
        self.assertEqual((await c.store.live_source_status())['totals']['source_new_players'], 1)

    async def test_failed_refresh_keeps_last_good_cache_and_expiry(self):
        url = 'http://offline/players/leaderboard'
        c = self.crawler({url: [(200, '<a href="/player/AAA">a</a>'), (200, '<html>please login</html>')]})
        await c._live_sources.initialize()
        await self.source_once(c)
        first = dict(c.store.conn.execute('SELECT * FROM seed_sources').fetchone())
        self.clock.advance(100)
        await c._live_sources.tick()
        await self.source_once(c)
        second = dict(c.store.conn.execute('SELECT * FROM seed_sources').fetchone())
        for field in ('fetched_at', 'cache_expires_at', 'cached_tags', 'html_sha256'):
            self.assertEqual(first[field], second[field])
        self.assertEqual(second['failures'], 1)
        self.assertEqual(second['next_refresh_at'], self.clock() + 5)
        self.assertIsNotNone(second['last_error'])
        self.clock.advance(1)
        await c._live_sources.tick()
        self.assertEqual(len(c.fetcher.calls), 2)
        self.assertFalse(c._stop.is_set())

    async def test_new_country_sources_are_bounded_and_same_origin(self):
        url = 'http://offline/players/leaderboard'
        html = '<a href="/player/AAA">a</a>' + ''.join(f'<a href="/players/leaderboard/{code}">country</a>' for code in ['us','fr','de','jp'])
        html += '<a href="https://elsewhere/players/leaderboard/gb">other</a>'
        c = self.crawler({url: [(200, html)]})
        c.cfg.source_refresh.max_sources = 3
        c.cfg.source_refresh.max_pending_tasks = 1
        await c._live_sources.initialize()
        await self.source_once(c)
        self.assertEqual(c.store.conn.execute('SELECT COUNT(*) FROM seed_sources').fetchone()[0], 3)
        await c._live_sources.tick()
        await c._live_sources.tick()
        self.assertEqual(c.store.conn.execute("SELECT COUNT(*) FROM tasks WHERE kind='source' AND status='pending'").fetchone()[0], 1)
        self.assertFalse(any('elsewhere' in row[0] for row in c.store.conn.execute('SELECT url FROM seed_sources')))

    async def test_restart_respects_source_due_time(self):
        url = 'http://offline/players/leaderboard'
        c = self.crawler({url: [(200, '<a href="/player/AAA">a</a>')]})
        await c._live_sources.initialize()
        await self.source_once(c)
        cfg = c.cfg
        c.close()
        self.clock.advance(50)
        resumed = self.crawler({}, cfg)
        await resumed._live_sources.initialize()
        self.assertEqual(resumed.fetcher.calls, [])
        self.assertEqual(resumed.store.conn.execute('SELECT generation FROM seed_sources').fetchone()[0], 1)

    async def test_restart_does_not_duplicate_inflight_source(self):
        c = self.crawler({})
        await c._live_sources.initialize()
        task = (await c.store.pending(1))[0]
        c.close()
        resumed = self.crawler({}, c.cfg)
        await resumed.store.requeue_inflight()
        await resumed._live_sources.initialize()
        rows = await resumed.store.pending(2)
        self.assertEqual([row['id'] for row in rows], [task['id']])

    async def test_seed_file_hot_reload_and_unchanged_file_not_reparsed(self):
        c = self.crawler({})
        path = self.root / 'seeds.txt'
        path.write_text('#AAA\n', encoding='utf-8')
        c.cfg.seeds_file = str(path)
        with patch('crawler.live_sources.seed_snapshot', wraps=seed_snapshot) as reader:
            await c._live_sources.initialize()
            self.clock.advance(60)
            await c._live_sources.tick()
            self.assertEqual(reader.call_count, 1)
            path.write_text('#AAA\n#BBB\n', encoding='utf-8')
            self.clock.advance(60)
            await c._live_sources.tick()
            self.assertEqual(reader.call_count, 2)
        self.assertEqual(c.store.conn.execute("SELECT COUNT(*) FROM tasks WHERE kind='list'").fetchone()[0], 2)
        self.assertEqual(c._live_sources.state['seed_file_new_tasks'], 2)

    async def test_memory_and_backlog_pause_source_creation(self):
        c = self.crawler({})
        with patch.object(c, '_refresh_list_memory_guard', return_value=True):
            await c._live_sources.initialize()
        self.assertEqual(c._live_sources.state['paused_reason'], 'low_memory')
        self.assertEqual(await c.store.stats(), {})
        c.cfg.detail_backlog_high = 1
        await c.store.add('http://offline/replay')
        await c._live_sources.tick()
        self.assertEqual(c._live_sources.state['paused_reason'], 'replay_backlog')
        self.assertEqual(c.store.conn.execute("SELECT COUNT(*) FROM tasks WHERE kind='source'").fetchone()[0], 0)
        await c.store.mark_done('http://offline/replay', 'test.json')
        await c._live_sources.tick()
        self.assertIsNone(c._live_sources.state['paused_reason'])
        self.assertEqual(c.store.conn.execute("SELECT COUNT(*) FROM tasks WHERE kind='source'").fetchone()[0], 1)

    async def test_revisit_due_backoff_and_pending_cap(self):
        c = self.crawler({})
        await c.store.add('http://offline/player/AAA/battles', '', 'list', {'tag':'AAA','page':1}, 'player:AAA')
        task = (await c.store.pending(1))[0]
        await c.store.commit_list_navigation(task_id=task['id'], url=task['url'], saved_path='', fingerprint='x',
            candidates=3, new_candidates=3, history_cutoff=False, reason=None, player_refresh=self.visit_args(c, 'AAA'))
        self.assertEqual(await c.store.schedule_player_revisits(1,1,10,80), 0)
        self.clock.advance(10)
        self.assertEqual(await c.store.schedule_player_revisits(1,1,10,80), 1)
        self.assertEqual(await c.store.schedule_player_revisits(1,1,10,80), 0)
        revisit = (await c.store.pending(1))[0]
        self.assertEqual(revisit['meta']['player_refresh_key'], 'player:AAA')
        await c.store.commit_list_navigation(task_id=revisit['id'], url=revisit['url'], saved_path='', fingerprint='x',
            candidates=3, new_candidates=0, history_cutoff=False, reason=None, player_refresh=self.visit_args(c, 'AAA'))
        row = c.store.conn.execute('SELECT * FROM player_revisits').fetchone()
        self.assertEqual(row['next_visit_at'] - self.clock(), 20)
        self.assertIsNone(row['active_task_id'])

    async def test_waiting_for_sources_does_not_finish_early(self):
        c = self.crawler({})
        await c.store.register_seed_sources(['http://offline/players/leaderboard'], 3)
        c.store.conn.execute('UPDATE seed_sources SET next_refresh_at=?', (self.clock() + 100,))
        c.store.conn.commit()
        c.cfg.source_refresh.poll_interval = 0.02
        running = asyncio.create_task(c.run(seeds=[]))
        try:
            await asyncio.sleep(0.08)
            self.assertFalse(running.done())
            self.assertEqual(c._phase, 'waiting_for_sources')
        finally:
            c._request_stop()
            await asyncio.wait_for(running, 3)
        self.assertEqual(c.fetcher.calls, [])

    async def test_large_player_pool_index_and_ready_frontier_gate(self):
        c = self.crawler({})
        conn = c.store.conn
        conn.executemany('INSERT INTO player_revisits(player_key,player_tag,url,next_visit_at,last_visit_at,last_new_candidates) VALUES(?,?,?,?,?,1)', (
            (f'player:P{i:06d}', f'P{i:06d}', f'http://offline/player/P{i:06d}/battles', 0, 0) for i in range(100000)))
        conn.commit()
        plan = '\n'.join(row[3] for row in conn.execute('EXPLAIN QUERY PLAN SELECT * FROM player_revisits WHERE active_task_id IS NULL AND next_visit_at<=? ORDER BY next_visit_at,player_key LIMIT 32', (self.clock(),)))
        self.assertIn('player_revisits_due', plan)
        self.assertNotIn('TEMP B-TREE', plan)
        for tag in ('FRESH1','FRESH2'):
            await c.store.add('http://offline/player/'+tag+'/battles', '', 'list', {'tag':tag,'page':1}, 'player:'+tag)
        self.assertEqual(await c.store.schedule_player_revisits(32,32,2,80), 0)
        # Future retries are not actually available new input. They must not
        # falsely suppress useful, due player revisits for hours.
        conn.execute("UPDATE tasks SET next_retry_at=? WHERE status='pending'", (self.clock()+100,))
        conn.commit()
        self.assertEqual(await c.store.schedule_player_revisits(32,32,2,80), 32)
        self.assertEqual(await c.store.schedule_player_revisits(32,32,2,80), 0)

    async def test_pipeline_boots_from_source_and_stops_at_target(self):
        source = 'http://offline/players/leaderboard'
        listing = 'http://offline/player/OWNER/battles'
        html = page_html([('MATCH','OTHER')])
        detail = parse_battles(html, 'http://offline', listing)[0]['url']
        replay = ('<div class="battle_replay" data-tag="MATCH">'
            '<div class="blue marker" data-x="8499" data-y="500" data-i="0" data-c="card0" data-t="203" data-s="t"></div>'
            '<table class="replay_elixir_table"><tr><td class="title">Total</td><td class="count">1</td><td class="elixir">3</td></tr></table>'
            '<table class="replay_elixir_table"><tr><td class="title">Total</td><td class="count">0</td><td class="elixir">0</td></tr></table>'
            '<div class="marker">0:30</div></div>')
        c = self.crawler({source: [(200, '<a href="/player/OWNER">owner</a>')],
                          listing: [(200, html)], detail: [(200, {'success': True, 'html': replay})]})
        c.cfg.max_battles = 1
        result = await asyncio.wait_for(c.run(seeds=[]), 8)
        self.assertEqual(result['stats']['ok'], 1)
        self.assertEqual(c.fetcher.calls, [source, listing, detail])
        self.assertEqual(readonly_status(c.cfg)['runtime']['phase'], 'target_reached')
        self.assertEqual((await c.store.live_source_status())['totals']['players_tracked'], 1)

    async def test_24h_accelerated_source_cache_and_queues_stay_bounded(self):
        urls = [f'http://offline/players/leaderboard/{code}' for code in ('us','fr','de','jp')]
        c = self.crawler({url: [(200, '<a href="/player/PLAYER000">p</a>')] for url in urls})
        c.cfg.source_refresh.urls = urls
        c.cfg.source_refresh.max_pending_tasks = 2
        c.cfg.source_refresh.refresh_interval = 1800
        c.cfg.source_refresh.max_interval = 14400
        c.cfg.source_refresh.player_revisit_batch_size = 4
        c.cfg.source_refresh.player_revisit_pending_limit = 4
        # Seed a synthetic visited-player pool, not actual user data.
        conn = c.store.conn
        conn.executemany("INSERT INTO tasks(url,dedup_key,kind,status,meta,created_at,updated_at) VALUES(?,?,'list','done',NULL,?,?)", (
            (f'http://offline/player/PLAYER{i:03d}/battles', f'player:PLAYER{i:03d}', self.clock(), self.clock()) for i in range(500)))
        conn.executemany('INSERT INTO player_revisits(player_key,player_tag,url,next_visit_at,last_visit_at,last_new_candidates) VALUES(?,?,?,?,?,1)', (
            (f'player:PLAYER{i:03d}',f'PLAYER{i:03d}',f'http://offline/player/PLAYER{i:03d}/battles',self.clock(),self.clock()) for i in range(500)))
        conn.commit()
        await c._live_sources.initialize()
        maximum_sources = maximum_revisits = 0
        for _ in range(48):
            self.clock.advance(1800)
            await c._live_sources.tick()
            maximum_sources = max(maximum_sources, conn.execute("SELECT COUNT(*) FROM tasks WHERE kind='source' AND status IN ('pending','inflight')").fetchone()[0])
            maximum_revisits = max(maximum_revisits, conn.execute('SELECT COUNT(*) FROM player_revisits WHERE active_task_id IS NOT NULL').fetchone()[0])
            for task in await c.store.pending(100):
                if task['kind'] == 'source':
                    await c._process(task)
                else:
                    await c.store.commit_list_navigation(task_id=task['id'], url=task['url'], saved_path='', fingerprint='same',
                        candidates=1, new_candidates=0, history_cutoff=False, reason=None,
                        player_refresh=self.visit_args(c, task['meta']['tag']))
        self.assertLessEqual(maximum_sources, 2)
        self.assertLessEqual(maximum_revisits, 4)
        self.assertEqual(conn.execute('SELECT COUNT(*) FROM seed_sources').fetchone()[0], 4)
        self.assertLess(conn.execute('SELECT SUM(length(cached_tags)) FROM seed_sources').fetchone()[0], 200)
        self.assertEqual(conn.execute('SELECT COUNT(*) FROM player_revisits').fetchone()[0], 500)
        self.assertTrue(all(row[0] for row in conn.execute('SELECT fetched_at FROM seed_sources')))
        self.assertEqual(conn.execute('PRAGMA quick_check').fetchone()[0], 'ok')


class SourcePolicyTests(unittest.TestCase):
    def test_standalone_cache_ttl_and_invalid_records(self):
        now = 1_800_000_000
        record = {'url':'http://offline/players/pro', 'tags':['AAA'],
                  'fetched_utc': dt.datetime.fromtimestamp(now-100, dt.timezone.utc).isoformat()}
        self.assertTrue(cached_source_is_fresh(record, record['url'], now, 101))
        self.assertFalse(cached_source_is_fresh(record, record['url'], now, 100))
        self.assertFalse(cached_source_is_fresh(record, record['url'], now, 0))
        for change in ({'tags':[]}, {'fetched_utc':'bad'}, {'fetched_utc':'2030-01-01T00:00:00+00:00'}, {'url':'wrong'}):
            self.assertFalse(cached_source_is_fresh({**record, **change}, record['url'], now, 3600))

    def test_source_url_scope_and_empty_page(self):
        for value in ('https://evil.test/players/pro', '/unrelated', 'file:///players/pro'):
            with self.assertRaises(ValueError):
                source_url(value, 'http://offline')
        with self.assertRaises(ValueError):
            parse_source_page('<html>verify browser</html>', 'http://offline', 100, 20)
        with self.assertRaises(ValueError):
            parse_source_page('<a href="/player/AAA">a</a><a href="/player/BBB">b</a>', 'http://offline', 1, 20)

    def test_new_campaign_enables_live_sources_without_mutating_base(self):
        cfg = CrawlConfig()
        campaign = fresh_campaign(cfg, 'sample', '2026-09-01', 1000000)
        self.assertFalse(cfg.source_refresh.enabled)
        self.assertTrue(campaign.source_refresh.enabled)
        bad = dataclasses.replace(campaign, source_refresh=dataclasses.replace(campaign.source_refresh, poll_interval=0))
        with self.assertRaises(ValueError):
            validate_config(bad)


if __name__ == '__main__':
    unittest.main()
