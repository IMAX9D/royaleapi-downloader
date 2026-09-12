"""Offline duplicate-navigation regressions; no production URLs are requested."""
from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from .campaign import fresh_campaign, readonly_status
from .config import CrawlConfig
from .crawler import Crawler
from .discovery import canonical_list_url, navigation_decision, page_fingerprint
from .parsers import parse_battles
from .queue import TaskStore
from .selftest import MockFetcher, _fast_cfg


def page_html(records, next_cursor=None):
    cards = ''.join(f'<div><img class="deck_card" data-card-key="card{i}"><span class="card-level">Lvl 14</span></div>' for i in range(8))
    parts = []
    for battle, opponent in records:
        parts.append(
            '<div class="battle_list_battle" data-battle-type="PvP">'
            '<h4 class="game_mode_header">Ladder</h4><div class="battle-team-segment-container">'
            '<a class="player_name_header" href="/player/OWNER/battles">OWNER</a>'
            f'<div id="deck_a">{cards}</div>'
            f'<a class="player_name_header" href="/player/{opponent}/battles">{opponent}</a>'
            f'<div id="deck_b">{cards}</div></div>'
            f'<button class="replay_button" data-replay="{battle}" data-team-tags="OWNER" '
            f'data-opponent-tags="{opponent}" data-team-crowns="1" data-opponent-crowns="0" data-draft="0"></button></div>'
        )
    if next_cursor is not None:
        parts.append(f'<a href="/player/OWNER/battles/history?before={next_cursor}">next</a>')
    return ''.join(parts)


class DiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.root = Path(self.temp.name)
        self.resources = []

    async def asyncTearDown(self):
        for value in reversed(self.resources):
            value.close()
        self.temp.cleanup()

    def store(self):
        value = TaskStore(str(self.root / 'queue.sqlite3'), 3)
        self.resources.append(value)
        return value

    def crawler(self, routes, *, discover=False, max_pages=20):
        cfg = _fast_cfg(str(self.root))
        cfg.base_url = 'http://offline'
        cfg.adaptive_discovery = True
        cfg.discover_players = discover
        cfg.max_pages_per_player = max_pages
        cfg.save_lists = False
        cfg.list_pause_free_memory_gb = 0
        cfg.list_resume_free_memory_gb = 0.01
        value = Crawler(cfg, MockFetcher(routes))
        self.resources.append(value)
        return value

    async def test_four_fresh_one_overlap_even_one_slot_and_restart(self):
        store = self.store()
        for prefix, band, count in [('old', 'overlap', 10), ('new', 'fresh', 40)]:
            await store.add_many([(f'http://offline/{prefix}{i}', '', 'list', {'discovery_band': band}, f'{prefix}{i}') for i in range(count)])
        selected = []
        for i in range(25):
            if i == 3:
                store.close()
                store = self.store()
            rows = await store.pending_authoritative_fair(limit=1, ready_limit=4, list_limit=1)
            self.assertEqual(len(rows), 1)
            row = rows[0]
            selected.append(row['meta']['discovery_band'])
            await store.mark_done(row['url'], '', task_id=row['id'])
        self.assertEqual(selected, ['fresh'] * 4 + ['overlap'] + (['fresh'] * 4 + ['overlap']) * 4)

    async def test_overlap_frontier_is_not_blacklisted_when_fresh_is_empty(self):
        store = self.store()
        await store.add_many([(f'http://offline/{i}', '', 'list', {'discovery_band': 'overlap'}, str(i)) for i in range(6)])
        rows = await store.pending_authoritative_fair(limit=6, ready_limit=20, list_limit=6)
        self.assertEqual(len(rows), 6)
        self.assertTrue(all(row['meta']['discovery_band'] == 'overlap' for row in rows))

    async def test_new_evidence_promotes_pending_player_without_duplicate(self):
        store = self.store()
        await store.add('http://offline/P', '', 'list', {'discovery_band': 'overlap'}, 'player:P')
        before = store.conn.execute("SELECT id FROM tasks WHERE dedup_key='player:P'").fetchone()[0]
        self.assertFalse(await store.add('http://offline/P', '', 'list', {'discovery_band': 'fresh'}, 'player:P'))
        row = store.conn.execute("SELECT * FROM tasks WHERE dedup_key='player:P'").fetchone()
        self.assertEqual(row['id'], before)
        self.assertEqual(row['dispatch_rank'], 2)
        self.assertEqual(row['discovery_band'], 0)
        self.assertEqual((await store.stats())['pending'], 1)

    async def test_duplicate_chain_stops_before_third_navigation(self):
        first = 'http://offline/player/OWNER/battles'
        second = first + '/history?before=200'
        third = first + '/history?before=100'
        crawler = self.crawler({
            first: [(200, page_html([('KNOWN1', 'P1')], 200))],
            second: [(200, page_html([('KNOWN2', 'P2')], 100))],
            third: [(200, page_html([('KNOWN3', 'P3')]))],
        })
        await crawler.store.import_excluded_battles(['KNOWN1', 'KNOWN2', 'KNOWN3'], 'offline test')
        result = await asyncio.wait_for(crawler.run([('OWNER', 'test')]), 5)
        self.assertEqual(crawler.fetcher.calls, [first, second])
        self.assertEqual(result['navigation']['history_cutoffs'], 1)
        self.assertEqual(result['navigation']['duplicate_candidates'], 2)
        self.assertEqual(result['navigation']['eligible_duplicate_ratio'], 1.0)
        self.assertEqual(result['stats']['ok'], 0)
        self.assertEqual(readonly_status(crawler.cfg)['navigation']['pages'], 2)

    async def test_fresh_players_prioritized_and_overlap_kept(self):
        url = 'http://offline/player/OWNER/battles'
        crawler = self.crawler({url: [(200, page_html([('OLD', 'OLDPLAYER'), ('NEW', 'NEWPLAYER')]))]}, discover=True, max_pages=1)
        await crawler.store.import_excluded_battles(['OLD'], 'offline test')
        await crawler._seed([('OWNER', '')])
        task = (await crawler.store.pending(1))[0]
        await crawler._process_list(task)
        rows = {row['dedup_key']: dict(row) for row in crawler.store.conn.execute("SELECT * FROM tasks WHERE kind='list'")}
        self.assertEqual(rows['player:NEWPLAYER']['dispatch_rank'], 2)
        self.assertEqual(rows['player:OLDPLAYER']['dispatch_rank'], 4)
        self.assertEqual(rows['player:OLDPLAYER']['status'], 'pending')
        self.assertEqual((await crawler.store.navigation_stats())['new_candidates'], 1)
        self.assertEqual((await crawler.store.navigation_stats())['duplicate_candidates'], 1)
        self.assertEqual(await crawler.store.list_new_battle_tags(task['id']), set())

    async def test_crash_after_replay_enqueue_keeps_navigation_credit(self):
        url = 'http://offline/player/OWNER/battles'
        html = page_html([('NEW', 'NEWPLAYER')])
        crawler = self.crawler({url: [(200, html)]}, discover=True, max_pages=1)
        await crawler._seed([('OWNER', '')])
        task = (await crawler.store.pending(1))[0]
        battle = parse_battles(html, crawler.cfg.base_url, url)[0]
        await crawler.store.add_battles([(
            battle['url'], url, {'battle_tag': 'NEW'}, 'battle:NEW', 'NEW', battle['metadata'],
        )], source_task_id=task['id'])
        cfg = crawler.cfg
        crawler.close()
        resumed = Crawler(cfg, MockFetcher({url: [(200, html)]}))
        self.resources.append(resumed)
        await resumed._process_list(task)
        self.assertEqual((await resumed.store.navigation_stats())['new_candidates'], 1)
        row = resumed.store.conn.execute("SELECT discovery_band FROM tasks WHERE dedup_key='player:NEWPLAYER'").fetchone()
        self.assertEqual(row[0], 0)

    async def test_history_url_normalization_prevents_duplicate_task(self):
        crawler = self.crawler({})
        await crawler._seed([
            ('http://offline/player/OWNER/battles/history?before=123&filter=1', 'a'),
            ('http://offline/player/owner/battles/history/?filter=1&before=123#ignored', 'b'),
            ('http://offline/player/OWNER/battles/history?before=124&filter=1', 'c'),
        ])
        self.assertEqual((await crawler.store.stats())['pending'], 2)

    async def test_counter_commit_is_idempotent(self):
        store = self.store()
        await store.add('http://offline/list', '', 'list')
        task = (await store.pending(1))[0]
        args = dict(task_id=task['id'], url=task['url'], saved_path='', fingerprint='a'*64,
                    candidates=10, new_candidates=2, history_cutoff=False, reason=None)
        await store.commit_list_navigation(**args)
        await store.commit_list_navigation(**args)
        self.assertEqual((await store.navigation_stats())['pages'], 1)
        self.assertEqual((await store.navigation_stats())['duplicate_candidates'], 8)


class DiscoveryPolicyTests(unittest.TestCase):
    def test_zero_chain_resets_when_any_new_candidate_appears(self):
        value = navigation_decision(eligible_count=10, new_count=1, previous_zero=8,
                                    fingerprint='new', previous_fingerprint='old', zero_limit=2)
        self.assertEqual(value['zero_chain'], 0)
        self.assertFalse(value['stop_history'])

    def test_identical_battle_page_stops_loop(self):
        fingerprint = page_fingerprint(['A', 'B'])
        self.assertEqual(fingerprint, page_fingerprint(['B', 'A', 'A']))
        value = navigation_decision(eligible_count=2, new_count=0, previous_zero=0,
                                    fingerprint=fingerprint, previous_fingerprint=fingerprint, zero_limit=5)
        self.assertTrue(value['stop_history'])
        self.assertEqual(value['reason'], 'same_battle_page')

    def test_canonicalization_preserves_meaningful_query_parameters(self):
        base = 'https://example.com/player/P/battles/history'
        self.assertNotEqual(canonical_list_url(base+'?before=1'), canonical_list_url(base+'?before=2'))
        self.assertNotEqual(canonical_list_url(base+'?before=1&mode=a'), canonical_list_url(base+'?before=1&mode=b'))

    def test_legacy_default_unchanged_new_campaign_enabled(self):
        cfg = CrawlConfig()
        self.assertFalse(cfg.adaptive_discovery)
        result = fresh_campaign(cfg, 'example', '2026-09-01', 1000000)
        self.assertTrue(result.adaptive_discovery)
        self.assertEqual(result.max_pages_per_player, 1)


if __name__ == '__main__':
    unittest.main()
