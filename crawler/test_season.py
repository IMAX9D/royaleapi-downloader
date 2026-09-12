"""Closed-season regression tests: synthetic network/data, no live requests."""
from __future__ import annotations

import asyncio
import dataclasses
import html
import json
import tempfile
import unittest
from pathlib import Path

from .campaign import readonly_status, season_campaign
from .config import validate_config
from .crawler import Crawler
from .parsers import parse_battles
from .queue import TaskStore
from .season import (battle_timestamp, historical_pool, parse_manifest,
                     parse_profile_history_html, parse_roster_html, utc_timestamp)
from .selftest import MockFetcher, _fast_cfg
from .test_discovery import page_html

START = utc_timestamp('2026-08-03T09:00:00Z')
END = utc_timestamp('2026-09-07T09:00:00Z')


def manifest(tags=('OWNER',)):
    return {'schema_version': 1, 'season_id': '2026-08', 'top_n': len(tags),
        'ranking_type': 'path_of_legends', 'eligibility': 'historical_top10000',
        'start_utc': '2026-08-03T09:00:00Z', 'end_utc': '2026-09-07T09:00:00Z',
        'source_urls': ['https://example.test/season/2026-08'],
        'boundary_source': 'synthetic test boundary, not live calendar evidence',
        'players': [{'rank': i + 1, 'tag': tag, 'rank_season': '2026-08'} for i, tag in enumerate(tags)]}


def battle_page(records, cursor=None, owner='OWNER'):
    # records: (replay ID, opponent, actual battle timestamp)
    result = ''.join(page_html([(tag, opponent)]).replace(
        'data-battle-type="PvP"', f'data-battle-type="PvP" data-timestamp="{stamp}"')
        for tag, opponent, stamp in records).replace('OWNER', owner)
    if cursor is not None:
        result += f'<a href="/player/{owner}/battles/history?before={cursor}">next</a>'
    return result


def payload(tag):
    return {'success': True, 'html': (
        f'<div class="battle_replay" data-tag="{tag}">'
        '<div class="blue marker" data-x="8499" data-y="500" data-i="0" data-c="card0" '
        'data-t="203" data-s="t"></div>'
        '<table class="replay_elixir_table"><tr><td class="title">Total</td>'
        '<td class="count">1</td><td class="elixir">3</td></tr></table>'
        '<table class="replay_elixir_table"><tr><td class="title">Total</td>'
        '<td class="count">0</td><td class="elixir">0</td></tr></table>'
        '<div class="marker">0:30</div></div>')}


class SeasonManifestTests(unittest.TestCase):
    def test_timezone_bounds_hash_and_incomplete_roster(self):
        roster = parse_manifest(manifest())
        self.assertEqual((roster.start, roster.end), (START, END))
        self.assertEqual(parse_manifest(roster.manifest()).sha256, roster.sha256)
        for change in ({'top_n': 2}, {'end_utc': '2026-09-07'},
                       {'roster_sha256': '0' * 64}, {'ranking_type': 'trophy_road'},
                       {'season_id': '2026-07'}, {'start_utc': '2026-09-08T00:00:00Z'}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                parse_manifest({**manifest(), **change})
        with self.assertRaises(ValueError):
            parse_manifest(manifest(), now=END - 1)

    def test_historical_pool_accepts_sparse_ranks_caps_and_keeps_proof(self):
        rows = [{'tag': 'AAA', 'rank': 9999, 'rank_season': '2025-08'},
                {'tag': '#AAA', 'rank': 30, 'rank_season': '2026-07'},
                {'tag': 'BBB', 'rank': 10001, 'rank_season': '2026-08'},
                {'tag': 'CCC', 'rank': 10000, 'rank_season': '2026-06'},
                {'tag': 'DDD', 'rank': 2, 'rank_season': '2026-08'}]
        pool = historical_pool(rows, 2, '2026-08')
        self.assertEqual(pool, (('AAA', '2025-08', 9999), ('CCC', '2026-06', 10000)))
        roster = parse_manifest({**manifest(('AAA', 'CCC')), 'players': rows})
        self.assertEqual(roster.rank_season('AAA'), '2025-08')
        with self.assertRaisesRegex(ValueError, '不足'):
            historical_pool(rows, 4, '2026-08')
        with self.assertRaises(ValueError):
            historical_pool([{'tag': 'AAA', 'rank': 2, 'rank_season': '2026-09'}], 1, '2026-08')

    def test_leaderboard_parser_does_not_evaluate_script_or_use_unrelated_links(self):
        source = ('<h1>Top Global Players for Season 2026-08</h1><a href="/player/WRONG">w</a>'
            '<script>initRoster($(\'#roster\'), [{"rank":1,"tag":"AAA"}],null,is_season);throw "never execute";</script>')
        self.assertEqual(parse_roster_html(source, '2026-08'), [{'rank': 1, 'tag': 'AAA'}])
        with self.assertRaises(ValueError):
            parse_roster_html(source, '2026-07')
        with self.assertRaises(ValueError):
            parse_roster_html('<h1>Just a moment...</h1>', '2026-08')

    def test_profile_qualifies_only_own_history_not_current_stats_or_esport_badges(self):
        def popup(rank, season):
            return html.escape(f'<div class="hist_heatmap__popup_rank">{rank}</div><div class="hist_heatmap__popup_season">{season}</div>', quote=True)
        source = ('<h2>#AAA</h2><table>Current Season Rank 1</table>'
            f'<div data-html="{popup(1,"2026-08")}"></div>'
            '<div class="player__ladder_history_container">'
            f'<div data-html="{popup("9,999","2026-07")}"></div>'
            f'<div data-html="{popup(10001,"2026-08")}"></div></div>')
        self.assertEqual(parse_profile_history_html(source, '2026-08'), {'tag': 'AAA', 'rank': 9999, 'rank_season': '2026-07'})
        self.assertIsNone(parse_profile_history_html('<h2>#AAA</h2><p>Rank 1</p>', '2026-08'))
        with self.assertRaises(ValueError):
            parse_profile_history_html('<h1>Login</h1>', '2026-08')

    def test_battle_timestamp_is_strict_and_handles_milliseconds(self):
        self.assertEqual(battle_timestamp({'timestamp': START * 1000}), START)
        self.assertEqual(battle_timestamp({'timestamp': str(START)}), START)
        for value in (None, True, START + 0.9, 'yesterday', float('nan')):
            self.assertIsNone(battle_timestamp({'timestamp': value}))


class SeasonCrawlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.root = Path(self.temp.name)
        self.resources = []

    async def asyncTearDown(self):
        for value in reversed(self.resources):
            value.close()
        self.temp.cleanup()

    def crawler(self, routes=None, tags=('OWNER',), excludes=()):
        path = self.root / 'roster.json'
        path.write_text(json.dumps(manifest(tags)), encoding='utf-8')
        base = _fast_cfg(str(self.root))
        base.base_url = 'http://offline'
        base.save_lists = False
        base.list_pause_free_memory_gb = 0
        base.list_resume_free_memory_gb = 0.01
        cfg = season_campaign(base, 'closed', '2026-08', str(path), len(tags), list(excludes))
        value = Crawler(cfg, MockFetcher(routes or {}))
        self.resources.append(value)
        return value

    async def test_season_window_membership_and_guarded_seeds(self):
        crawler = self.crawler()
        md = parse_battles(battle_page([('BATTLE', 'OTHER', START)]), 'http://offline', '/')[0]['metadata']
        for stamp, expected in ((START - 1, False), (START, True), (END - 1, True), (END, False), (END + 1, False)):
            self.assertEqual(crawler._eligible_metadata({**md, 'timestamp': stamp}), expected)
        self.assertFalse(crawler._eligible_metadata({**md, 'timestamp': None}))
        self.assertFalse(crawler._eligible_metadata({**md, 'team_tags': ['OUTSIDE']}))
        for seed in ('OUTSIDE', 'http://evil/player/OWNER/battles', 'http://offline/data/replay?tag=BATTLE'):
            with self.assertRaises(ValueError):
                await crawler._seed([(seed, 'manual')])
        with self.assertRaises(ValueError):
            validate_config(dataclasses.replace(crawler.cfg, discover_players=True))
        with self.assertRaises(ValueError):
            validate_config(dataclasses.replace(crawler.cfg, source_refresh=dataclasses.replace(crawler.cfg.source_refresh, enabled=True)))

    async def test_backfill_passes_duplicate_pages_and_excludes_both_time_boundaries(self):
        first = f'http://offline/player/OWNER/battles/history?before={END * 1000}'
        second = f'http://offline/player/OWNER/battles/history?before={(END - 100) * 1000}'
        third = f'http://offline/player/OWNER/battles/history?before={(END - 200) * 1000}'
        pages = {
            first: battle_page([('KNOWN1', 'OTHER', END - 10), ('TOONEW', 'OTHER', END)], (END - 100) * 1000),
            second: battle_page([('KNOWN2', 'OTHER', END - 110)], (END - 200) * 1000),
            third: battle_page([('NEW', 'OTHER', START), ('TOOOLD', 'OTHER', START - 1)], START * 1000),
        }
        routes = {url: [(200, page)] for url, page in pages.items()}
        replay_url = parse_battles(pages[third], 'http://offline', third)[0]['url']
        routes[replay_url] = [(200, payload('NEW'))]
        crawler = self.crawler(routes)
        await crawler.store.import_excluded_battles(['KNOWN1', 'KNOWN2'], 'fixture old corpus')
        result = await asyncio.wait_for(crawler.run(), 8)
        self.assertEqual(result['stats']['ok'], 1)
        self.assertEqual(result['stats']['list_pages'], 3)
        self.assertEqual(result['navigation']['duplicate_candidates'], 2)
        self.assertEqual(set(crawler.fetcher.calls), {first, second, third, replay_url})
        paths = list(Path(crawler.cfg.output_dir).glob('raw/battles/*/*.json'))
        self.assertEqual(len(paths), 1)
        saved = json.loads(paths[0].read_text(encoding='utf-8'))
        self.assertEqual(saved['timestamp'], START)
        self.assertEqual(saved['collection_cohort']['expert_sides'][0]['tag'], 'OWNER')
        self.assertTrue((Path(crawler.cfg.output_dir) / 'collection-roster.json').exists())
        self.assertEqual(readonly_status(crawler.cfg)['runtime']['phase'], 'completed')

    async def test_same_replay_from_opposite_views_keeps_original_metadata_and_one_task(self):
        crawler = self.crawler(tags=('OWNER', 'OTHER'))
        first = parse_battles(battle_page([('SAME', 'OTHER', START)]), 'http://offline', '/player/OWNER/battles')[0]
        second = parse_battles(battle_page([('SAME', 'OWNER', START)], owner='OTHER'), 'http://offline', '/player/OTHER/battles')[0]
        # The helper's global owner replacement is not used for perspective proof.
        second['metadata'] = {**first['metadata'], 'team_tags': ['OTHER'], 'opponent_tags': ['OWNER']}
        for battle in (first, second):
            await crawler.store.add_battles([(battle['url'], 'fixture', {}, 'battle:SAME', 'SAME', battle['metadata'])])
        self.assertEqual((await crawler.store.stats())['pending'], 1)
        self.assertEqual((await crawler.store.get_battle_metadata('SAME'))['team_tags'], ['OWNER'])

    async def test_unchanged_roster_resumes_but_changed_proof_is_rejected(self):
        crawler = self.crawler()
        await crawler.run(limit=1)
        cfg = crawler.cfg
        crawler.close()
        changed = manifest()
        changed['players'][0]['rank'] = 9999
        Path(cfg.season_roster_file).write_text(json.dumps(changed), encoding='utf-8')
        resumed = Crawler(cfg, MockFetcher({}))
        self.resources.append(resumed)
        with self.assertRaisesRegex(ValueError, '已冻结'):
            await resumed.run()

    async def test_old_database_is_read_only_and_only_completed_ids_are_excluded(self):
        old_path = self.root / 'old.sqlite3'
        old = TaskStore(str(old_path), 2)
        await old.add('http://offline/old', dedup_key='battle:OLD')
        await old.mark_done('http://offline/old', 'old.json')
        await old.add('http://offline/pending', dedup_key='battle:NOTDONE')
        old.close()
        before = old_path.read_bytes()
        crawler = self.crawler(excludes=(str(old_path),))
        await crawler.run(limit=1)
        self.assertEqual(await crawler.store.excluded_battles_count(), 1)
        self.assertFalse(await crawler.store.add('http://offline/old2', dedup_key='battle:OLD'))
        self.assertTrue(await crawler.store.add('http://offline/new', dedup_key='battle:NOTDONE'))
        self.assertEqual(old_path.read_bytes(), before)

    async def test_bad_history_cursor_fails_instead_of_looping(self):
        first = f'http://offline/player/OWNER/battles/history?before={END}'
        crawler = self.crawler({first: [(200, battle_page([], END))]})
        await crawler._seed([(first, 'fixture')])
        task = (await crawler.store.pending(1))[0]
        with self.assertRaisesRegex(ValueError, '游标'):
            await crawler._process_list(task)

    async def test_challenge_page_is_not_misreported_as_empty_season(self):
        first = f'http://offline/player/OWNER/battles/history?before={END * 1000}'
        crawler = self.crawler({first: [(200, '<h1>Please log in</h1>')]})
        await crawler._seed([(first, 'fixture')])
        task = (await crawler.store.pending(1))[0]
        with self.assertRaisesRegex(ValueError, '列表结构'):
            await crawler._process_list(task)


if __name__ == '__main__':
    unittest.main()
