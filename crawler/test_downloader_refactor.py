# 用途：配置、存储、事务恢复与队列规模回归。
# 分类：离线测试；使用：测试使用合成数据或模拟对象，不参与生产下载。
# 相关文件与阅读顺序：见同目录 README.md。

"""Offline correctness, recovery and scaling regressions; never use production data."""
from __future__ import annotations

import asyncio
import dataclasses
import datetime as dt
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from .campaign import fresh_campaign, readonly_status
from .config import CrawlConfig, load_config
from .crawler import Crawler
from .file_io import FileIO, PersistenceError
from .queue import TaskStore
from .run_lock import RunLock
from .selftest import MockFetcher, _fast_cfg
from .storage import Storage


class RefactorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.root = Path(self.temp.name)
        self.stores = []
        self.crawlers = []

    async def asyncTearDown(self):
        for crawler in reversed(self.crawlers):
            crawler.close()
        for store in reversed(self.stores):
            store.close()
        self.temp.cleanup()

    def store(self, name='queue.sqlite3'):
        store = TaskStore(str(self.root / name), 3)
        self.stores.append(store)
        return store

    def crawler(self, name='run'):
        cfg = _fast_cfg(str(self.root / name))
        cfg.list_pause_free_memory_gb = 0
        cfg.list_resume_free_memory_gb = 0.01
        crawler = Crawler(cfg, MockFetcher({}))
        self.crawlers.append(crawler)
        return crawler

    async def test_legacy_migration_preserves_tasks(self):
        path = self.root / 'legacy.sqlite3'
        conn = sqlite3.connect(path)
        conn.executescript('''CREATE TABLE tasks (
            id INTEGER PRIMARY KEY,url TEXT,seed TEXT,status TEXT,
            attempts INTEGER DEFAULT 0,error TEXT,next_retry_at REAL DEFAULT 0,
            saved_path TEXT,created_at REAL,updated_at REAL);
            INSERT INTO tasks VALUES(1,'http://x/old',NULL,'done',0,NULL,0,'old.json',1,1);
        ''')
        conn.close()
        store = self.store('legacy.sqlite3')
        self.assertEqual(await store.done_details(), 1)
        row = store.conn.execute('SELECT * FROM tasks').fetchone()
        self.assertEqual(row['saved_path'], 'old.json')
        self.assertEqual(row['dedup_key'], 'http://x/old')
        self.assertEqual(row['dispatch_rank'], 0)

    async def test_counters_track_transitions_and_reopen(self):
        store = self.store()
        await store.add_many([(f'http://x/{i}', '', 'list' if i % 2 else 'detail', {}, str(i)) for i in range(60)])
        tasks = await store.pending(limit=30)
        for i, task in enumerate(tasks):
            if i % 3 == 0:
                await store.mark_done(task['url'], 'file.json', task_id=task['id'])
            elif i % 3 == 1:
                await store.mark_retry(task['url'], 'retry', 0, task_id=task['id'])
            else:
                await store.mark_skipped(task['url'], 'skip', task_id=task['id'])
        expected = dict(store.conn.execute('SELECT status,COUNT(*) FROM tasks GROUP BY status'))
        self.assertEqual(await store.stats(), expected)
        self.assertEqual(await store.done_details(), 10)
        await store.record_authoritative_result('A', status='queued', tier='pending')
        await store.record_authoritative_result('A', status='accepted', tier='native_static_v2', contract_sha256='a'*64)
        self.assertEqual(await store.authoritative_accepted_count(), 1)
        self.assertEqual((await store.authoritative_stats())['status'], {'accepted': 1})
        store.close()
        reopened = self.store()
        self.assertEqual(await reopened.stats(), expected)
        self.assertEqual(await reopened.authoritative_accepted_count('a'*64), 1)
        self.assertEqual(reopened.conn.execute('PRAGMA quick_check').fetchone()[0], 'ok')

    async def test_atomic_multi_connection_claim_and_index(self):
        left = self.store()
        right = self.store()
        await left.add_many([(f'http://x/{i}', '', 'detail', {}, str(i)) for i in range(100)])
        groups = await asyncio.gather(left.pending(50), right.pending(50))
        ids = [row['id'] for group in groups for row in group]
        self.assertEqual(len(ids), 100)
        self.assertEqual(len(set(ids)), 100)
        plan = str([tuple(row) for row in left.conn.execute(
            "EXPLAIN QUERY PLAN SELECT id FROM tasks WHERE status='pending' AND dispatch_rank=0 AND next_retry_at<=? ORDER BY next_retry_at,id LIMIT 50", (time.time(),),
        )])
        self.assertIn('idx_tasks_ready_v2', plan)
        self.assertNotIn('TEMP B-TREE', plan)

    async def test_database_work_does_not_block_event_loop(self):
        store = self.store()
        original = store._database.stats
        def slow_stats():
            time.sleep(0.15)
            return original()
        ticks = 0
        async def ticker():
            nonlocal ticks
            end = time.perf_counter() + 0.12
            while time.perf_counter() < end:
                ticks += 1
                await asyncio.sleep(0.005)
        with patch.object(store._database, 'stats', slow_stats):
            await asyncio.gather(store.stats(), ticker())
        self.assertGreater(ticks, 5)

    async def test_campaign_cannot_change_date_in_place(self):
        store = self.store()
        contract = {'campaign_id': 'batch-1', 'min_battle_timestamp': 123}
        await store.assert_campaign_contract(contract)
        await store.add('http://x/A')
        await store.assert_campaign_contract(contract)
        with self.assertRaisesRegex(ValueError, '已冻结'):
            await store.assert_campaign_contract({**contract, 'min_battle_timestamp': 456})
        self.assertEqual((await store.stats())['pending'], 1)

    async def test_file_work_does_not_block_event_loop(self):
        io = FileIO()
        try:
            slow = asyncio.create_task(io.call(time.sleep, 0.15))
            await asyncio.sleep(0.025)
            self.assertFalse(slow.done())
            await slow
        finally:
            io.close()

    async def test_prepare_failure_always_closes_fetcher(self):
        crawler = self.crawler()
        from unittest.mock import AsyncMock
        prepare = AsyncMock(side_effect=RuntimeError('prepare failed'))
        close = AsyncMock()
        with patch.object(crawler.fetcher, 'prepare', prepare, create=True), patch.object(crawler.fetcher, 'aclose', close):
            with self.assertRaisesRegex(RuntimeError, 'prepare failed'):
                await crawler.run([('http://x/A', 'test')])
            close.assert_awaited_once()

    async def test_pending_metadata_upgrades_without_downgrade(self):
        store = self.store()
        def battle(metadata):
            return ('http://x/data/replay?tag=A', 'http://x/player/P/battles', {}, 'battle:A', 'A', metadata)
        await store.add_battles([battle({'complete': False})])
        await store.add_battles([battle({'complete': True, 'authoritative_complete': True, 'proof': 'original'})])
        await store.add_battles([battle({'complete': True, 'proof': 'weaker'})])
        self.assertEqual((await store.get_battle_metadata('A'))['proof'], 'original')
        self.assertEqual((await store.stats())['pending'], 1)

    async def test_ordinary_cap_and_outbox_recovery(self):
        crawler = self.crawler()
        store = crawler.store
        await store.add_many([(f'http://x/data/replay?tag={i}', '', 'detail', {}, f'battle:{i}') for i in range(3)])
        tasks = await store.pending(3)
        for task in tasks:
            saved = crawler.storage.save_json(task['url'], {'battle_tag': str(task['id'])})
            await store.commit_replay(task_id=task['id'], saved_path=saved, target=1,
                index_root=str(crawler.storage.root), index_record={
                    'kind': 'battle', 'battle_tag': str(task['id']), 'saved_path': saved,
                })
        self.assertEqual(await store.done_details(), 1)
        rows = await store.pending_index()
        self.assertEqual(len(rows), 1)
        # Crash after append but before acknowledging the outbox.
        crawler.storage.append_index_idempotent(rows[0]['record'])
        cfg = crawler.cfg
        crawler.close()
        resumed = Crawler(cfg, MockFetcher({}))
        self.crawlers.append(resumed)
        await resumed.run(seeds=[])
        self.assertEqual(await resumed.store.pending_index(), [])
        self.assertEqual(len(resumed.storage.index_path.read_text(encoding='utf-8').splitlines()), 1)
        self.assertEqual(resumed.fetcher.calls, [])

    async def test_authoritative_cap_never_publishes_loser(self):
        store = self.store()
        await store.add_many([(f'http://x/{i}', '', 'detail', {}, str(i)) for i in range(2)])
        tasks = await store.pending(2)
        for task in tasks:
            await store.commit_authoritative_acceptance(
                task_id=task['id'], battle_tag=str(task['id']), saved_path='saved.json',
                source_path=None, source_schema_version=5, contract_sha256='b'*64, target=1,
                index_root=str(self.root), index_record={'kind': 'authoritative_battle', 'battle_tag': str(task['id'])},
            )
        self.assertEqual(await store.authoritative_accepted_count('b'*64), 1)
        self.assertEqual(len(await store.pending_index()), 1)

    async def test_outbox_failure_stops_without_network_or_lost_commit(self):
        crawler = self.crawler()
        await crawler.store.add('http://x/data/replay?tag=A')
        task = (await crawler.store.pending(1))[0]
        saved = crawler.storage.save_json(task['url'], {'battle_tag': 'A'})
        await crawler.store.commit_replay(task_id=task['id'], saved_path=saved,
            target=None, index_root=str(crawler.storage.root),
            index_record={'kind': 'battle', 'battle_tag': 'A', 'saved_path': saved})
        with patch.object(crawler.storage, 'append_index_idempotent', side_effect=OSError('disk full')):
            with self.assertRaises(PersistenceError):
                await crawler.run(seeds=[])
        self.assertEqual(await crawler.store.done_details(), 1)
        self.assertEqual(len(await crawler.store.pending_index()), 1)
        self.assertEqual(crawler.fetcher.calls, [])

    async def test_output_lock_blocks_second_crawler(self):
        crawler = self.crawler()
        cfg = dataclasses.replace(crawler.cfg, db_path=str(self.root / 'another.sqlite3'))
        with self.assertRaisesRegex(RuntimeError, '已有采集进程'):
            Crawler(cfg, MockFetcher({}))
        crawler.close()
        resumed = Crawler(cfg, MockFetcher({}))
        resumed.close()

    async def test_url_seeds_are_typed_and_deduplicated(self):
        crawler = self.crawler()
        await crawler._seed([
            ('#abc123', 'manual'), ('https://royaleapi.com/player/ABC123/battles', 'manual'),
            ('https://royaleapi.com/data/replay?tag=BATTLE&team_tags=A', 'manual'),
            ('https://royaleapi.com/data/replay?opponent_tags=B&tag=BATTLE', 'manual'),
        ])
        rows = await crawler.store.pending(10)
        self.assertEqual(sorted(row['kind'] for row in rows), ['detail', 'list'])

    async def test_completed_id_import_does_not_copy_old_pending(self):
        old = self.store('old.sqlite3')
        await old.add('http://x/A', dedup_key='battle:A')
        await old.mark_done('http://x/A', 'a.json')
        await old.add('http://x/B', dedup_key='battle:B')
        before = (await old.stats()).copy()
        new = self.store('new.sqlite3')
        result = await new.import_completed_database(str(self.root / 'old.sqlite3'))
        self.assertEqual(result['inserted'], 1)
        self.assertFalse(await new.add('http://x/A', dedup_key='battle:A'))
        self.assertTrue(await new.add('http://x/B', dedup_key='battle:B'))
        self.assertEqual(await old.stats(), before)

    async def test_small_queue_shutdown_and_terminal_status(self):
        cfg = _fast_cfg(str(self.root))
        cfg.queue_capacity = 1
        cfg.global_concurrency = 12
        cfg.list_pause_free_memory_gb = 0
        cfg.list_resume_free_memory_gb = 0.01
        crawler = Crawler(cfg, MockFetcher({}))
        self.crawlers.append(crawler)
        await asyncio.wait_for(crawler.run([('http://x/missing', 'test')]), timeout=5)
        status = readonly_status(cfg)
        self.assertEqual(status['runtime']['phase'], 'completed')
        self.assertFalse(status['runtime']['stale'])

    async def test_background_failure_is_not_a_silent_hang(self):
        crawler = self.crawler()
        async def broken_loader(limit):
            raise RuntimeError('injected queue failure')
        with patch.object(crawler, '_loader_loop', broken_loader):
            with self.assertRaisesRegex(RuntimeError, 'injected queue failure'):
                await asyncio.wait_for(crawler.run([('http://x/replay', '')]), timeout=5)
        self.assertEqual(readonly_status(crawler.cfg)['runtime']['phase'], 'failed')


class ConfigStorageTests(unittest.TestCase):
    def test_path_safety_and_partial_index_tail(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = Storage(root)
            path = Path(storage.save_json('http://x/?tag=..\\..\\escape', {'battle_tag': '..\\..\\escape'}))
            self.assertTrue(path.resolve().is_relative_to((root / 'raw' / 'battles').resolve()))
            storage.index_path.write_bytes(b'{"partial":')
            storage.append_index_idempotent({'kind': 'battle', 'battle_tag': 'A'})
            lines = storage.index_path.read_text().splitlines()
            self.assertEqual(lines[0], '{"partial":')
            self.assertEqual(json.loads(lines[1])['battle_tag'], 'A')
            self.assertFalse(Storage(root).append_index_idempotent({'kind': 'battle', 'battle_tag': 'A'}))

    def test_unknown_invalid_config_and_relative_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'config.toml'
            for content in ('global_concurency=10', 'global_concurrency="10"', 'global_concurrency=0', '[retry]\nnetwork_attempts=0'):
                path.write_text(content)
                with self.assertRaises(ValueError):
                    load_config(path)
            path.write_text('output_dir="data"\n[rate_limit]\nrequests_per_second=1')
            cfg = load_config(path)
            # Windows runners may expose TEMP through an 8.3 path alias.
            self.assertEqual(cfg.output_dir, str((Path(tmp) / 'data').resolve()))
            with self.assertRaises(FileNotFoundError):
                load_config(Path(tmp) / 'missing.toml')

    def test_fresh_campaign_and_status_are_read_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = dataclasses.replace(CrawlConfig(), output_dir=tmp)
            campaign = fresh_campaign(cfg, 'batch-1', '2026-09-01', 1000000)
            self.assertEqual(campaign.min_battle_timestamp, 1788220800)
            self.assertEqual(campaign.max_pages_per_player, 1)
            self.assertEqual(campaign.retry.network_attempts, 2)
            self.assertEqual(cfg.retry.network_attempts, None)
            self.assertFalse(readonly_status(campaign)['exists'])
            self.assertFalse(Path(campaign.output_dir).exists())
            for name in ('../bad', 'D:\\bad', ''):
                with self.assertRaises(ValueError):
                    fresh_campaign(cfg, name, '2026-09-01', 1)
            with self.assertRaises(ValueError):
                fresh_campaign(dataclasses.replace(cfg, authoritative_target=100000), 'batch', '2026-09-01', 1)

    def test_retry_after_numeric_date_and_invalid(self):
        self.assertEqual(Crawler._parse_retry_after('120'), 120)
        for value in ('nan', 'inf', '-1', 'garbage', None):
            self.assertIsNone(Crawler._parse_retry_after(value))
        from email.utils import format_datetime
        value = format_datetime(dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=60))
        self.assertTrue(58 < Crawler._parse_retry_after(value) <= 60)

    def test_lock_released_on_exception(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'run.lock'
            with self.assertRaises(ValueError):
                with RunLock(path):
                    raise ValueError('stop')
            with RunLock(path):
                pass


if __name__ == '__main__':
    unittest.main()
