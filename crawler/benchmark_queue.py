"""Synthetic SELECT-only scaling comparison. No network/production DB access."""
from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path

from .queue import TaskStore


def benchmark(count: int = 1_000_000, repeats: int = 7) -> dict:
    with tempfile.TemporaryDirectory(prefix='royaleapi-queue-bench-') as tmp:
        store = TaskStore(str(Path(tmp) / 'synthetic.sqlite3'), 3)
        try:
            conn = store.conn  # isolated offline maintenance; no async consumers
            started = time.perf_counter()
            conn.executemany(
                'INSERT INTO tasks(url,dedup_key,kind,status,created_at,updated_at) VALUES(?,?,?,?,?,?)',
                ((f'http://offline/{i}', f'test:{i}', 'detail' if i % 5 else 'list',
                  'done' if i % 10 < 6 else 'pending', 0, 0) for i in range(count)),
            )
            conn.commit()
            populate_seconds = time.perf_counter() - started
            queries = {
                'old_stats': "SELECT status,COUNT(*) FROM tasks GROUP BY status",
                'new_stats': "SELECT status,SUM(total) FROM task_counts WHERE total>0 GROUP BY status",
                'old_claim_select': "SELECT id FROM tasks WHERE status='pending' AND next_retry_at<=0 ORDER BY CASE WHEN kind IN ('detail','upgrade') THEN 0 WHEN kind='upgrade_list' THEN 1 WHEN kind='list' AND url NOT LIKE '%/history?before=%' THEN 2 ELSE 3 END,id LIMIT 200",
                'new_claim_select': "SELECT id FROM tasks WHERE status='pending' AND dispatch_rank=0 AND next_retry_at<=0 ORDER BY next_retry_at,id LIMIT 200",
            }
            values = {}
            timings = {}
            plans = {}
            for name, sql in queries.items():
                values[name] = [tuple(row) for row in conn.execute(sql)]
                samples = []
                for _ in range(repeats):
                    start = time.perf_counter()
                    conn.execute(sql).fetchall()
                    samples.append((time.perf_counter() - start) * 1000)
                timings[name + '_median_ms'] = round(statistics.median(samples), 4)
                plans[name] = [row[3] for row in conn.execute('EXPLAIN QUERY PLAN ' + sql)]
            assert values['old_stats'] == values['new_stats']
            assert values['old_claim_select'] == values['new_claim_select']
            assert conn.execute('PRAGMA quick_check').fetchone()[0] == 'ok'
            return {
                'tasks': count, 'repeats': repeats, 'source': 'synthetic, offline, SELECT only',
                'populate_seconds': round(populate_seconds, 3), 'timings': timings,
                'query_plans': plans, 'same_results': True, 'quick_check': 'ok',
            }
        finally:
            store.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tasks', type=int, default=1_000_000)
    parser.add_argument('--repeats', type=int, default=7)
    args = parser.parse_args()
    if args.tasks < 1000 or args.repeats < 1:
        parser.error('tasks >= 1000, repeats >= 1 required')
    print(json.dumps(benchmark(args.tasks, args.repeats), indent=2))
