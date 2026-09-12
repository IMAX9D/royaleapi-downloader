"""Persistent bounded source scheduling; executed only on the SQLite worker."""
from __future__ import annotations

import hashlib
import json


SOURCE_SCHEMA = '''
CREATE TABLE IF NOT EXISTS seed_sources (
    url TEXT PRIMARY KEY,
    next_refresh_at REAL NOT NULL DEFAULT 0,
    active_task_id INTEGER,
    generation INTEGER NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0,
    zero_new_streak INTEGER NOT NULL DEFAULT 0,
    fetched_at REAL,
    cache_expires_at REAL,
    html_sha256 TEXT,
    cached_tags TEXT,
    last_new_players INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE INDEX IF NOT EXISTS seed_sources_due ON seed_sources(active_task_id,next_refresh_at,url);
CREATE TABLE IF NOT EXISTS player_revisits (
    player_key TEXT PRIMARY KEY,
    player_tag TEXT NOT NULL,
    url TEXT NOT NULL,
    next_visit_at REAL NOT NULL,
    active_task_id INTEGER,
    generation INTEGER NOT NULL DEFAULT 0,
    zero_new_streak INTEGER NOT NULL DEFAULT 0,
    last_visit_at REAL NOT NULL,
    last_new_candidates INTEGER NOT NULL,
    last_error TEXT
);
CREATE INDEX IF NOT EXISTS player_revisits_due ON player_revisits(active_task_id,next_visit_at,player_key);
CREATE TABLE IF NOT EXISTS live_source_totals (name TEXT PRIMARY KEY,total INTEGER NOT NULL);
'''


def read_live_source_status(conn) -> dict:
    # seed_sources is capped at max_sources. Player count/queue totals are not
    # recomputed by scanning the much larger historical player/task tables.
    fields = ('url','next_refresh_at','active_task_id','fetched_at','cache_expires_at','failures','zero_new_streak','last_new_players','last_error')
    sources = [dict(zip(fields, row)) for row in conn.execute(
        'SELECT url,next_refresh_at,active_task_id,fetched_at,cache_expires_at,failures,zero_new_streak,last_new_players,last_error FROM seed_sources ORDER BY next_refresh_at,url'
    )]
    due = conn.execute(
        'SELECT next_visit_at FROM player_revisits WHERE active_task_id IS NULL ORDER BY next_visit_at LIMIT 1'
    ).fetchone()
    return {
        'sources': sources,
        'totals': dict(conn.execute('SELECT name,total FROM live_source_totals')),
        'next_player_revisit_at': due[0] if due else None,
    }


class SourceStoreMixin:
    def _source_total(self, key: str, count: int = 1) -> None:
        if count == 0:
            return
        self.conn.execute('INSERT INTO live_source_totals VALUES(?,?) ON CONFLICT(name) DO UPDATE SET total=total+excluded.total', (key, count))

    def _register_source_rows(self, urls: list[str], max_sources: int) -> int:
        size = int(self.conn.execute('SELECT COUNT(*) FROM seed_sources').fetchone()[0])
        inserted = 0
        for url in dict.fromkeys(urls):
            if size >= max_sources:
                break
            cur = self.conn.execute('INSERT OR IGNORE INTO seed_sources(url) VALUES(?)', (url,))
            size += cur.rowcount
            inserted += cur.rowcount
        return inserted

    def register_seed_sources(self, urls: list[str], max_sources: int) -> int:
        with self.conn:
            return self._register_source_rows(urls, max_sources)

    def schedule_seed_sources(self, max_pending: int, failure_delay: float) -> int:
        with self.conn:
            self.conn.execute('BEGIN IMMEDIATE')
            now = self._now()
            # A stopped process keeps pending/inflight tasks. Only terminal
            # unexpected failures release their slot, with a new cooldown.
            for row in self.conn.execute('SELECT s.url,t.status FROM seed_sources s JOIN tasks t ON t.id=s.active_task_id WHERE s.active_task_id IS NOT NULL').fetchall():
                if row['status'] not in ('pending', 'inflight'):
                    self.conn.execute("UPDATE seed_sources SET active_task_id=NULL,next_refresh_at=?,last_error='previous source task ended unexpectedly' WHERE url=?", (now + failure_delay, row['url']))
            active = int(self.conn.execute("SELECT COALESCE(SUM(total),0) FROM task_counts WHERE kind='source' AND status IN ('pending','inflight')").fetchone()[0])
            rows = self.conn.execute('SELECT url,generation FROM seed_sources WHERE active_task_id IS NULL AND next_refresh_at<=? ORDER BY next_refresh_at,url LIMIT ?', (now, max(0, max_pending - active))).fetchall()
            for row in rows:
                generation = int(row['generation']) + 1
                key = 'source:' + hashlib.sha256(row['url'].encode()).hexdigest() + ':' + str(generation)
                self._insert_task_row(row['url'], key, 'live-source', 'source', {'source_url': row['url']}, now)
                task_id = self.conn.execute('SELECT id FROM tasks WHERE dedup_key=?', (key,)).fetchone()[0]
                self.conn.execute('UPDATE seed_sources SET generation=?,active_task_id=? WHERE url=?', (generation, task_id, row['url']))
            self._source_total('source_tasks_scheduled', len(rows))
            return len(rows)

    def finish_seed_source(self, *, task_id: int, url: str, tags: list[str],
                           player_tasks: list[tuple], discovered_urls: list[str],
                           html_sha256: str, refresh_interval: float, max_interval: float,
                           max_sources: int) -> dict:
        with self.conn:
            self.conn.execute('BEGIN IMMEDIATE')
            source = self.conn.execute('SELECT * FROM seed_sources WHERE url=? AND active_task_id=?', (url, task_id)).fetchone()
            if source is None:
                return {'new_players': 0, 'already_committed': True}
            new_players = 0
            now = self._now()
            for player_url, seed, kind, meta, key in player_tasks:
                new_players += int(self._insert_task_row(player_url, key, seed, kind, meta, now))
            streak = 0 if new_players else int(source['zero_new_streak']) + 1
            interval = min(max_interval, refresh_interval * (2 ** min(streak, 8)))
            added_sources = self._register_source_rows(discovered_urls, max_sources)
            self.conn.execute(
                'UPDATE seed_sources SET next_refresh_at=?,active_task_id=NULL,failures=0,zero_new_streak=?,fetched_at=?,cache_expires_at=?,html_sha256=?,cached_tags=?,last_new_players=?,last_error=NULL WHERE url=?',
                (now + interval, streak, now, now + refresh_interval, html_sha256, json.dumps(tags), new_players, url),
            )
            self.conn.execute("UPDATE tasks SET status='done',error=NULL,updated_at=? WHERE id=?", (now, task_id))
            for name, value in {'source_pages_ok': 1, 'source_new_players': new_players,
                                'source_seen_players': len(tags) - new_players, 'sources_discovered': added_sources}.items():
                self._source_total(name, value)
            return {'new_players': new_players, 'already_committed': False, 'next_refresh_at': now + interval}

    def fail_seed_source(self, task_id: int, url: str, error: str, retry_delay: float, max_interval: float) -> None:
        with self.conn:
            self.conn.execute('BEGIN IMMEDIATE')
            row = self.conn.execute('SELECT failures FROM seed_sources WHERE url=? AND active_task_id=?', (url, task_id)).fetchone()
            if row is None:
                return
            failures = int(row[0]) + 1
            due = self._now() + min(max_interval, retry_delay * 2 ** min(failures - 1, 8))
            # Last-good cache is retained, but neither fetched_at nor its TTL
            # is advanced on error/empty/challenge responses.
            self.conn.execute('UPDATE seed_sources SET active_task_id=NULL,next_refresh_at=?,failures=?,last_error=? WHERE url=?', (due, failures, error[:500], url))
            self.conn.execute("UPDATE tasks SET status='dead',error=?,updated_at=? WHERE id=?", (error[:500], self._now(), task_id))
            self._source_total('source_pages_failed')

    def _record_player_visit(self, task_id: int, new_candidates: int, refresh: dict) -> None:
        key = refresh['player_key']
        row = self.conn.execute('SELECT generation,zero_new_streak,last_visit_at FROM player_revisits WHERE player_key=?', (key,)).fetchone()
        streak = 0 if new_candidates else (int(row['zero_new_streak']) + 1 if row else 1)
        now = self._now()
        interval = min(refresh['max_interval'], refresh['interval'] * 2 ** min(streak, 8))
        self.conn.execute(
            '''INSERT INTO player_revisits(player_key,player_tag,url,next_visit_at,active_task_id,zero_new_streak,last_visit_at,last_new_candidates)
               VALUES(?,?,?,?,NULL,?,?,?) ON CONFLICT(player_key) DO UPDATE SET
               url=excluded.url,next_visit_at=excluded.next_visit_at,active_task_id=NULL,
               zero_new_streak=excluded.zero_new_streak,last_visit_at=excluded.last_visit_at,
               last_new_candidates=excluded.last_new_candidates,last_error=NULL''',
            (key, refresh['player_tag'], refresh['url'], now + interval, streak, now, new_candidates),
        )
        if row is None:
            self._source_total('players_tracked')
        self._source_total('player_visits_recorded')

    def schedule_player_revisits(self, batch_size: int, pending_limit: int,
                                 fresh_low_watermark: int, failure_delay: float) -> int:
        with self.conn:
            self.conn.execute('BEGIN IMMEDIATE')
            now = self._now()
            # Release only the bounded currently active revisit set, not all
            # historical tasks; this stays cheap after weeks of collection.
            active_rows = self.conn.execute(
                'SELECT p.player_key,t.status FROM player_revisits p JOIN tasks t ON t.id=p.active_task_id WHERE p.active_task_id IS NOT NULL'
            ).fetchall()
            active = 0
            for row in active_rows:
                if row['status'] in ('pending', 'inflight'):
                    active += 1
                else:
                    self.conn.execute('UPDATE player_revisits SET active_task_id=NULL,next_visit_at=?,last_error=? WHERE player_key=?', (now + failure_delay, 'previous revisit failed or was skipped', row['player_key']))
            enough_fresh = self.conn.execute(
                "SELECT id FROM tasks WHERE status='pending' AND dispatch_rank=2 AND next_retry_at<=? LIMIT 1 OFFSET ?", (now, max(0, fresh_low_watermark - 1)),
            ).fetchone()
            if enough_fresh:
                return 0
            limit = min(batch_size, max(0, pending_limit - active))
            rows = self.conn.execute('SELECT * FROM player_revisits WHERE active_task_id IS NULL AND next_visit_at<=? ORDER BY next_visit_at,player_key LIMIT ?', (now, limit)).fetchall()
            for row in rows:
                generation = int(row['generation']) + 1
                key = f"{row['player_key']}:revisit:{generation}"
                self._insert_task_row(row['url'], key, 'live-player-revisit', 'list', {
                    'tag': row['player_tag'], 'page': 1, 'discovery_band': 'fresh',
                    'player_refresh_key': row['player_key'],
                }, now)
                task_id = self.conn.execute('SELECT id FROM tasks WHERE dedup_key=?', (key,)).fetchone()[0]
                self.conn.execute('UPDATE player_revisits SET generation=?,active_task_id=? WHERE player_key=?', (generation, task_id, row['player_key']))
            self._source_total('player_revisits_scheduled', len(rows))
            return len(rows)

    def live_source_status(self) -> dict:
        return read_live_source_status(self.conn)
