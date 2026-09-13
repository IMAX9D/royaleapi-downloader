"""Persistent evidence-backed expert membership, bounded independently of tasks."""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from .season import normalize_player, SEASON_ID


def latest_finished_season(now=None):
    now = now or dt.datetime.now(dt.timezone.utc)
    first = now.replace(day=1, hour=9, minute=0, second=0, microsecond=0)
    settlement = first + dt.timedelta(days=(-first.weekday()) % 7)
    month = first - dt.timedelta(days=1)
    if now < settlement:
        month = month.replace(day=1) - dt.timedelta(days=1)
    return month.strftime('%Y-%m')


class ExpertPool:
    def __init__(self, path, target=10000):
        if not 1 <= target <= 10000:
            raise ValueError('expert target must be 1..10000')
        self.path, self.target = Path(path), target
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript('''
            CREATE TABLE IF NOT EXISTS members (
                tag TEXT PRIMARY KEY, season TEXT NOT NULL, rank INTEGER NOT NULL,
                source TEXT NOT NULL, sha256 TEXT NOT NULL, verified_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS candidates (
                tag TEXT PRIMARY KEY, source TEXT NOT NULL, due REAL NOT NULL DEFAULT 0,
                failures INTEGER NOT NULL DEFAULT 0, last_error TEXT);
            CREATE INDEX IF NOT EXISTS candidate_due ON candidates(due,tag);
            CREATE TABLE IF NOT EXISTS sources (
                url TEXT PRIMARY KEY, season TEXT, due REAL NOT NULL DEFAULT 0,
                failures INTEGER NOT NULL DEFAULT 0, last_error TEXT);
            CREATE INDEX IF NOT EXISTS source_due ON sources(due,url);
            ''')

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        try:
            with db:
                yield db
        finally:
            db.close()

    def members(self):
        with self.connect() as db:
            return {r['tag']: dict(r) for r in db.execute('SELECT * FROM members')}

    def admit(self, rows, source, sha256, latest=None):
        latest = latest or latest_finished_season()
        valid = []
        for r in rows:
            tag = normalize_player(r.get('tag'))
            rank, season = r.get('rank'), r.get('rank_season')
            if type(rank) is not int or not 1 <= rank <= 10000 or not isinstance(season, str) or not SEASON_ID.fullmatch(season) or season > latest:
                raise ValueError('membership needs a completed-season Top10000 proof')
            valid.append((tag, season, rank, source, sha256, time.time()))
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            count = db.execute('SELECT COUNT(*) FROM members').fetchone()[0]
            for row in valid:
                old = db.execute('SELECT season,rank FROM members WHERE tag=?', (row[0],)).fetchone()
                if old:
                    if old['season'] == row[1] and old['rank'] != row[2]:
                        raise ValueError('contradictory membership evidence')
                    continue
                if count >= self.target:
                    break
                db.execute('INSERT INTO members VALUES(?,?,?,?,?,?)', row)
                db.execute('DELETE FROM candidates WHERE tag=?', (row[0],))
                count += 1
        return self.members()

    def offer(self, tags, source):
        valid = list(dict.fromkeys(normalize_player(t) for t in tags))
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT COUNT(*) FROM members').fetchone()[0] >= self.target:
                return
            capacity = max(0, 30000 - db.execute('SELECT COUNT(*) FROM candidates').fetchone()[0])
            for tag in valid:
                if capacity <= 0:
                    break
                if db.execute('SELECT 1 FROM members WHERE tag=?', (tag,)).fetchone():
                    continue
                cur = db.execute('INSERT OR IGNORE INTO candidates(tag,source) VALUES(?,?)', (tag, source))
                capacity -= cur.rowcount

    def register_sources(self, sources):
        with self.connect() as db:
            db.executemany('INSERT OR IGNORE INTO sources(url,season) VALUES(?,?)', sources)

    def due(self, table):
        if table not in ('sources', 'candidates'):
            raise ValueError('invalid pool table')
        with self.connect() as db:
            row = db.execute(f'SELECT * FROM {table} WHERE due<=? ORDER BY due,rowid LIMIT 1', (time.time(),)).fetchone()
            return dict(row) if row else None

    def finish(self, table, key, error=None):
        if table not in ('sources', 'candidates'):
            raise ValueError('invalid pool table')
        field = 'url' if table == 'sources' else 'tag'
        with self.connect() as db:
            row = db.execute(f'SELECT failures FROM {table} WHERE {field}=?', (key,)).fetchone()
            if row is None:
                return
            failures = row[0] + 1 if error else 0
            delay = min(3600, 120 * 2 ** min(failures, 5)) if error else (86400 if table == 'sources' else 7 * 86400)
            db.execute(f'UPDATE {table} SET due=?,failures=?,last_error=? WHERE {field}=?',
                       (time.time() + delay, failures, str(error)[:300] if error else None, key))

    def stats(self):
        with self.connect() as db:
            return {'target': self.target, 'verified': db.execute('SELECT COUNT(*) FROM members').fetchone()[0],
                    'candidates': db.execute('SELECT COUNT(*) FROM candidates').fetchone()[0],
                    'sources': db.execute('SELECT COUNT(*) FROM sources').fetchone()[0]}
