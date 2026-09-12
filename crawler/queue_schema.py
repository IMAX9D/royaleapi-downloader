"""Additive queue indexes and transactional counters for large task stores."""
from __future__ import annotations

import sqlite3


DISPATCH_RANK_SQL = """CASE
    WHEN kind IN ('detail','upgrade') THEN 0
    WHEN kind IN ('upgrade_list','source') THEN 1
    WHEN kind='list' AND discovery_band>0 THEN 4
    WHEN kind='list' AND url NOT LIKE '%/history?before=%' THEN 2
    ELSE 3 END"""


def migrate_queue_indexes(conn: sqlite3.Connection) -> None:
    """Migrate once; ordinary inserts/updates keep counters correct by trigger.

    No task, replay, exclusion or authoritative result is removed. Bootstrap
    counts are computed once inside the same write transaction as the triggers.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS queue_schema_versions (name TEXT PRIMARY KEY, version INTEGER NOT NULL)")
        cols = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        added_rank = "dispatch_rank" not in cols
        if added_rank:
            conn.execute("ALTER TABLE tasks ADD COLUMN dispatch_rank INTEGER NOT NULL DEFAULT 3")
        if 'discovery_band' not in cols:
            conn.execute('ALTER TABLE tasks ADD COLUMN discovery_band INTEGER NOT NULL DEFAULT 0')
        version = conn.execute("SELECT version FROM queue_schema_versions WHERE name='counters_priority'").fetchone()
        conn.execute("CREATE TABLE IF NOT EXISTS task_counts (kind TEXT NOT NULL,status TEXT NOT NULL,total INTEGER NOT NULL,PRIMARY KEY(kind,status)) WITHOUT ROWID")
        conn.execute("CREATE TABLE IF NOT EXISTS authoritative_counts (contract_sha256 TEXT NOT NULL,status TEXT NOT NULL,tier TEXT NOT NULL,total INTEGER NOT NULL,PRIMARY KEY(contract_sha256,status,tier)) WITHOUT ROWID")
        if added_rank or version is None or int(version[0]) < 4:
            conn.execute("UPDATE tasks SET dispatch_rank=" + DISPATCH_RANK_SQL)
            conn.execute('DROP TRIGGER IF EXISTS tasks_dispatch_insert_v2')
            conn.execute('DROP TRIGGER IF EXISTS tasks_dispatch_update_v2')
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_ready_v2 ON tasks(status,dispatch_rank,next_retry_at,id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_inflight_v2 ON tasks(status,kind)")
        rank_new = DISPATCH_RANK_SQL.replace("kind", "NEW.kind").replace("url", "NEW.url").replace('discovery_band', 'NEW.discovery_band')
        definitions = {
            "tasks_dispatch_insert_v2": f"AFTER INSERT ON tasks BEGIN UPDATE tasks SET dispatch_rank={rank_new} WHERE id=NEW.id; END",
            "tasks_dispatch_update_v2": f"AFTER UPDATE OF kind,url,discovery_band ON tasks BEGIN UPDATE tasks SET dispatch_rank={rank_new} WHERE id=NEW.id; END",
            "tasks_count_insert_v2": "AFTER INSERT ON tasks BEGIN INSERT INTO task_counts VALUES(NEW.kind,NEW.status,1) ON CONFLICT(kind,status) DO UPDATE SET total=total+1; END",
            "tasks_count_delete_v2": "AFTER DELETE ON tasks BEGIN UPDATE task_counts SET total=total-1 WHERE kind=OLD.kind AND status=OLD.status; END",
            "tasks_count_update_v2": "AFTER UPDATE OF kind,status ON tasks WHEN OLD.kind IS NOT NEW.kind OR OLD.status IS NOT NEW.status BEGIN UPDATE task_counts SET total=total-1 WHERE kind=OLD.kind AND status=OLD.status; INSERT INTO task_counts VALUES(NEW.kind,NEW.status,1) ON CONFLICT(kind,status) DO UPDATE SET total=total+1; END",
            "authoritative_count_insert_v2": "AFTER INSERT ON authoritative_results BEGIN INSERT INTO authoritative_counts VALUES(COALESCE(NEW.contract_sha256,''),NEW.status,NEW.tier,1) ON CONFLICT(contract_sha256,status,tier) DO UPDATE SET total=total+1; END",
            "authoritative_count_delete_v2": "AFTER DELETE ON authoritative_results BEGIN UPDATE authoritative_counts SET total=total-1 WHERE contract_sha256=COALESCE(OLD.contract_sha256,'') AND status=OLD.status AND tier=OLD.tier; END",
            "authoritative_count_update_v2": "AFTER UPDATE OF contract_sha256,status,tier ON authoritative_results WHEN OLD.contract_sha256 IS NOT NEW.contract_sha256 OR OLD.status IS NOT NEW.status OR OLD.tier IS NOT NEW.tier BEGIN UPDATE authoritative_counts SET total=total-1 WHERE contract_sha256=COALESCE(OLD.contract_sha256,'') AND status=OLD.status AND tier=OLD.tier; INSERT INTO authoritative_counts VALUES(COALESCE(NEW.contract_sha256,''),NEW.status,NEW.tier,1) ON CONFLICT(contract_sha256,status,tier) DO UPDATE SET total=total+1; END",
        }
        existing = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}
        rebuild = version is None or any(name not in existing for name in definitions if '_count_' in name)
        for name, definition in definitions.items():
            conn.execute(f"CREATE TRIGGER IF NOT EXISTS {name} {definition}")
        if rebuild:
            conn.execute("DELETE FROM task_counts")
            conn.execute("INSERT INTO task_counts SELECT kind,status,COUNT(*) FROM tasks GROUP BY kind,status")
            conn.execute("DELETE FROM authoritative_counts")
            conn.execute("INSERT INTO authoritative_counts SELECT COALESCE(contract_sha256,''),status,tier,COUNT(*) FROM authoritative_results GROUP BY COALESCE(contract_sha256,''),status,tier")
        conn.execute("INSERT INTO queue_schema_versions VALUES('counters_priority',4) ON CONFLICT(name) DO UPDATE SET version=excluded.version")
        conn.execute('CREATE TABLE IF NOT EXISTS discovery_scheduler (id INTEGER PRIMARY KEY CHECK(id=1), cursor INTEGER NOT NULL)')
        conn.execute('INSERT OR IGNORE INTO discovery_scheduler VALUES(1,0)')
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
