"""任务队列与断点续传：基于 SQLite 持久化任务状态。

任务类型（kind）：
- list   ：第一级，抓"对局列表"页，解析出多个对局。
- detail ：第二级，抓"单场对局回放 JSON"（二次点击）。
- upgrade_list：authoritative 历史升级的列表元数据依赖。
- upgrade：对应 tag 的 schema3/4 本地复用或 schema1/2 回放重抓。

去重（关键）：用 dedup_key 而不是 url 做唯一键。
- list   任务的 dedup_key = 页面 URL（每个玩家每页唯一）。
- detail 任务的 dedup_key = "battle:{对局tag}"——同一场对局从双方玩家视角各出现一次
  （team_tags/opponent_tags 不同），但对局 tag 相同，因此只抓一次。

状态机：pending -> inflight -> done / dead / skipped
数据库核心在独立单线程执行器内串行运行；网络事件循环不执行 SQLite I/O。
"""
from __future__ import annotations

import asyncio
import functools
import threading
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Optional

from .queue_schema import migrate_queue_indexes
from .discovery import navigation_summary
from .source_store import SOURCE_SCHEMA, SourceStoreMixin

log = logging.getLogger("crawler.store")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL,
    dedup_key TEXT,
    seed TEXT,
    kind TEXT NOT NULL DEFAULT 'detail',
    meta TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    next_retry_at REAL NOT NULL DEFAULT 0,
    saved_path TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status, next_retry_at);
CREATE TABLE IF NOT EXISTS battle_metadata (
    battle_tag TEXT PRIMARY KEY,
    metadata TEXT NOT NULL,
    complete INTEGER NOT NULL DEFAULT 0,
    source_list_url TEXT,
    schema_version INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_battle_metadata_complete ON battle_metadata(complete);
CREATE TABLE IF NOT EXISTS excluded_battles (
    battle_tag TEXT PRIMARY KEY,
    source TEXT,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS authoritative_results (
    battle_tag TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    tier TEXT NOT NULL,
    reason TEXT,
    saved_path TEXT,
    source_path TEXT,
    source_schema_version INTEGER,
    contract_sha256 TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_authoritative_status
ON authoritative_results(status, tier);
CREATE TABLE IF NOT EXISTS authoritative_imports (
    manifest_path TEXT PRIMARY KEY,
    manifest_size INTEGER NOT NULL,
    manifest_mtime_ns INTEGER NOT NULL,
    imported_candidates INTEGER NOT NULL,
    imported_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS index_outbox (
    task_id INTEGER PRIMARY KEY,
    root TEXT NOT NULL,
    record TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS collection_contract (
    name TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS list_new_battles (
    task_id INTEGER NOT NULL,
    battle_tag TEXT NOT NULL,
    PRIMARY KEY(task_id,battle_tag)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS list_navigation (
    task_id INTEGER PRIMARY KEY,
    url TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    candidates INTEGER NOT NULL,
    new_candidates INTEGER NOT NULL,
    duplicate_candidates INTEGER NOT NULL,
    history_cutoff INTEGER NOT NULL,
    reason TEXT,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS navigation_totals (
    name TEXT PRIMARY KEY,
    total INTEGER NOT NULL
);
"""


class _SQLiteTaskStore(SourceStoreMixin):
    def __init__(self, db_path: str, max_retries: int):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(_SCHEMA + SOURCE_SCHEMA)
        self._migrate()
        self.conn.commit()
        migrate_queue_indexes(self.conn)
        # The exclusion set is small enough to keep in memory (roughly a few MB
        # for 100k tags). List-page ingestion then pays one O(1) set lookup per
        # battle instead of one SQLite query per candidate.
        self._excluded_battles = {
            str(row[0])
            for row in self.conn.execute("SELECT battle_tag FROM excluded_battles")
        }
        self.max_retries = max_retries
        self._lock = threading.RLock()

    def _migrate(self) -> None:
        cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(tasks)").fetchall()}
        if "kind" not in cols:
            self.conn.execute("ALTER TABLE tasks ADD COLUMN kind TEXT NOT NULL DEFAULT 'detail'")
        if "meta" not in cols:
            self.conn.execute("ALTER TABLE tasks ADD COLUMN meta TEXT")
        if "dedup_key" not in cols:
            self.conn.execute("ALTER TABLE tasks ADD COLUMN dedup_key TEXT")
            self.conn.execute("UPDATE tasks SET dedup_key = url WHERE dedup_key IS NULL")
        self.conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_dedup ON tasks(dedup_key)")
        authoritative_cols = {
            r["name"] for r in self.conn.execute(
                "PRAGMA table_info(authoritative_results)"
            ).fetchall()
        }
        if "contract_sha256" not in authoritative_cols:
            self.conn.execute(
                "ALTER TABLE authoritative_results ADD COLUMN contract_sha256 TEXT"
            )

    @staticmethod
    def _now() -> float:
        return time.time()

    @staticmethod
    def _encode_meta(meta: Optional[dict]) -> Optional[str]:
        return json.dumps(meta, ensure_ascii=False) if meta else None

    @staticmethod
    def _decode_meta(s: Optional[str]) -> dict:
        if not s:
            return {}
        try:
            return json.loads(s)
        except Exception:  # noqa: BLE001
            return {}

    def _is_excluded_detail(self, kind: str, dedup_key: Optional[str]) -> bool:
        if kind != "detail" or not dedup_key or not dedup_key.startswith("battle:"):
            return False
        battle_tag = dedup_key.removeprefix("battle:")
        return battle_tag in self._excluded_battles

    def assert_authoritative_contract(self, contract_sha256: str) -> None:
        """Fail closed if a DB already contains another/unversioned contract."""
        rows = self.conn.execute(
            "SELECT contract_sha256,COUNT(*) AS n FROM authoritative_results "
            "GROUP BY contract_sha256"
        ).fetchall()
        mismatched = [
            (row["contract_sha256"], int(row["n"]))
            for row in rows
            if row["contract_sha256"] != contract_sha256
        ]
        if mismatched:
            raise ValueError(
                "authoritative DB is pinned to a different or missing contract: "
                f"expected={contract_sha256}, found={mismatched}"
            )

    def add(self, url: str, seed: str | None = None, kind: str = "detail",
                  meta: Optional[dict] = None, dedup_key: Optional[str] = None) -> bool:
        with self._lock:
            effective_key = dedup_key or url
            if self._is_excluded_detail(kind, effective_key):
                return False
            inserted = self._insert_task_row(url, effective_key, seed, kind, meta, self._now())
            self.conn.commit()
            return inserted

    def _insert_task_row(self, url, key, seed, kind, meta, now) -> bool:
        band = 1 if kind == 'list' and (meta or {}).get('discovery_band') == 'overlap' else 0
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO tasks(url,dedup_key,seed,kind,meta,status,created_at,updated_at,discovery_band) VALUES(?,?,?,?,?,'pending',?,?,?)",
            (url, key, seed, kind, self._encode_meta(meta), now, now, band),
        )
        inserted = cur.rowcount > 0
        if not inserted and kind == 'list' and band == 0:
            # New evidence can promote a waiting overlap-only player; never
            # reset done/inflight/retry status or create a second navigation.
            self.conn.execute(
                "UPDATE tasks SET discovery_band=0,meta=?,updated_at=? WHERE dedup_key=? AND kind='list' AND status='pending' AND discovery_band>0",
                (self._encode_meta(meta), now, key),
            )
        return inserted

    def add_many(self, items: list[tuple[str, str | None, str, Optional[dict], Optional[str]]]) -> list[str]:
        """items: (url, seed, kind, meta, dedup_key)。返回实际新增的 url 列表（按 dedup_key 幂等去重）。"""
        inserted: list[str] = []
        with self._lock:
            now = self._now()
            for url, seed, kind, meta, dedup_key in items:
                effective_key = dedup_key or url
                if self._is_excluded_detail(kind, effective_key):
                    continue
                if self._insert_task_row(url, effective_key, seed, kind, meta, now):
                    inserted.append(url)
            self.conn.commit()
        return inserted

    def _upsert_battle_metadata_row(self, battle_tag: str, metadata: dict, now: float) -> None:
        existing = self.conn.execute(
            "SELECT metadata FROM battle_metadata WHERE battle_tag=?", (battle_tag,)
        ).fetchone()
        if existing is not None:
            previous = self._decode_meta(existing["metadata"])
            # A globally duplicated replay may arrive from the OTHER player.
            # Its side-labelled decks/HP must not replace metadata associated
            # with the original request URL. Keep a coherent perspective.
            for side in ('team_tags', 'opponent_tags'):
                before = tuple(str(tag).lstrip('#').upper() for tag in previous.get(side) or [])
                after = tuple(str(tag).lstrip('#').upper() for tag in metadata.get(side) or [])
                if before and after and before != after:
                    return
            # Both records can have complete eight-card decks while only one
            # has authoritative tower/time/mode evidence. Do not downgrade it.
            if previous.get("authoritative_complete") and not metadata.get("authoritative_complete"):
                return
        complete = 1 if metadata.get("complete") else 0
        encoded = json.dumps(metadata, ensure_ascii=False)
        self.conn.execute(
            "INSERT INTO battle_metadata(battle_tag,metadata,complete,source_list_url,schema_version,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?) "
            "ON CONFLICT(battle_tag) DO UPDATE SET "
            "metadata=CASE WHEN excluded.complete>=battle_metadata.complete THEN excluded.metadata ELSE battle_metadata.metadata END, "
            "complete=MAX(battle_metadata.complete,excluded.complete), "
            "source_list_url=CASE WHEN excluded.complete>=battle_metadata.complete THEN excluded.source_list_url ELSE battle_metadata.source_list_url END, "
            "schema_version=MAX(battle_metadata.schema_version,excluded.schema_version), updated_at=excluded.updated_at",
            (
                battle_tag, encoded, complete, metadata.get("source_list_url"),
                int(metadata.get("schema_version", 1)), now, now,
            ),
        )

    def add_battles(
        self,
        items: list[tuple[str, str, dict, str, str, dict]],
        *, source_task_id: Optional[int] = None,
    ) -> list[str]:
        """原子新增 detail 任务与卡组元数据。

        items: (url, source_list_url, task_meta, dedup_key, battle_tag, battle_metadata)。
        新任务保存卡组；已有未完成任务允许补全元数据，已完成任务不重新膨胀元数据表。
        """
        inserted: list[str] = []
        if not items:
            return inserted
        with self._lock:
            now = self._now()
            for url, seed, meta, dedup_key, battle_tag, metadata in items:
                if battle_tag in self._excluded_battles:
                    continue
                cur = self.conn.execute(
                    "INSERT OR IGNORE INTO tasks(url,dedup_key,seed,kind,meta,status,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,'pending',?,?)",
                    (url, dedup_key, seed, "detail", self._encode_meta(meta), now, now),
                )
                if cur.rowcount > 0:
                    inserted.append(url)
                    self._upsert_battle_metadata_row(battle_tag, metadata, now)
                    if source_task_id is not None:
                        self.conn.execute('INSERT OR IGNORE INTO list_new_battles VALUES(?,?)', (source_task_id, battle_tag))
                else:
                    current = self.conn.execute(
                        "SELECT status FROM tasks WHERE dedup_key=?", (dedup_key,)
                    ).fetchone()
                    if current and current["status"] in {"pending", "inflight"}:
                        self._upsert_battle_metadata_row(battle_tag, metadata, now)
            self.conn.commit()
        return inserted

    def list_new_battle_tags(self, task_id: int) -> set[str]:
        return {row[0] for row in self.conn.execute('SELECT battle_tag FROM list_new_battles WHERE task_id=?', (task_id,))}

    def commit_list_navigation(self, *, task_id: int, url: str, saved_path: str,
                               fingerprint: str, candidates: int, new_candidates: int,
                               history_cutoff: bool, reason: Optional[str],
                               player_refresh: Optional[dict] = None) -> None:
        if not 0 <= new_candidates <= candidates:
            raise ValueError('navigation candidate counts are inconsistent')
        duplicate = candidates - new_candidates
        metrics = {'pages': 1, 'eligible_candidates': candidates, 'new_candidates': new_candidates,
                   'duplicate_candidates': duplicate, 'zero_new_pages': int(new_candidates == 0),
                   'history_cutoffs': int(history_cutoff)}
        with self.conn:
            self.conn.execute('BEGIN IMMEDIATE')
            prior = self.conn.execute('SELECT * FROM list_navigation WHERE task_id=?', (task_id,)).fetchone()
            if prior is None:
                self.conn.execute('INSERT INTO list_navigation VALUES(?,?,?,?,?,?,?,?,?)', (
                    task_id, url, fingerprint, candidates, new_candidates, duplicate,
                    int(history_cutoff), reason, self._now(),
                ))
                self.conn.executemany('INSERT INTO navigation_totals VALUES(?,?) ON CONFLICT(name) DO UPDATE SET total=total+excluded.total', metrics.items())
                if player_refresh is not None:
                    self._record_player_visit(task_id, new_candidates, player_refresh)
            changed = self.conn.execute("UPDATE tasks SET status='done',saved_path=?,error=NULL,updated_at=? WHERE id=?", (saved_path, self._now(), task_id))
            if changed.rowcount != 1:
                raise RuntimeError(f'list task id not found: {task_id}')
            # Only the temporary recovery ledger is cleared; the page audit,
            # replay queue and original page contents are retained.
            self.conn.execute('DELETE FROM list_new_battles WHERE task_id=?', (task_id,))

    def navigation_stats(self) -> dict:
        return navigation_summary(dict(self.conn.execute('SELECT name,total FROM navigation_totals')))

    def add_authoritative_upgrades(
        self,
        items: list[tuple[str, str, dict, str, Optional[dict]]],
    ) -> list[str]:
        """Queue explicit legacy upgrades without bypassing global tag identity.

        items: (replay_url, source_list_url, task_meta, battle_tag, metadata).
        The dedup key is separate from the legacy ``battle:{tag}`` task so a
        historical globally-excluded tag can be upgraded exactly once into the
        independent authoritative root.
        """
        inserted: list[str] = []
        if not items:
            return inserted
        with self._lock:
            now = self._now()
            for url, seed, meta, battle_tag, metadata in items:
                accepted = self.conn.execute(
                    "SELECT 1 FROM authoritative_results "
                    "WHERE battle_tag=? AND status='accepted'",
                    (battle_tag,),
                ).fetchone()
                if accepted:
                    continue
                cur = self.conn.execute(
                    "INSERT OR IGNORE INTO tasks(url,dedup_key,seed,kind,meta,status,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,'pending',?,?)",
                    (
                        url,
                        "authoritative:" + battle_tag,
                        seed,
                        "upgrade",
                        self._encode_meta(meta),
                        now,
                        now,
                    ),
                )
                if cur.rowcount <= 0:
                    continue
                inserted.append(url)
                if metadata:
                    self._upsert_battle_metadata_row(battle_tag, metadata, now)
                self.conn.execute(
                    "INSERT INTO authoritative_results("
                    "battle_tag,status,tier,reason,saved_path,source_path,source_schema_version,contract_sha256,created_at,updated_at"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(battle_tag) DO UPDATE SET "
                    "status=CASE WHEN authoritative_results.status='accepted' THEN 'accepted' ELSE excluded.status END, "
                    "tier=CASE WHEN authoritative_results.status='accepted' THEN authoritative_results.tier ELSE excluded.tier END, "
                    "reason=CASE WHEN authoritative_results.status='accepted' THEN authoritative_results.reason ELSE excluded.reason END, "
                    "source_path=COALESCE(authoritative_results.source_path,excluded.source_path), "
                    "source_schema_version=COALESCE(authoritative_results.source_schema_version,excluded.source_schema_version), "
                    "updated_at=excluded.updated_at",
                    (
                        battle_tag,
                        "queued",
                        str(meta.get("upgrade_tier") or "legacy_upgrade"),
                        None,
                        None,
                        meta.get("source_path"),
                        meta.get("source_schema_version"),
                        meta.get("contract_sha256"),
                        now,
                        now,
                    ),
                )
            self.conn.commit()
        return inserted

    def add_authoritative_upgrade_pages(
        self,
        pages: list[tuple[str, dict, str]],
    ) -> list[str]:
        """Queue phase-one list refreshes before any dependent replay task.

        pages: (source_list_url, task_meta, deterministic_dedup_key).
        """
        inserted: list[str] = []
        if not pages:
            return inserted
        with self._lock:
            now = self._now()
            for url, meta, dedup_key in pages:
                cur = self.conn.execute(
                    "INSERT OR IGNORE INTO tasks(url,dedup_key,seed,kind,meta,status,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,'pending',?,?)",
                    (
                        url, dedup_key, "authoritative_manifest", "upgrade_list",
                        self._encode_meta(meta), now, now,
                    ),
                )
                if cur.rowcount > 0:
                    inserted.append(url)
            self.conn.commit()
        return inserted

    def seed_authoritative_candidates(self, candidates: list[dict]) -> int:
        """Publish manifest-wide queued progress in one SQLite transaction."""
        if not candidates:
            return 0
        with self._lock:
            now = self._now()
            before = int(self.conn.execute(
                "SELECT COUNT(*) FROM authoritative_results"
            ).fetchone()[0])
            self.conn.executemany(
                "INSERT INTO authoritative_results("
                "battle_tag,status,tier,reason,saved_path,source_path,"
                "source_schema_version,contract_sha256,created_at,updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(battle_tag) DO NOTHING",
                [
                    (
                        str(item["battle_tag"]),
                        "queued",
                        str(item.get("upgrade_tier") or "legacy_upgrade"),
                        None,
                        None,
                        item.get("source_path"),
                        item.get("source_schema_version"),
                        item.get("contract_sha256"),
                        now,
                        now,
                    )
                    for item in candidates
                ],
            )
            after = int(self.conn.execute(
                "SELECT COUNT(*) FROM authoritative_results"
            ).fetchone()[0])
            self.conn.commit()
            return after - before

    def record_authoritative_result(
        self,
        battle_tag: str,
        *,
        status: str,
        tier: str,
        reason: Optional[str] = None,
        saved_path: Optional[str] = None,
        source_path: Optional[str] = None,
        source_schema_version: Optional[int] = None,
        contract_sha256: Optional[str] = None,
    ) -> dict[str, int | bool]:
        if status not in ("accepted", "rejected", "queued"):
            raise ValueError(f"invalid authoritative status: {status}")
        with self._lock:
            now = self._now()
            previous = self.conn.execute(
                "SELECT status FROM authoritative_results WHERE battle_tag=?",
                (battle_tag,),
            ).fetchone()
            previous_status = str(previous["status"]) if previous else None
            self.conn.execute(
                "INSERT INTO authoritative_results("
                "battle_tag,status,tier,reason,saved_path,source_path,source_schema_version,contract_sha256,created_at,updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(battle_tag) DO UPDATE SET "
                "status=CASE WHEN authoritative_results.status='accepted' THEN 'accepted' ELSE excluded.status END, "
                "tier=CASE WHEN authoritative_results.status='accepted' THEN authoritative_results.tier ELSE excluded.tier END, "
                "reason=CASE WHEN authoritative_results.status='accepted' THEN authoritative_results.reason ELSE excluded.reason END, "
                "saved_path=COALESCE(authoritative_results.saved_path,excluded.saved_path), "
                "source_path=COALESCE(authoritative_results.source_path,excluded.source_path), "
                "source_schema_version=COALESCE(authoritative_results.source_schema_version,excluded.source_schema_version), "
                "contract_sha256=COALESCE(authoritative_results.contract_sha256,excluded.contract_sha256), "
                "updated_at=excluded.updated_at",
                (
                    battle_tag, status, tier, reason, saved_path, source_path,
                    source_schema_version, contract_sha256, now, now,
                ),
            )
            total = int(self.conn.execute(
                "SELECT COALESCE(SUM(total),0) FROM authoritative_counts WHERE status='accepted'"
            ).fetchone()[0])
            self.conn.commit()
            return {
                "newly_accepted": status == "accepted" and previous_status != "accepted",
                "accepted_total": total,
            }

    def commit_authoritative_acceptance(
        self,
        *,
        task_id: int,
        battle_tag: str,
        saved_path: str,
        source_path: Optional[str],
        source_schema_version: Optional[int],
        contract_sha256: str,
        target: int,
        index_root: Optional[str] = None,
        index_record: Optional[dict] = None,
    ) -> dict[str, int | bool]:
        """Atomically publish task completion and accepted target membership.

        Write the replay first. Completion, target admission and the index
        outbox commit together; index publication can then recover on restart.
        """
        if target <= 0:
            raise ValueError("authoritative target must be positive")
        if len(str(contract_sha256)) != 64:
            raise ValueError("authoritative contract SHA-256 is malformed")
        with self._lock:
            # The cap decision and accepted-row publication share one SQLite
            # write transaction.  This is the authoritative fence against a
            # burst of concurrent replay completions overshooting the target.
            # A cap-losing file may be orphaned, but cannot enter the outbox or
            # accepted membership. Historical callers may omit the outbox.
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                now = self._now()
                previous = self.conn.execute(
                    "SELECT status,tier,contract_sha256 FROM authoritative_results "
                    "WHERE battle_tag=?",
                    (battle_tag,),
                ).fetchone()
                already_accepted = bool(
                    previous
                    and previous["status"] == "accepted"
                    and previous["tier"] == "native_static_v2"
                    and previous["contract_sha256"] == contract_sha256
                )
                total_before = int(self.conn.execute(
                    "SELECT COALESCE(SUM(total),0) FROM authoritative_counts "
                    "WHERE status='accepted' AND tier='native_static_v2' "
                    "AND contract_sha256=?",
                    (contract_sha256,),
                ).fetchone()[0])
                if not already_accepted and total_before >= int(target):
                    cur = self.conn.execute(
                        "UPDATE tasks SET status='skipped',saved_path=?,"
                        "error='authoritative target reached before atomic commit',"
                        "updated_at=? WHERE id=?",
                        (saved_path, now, int(task_id)),
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError(
                            f"authoritative task id not found: {task_id}"
                        )
                    self.conn.commit()
                    return {
                        "accepted": False,
                        "newly_accepted": False,
                        "cap_rejected": True,
                        "accepted_total": total_before,
                    }

                cur = self.conn.execute(
                    "UPDATE tasks SET status='done',saved_path=?,error=NULL,updated_at=? "
                    "WHERE id=?",
                    (saved_path, now, int(task_id)),
                )
                if cur.rowcount != 1:
                    raise RuntimeError(
                        f"authoritative task id not found: {task_id}"
                    )
                self.conn.execute(
                    "INSERT INTO authoritative_results("
                    "battle_tag,status,tier,reason,saved_path,source_path,"
                    "source_schema_version,contract_sha256,created_at,updated_at"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(battle_tag) DO UPDATE SET "
                    "status='accepted',tier='native_static_v2',reason=NULL,"
                    "saved_path=excluded.saved_path,"
                    "source_path=COALESCE(authoritative_results.source_path,excluded.source_path),"
                    "source_schema_version=COALESCE(authoritative_results.source_schema_version,excluded.source_schema_version),"
                    "contract_sha256=excluded.contract_sha256,updated_at=excluded.updated_at",
                    (
                        battle_tag, "accepted", "native_static_v2", None,
                        saved_path, source_path, source_schema_version,
                        contract_sha256, now, now,
                    ),
                )
                total = int(self.conn.execute(
                    "SELECT COALESCE(SUM(total),0) FROM authoritative_counts "
                    "WHERE status='accepted' AND tier='native_static_v2' "
                    "AND contract_sha256=?",
                    (contract_sha256,),
                ).fetchone()[0])
                if total > int(target):
                    raise RuntimeError(
                        f"atomic authoritative cap violated: {total}/{target}"
                    )
                if index_record is not None:
                    self._enqueue_index(task_id, index_root, index_record)
                self.conn.commit()
                return {
                    "accepted": True,
                    "newly_accepted": not already_accepted,
                    "cap_rejected": False,
                    "accepted_total": total,
                }
            except BaseException:
                self.conn.rollback()
                raise

    def _enqueue_index(self, task_id: int, root: str, record: dict) -> None:
        if not root:
            raise ValueError("index root is required")
        self.conn.execute(
            "INSERT INTO index_outbox VALUES(?,?,?) ON CONFLICT(task_id) DO NOTHING",
            (task_id, str(Path(root).resolve()), json.dumps(record, ensure_ascii=False)),
        )

    def commit_replay(self, *, task_id: int, saved_path: str,
                      target: Optional[int], index_root: str, index_record: dict) -> dict:
        with self._lock, self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            row = self.conn.execute("SELECT status FROM tasks WHERE id=?", (task_id,)).fetchone()
            if row is None:
                raise RuntimeError(f"replay task id not found: {task_id}")
            total = self.done_details()
            already = row["status"] == "done"
            if not already and target is not None and total >= target:
                self.conn.execute(
                    "UPDATE tasks SET status='skipped',error='target reached before atomic commit',updated_at=? WHERE id=?",
                    (self._now(), task_id),
                )
                return {"newly_accepted": False, "accepted_total": total}
            self.conn.execute(
                "UPDATE tasks SET status='done',saved_path=?,error=NULL,updated_at=? WHERE id=?",
                (saved_path, self._now(), task_id),
            )
            self._enqueue_index(task_id, index_root, index_record)
            return {"newly_accepted": not already, "accepted_total": self.done_details()}

    def pending_index(self, limit: int = 100) -> list[dict]:
        return [dict(row, record=json.loads(row['record'])) for row in self.conn.execute(
            "SELECT task_id,root,record FROM index_outbox ORDER BY task_id LIMIT ?", (limit,),
        )]

    def assert_campaign_contract(self, payload: dict) -> None:
        encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        with self.conn:
            self.conn.execute('BEGIN IMMEDIATE')
            row = self.conn.execute("SELECT payload FROM collection_contract WHERE name='campaign'").fetchone()
            if row is not None and row[0] != encoded:
                raise ValueError('批次数据条件已冻结，不能在同一批次改变日期/完整卡组/来源；请使用新 --campaign 名称')
            if row is None:
                if self.conn.execute('SELECT COALESCE(SUM(total),0) FROM task_counts').fetchone()[0]:
                    raise ValueError('不能把无批次契约的旧队列当作新批次，请换一个 --campaign 名称')
                self.conn.execute("INSERT INTO collection_contract VALUES('campaign',?)", (encoded,))

    def index_published(self, task_id: int) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM index_outbox WHERE task_id=?", (task_id,))

    def authoritative_stats(self) -> dict:
        with self._lock:
            status = {
                str(row["status"]): int(row["n"])
                for row in self.conn.execute(
                    "SELECT status,SUM(total) AS n FROM authoritative_counts WHERE total>0 GROUP BY status"
                )
            }
            tiers = {
                f"{row['status']}:{row['tier']}": int(row["n"])
                for row in self.conn.execute(
                    "SELECT status,tier,SUM(total) AS n FROM authoritative_counts WHERE total>0 "
                    "GROUP BY status,tier"
                )
            }
            return {"status": status, "tiers": tiers}

    def authoritative_accepted_count(
        self, contract_sha256: Optional[str] = None,
    ) -> int:
        with self._lock:
            if contract_sha256 is None:
                return int(self.conn.execute(
                    "SELECT COALESCE(SUM(total),0) FROM authoritative_counts "
                    "WHERE status='accepted' AND tier='native_static_v2'"
                ).fetchone()[0])
            return int(self.conn.execute(
                "SELECT COALESCE(SUM(total),0) FROM authoritative_counts "
                "WHERE status='accepted' AND tier='native_static_v2' "
                "AND contract_sha256=?",
                (contract_sha256,),
            ).fetchone()[0])

    def authoritative_manifest_imported(
        self, path: str, *, size: int, mtime_ns: int
    ) -> bool:
        with self._lock:
            row = self.conn.execute(
                "SELECT manifest_size,manifest_mtime_ns FROM authoritative_imports "
                "WHERE manifest_path=?",
                (path,),
            ).fetchone()
            return bool(
                row
                and int(row["manifest_size"]) == int(size)
                and int(row["manifest_mtime_ns"]) == int(mtime_ns)
            )

    def record_authoritative_manifest_import(
        self, path: str, *, size: int, mtime_ns: int, candidates: int
    ) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO authoritative_imports("
                "manifest_path,manifest_size,manifest_mtime_ns,imported_candidates,imported_at"
                ") VALUES(?,?,?,?,?) "
                "ON CONFLICT(manifest_path) DO UPDATE SET "
                "manifest_size=excluded.manifest_size,"
                "manifest_mtime_ns=excluded.manifest_mtime_ns,"
                "imported_candidates=excluded.imported_candidates,"
                "imported_at=excluded.imported_at",
                (path, int(size), int(mtime_ns), int(candidates), self._now()),
            )
            self.conn.commit()

    def import_excluded_battles(
        self, battle_tags: list[str], source: str, *, skip_completed: bool = False
    ) -> dict[str, int]:
        """Persist a global replay-download exclusion set.

        Exclusions do not count as completed downloads. Existing unfinished detail
        tasks are marked skipped, while already downloaded tasks stay done.
        """
        tags = sorted({str(tag).strip() for tag in battle_tags if str(tag).strip()})
        with self._lock:
            now = self._now()
            before = int(self.conn.execute(
                "SELECT COUNT(*) FROM excluded_battles"
            ).fetchone()[0])
            self.conn.executemany(
                "INSERT OR IGNORE INTO excluded_battles(battle_tag,source,created_at) "
                "VALUES(?,?,?)",
                [(tag, source, now) for tag in tags],
            )
            keys = [("battle:" + tag,) for tag in tags]
            self.conn.execute("DROP TABLE IF EXISTS temp.imported_exclusions")
            self.conn.execute(
                "CREATE TEMP TABLE imported_exclusions(dedup_key TEXT PRIMARY KEY)"
            )
            self.conn.executemany(
                "INSERT OR IGNORE INTO imported_exclusions(dedup_key) VALUES(?)", keys
            )
            statuses = "'pending','inflight','dead','done'" if skip_completed else "'pending','inflight','dead'"
            cur = self.conn.execute(
                "UPDATE tasks SET status='skipped', error='excluded: already in training dataset', "
                f"updated_at=? WHERE kind='detail' AND status IN ({statuses}) "
                "AND dedup_key IN (SELECT dedup_key FROM imported_exclusions)",
                (now,),
            )
            self.conn.execute(
                "DELETE FROM battle_metadata WHERE battle_tag IN "
                "(SELECT substr(dedup_key,8) FROM imported_exclusions)"
            )
            self.conn.execute("DROP TABLE imported_exclusions")
            after = int(self.conn.execute(
                "SELECT COUNT(*) FROM excluded_battles"
            ).fetchone()[0])
            self.conn.commit()
            self._excluded_battles.update(tags)
            return {
                "requested": len(tags),
                "inserted": after - before,
                "total": after,
                "unfinished_tasks_skipped": int(cur.rowcount),
            }

    def excluded_battles_count(self) -> int:
        with self._lock:
            return len(self._excluded_battles)

    def import_completed_database(self, path: str) -> dict:
        """Read-only streaming import of saved battle IDs, never old tasks/files."""
        source = Path(path).resolve(strict=True)
        inserted = skipped = scanned = 0
        conn = sqlite3.connect(source.as_uri() + '?mode=ro', uri=True)
        try:
            conn.execute('PRAGMA query_only=ON')
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            queries = []
            if 'tasks' in tables:
                queries.append("SELECT substr(dedup_key,8) FROM tasks WHERE kind='detail' AND status='done' AND dedup_key LIKE 'battle:%'")
            if 'authoritative_results' in tables:
                queries.append("SELECT battle_tag FROM authoritative_results WHERE status='accepted'")
            for query in queries:
                cursor = conn.execute(query)
                while rows := cursor.fetchmany(5000):
                    result = self.import_excluded_battles([row[0] for row in rows], str(source))
                    scanned += len(rows)
                    inserted += result['inserted']
                    skipped += result['unfinished_tasks_skipped']
        finally:
            conn.close()
        return {'scanned': scanned, 'inserted': inserted, 'total': self.excluded_battles_count(), 'unfinished_tasks_skipped': skipped}

    def upsert_battle_metadata(self, items: list[tuple[str, dict]]) -> int:
        """按 battle_tag 幂等保存卡组；完整数据不会被不完整数据覆盖。"""
        if not items:
            return 0
        with self._lock:
            now = self._now()
            for battle_tag, metadata in items:
                self._upsert_battle_metadata_row(battle_tag, metadata, now)
            self.conn.commit()
            return len(items)

    def get_battle_metadata(self, battle_tag: str) -> Optional[dict]:
        with self._lock:
            row = self.conn.execute(
                "SELECT metadata FROM battle_metadata WHERE battle_tag=?", (battle_tag,)
            ).fetchone()
            return self._decode_meta(row["metadata"]) if row else None

    def delete_battle_metadata(self, battle_tag: str) -> None:
        if not battle_tag:
            return
        with self._lock:
            self.conn.execute("DELETE FROM battle_metadata WHERE battle_tag=?", (battle_tag,))
            self.conn.commit()

    def battle_metadata_stats(self) -> dict[str, int]:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS total, SUM(complete) AS complete FROM battle_metadata"
            ).fetchone()
            return {
                "total": int(row["total"] or 0),
                "complete": int(row["complete"] or 0),
                "incomplete": int((row["total"] or 0) - (row["complete"] or 0)),
            }

    def pending(self, limit: int | None = None) -> list[dict]:
        """Atomically claim indexed priority buckets, oldest due retry first."""
        if limit is not None and limit <= 0:
            return []
        with self._lock, self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            rows = self._ready_rows(range(5), limit)
            return self._lease_rows(rows)

    def _ready_rows(self, ranks, limit: int | None) -> list[sqlite3.Row]:
        rows = []
        now = self._now()
        for rank in ranks:
            remaining = -1 if limit is None else limit - len(rows)
            if remaining == 0:
                break
            rows.extend(self.conn.execute(
                "SELECT id,url,seed,kind,meta,attempts FROM tasks "
                "WHERE status='pending' AND dispatch_rank=? AND next_retry_at<=? "
                "ORDER BY next_retry_at,id LIMIT ?", (rank, now, remaining),
            ).fetchall())
        return rows

    def _lease_rows(self, rows) -> list[dict]:
        ids = [row['id'] for row in rows]
        for offset in range(0, len(ids), 500):
            chunk = ids[offset:offset + 500]
            placeholders = ','.join('?' for _ in chunk)
            self.conn.execute(
                f"UPDATE tasks SET status='inflight',updated_at=? WHERE id IN ({placeholders})",
                [self._now(), *chunk],
            )
        return [dict(row, meta=self._decode_meta(row['meta'])) for row in rows]

    def _discovery_rows(self, limit: int) -> list[sqlite3.Row]:
        rows = self._ready_rows((1,), limit)  # Explicit metadata dependencies first.
        remaining = limit - len(rows)
        if not remaining:
            return rows
        roots = self._ready_rows((2,), remaining)
        history = self._ready_rows((3,), remaining)
        overlap = self._ready_rows((4,), remaining)
        cursor = int(self.conn.execute('SELECT cursor FROM discovery_scheduler WHERE id=1').fetchone()[0])
        root_i = history_i = overlap_i = 0
        for _ in range(remaining):
            prefer_overlap = cursor % 5 == 4
            prefer_history = cursor % 5 in (1, 3)
            if overlap_i < len(overlap) and (prefer_overlap or (root_i>=len(roots) and history_i>=len(history))):
                rows.append(overlap[overlap_i])
                overlap_i += 1
            elif history_i<len(history) and (prefer_history or root_i>=len(roots)):
                rows.append(history[history_i]);history_i+=1
            elif root_i<len(roots):
                rows.append(roots[root_i]);root_i+=1
            else:
                break
            cursor += 1
        self.conn.execute('UPDATE discovery_scheduler SET cursor=? WHERE id=1', (cursor,))
        return rows

    def pending_authoritative_fair(
        self,
        *,
        limit: int,
        ready_limit: int,
        list_limit: int,
    ) -> list[dict]:
        """Bound replay/list inflight work; used by both collection modes."""
        if limit <= 0 or ready_limit <= 0 or list_limit < 0:
            return []
        with self._lock, self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            inflight = {
                str(row["task_group"]): int(row["total"] or 0)
                for row in self.conn.execute(
                    "SELECT CASE WHEN kind IN ('detail','upgrade') "
                    "THEN 'ready' ELSE 'list' END AS task_group, "
                    "SUM(total) AS total FROM task_counts WHERE status='inflight' "
                    "GROUP BY task_group"
                ).fetchall()
            }
            ready_slots = max(0, ready_limit - inflight.get("ready", 0))
            list_slots = max(0, list_limit - inflight.get("list", 0))

            list_remaining = int(self.conn.execute(
                "SELECT COALESCE(SUM(total),0) FROM task_counts WHERE status IN ('pending','inflight') "
                "AND kind NOT IN ('detail','upgrade')"
            ).fetchone()[0])
            if list_remaining == 0:
                ready_slots += list_limit

            ready_take = min(ready_slots, limit)
            ready_rows = self._ready_rows((0,), ready_take)
            list_take = min(list_slots, max(0, limit - len(ready_rows)))
            list_rows = self._discovery_rows(list_take)

            rows: list[sqlite3.Row] = []
            ready_index = 0
            list_index = 0
            while ready_index < len(ready_rows) or list_index < len(list_rows):
                for _ in range(4):
                    if ready_index >= len(ready_rows):
                        break
                    rows.append(ready_rows[ready_index])
                    ready_index += 1
                if list_index < len(list_rows):
                    rows.append(list_rows[list_index])
                    list_index += 1
                if ready_index >= len(ready_rows):
                    rows.extend(list_rows[list_index:])
                    break
                if list_index >= len(list_rows):
                    rows.extend(ready_rows[ready_index:])
                    break

            return self._lease_rows(rows)

    def update_task_meta(self, task_id: int, meta: dict) -> None:
        """Persist an inflight task's narrowed dependency/audit state."""
        with self._lock:
            cur = self.conn.execute(
                "UPDATE tasks SET meta=?,updated_at=? WHERE id=?",
                (self._encode_meta(meta), self._now(), int(task_id)),
            )
            if cur.rowcount != 1:
                self.conn.rollback()
                raise RuntimeError(f"task id not found while updating metadata: {task_id}")
            self.conn.commit()

    def mark_done(
        self, url: str, saved_path: str, *, task_id: Optional[int] = None
    ) -> None:
        with self._lock:
            if task_id is None:
                self.conn.execute(
                    "UPDATE tasks SET status='done', saved_path=?, error=NULL, updated_at=? WHERE url=?",
                    (saved_path, self._now(), url),
                )
            else:
                self.conn.execute(
                    "UPDATE tasks SET status='done', saved_path=?, error=NULL, updated_at=? WHERE id=?",
                    (saved_path, self._now(), task_id),
                )
            self.conn.commit()

    def mark_dead(
        self, url: str, error: str, *, task_id: Optional[int] = None
    ) -> None:
        with self._lock:
            column, value = ("id", task_id) if task_id is not None else ("url", url)
            self.conn.execute(
                f"UPDATE tasks SET status='dead', error=?, updated_at=? WHERE {column}=?",
                (error, self._now(), value),
            )
            self.conn.commit()

    def mark_skipped(
        self, url: str, reason: str, *, task_id: Optional[int] = None
    ) -> None:
        with self._lock:
            column, value = ("id", task_id) if task_id is not None else ("url", url)
            self.conn.execute(
                f"UPDATE tasks SET status='skipped', error=?, updated_at=? WHERE {column}=?",
                (reason[:2000], self._now(), value),
            )
            self.conn.commit()

    def mark_retry(
        self, url: str, error: str, delay: float, *, task_id: Optional[int] = None
    ) -> str:
        """持久重试：递增 attempts，并返回 pending/dead 新状态。"""
        with self._lock:
            column, value = ("id", task_id) if task_id is not None else ("url", url)
            row = self.conn.execute(
                f"SELECT attempts FROM tasks WHERE {column}=?", (value,)
            ).fetchone()
            attempts = (int(row["attempts"]) if row else 0) + 1
            status = "dead" if attempts >= self.max_retries else "pending"
            next_at = self._now() + max(0.0, delay) if status == "pending" else 0.0
            self.conn.execute(
                f"UPDATE tasks SET status=?, attempts=?, error=?, next_retry_at=?, updated_at=? WHERE {column}=?",
                (status, attempts, error[:2000], next_at, self._now(), value),
            )
            self.conn.commit()
            return status

    def requeue_inflight(self) -> int:
        with self._lock:
            cur = self.conn.execute(
                "UPDATE tasks SET status='pending', updated_at=? WHERE status='inflight'",
                (self._now(),),
            )
            self.conn.commit()
            return cur.rowcount

    def stats(self) -> dict[str, int]:
        with self._lock:
            rows = self.conn.execute("SELECT status,SUM(total) AS n FROM task_counts WHERE total>0 GROUP BY status").fetchall()
            return {r["status"]: r["n"] for r in rows}

    def work_state(self) -> dict[str, int | float | None]:
        """供有界队列装载器判断是否还有到期/未来/执行中任务。"""
        with self._lock:
            rows = self.conn.execute(
                "SELECT status,SUM(total) AS n FROM task_counts WHERE total>0 GROUP BY status"
            ).fetchall()
            out: dict[str, int | float | None] = {r["status"]: r["n"] for r in rows}
            out['ready_backlog'] = int(self.conn.execute(
                "SELECT COALESCE(SUM(total),0) FROM task_counts WHERE kind IN ('detail','upgrade') AND status IN ('pending','inflight')"
            ).fetchone()[0])
            row = self.conn.execute(
                "SELECT MIN(next_retry_at) AS t FROM tasks WHERE status='pending'"
            ).fetchone()
            out["next_retry_at"] = row["t"] if row else None
            return out

    def done_details(self) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COALESCE(SUM(total),0) AS n FROM task_counts WHERE status='done' AND kind='detail'"
            ).fetchone()
            return int(row["n"])

    def close(self) -> None:
        self.conn.close()


# Keep the established awaitable API while moving SQLite work off the network
# event loop. A single executor preserves transaction ordering and keeps the
# connection on its owning worker for normal API calls.
_ASYNC_DATABASE_METHODS = (
    "add", "add_many", "add_battles", "add_authoritative_upgrades",
    "add_authoritative_upgrade_pages", "seed_authoritative_candidates",
    "record_authoritative_result", "commit_authoritative_acceptance",
    "authoritative_stats", "authoritative_accepted_count",
    "authoritative_manifest_imported", "record_authoritative_manifest_import",
    "import_excluded_battles", "excluded_battles_count", "upsert_battle_metadata",
    "get_battle_metadata", "delete_battle_metadata", "battle_metadata_stats",
    "pending", "pending_authoritative_fair", "update_task_meta", "mark_done",
    "mark_dead", "mark_skipped", "mark_retry", "requeue_inflight", "stats",
    "work_state", "done_details",
    "commit_replay", "pending_index", "index_published",
    "import_completed_database",
    "assert_campaign_contract",
    "list_new_battle_tags", "commit_list_navigation", "navigation_stats",
    "register_seed_sources", "schedule_seed_sources", "finish_seed_source",
    "fail_seed_source", "schedule_player_revisits", "live_source_status",
)


class TaskStore:
    """Async façade over one serialized SQLite worker; no browser dependencies."""

    def __init__(self, db_path: str, max_retries: int):
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="crawler-sqlite")
        self._closed = False
        try:
            self._database = self._executor.submit(_SQLiteTaskStore, db_path, max_retries).result()
        except BaseException:
            self._executor.shutdown(wait=True)
            raise

    @property
    def conn(self):
        """Legacy maintenance/test access; do not use from the live hot path."""
        return self._database.conn

    @property
    def max_retries(self):
        return self._database.max_retries

    def assert_authoritative_contract(self, contract_sha256: str) -> None:
        self._executor.submit(self._database.assert_authoritative_contract, contract_sha256).result()

    async def _invoke(self, name, *args, **kwargs):
        if self._closed:
            raise RuntimeError("task store is closed")
        future = self._executor.submit(self._call, name, args, kwargs)
        return await asyncio.wrap_future(future)

    def _call(self, name, args, kwargs):
        try:
            return getattr(self._database, name)(*args, **kwargs)
        except BaseException:
            if self._database.conn.in_transaction:
                self._database.conn.rollback()
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._executor.submit(self._database.close).result()
        finally:
            self._executor.shutdown(wait=True)


def _async_database_method(name):
    @functools.wraps(getattr(_SQLiteTaskStore, name))
    async def invoke(self, *args, **kwargs):
        return await self._invoke(name, *args, **kwargs)
    return invoke


for _method_name in _ASYNC_DATABASE_METHODS:
    setattr(TaskStore, _method_name, _async_database_method(_method_name))
