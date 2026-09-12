"""Offline, fail-closed migration of an authoritative corpus to contract v2.

The migration keeps the schema-5 v1 output directory immutable.  Previously
accepted rows and rows rejected only by the old source-mode allowlist are
requeued in-place, while the production config is expected to point at a new
empty output directory before collection resumes.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from typing import Any

from .authoritative import native_contract_payload_sha256
from .authoritative_production import LOCK, _pid_alive, _read_json


MODE_REASON = "numeric_game_mode_not_allowed"
MODE_TASK_ERROR = "authoritative:mode:numeric_game_mode_not_allowed"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _contract(path: Path) -> tuple[dict[str, Any], str, str]:
    raw = path.read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f"contract root is not an object: {path}")
    claimed = str(value.get("contract_sha256") or "")
    actual = native_contract_payload_sha256(value)
    if len(claimed) != 64 or claimed != actual:
        raise ValueError(f"contract canonical SHA-256 mismatch: {path}")
    return value, actual, hashlib.sha256(raw).hexdigest()


def _replace_contract_sha(value: Any, old_sha: str, new_sha: str) -> Any:
    if isinstance(value, dict):
        return {
            key: (
                new_sha
                if key == "contract_sha256" and item == old_sha
                else _replace_contract_sha(item, old_sha, new_sha)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_replace_contract_sha(item, old_sha, new_sha) for item in value]
    return value


def _counts(conn: sqlite3.Connection) -> dict[str, Any]:
    return {
        "results": {
            f"{status}:{tier}": int(total)
            for status, tier, total in conn.execute(
                "SELECT status,tier,COUNT(*) FROM authoritative_results "
                "GROUP BY status,tier ORDER BY status,tier"
            )
        },
        "tasks": {
            f"{status}:{kind}": int(total)
            for status, kind, total in conn.execute(
                "SELECT status,kind,COUNT(*) FROM tasks "
                "GROUP BY status,kind ORDER BY status,kind"
            )
        },
        "contracts": {
            str(contract): int(total)
            for contract, total in conn.execute(
                "SELECT contract_sha256,COUNT(*) FROM authoritative_results "
                "GROUP BY contract_sha256 ORDER BY contract_sha256"
            )
        },
    }


def _target_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT ar.battle_tag,ar.status result_status,ar.tier,ar.reason,"
        "ar.saved_path result_saved_path,ar.source_path,ar.contract_sha256,"
        "t.id task_id,t.kind,t.status task_status,t.error,t.saved_path task_saved_path "
        "FROM authoritative_results ar JOIN tasks t "
        "ON json_extract(t.meta,'$.battle_tag')=ar.battle_tag "
        "WHERE ar.status='accepted' OR "
        "(ar.status='rejected' AND ar.tier='mode' AND ar.reason=?) "
        "ORDER BY ar.battle_tag",
        (MODE_REASON,),
    ).fetchall()


def _validate_preconditions(
    conn: sqlite3.Connection,
    *,
    old_sha: str,
    new_sha: str,
    expected_total: int,
    expected_accepted: int,
    expected_mode_rejected: int,
) -> tuple[list[sqlite3.Row], bool]:
    total = int(conn.execute("SELECT COUNT(*) FROM authoritative_results").fetchone()[0])
    if total != expected_total:
        raise RuntimeError(f"authoritative row count {total} != expected {expected_total}")
    contract_counts = {
        str(row[0]): int(row[1])
        for row in conn.execute(
            "SELECT contract_sha256,COUNT(*) FROM authoritative_results GROUP BY contract_sha256"
        )
    }
    if contract_counts == {new_sha: expected_total}:
        return [], True
    if contract_counts != {old_sha: expected_total}:
        raise RuntimeError(f"unexpected authoritative contract set: {contract_counts}")

    accepted = int(conn.execute(
        "SELECT COUNT(*) FROM authoritative_results WHERE status='accepted'"
    ).fetchone()[0])
    rejected_mode = int(conn.execute(
        "SELECT COUNT(*) FROM authoritative_results "
        "WHERE status='rejected' AND tier='mode' AND reason=?",
        (MODE_REASON,),
    ).fetchone()[0])
    if accepted != expected_accepted or rejected_mode != expected_mode_rejected:
        raise RuntimeError(
            "migration target count mismatch: "
            f"accepted={accepted}/{expected_accepted}, "
            f"mode={rejected_mode}/{expected_mode_rejected}"
        )
    rows = _target_rows(conn)
    if len(rows) != expected_accepted + expected_mode_rejected:
        raise RuntimeError("migration targets do not have exactly one upgrade task each")
    seen: set[str] = set()
    for row in rows:
        tag = str(row["battle_tag"])
        if tag in seen:
            raise RuntimeError(f"duplicate migration task for {tag}")
        seen.add(tag)
        if row["kind"] != "upgrade" or not Path(str(row["source_path"])).is_file():
            raise RuntimeError(f"migration source/task is incomplete for {tag}")
        if row["result_status"] == "accepted":
            if row["task_status"] != "done" or not row["result_saved_path"]:
                raise RuntimeError(f"accepted migration row is not durable for {tag}")
        elif row["task_status"] != "skipped" or row["error"] != MODE_TASK_ERROR:
            raise RuntimeError(f"mode migration row has unexpected task state for {tag}")
    return rows, False


def _backup(conn: sqlite3.Connection, destination: Path) -> dict[str, Any]:
    if destination.exists():
        raise FileExistsError(f"backup already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    backup = sqlite3.connect(destination)
    try:
        conn.backup(backup)
        backup.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        check = str(backup.execute("PRAGMA quick_check").fetchone()[0])
    finally:
        backup.close()
    if check != "ok":
        raise RuntimeError(f"backup quick_check failed: {check}")
    # Windows' FlushFileBuffers (used by os.fsync) requires a writable handle.
    with destination.open("r+b") as stream:
        os.fsync(stream.fileno())
    return {
        "path": str(destination),
        "size": destination.stat().st_size,
        "sha256": _sha256(destination),
        "quick_check": check,
    }


def migrate(args: argparse.Namespace) -> dict[str, Any]:
    lock = _read_json(LOCK)
    if _pid_alive(int(lock.get("pid", 0))):
        raise RuntimeError("authoritative collector is active; stop it before migration")

    db_path = Path(args.db).resolve(strict=True)
    old_path = Path(args.old_contract).resolve(strict=True)
    new_path = Path(args.new_contract).resolve(strict=True)
    old_value, old_sha, old_file_sha = _contract(old_path)
    new_value, new_sha, new_file_sha = _contract(new_path)
    if old_sha == new_sha:
        raise RuntimeError("old and new contracts are identical")
    if old_value.get("schema_version") != 1 or new_value.get("schema_version") != 2:
        raise RuntimeError("migration requires contract schema 1 -> 2")

    output = Path(args.new_output_dir).resolve()

    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        before = _counts(conn)
        targets, already_applied = _validate_preconditions(
            conn,
            old_sha=old_sha,
            new_sha=new_sha,
            expected_total=args.expected_total,
            expected_accepted=args.expected_accepted,
            expected_mode_rejected=args.expected_mode_rejected,
        )
        result: dict[str, Any] = {
            "kind": "authoritative_contract_v1_to_v2",
            "mode": "apply" if args.apply else "dry_run",
            "already_applied": already_applied,
            "database": str(db_path),
            "old_contract": {
                "path": str(old_path), "canonical_sha256": old_sha,
                "file_sha256": old_file_sha,
            },
            "new_contract": {
                "path": str(new_path), "canonical_sha256": new_sha,
                "file_sha256": new_file_sha,
            },
            "new_output_dir": str(output),
            "target_rows": len(targets),
            "before": before,
        }
        if (
            not already_applied
            and output.exists()
            and any(output.iterdir())
        ):
            raise RuntimeError(f"new authoritative output is not empty: {output}")
        if not args.apply or already_applied:
            result["after"] = before
            return result

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_path = db_path.with_name(f"{db_path.stem}.pre-contract-v2-{stamp}.sqlite3")
        result["backup"] = _backup(conn, backup_path)
        target_tags = [str(row["battle_tag"]) for row in targets]

        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "UPDATE tasks SET status='pending',updated_at=strftime('%s','now') "
                "WHERE status='inflight'"
            )
            placeholders = ",".join("?" * len(target_tags))
            conn.execute(
                f"UPDATE tasks SET status='pending',attempts=0,error=NULL,"
                f"next_retry_at=0,saved_path=NULL,updated_at=strftime('%s','now') "
                f"WHERE json_extract(meta,'$.battle_tag') IN ({placeholders})",
                target_tags,
            )
            conn.execute(
                f"UPDATE authoritative_results SET status='queued',"
                f"tier='contract_v2_revalidation',reason=NULL,saved_path=NULL,"
                f"updated_at=strftime('%s','now') "
                f"WHERE battle_tag IN ({placeholders})",
                target_tags,
            )
            conn.execute(
                "UPDATE authoritative_results SET contract_sha256=?,"
                "updated_at=strftime('%s','now')",
                (new_sha,),
            )

            updates: list[tuple[str, int]] = []
            for row in conn.execute("SELECT id,meta FROM tasks WHERE meta IS NOT NULL"):
                try:
                    value = json.loads(row["meta"])
                except (TypeError, json.JSONDecodeError):
                    continue
                replaced = _replace_contract_sha(value, old_sha, new_sha)
                if replaced != value:
                    updates.append((
                        json.dumps(replaced, ensure_ascii=False, sort_keys=True),
                        int(row["id"]),
                    ))
            conn.executemany("UPDATE tasks SET meta=? WHERE id=?", updates)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise

        quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
        if quick_check != "ok":
            raise RuntimeError(f"post-migration quick_check failed: {quick_check}")
        after = _counts(conn)
        expected_queued = args.expected_total - (
            before["results"].get("rejected:schema", 0)
            + before["results"].get("rejected:version", 0)
            + before["results"].get("rejected:dependency_unresolved", 0)
        )
        queued = sum(
            total for key, total in after["results"].items()
            if key.startswith("queued:")
        )
        if (
            after["contracts"] != {new_sha: args.expected_total}
            or after["results"].get("accepted:native_static_v2", 0) != 0
            or queued != expected_queued
        ):
            raise RuntimeError(f"post-migration invariant failed: {after}")
        output.mkdir(parents=True, exist_ok=True)
        result["updated_task_meta_rows"] = len(updates)
        result["quick_check"] = quick_check
        result["after"] = after
        manifest = output / "contract-migration.json"
        payload = (json.dumps(result, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        temp = manifest.with_suffix(".tmp")
        temp.write_bytes(payload)
        temp.replace(manifest)
        result["migration_manifest"] = str(manifest)
        return result
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--old-contract", required=True)
    parser.add_argument("--new-contract", required=True)
    parser.add_argument("--new-output-dir", required=True)
    parser.add_argument("--expected-total", type=int, default=100_000)
    parser.add_argument("--expected-accepted", type=int, default=90)
    parser.add_argument("--expected-mode-rejected", type=int, default=3_916)
    parser.add_argument("--apply", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    result = migrate(build_parser().parse_args(argv))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
