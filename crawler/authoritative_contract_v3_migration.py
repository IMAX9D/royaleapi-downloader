"""Plan/apply the fail-closed authoritative contract v2 -> v3 migration.

Dry-run is read-only and safe while production is active.  Apply requires the
collector to be stopped, creates an SQLite online backup, preserves the v2
output tree, requeues accepted rows for local schema-5 restamping, and
requeues old ``king_level`` rejects for fresh list evidence (and replay
refetch where their original body is not exact schema 3/4).
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from typing import Any, Mapping

from .authoritative import (
    KING_TOWER_LEVEL_EVIDENCE_SCHEMA,
    load_native_contract,
    native_contract_payload_sha256,
)
from .authoritative_production import LOCK, _pid_alive, _read_json


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


def _validate_contract_delta(
    old_value: Mapping[str, Any], new_value: Mapping[str, Any]
) -> None:
    if (
        (old_value.get("schema_version"), old_value.get("kind"))
        != (2, "cr_native_authoritative_contract_v2")
        or (new_value.get("schema_version"), new_value.get("kind"))
        != (3, "cr_native_authoritative_contract_v3")
    ):
        raise RuntimeError("migration requires contract v2 -> v3")
    if new_value.get("king_tower_level_evidence") != (
        KING_TOWER_LEVEL_EVIDENCE_SCHEMA
    ):
        raise RuntimeError("new contract King Tower evidence schema mismatch")

    stable_keys = {
        "game_version", "allowed_card_tokens", "allowed_tower_troops",
        "ability_source_tokens", "source_numeric_game_mode_ids",
        "native_execution_mode_by_source", "king_tower_max_hp_by_level",
        "runtime", "cards", "tower_troops", "ability_source_card_ids",
        "ability_sources", "counts", "source_catalog_generated_utc",
    }
    changed = [
        key for key in sorted(stable_keys)
        if old_value.get(key) != new_value.get(key)
    ]
    if changed:
        raise RuntimeError(
            "v3 migration changes non-evidence semantics: " + ",".join(changed)
        )

    old_schema = dict(old_value.get("ingest_schema") or {})
    new_schema = dict(new_value.get("ingest_schema") or {})
    if new_schema.pop("king_tower_level", None) != KING_TOWER_LEVEL_EVIDENCE_SCHEMA:
        raise RuntimeError("v3 ingest schema lacks exact King evidence")
    if old_schema != new_schema:
        raise RuntimeError("v3 ingest schema changes more than King evidence")

    old_components = dict(old_value.get("components") or {})
    new_components = dict(new_value.get("components") or {})
    # Both files containing the evidence implementation necessarily change:
    # the generator emits v3 and native_replay_plan validates its provenance.
    # Their derived card/alias surface is already compared above.  Runtime,
    # catalog and capability source bytes must remain identical.
    allowed_code_changes = {"contract_generator", "royaleapi_aliases"}
    old_changed = {
        key: old_components.pop(key, None) for key in allowed_code_changes
    }
    new_changed = {
        key: new_components.pop(key, None) for key in allowed_code_changes
    }
    if old_components != new_components:
        raise RuntimeError("runtime/card/native components changed during migration")
    for key in sorted(allowed_code_changes):
        old_component = old_changed[key]
        new_component = new_changed[key]
        if not isinstance(old_component, Mapping) or not isinstance(
            new_component, Mapping
        ):
            raise RuntimeError(f"{key} component is missing")
        if old_component.get("path") != new_component.get("path"):
            raise RuntimeError(f"{key} component path changed")


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
    rows = conn.execute(
        "SELECT ar.battle_tag,ar.status result_status,ar.tier,ar.reason,"
        "ar.saved_path result_saved_path,ar.source_path,"
        "ar.source_schema_version,ar.contract_sha256,"
        "t.id task_id,t.url,t.kind,t.status task_status,t.meta "
        "FROM authoritative_results ar JOIN tasks t "
        "ON json_extract(t.meta,'$.battle_tag')=ar.battle_tag "
        "WHERE ar.status='accepted' OR "
        "(ar.status='rejected' AND ar.tier='king_level') "
        "ORDER BY ar.battle_tag"
    ).fetchall()
    expected = int(conn.execute(
        "SELECT COUNT(*) FROM authoritative_results WHERE status='accepted' OR "
        "(status='rejected' AND tier='king_level')"
    ).fetchone()[0])
    if len(rows) != expected:
        raise RuntimeError(
            "accepted/king_level rows do not have exactly one upgrade task: "
            f"joined={len(rows)} expected={expected}"
        )
    return rows


def _source_replay_is_exact(row: sqlite3.Row) -> bool:
    try:
        meta = json.loads(row["meta"] or "{}")
    except json.JSONDecodeError:
        return False
    return bool(
        meta.get("local_exact_replay_body")
        and int(row["source_schema_version"] or 0) in (3, 4)
        and Path(str(row["source_path"] or "")).is_file()
    )


def _plan(
    conn: sqlite3.Connection,
    *,
    old_sha: str,
    old_file_sha: str,
    old_game_version: str,
) -> dict[str, Any]:
    contract_counts = {
        str(row[0]): int(row[1])
        for row in conn.execute(
            "SELECT contract_sha256,COUNT(*) FROM authoritative_results "
            "GROUP BY contract_sha256"
        )
    }
    if contract_counts and contract_counts != {old_sha: sum(contract_counts.values())}:
        raise RuntimeError(f"database is not pinned solely to old contract: {contract_counts}")
    rows = _target_rows(conn)
    accepted = [row for row in rows if row["result_status"] == "accepted"]
    king = [row for row in rows if row["tier"] == "king_level"]
    missing_accepted: list[str] = []
    invalid_accepted_stamp: list[str] = []
    for row in accepted:
        path = Path(str(row["result_saved_path"] or ""))
        if not path.is_file():
            missing_accepted.append(str(row["battle_tag"]))
            continue
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            invalid_accepted_stamp.append(str(row["battle_tag"]))
            continue
        stamp = value.get("authoritative_native_contract")
        if (
            value.get("schema_version") != 5
            or not isinstance(stamp, Mapping)
            or stamp.get("contract_sha256") != old_sha
            or stamp.get("contract_file_sha256") != old_file_sha
            or stamp.get("game_version") != old_game_version
        ):
            invalid_accepted_stamp.append(str(row["battle_tag"]))
    if missing_accepted or invalid_accepted_stamp:
        raise RuntimeError(
            "accepted reuse sources failed closed: "
            f"missing={len(missing_accepted)} invalid={len(invalid_accepted_stamp)}"
        )

    metadata_rows = int(conn.execute(
        "SELECT COUNT(*) FROM battle_metadata bm JOIN authoritative_results ar "
        "ON ar.battle_tag=bm.battle_tag WHERE ar.status='rejected' "
        "AND ar.tier='king_level'"
    ).fetchone()[0])
    exact_replay = sum(_source_replay_is_exact(row) for row in king)
    reason_counts: dict[str, int] = {}
    source_schema_counts: dict[str, int] = {}
    for row in king:
        reason = str(row["reason"] or "")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        schema = str(row["source_schema_version"])
        source_schema_counts[schema] = source_schema_counts.get(schema, 0) + 1
    return {
        "accepted_local_schema5_reuse": len(accepted),
        "accepted_missing_or_invalid_reuse_sources": 0,
        "king_level_rejected": len(king),
        "king_level_recoverable_without_list_refetch_proven_lower_bound": 0,
        "king_level_with_retained_list_metadata": metadata_rows,
        "king_level_require_list_metadata_refetch": len(king) - metadata_rows,
        "king_level_reusable_exact_replay_body": exact_replay,
        "king_level_require_replay_refetch": len(king) - exact_replay,
        "king_level_reason_counts": dict(sorted(reason_counts.items())),
        "king_level_source_schema_counts": dict(sorted(source_schema_counts.items())),
        "why_recovery_is_unknown": (
            "Tower Troop levels and terminal list metadata were deleted after the "
            "old early king gate; exact future recovery requires refetching each "
            "battle's list page. Zero is the only provable no-refetch lower bound."
        ),
    }


def _backup(conn: sqlite3.Connection, destination: Path) -> dict[str, Any]:
    if destination.exists():
        raise FileExistsError(f"backup already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    backup = sqlite3.connect(destination)
    try:
        conn.backup(backup)
        check = str(backup.execute("PRAGMA quick_check").fetchone()[0])
    finally:
        backup.close()
    if check != "ok":
        raise RuntimeError(f"backup quick_check failed: {check}")
    with destination.open("r+b") as stream:
        os.fsync(stream.fileno())
    return {
        "path": str(destination),
        "size": destination.stat().st_size,
        "sha256": _sha256(destination),
        "quick_check": check,
    }


def migrate(args: argparse.Namespace) -> dict[str, Any]:
    old_path = Path(args.old_contract).resolve(strict=True)
    new_path = Path(args.new_contract).resolve(strict=True)
    old_value, old_sha, old_file_sha = _contract(old_path)
    new_value, new_sha, new_file_sha = _contract(new_path)
    _validate_contract_delta(old_value, new_value)
    # Fully parse the v3 contract with the same production reader.
    load_native_contract(new_path, expected_game_version=str(new_value["game_version"]))
    if old_sha == new_sha:
        raise RuntimeError("old and new contracts are identical")

    lock = _read_json(LOCK)
    active = _pid_alive(int(lock.get("pid", 0)))
    if args.apply and active:
        raise RuntimeError("authoritative collector is active; stop it before apply")

    db_path = Path(args.db).resolve(strict=True)
    output = Path(args.new_output_dir).resolve()
    uri = f"file:{db_path.as_posix()}?mode={'rw' if args.apply else 'ro'}"
    conn = sqlite3.connect(uri, uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        conn.execute("BEGIN")
        before = _counts(conn)
        already_applied = before["contracts"] == {
            new_sha: sum(before["contracts"].values())
        }
        migration_plan = (
            {
                "accepted_local_schema5_reuse": 0,
                "king_level_rejected": 0,
                "already_applied": True,
            }
            if already_applied
            else _plan(
                conn,
                old_sha=old_sha,
                old_file_sha=old_file_sha,
                old_game_version=str(old_value["game_version"]),
            )
        )
        conn.rollback()
        result: dict[str, Any] = {
            "kind": "authoritative_contract_v2_to_v3_king_evidence",
            "mode": "apply" if args.apply else "dry_run",
            "collector_active": active,
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
            "old_output_immutable": True,
            "new_output_dir": str(output),
            "plan": migration_plan,
            "before": before,
        }
        if not args.apply or already_applied:
            result["after"] = before
            return result
        if output.exists() and any(output.iterdir()):
            raise RuntimeError(f"new authoritative output is not empty: {output}")

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_path = db_path.with_name(
            f"{db_path.stem}.pre-contract-v3-{stamp}.sqlite3"
        )
        result["backup"] = _backup(conn, backup_path)
        targets = _target_rows(conn)

        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "UPDATE tasks SET status='pending',updated_at=strftime('%s','now') "
                "WHERE status='inflight'"
            )
            for row in targets:
                meta = json.loads(row["meta"] or "{}")
                meta = _replace_contract_sha(meta, old_sha, new_sha)
                if row["result_status"] == "accepted":
                    source = Path(str(row["result_saved_path"])).resolve(strict=True)
                    meta.update({
                        "source_path": str(source),
                        "source_schema_version": 5,
                        "source_contract_sha256": old_sha,
                        "source_contract_file_sha256": old_file_sha,
                        "source_contract_game_version": str(
                            old_value["game_version"]
                        ),
                        "source_file_sha256": _sha256(source),
                        "local_exact_replay_body": False,
                        "local_schema5_contract_upgrade": True,
                        "upgrade_tier": "contract_v3_local_schema5_reuse",
                    })
                    queued_tier = "contract_v3_local_schema5_reuse"
                else:
                    meta.pop("local_schema5_contract_upgrade", None)
                    meta["contract_sha256"] = new_sha
                    queued_tier = "contract_v3_king_evidence_recheck"
                conn.execute(
                    "UPDATE tasks SET kind='upgrade',meta=?,status='pending',"
                    "attempts=0,error=NULL,next_retry_at=0,saved_path=NULL,"
                    "updated_at=strftime('%s','now') WHERE id=?",
                    (json.dumps(meta, ensure_ascii=False, sort_keys=True), row["task_id"]),
                )
                conn.execute(
                    "UPDATE authoritative_results SET status='queued',tier=?,"
                    "reason=NULL,saved_path=NULL,contract_sha256=?,"
                    "updated_at=strftime('%s','now') WHERE battle_tag=?",
                    (queued_tier, new_sha, row["battle_tag"]),
                )

            task_updates: list[tuple[str, int]] = []
            target_ids = {int(row["task_id"]) for row in targets}
            for row in conn.execute("SELECT id,meta FROM tasks WHERE meta IS NOT NULL"):
                if int(row["id"]) in target_ids:
                    continue
                try:
                    meta = json.loads(row["meta"])
                except (TypeError, json.JSONDecodeError):
                    continue
                replaced = _replace_contract_sha(meta, old_sha, new_sha)
                if replaced != meta:
                    task_updates.append((
                        json.dumps(replaced, ensure_ascii=False, sort_keys=True),
                        int(row["id"]),
                    ))
            conn.executemany("UPDATE tasks SET meta=? WHERE id=?", task_updates)
            conn.execute(
                "UPDATE authoritative_results SET contract_sha256=?,"
                "updated_at=strftime('%s','now') WHERE contract_sha256=?",
                (new_sha, old_sha),
            )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise

        quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
        if quick_check != "ok":
            raise RuntimeError(f"post-migration quick_check failed: {quick_check}")
        after = _counts(conn)
        if after["contracts"] != {new_sha: sum(after["contracts"].values())}:
            raise RuntimeError(f"post-migration contract invariant failed: {after}")
        output.mkdir(parents=True, exist_ok=True)
        result["updated_non_target_task_meta_rows"] = len(task_updates)
        result["quick_check"] = quick_check
        result["after"] = after
        manifest = output / "contract-migration-v3.json"
        payload = (json.dumps(result, ensure_ascii=False, indent=2) + "\n").encode(
            "utf-8"
        )
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
    parser.add_argument("--apply", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    result = migrate(build_parser().parse_args(argv))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
