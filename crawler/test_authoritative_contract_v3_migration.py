from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from .authoritative import (
    KING_TOWER_LEVEL_EVIDENCE_SCHEMA,
    native_contract_payload_sha256,
)
from .authoritative_contract_v3_migration import migrate


def _write_contract(path: Path, version: int) -> str:
    value = {
        "schema_version": version,
        "kind": f"cr_native_authoritative_contract_v{version}",
        "game_version": "15.535.29",
        "allowed_card_tokens": ["knight"],
        "allowed_tower_troops": ["tower-princess"],
        "ability_source_tokens": ["knight"],
        "source_numeric_game_mode_ids": [72000006],
        "native_execution_mode_by_source": {"72000006": 72000006},
        "king_tower_max_hp_by_level": {"16": 7728},
        "runtime": {"runtime_version": "150535029"},
        "cards": [],
        "tower_troops": [],
        "ability_source_card_ids": [],
        "ability_sources": [],
        "counts": {},
        "source_catalog_generated_utc": "frozen",
        "ingest_schema": {"numeric_game_mode": "stable"},
        "components": {
            "binding": {"path": "binding", "sha256": "a" * 64},
            "contract_generator": {
                "path": "expert_v1/native_ingest_contract.py",
                "sha256": ("b" if version == 2 else "c") * 64,
            },
            "royaleapi_aliases": {
                "path": "expert_v1/native_replay_plan.py",
                "sha256": ("d" if version == 2 else "e") * 64,
            },
        },
    }
    if version == 3:
        value["king_tower_level_evidence"] = KING_TOWER_LEVEL_EVIDENCE_SCHEMA
        value["ingest_schema"]["king_tower_level"] = (
            KING_TOWER_LEVEL_EVIDENCE_SCHEMA
        )
    value["contract_sha256"] = native_contract_payload_sha256(value)
    path.write_text(json.dumps(value), encoding="utf-8")
    return str(value["contract_sha256"])


class ContractV3MigrationTests(unittest.TestCase):
    def test_dry_run_and_apply_reuse_accepted_refetch_king_reject(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
            root = Path(directory)
            old_path, new_path = root / "v2.json", root / "v3.json"
            old_sha = _write_contract(old_path, 2)
            new_sha = _write_contract(new_path, 3)
            old_file_sha = hashlib.sha256(old_path.read_bytes()).hexdigest()
            accepted_path = root / "accepted.json"
            accepted_path.write_text(json.dumps({
                "schema_version": 5,
                "authoritative_native_contract": {
                    "game_version": "15.535.29",
                    "contract_sha256": old_sha,
                    "contract_file_sha256": old_file_sha,
                },
            }), encoding="utf-8")
            source_path = root / "legacy.json"
            source_path.write_text("{}", encoding="utf-8")

            db = root / "progress.sqlite3"
            conn = sqlite3.connect(db)
            conn.executescript("""
                CREATE TABLE tasks(
                    id INTEGER PRIMARY KEY,url TEXT,dedup_key TEXT,seed TEXT,
                    kind TEXT,meta TEXT,status TEXT,attempts INTEGER,error TEXT,
                    next_retry_at REAL,saved_path TEXT,created_at REAL,updated_at REAL
                );
                CREATE TABLE authoritative_results(
                    battle_tag TEXT PRIMARY KEY,status TEXT,tier TEXT,reason TEXT,
                    saved_path TEXT,source_path TEXT,source_schema_version INTEGER,
                    contract_sha256 TEXT,created_at REAL,updated_at REAL
                );
                CREATE TABLE battle_metadata(
                    battle_tag TEXT PRIMARY KEY,metadata TEXT,complete INTEGER,
                    source_list_url TEXT,schema_version INTEGER,created_at REAL,
                    updated_at REAL
                );
            """)
            rows = [
                (1, "A", "accepted", "native_static_v2", None, accepted_path, 5),
                (2, "K", "rejected", "king_level",
                 "opponent_king_tower_not_full_level_16", None, 2),
            ]
            for task_id, tag, status, tier, reason, saved, schema in rows:
                source = accepted_path if tag == "A" else source_path
                meta = {
                    "battle_tag": tag,
                    "contract_sha256": old_sha,
                    "source_path": str(source),
                    "source_schema_version": schema,
                    "local_exact_replay_body": False,
                    "upgrade_tier": "list_metadata_and_replay_refetch",
                }
                conn.execute(
                    "INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (task_id, f"https://x/?tag={tag}", f"authoritative:{tag}",
                     "seed", "upgrade", json.dumps(meta),
                     "done" if status == "accepted" else "skipped", 1,
                     None if status == "accepted" else "old king gate", 0,
                     str(saved) if saved else None, 0, 0),
                )
                conn.execute(
                    "INSERT INTO authoritative_results VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (tag, status, tier, reason, str(saved) if saved else None,
                     str(source), schema, old_sha, 0, 0),
                )
            conn.commit()
            conn.close()

            args = argparse.Namespace(
                db=str(db), old_contract=str(old_path), new_contract=str(new_path),
                new_output_dir=str(root / "v3-output"), apply=False,
            )
            with patch(
                "crawler.authoritative_contract_v3_migration.LOCK",
                root / "inactive.lock",
            ):
                dry = migrate(args)
                self.assertEqual(dry["plan"]["accepted_local_schema5_reuse"], 1)
                self.assertEqual(dry["plan"]["king_level_rejected"], 1)
                self.assertEqual(
                    dry["plan"]["king_level_require_list_metadata_refetch"], 1
                )
                args.apply = True
                applied = migrate(args)
            self.assertEqual(applied["quick_check"], "ok")

            conn = sqlite3.connect(db)
            conn.row_factory = sqlite3.Row
            try:
                contracts = dict(conn.execute(
                    "SELECT contract_sha256,COUNT(*) FROM authoritative_results "
                    "GROUP BY contract_sha256"
                ))
                self.assertEqual(contracts, {new_sha: 2})
                accepted = conn.execute(
                    "SELECT t.meta,t.status,ar.status,ar.tier FROM tasks t JOIN "
                    "authoritative_results ar ON json_extract(t.meta,'$.battle_tag')="
                    "ar.battle_tag WHERE ar.battle_tag='A'"
                ).fetchone()
                meta = json.loads(accepted["meta"])
                self.assertTrue(meta["local_schema5_contract_upgrade"])
                self.assertEqual(meta["source_contract_sha256"], old_sha)
                self.assertEqual(
                    meta["source_file_sha256"],
                    hashlib.sha256(accepted_path.read_bytes()).hexdigest(),
                )
                self.assertEqual(tuple(accepted)[1:], (
                    "pending", "queued", "contract_v3_local_schema5_reuse"
                ))
            finally:
                conn.close()
            self.assertTrue(accepted_path.is_file())


if __name__ == "__main__":
    unittest.main()
