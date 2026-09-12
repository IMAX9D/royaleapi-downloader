"""存储：把抓到的数据落盘，并维护一份可检索的索引。"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Optional


class Storage:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.raw = self.root / "raw"
        self.index_path = self.root / "index.jsonl"
        self._index_keys: set[tuple[str, str]] = set()
        for sub in ("lists", "battles"):
            (self.raw / sub).mkdir(parents=True, exist_ok=True)
        if self.index_path.exists():
            with self.index_path.open("r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    try:
                        record = json.loads(line)
                    except (ValueError, TypeError):
                        continue
                    key = self._record_index_key(record)
                    if key is not None:
                        self._index_keys.add(key)

    def save_json(self, url: str, data: Any, kind: str = "battle") -> str:
        """保存单条 JSON，按 battle_tag 前 2 位分片到 raw/battles/{prefix}/，返回落盘路径。

        大规模（百万级）时避免单个目录塞几百万文件导致文件系统变慢。
        """
        key = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
        ident = self._safe_ident(self._ident(data) or self._ident_from_url(url) or key)
        prefix = ident[:2].lower() if ident and ident != key else "xx"
        subdir = self.raw / "battles" / prefix
        subdir.mkdir(parents=True, exist_ok=True)
        path = subdir / f"{ident}_{key}.json"
        # Authoritative admission must never leave a half-written replay after
        # interruption. The temp file is in the same directory so replace is an
        # atomic rename on the target filesystem.
        self.atomic_text(path, json.dumps(data, ensure_ascii=False))
        return str(path)

    def save_html(self, url: str, html: str) -> str:
        """保存列表页原始 HTML 到 raw/lists/，返回路径。"""
        key = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
        ident = self._safe_ident(self._ident_from_url(url) or key)
        path = self.raw / "lists" / f"{ident}_{key}.html"
        self.atomic_text(path, html)
        return str(path)

    @staticmethod
    def _safe_ident(value: str) -> str:
        # Untrusted payloads/URL parameters must never supply Windows paths.
        return re.sub(r"[^a-zA-Z0-9_-]", "_", value)[:100] or "unknown"

    @staticmethod
    def atomic_text(path: Path, content: str) -> None:
        temp = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
        try:
            with temp.open("x", encoding="utf-8", newline="\n") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            temp.replace(path)
        finally:
            temp.unlink(missing_ok=True)

    def _append(self, record: dict) -> None:
        # Preserve a truncated crash tail, but never concatenate the next JSON
        # record onto it. Readers can report/skip the damaged line independently.
        with self.index_path.open("a+b") as handle:
            handle.seek(0, os.SEEK_END)
            if handle.tell():
                handle.seek(-1, os.SEEK_END)
                if handle.read(1) != b"\n":
                    handle.write(b"\n")
            handle.write((json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())

    def append_index(self, record: dict) -> None:
        self._append(record)
        key = self._record_index_key(record)
        if key is not None:
            self._index_keys.add(key)

    def append_index_idempotent(self, record: dict) -> bool:
        """Durably append one battle record at most once per kind/tag."""
        key = self._record_index_key(record)
        if key is None:
            raise ValueError("idempotent index record requires battle_tag")
        if key in self._index_keys:
            return False
        self._append(record)
        self._index_keys.add(key)
        return True

    @staticmethod
    def _record_index_key(record: Any) -> Optional[tuple[str, str]]:
        if not isinstance(record, dict):
            return None
        battle_tag = str(record.get("battle_tag") or "").strip()
        if not battle_tag:
            url = str(record.get("url") or "")
            if not url or record.get("kind") not in ("battle", "authoritative_battle"):
                return None
            battle_tag = "url:" + url
        return str(record.get("kind") or "battle"), battle_tag

    @staticmethod
    def _ident(data: Any) -> Optional[str]:
        if not isinstance(data, dict):
            return None
        for key in ("battleId", "battle_id", "battle_tag", "id", "tag", "battleTime", "name"):
            value = data.get(key)
            if isinstance(value, str) and value:
                return value.replace("#", "").replace("/", "_").replace(" ", "_")
        return None

    @staticmethod
    def _ident_from_url(url: str) -> Optional[str]:
        m = re.search(r"[?&]tag=([^&]+)", url)
        if m:
            return m.group(1)
        m = re.search(r"/player/([^/?]+)", url)
        if m:
            return m.group(1)
        return None
