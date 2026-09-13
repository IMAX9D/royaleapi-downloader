# 用途：只读整理旧数据升级需要的列表和回放依赖。
# 分类：高级数据校验；使用：已有旧语料
# 相关文件与阅读顺序：见同目录 README.md。

"""Read-only preparation of legacy authoritative-upgrade dependencies."""
from __future__ import annotations

from collections import Counter, defaultdict
import json
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import parse_qs, urlparse

from .parsers import build_replay_url


def _rows(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"manifest row {line_number} is not an object")
            yield value


def _find_index(source: Path) -> Path | None:
    for parent in source.parents:
        candidate = parent / "index.jsonl"
        if candidate.is_file():
            return candidate
    return None


def _load_replay_urls(index_path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in _rows(index_path):
        if item.get("kind") != "battle":
            continue
        url = str(item.get("url") or "")
        tag = str(item.get("battle_tag") or "")
        if not tag and url:
            tag = str((parse_qs(urlparse(url).query).get("tag") or [""])[0])
        if tag and url:
            result.setdefault(tag, url)
    return result


def _query(url: str, key: str) -> str:
    return str((parse_qs(urlparse(url).query).get(key) or [""])[0])


def prepare_upgrade_groups(manifest_path: str | Path) -> dict[str, Any]:
    """Group historical candidates by exact source list URL.

    The resulting ``upgrade_list`` pages form phase one. A replay upgrade is
    only enqueued after that page is fetched and its tag-specific schema-2 list
    metadata has been parsed, so detail priority can never outrun metadata.
    """
    manifest = Path(manifest_path).resolve(strict=True)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unaddressable: list[dict[str, Any]] = []
    schemas: Counter[int] = Counter()
    index_cache: dict[Path, dict[str, str]] = {}
    seen: set[str] = set()

    for row in _rows(manifest):
        source = Path(str(row.get("source_path") or "")).resolve(strict=True)
        value = json.loads(source.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError(f"replay source is not an object: {source}")
        tag = str(value.get("battle_tag") or row.get("battle_tag") or "").strip()
        if not tag:
            raise ValueError(f"battle tag missing: {source}")
        if tag in seen:
            raise ValueError(f"duplicate battle tag in upgrade manifest: {tag}")
        seen.add(tag)
        schema = int(value.get("schema_version") or row.get("schema_version") or 1)
        schemas[schema] += 1

        replay_url = str(row.get("replay_url") or "")
        if not replay_url:
            provenance = value.get("_provenance") or {}
            replay_url = str(provenance.get("replay_endpoint") or "")
        if not replay_url:
            index_path = _find_index(source)
            if index_path is not None:
                if index_path not in index_cache:
                    index_cache[index_path] = _load_replay_urls(index_path)
                replay_url = index_cache[index_path].get(tag, "")

        deck_meta = value.get("deck_metadata") or {}
        referrer = str(
            row.get("source_list_url")
            or deck_meta.get("source_list_url")
            or _query(replay_url, "referrer_path")
            or ""
        )
        team_tags = value.get("team_tags") or row.get("team_tags") or []
        opponent_tags = value.get("opponent_tags") or row.get("opponent_tags") or []
        team_crowns = value.get("team_crowns", row.get("team_crowns"))
        opponent_crowns = value.get("opponent_crowns", row.get("opponent_crowns"))
        if not replay_url and (
            referrer and len(team_tags) == 1 and len(opponent_tags) == 1
            and isinstance(team_crowns, int) and isinstance(opponent_crowns, int)
        ):
            base = f"{urlparse(referrer).scheme}://{urlparse(referrer).netloc}"
            replay_url = build_replay_url(
                base, tag, str(team_tags[0]), str(opponent_tags[0]),
                str(team_crowns), str(opponent_crowns), referrer,
            )
        candidate = {
            "battle_tag": tag,
            "source_path": str(source),
            "source_schema_version": schema,
            "replay_url": replay_url,
            "source_list_url": referrer,
            "local_exact_replay_body": schema in (3, 4),
            "upgrade_tier": (
                "list_metadata_only" if schema in (3, 4)
                else "list_metadata_and_replay_refetch"
            ),
        }
        if not replay_url or not referrer:
            candidate["reason"] = "exact_replay_or_referrer_address_missing"
            unaddressable.append(candidate)
            continue
        groups[referrer].append(candidate)

    return {
        "manifest": str(manifest),
        "unique_battles": len(seen),
        "schema_counts": dict(sorted(schemas.items())),
        "locally_reusable_exact_replay_bodies": sum(
            count for schema, count in schemas.items() if schema in (3, 4)
        ),
        "replay_refetch_required": sum(
            count for schema, count in schemas.items() if schema not in (3, 4)
        ),
        "unique_source_list_urls": len(groups),
        "addressable_battles": sum(len(items) for items in groups.values()),
        "unaddressable": unaddressable,
        "groups": dict(groups),
    }
