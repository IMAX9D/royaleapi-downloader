# 用途：从当前榜单生成带来源记录的玩家种子。
# 分类：玩家发现、批次与持续采集；使用：手动准备或更新种子
# 相关文件与阅读顺序：见同目录 README.md。

"""Build a fresh, auditable player frontier from current RoyaleAPI leaderboards."""
from __future__ import annotations

import argparse
import asyncio
import dataclasses
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time
from typing import Any

from .client import PatchrightFetcher
from .config import load_config
from .parsers import parse_player_links
from .live_sources import cached_source_is_fresh
from .storage import Storage


COUNTRY_RE = re.compile(r'/players/leaderboard/([a-z]{2})(?:[?"/])')
PLAYER_URL_RE = re.compile(r"/player/([A-Za-z0-9]{3,})/battles")


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Storage.atomic_text(path, json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + '\n')


def _seen_player_tags(database: Path) -> set[str]:
    connection = sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True, timeout=30)
    try:
        connection.execute('PRAGMA query_only=ON')
        tags: set[str] = set()
        for (url,) in connection.execute(
            "SELECT url FROM tasks WHERE kind IN ('list','upgrade_list')"
        ):
            match = PLAYER_URL_RE.search(str(url))
            if match:
                tags.add(match.group(1).upper())
        return tags
    finally:
        connection.close()


async def run(args: argparse.Namespace) -> dict[str, Any]:
    config_path = args.config.resolve(strict=True)
    config = load_config(config_path)
    config = dataclasses.replace(config, browser_headless=bool(args.headless))
    database = Path(config.db_path)
    if not database.is_absolute():
        database = config_path.parent / database
    seen = _seen_player_tags(database.resolve(strict=True))
    # Leaderboards are public.  Use more of the already distinct production
    # exits than the long-running crawler can afford to keep resident, because
    # this bounded refresh runs while the crawler itself is stopped.
    proxies = list(config.proxy.proxies[:8])
    if not proxies:
        proxies = [None]
    fetcher = PatchrightFetcher(config)
    cache_root = args.cache_root.resolve()
    cache_root.mkdir(parents=True, exist_ok=True)

    async def fetch(url: str, index: int) -> tuple[str, str]:
        errors: list[str] = []
        for attempt in range(max(2, len(proxies))):
            proxy = proxies[(index + attempt) % len(proxies)]
            try:
                response = await fetcher.fetch(proxy, url)
                if response.status_code == 200:
                    return response.text, str(proxy or "direct")
                errors.append(f"{proxy}:HTTP{response.status_code}")
            except Exception as error:  # noqa: BLE001
                errors.append(f"{proxy}:{type(error).__name__}")
        raise RuntimeError(f"leaderboard fetch failed: {url}: {errors[-4:]}")

    try:
        global_url = "https://royaleapi.com/players/leaderboard"
        global_html, global_proxy = await fetch(global_url, 0)
        countries = sorted(set(COUNTRY_RE.findall(global_html)))
        urls = [global_url, *(f"{global_url}/{code}" for code in countries)]
        semaphore = asyncio.Semaphore(max(1, len(proxies)))

        async def one(index_url: tuple[int, str]) -> dict[str, Any]:
            index, url = index_url
            cache = cache_root / f"page-{index:03d}.json"
            if url != global_url and cache.is_file():
                try:
                    cached = json.loads(cache.read_text(encoding='utf-8'))
                except (OSError, ValueError):
                    cached = None
                if cached_source_is_fresh(cached, url, time.time(), args.cache_ttl):
                    return cached
            if url == global_url:
                html, proxy = global_html, global_proxy  # do not fetch the root twice
            else:
                async with semaphore:
                    html, proxy = await fetch(url, index)
            tags = [str(tag).upper() for tag in parse_player_links(html)]
            if not tags:
                raise ValueError(f'来源页无可识别玩家，保留旧缓存而不更新 TTL: {url}')
            value = {
                "url": url,
                "proxy": proxy,
                "html_sha256": hashlib.sha256(html.encode("utf-8")).hexdigest(),
                "tags": tags,
                "fetched_utc": datetime.now(timezone.utc).isoformat(),
            }
            _atomic_json(cache, value)
            print(f"[{index + 1}/{len(urls)}] {url} tags={len(tags)}", flush=True)
            return value

        pages = await asyncio.gather(*(one(item) for item in enumerate(urls)))
    finally:
        await fetcher.aclose()

    all_tags = sorted({tag for page in pages for tag in page["tags"]})
    unseen = sorted(set(all_tags) - seen)
    selected = unseen if len(unseen) >= args.minimum_unseen else all_tags
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("".join(f"#{tag}\n" for tag in selected), encoding="utf-8")
    result = {
        "schema_version": 1,
        "kind": "current_royaleapi_leaderboard_seed_receipt_v1",
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "config": str(config_path),
        "database": str(database.resolve()),
        "pages": len(pages),
        "countries": len(countries),
        "all_unique_tags": len(all_tags),
        "previously_seen_tags": len(set(all_tags) & seen),
        "unseen_tags": len(unseen),
        "selection": "unseen_only" if selected is unseen else "all_current",
        "selected_tags": len(selected),
        "minimum_unseen": args.minimum_unseen,
        "output": str(output),
        "output_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        "cache_root": str(cache_root),
    }
    _atomic_json(output.with_suffix(output.suffix + ".receipt.json"), result)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config.authoritative.toml"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--minimum-unseen", type=int, default=5_000)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument('--cache-ttl', type=float, default=1800.0, help='榜单缓存有效秒数；0 强制刷新')
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if not math.isfinite(args.cache_ttl) or args.cache_ttl < 0:
        parser.error('--cache-ttl 必须为非负有限秒数')
    result = asyncio.run(run(args))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
