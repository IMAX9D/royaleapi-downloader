"""Import battle tags from a training union manifest into crawler exclusions."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from .queue import TaskStore


def read_tags(path: Path) -> list[str]:
    tags: list[str] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            value = json.loads(line)
            tag = str(value.get("battle_tag") or "").strip()
            if not tag:
                raise ValueError(f"missing battle_tag at line {line_number}")
            tags.append(tag)
    return tags


async def run(args: argparse.Namespace) -> dict[str, int]:
    tags = read_tags(args.manifest.resolve(strict=True))
    if len(set(tags)) != args.expected_count:
        raise RuntimeError(
            f"expected {args.expected_count} unique tags, got {len(set(tags))}"
        )
    store = TaskStore(str(args.db), max_retries=8)
    try:
        return await store.import_excluded_battles(
            tags, args.source, skip_completed=args.skip_completed
        )
    finally:
        store.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--expected-count", type=int, required=True)
    parser.add_argument("--source", default="expert-training-union")
    parser.add_argument(
        "--skip-completed", action="store_true",
        help="also convert already-done matching detail tasks to skipped",
    )
    result = asyncio.run(run(parser.parse_args()))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
