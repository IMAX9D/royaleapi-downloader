# 用途：列表页面缓存、请求延迟和通道统计。
# 分类：会话、观测与诊断；使用：内部
# 相关文件与阅读顺序：见同目录 README.md。

"""Content-addressed list-page cache and per-list-lane telemetry."""
from __future__ import annotations

import hashlib
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Callable


@dataclass(frozen=True)
class ParsedListPage:
    sha256: str
    by_tag: dict[str, dict]
    cache_hit: bool

    @property
    def valid(self) -> bool:
        # A successful authoritative parse must expose at least one replay row.
        # Empty/login/challenge/interstitial pages remain transient failures.
        return bool(self.by_tag)


class PageHashCache:
    """Bounded LRU keyed by URL plus exact response-body SHA-256."""

    def __init__(self, capacity: int = 128) -> None:
        if capacity < 1:
            raise ValueError("page cache capacity must be positive")
        self.capacity = int(capacity)
        self._entries: OrderedDict[tuple[str, str], dict[str, dict]] = OrderedDict()

    def parse(
        self,
        *,
        url: str,
        text: str,
        parser: Callable[[str], list[dict]],
    ) -> ParsedListPage:
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        key = (url, digest)
        cached = self._entries.get(key)
        if cached is not None:
            self._entries.move_to_end(key)
            return ParsedListPage(digest, cached, True)
        battles = parser(text)
        by_tag = {
            str(battle["tag"]): battle
            for battle in battles
            if battle.get("tag")
        }
        # Do not cache empty/unparseable documents.  A login or challenge page
        # that slipped through transport checks must get a fresh network retry.
        if by_tag:
            self._entries[key] = by_tag
            self._entries.move_to_end(key)
            while len(self._entries) > self.capacity:
                self._entries.popitem(last=False)
        return ParsedListPage(digest, by_tag, False)

    def __len__(self) -> int:
        return len(self._entries)


@dataclass
class _LaneCounters:
    requests: int = 0
    success: int = 0
    parsed_pages: int = 0
    missing_pages: int = 0
    missing_candidates: int = 0
    challenges: int = 0
    rate_limited: int = 0
    errors: int = 0
    not_found: int = 0
    cache_hits: int = 0
    latency_ms: deque[float] = field(default_factory=lambda: deque(maxlen=256))


class ListLaneTelemetry:
    """In-process counters exported with the existing crawler metrics JSON."""

    def __init__(self) -> None:
        self._lanes: dict[str, _LaneCounters] = {}

    def _row(self, lane: str) -> _LaneCounters:
        return self._lanes.setdefault(lane, _LaneCounters())

    def request(self, lane: str) -> None:
        self._row(lane).requests += 1

    def response(self, lane: str, latency_seconds: float, *, success: bool) -> None:
        row = self._row(lane)
        row.latency_ms.append(max(0.0, float(latency_seconds)) * 1000.0)
        if success:
            row.success += 1

    def error(self, lane: str, latency_seconds: float, *, challenge: bool) -> None:
        row = self._row(lane)
        row.latency_ms.append(max(0.0, float(latency_seconds)) * 1000.0)
        if challenge:
            row.challenges += 1
        else:
            row.errors += 1

    def failure(self, lane: str, *, challenge: bool = False) -> None:
        row = self._row(lane)
        if challenge:
            row.challenges += 1
        else:
            row.errors += 1

    def rate_limited(self, lane: str) -> None:
        self._row(lane).rate_limited += 1

    def not_found(self, lane: str) -> None:
        self._row(lane).not_found += 1

    def parsed(
        self,
        lane: str,
        *,
        missing_candidates: int = 0,
        cache_hit: bool = False,
    ) -> None:
        row = self._row(lane)
        row.parsed_pages += 1
        if missing_candidates:
            row.missing_pages += 1
            row.missing_candidates += int(missing_candidates)
        if cache_hit:
            row.cache_hits += 1

    def missing(self, lane: str, candidates: int) -> None:
        if candidates <= 0:
            return
        row = self._row(lane)
        row.missing_pages += 1
        row.missing_candidates += int(candidates)

    @staticmethod
    def _percentile(values: deque[float], fraction: float) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        index = min(len(ordered) - 1, int((len(ordered) - 1) * fraction))
        return round(ordered[index], 2)

    def snapshot(self) -> list[dict]:
        rows = []
        for lane, counters in sorted(self._lanes.items()):
            samples = counters.latency_ms
            rows.append({
                "lane": lane,
                "requests": counters.requests,
                "success": counters.success,
                "parsed_pages": counters.parsed_pages,
                "missing_pages": counters.missing_pages,
                "missing_candidates": counters.missing_candidates,
                "challenges": counters.challenges,
                "rate_limited": counters.rate_limited,
                "errors": counters.errors,
                "not_found": counters.not_found,
                "cache_hits": counters.cache_hits,
                "latency_avg_ms": (
                    round(sum(samples) / len(samples), 2) if samples else None
                ),
                "latency_p50_ms": self._percentile(samples, 0.50),
                "latency_p95_ms": self._percentile(samples, 0.95),
            })
        return rows
