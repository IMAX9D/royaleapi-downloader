"""代理池：轮换 + 健康检查 + 冷却/封禁检测 + 每 IP 独立令牌桶。

设计要点：
- 每个代理（含"直连"这个特殊代理）都配一个独立的 TokenBucket，
  因此总吞吐会随代理数量线性扩展，而每个 IP 的速率始终受控，避免封禁。
- pick() 选择"令牌最多（最空闲）且当前可用"的代理，天然做负载均衡。
- 429 → 短冷却；403 → 长冷却（疑似封 IP）；网络错误 → 短冷却并累计失败，
  连续失败过多则标记不健康，由后台探活任务恢复。
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import random
import time
from pathlib import Path
from typing import Awaitable, Callable, Optional

from .config import ProxyPoolConfig, RateLimitConfig
from .ratelimit import TokenBucket

log = logging.getLogger("crawler.proxy")


@dataclasses.dataclass
class ProxyState:
    url: Optional[str]  # None 表示直连（无代理）
    bucket: TokenBucket
    healthy: bool = True
    cooldown_until: float = 0.0
    consecutive_failures: int = 0
    success: int = 0
    fail: int = 0
    rate_limited: int = 0
    forbidden: int = 0
    errors: int = 0
    auth_failures: int = 0
    latency_ema: float = 0.0

    @property
    def available_now(self) -> bool:
        return self.healthy and time.monotonic() >= self.cooldown_until

    @property
    def label(self) -> str:
        return self.url or "direct"


class ProxyPool:
    def __init__(self, cfg: ProxyPoolConfig, rate_cfg: RateLimitConfig):
        self.cfg = cfg
        self._lock = asyncio.Lock()
        self.states: list[ProxyState] = []
        for p in self._load_proxies(cfg):
            self.states.append(
                ProxyState(
                    url=p,
                    bucket=TokenBucket(
                        rate_cfg.requests_per_second, rate_cfg.burst, rate_cfg.jitter
                    ),
                )
            )
        if not self.states:
            # 无代理 → 退化为"直连单 IP 保守限速"
            self.states.append(
                ProxyState(
                    url=None,
                    bucket=TokenBucket(
                        rate_cfg.requests_per_second, rate_cfg.burst, rate_cfg.jitter
                    ),
                )
            )

    @staticmethod
    def _load_proxies(cfg: ProxyPoolConfig) -> list[str]:
        """合并 config 里的 proxies 与 proxies_file 文件（每行一个，去重）。"""
        urls: list[str] = []
        for p in cfg.proxies:
            if p and p not in urls:
                urls.append(p)
        if cfg.proxies_file:
            f = Path(cfg.proxies_file)
            if f.exists():
                for line in f.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if line and not line.startswith("#") and line not in urls:
                        urls.append(line)
        return urls

    @property
    def size(self) -> int:
        return len(self.states)

    async def pick(self) -> ProxyState:
        """选择当前最空闲且可用的代理；若全部不可用则等待。"""
        while True:
            async with self._lock:
                candidates = [s for s in self.states if s.available_now]
                if candidates:
                    return max(candidates, key=lambda s: s.bucket.available)
            await asyncio.sleep(0.5)

    async def acquire(self) -> ProxyState:
        """Atomically select a lane and reserve its token.

        Selection followed by a separate ``bucket.acquire`` lets many Workers
        observe the same full bucket before any of them consumes it.  They then
        queue behind one lane while the rest remain idle.  Reserving under the
        pool lock keeps all independent exits busy.
        """
        while True:
            selected: ProxyState | None = None
            delay = 0.5
            async with self._lock:
                candidates = [state for state in self.states if state.available_now]
                candidates.sort(key=lambda state: state.bucket.available, reverse=True)
                for state in candidates:
                    if await state.bucket.try_acquire():
                        selected = state
                        break
                if candidates and selected is None:
                    delay = min(state.bucket.wait_seconds for state in candidates)
            if selected is not None:
                if selected.bucket.jitter > 0:
                    await asyncio.sleep(random.uniform(0.0, selected.bucket.jitter))
                return selected
            await asyncio.sleep(min(0.5, max(0.01, delay)))

    def _cooldown(self, s: ProxyState, seconds: float) -> None:
        s.cooldown_until = max(s.cooldown_until, time.monotonic() + seconds)

    def report_success(self, s: ProxyState, latency: float) -> None:
        s.consecutive_failures = 0
        s.healthy = True
        s.success += 1
        # 指数移动平均，用于观察每个代理的响应质量
        alpha = 0.2
        s.latency_ema = latency if s.latency_ema == 0.0 else alpha * latency + (1 - alpha) * s.latency_ema

    def report_rate_limited(self, s: ProxyState, retry_after: Optional[float] = None) -> None:
        delay = retry_after if retry_after is not None else self.cfg.cooldown_429
        self._cooldown(s, delay)
        s.fail += 1
        s.rate_limited += 1
        s.consecutive_failures += 1
        log.warning("proxy %s 触发限流(429)，冷却 %.1fs", s.label, delay)

    def report_forbidden(self, s: ProxyState) -> None:
        self._cooldown(s, self.cfg.cooldown_403)
        s.fail += 1
        s.forbidden += 1
        s.consecutive_failures += 1
        log.warning("proxy %s 疑似被封 IP(403)，冷却 %.1fs", s.label, self.cfg.cooldown_403)

    def report_error(self, s: ProxyState) -> None:
        self._cooldown(s, self.cfg.cooldown_error)
        s.fail += 1
        s.errors += 1
        s.consecutive_failures += 1
        if s.consecutive_failures >= self.cfg.max_consecutive_failures:
            s.healthy = False
            log.warning("proxy %s 连续失败 %d 次，标记为不健康，等待探活", s.label, s.consecutive_failures)

    def report_auth_failure(self, s: ProxyState) -> None:
        self._cooldown(s, self.cfg.cooldown_429)
        s.fail += 1
        s.auth_failures += 1
        log.warning("proxy %s 的 RoyaleAPI 登录会话失效/未登录", s.label)

    async def _probe(self, check: Callable[[ProxyState], Awaitable[bool]]) -> None:
        """仅探测不健康代理；健康请求不额外绕过正常限速预算。"""
        for s in (x for x in self.states if not x.healthy):
            try:
                s.healthy = await check(s)
                if s.healthy:
                    s.consecutive_failures = 0
            except Exception:  # noqa: BLE001 - 探活失败视为不健康
                s.healthy = False
            if not s.healthy:
                log.warning("proxy %s 探活失败", s.label)

    async def health_loop(self, check: Callable[[ProxyState], Awaitable[bool]], interval: Optional[float] = None) -> None:
        """后台周期探活。传入 check(proxy_state) -> bool。"""
        interval = interval if interval is not None else self.cfg.health_check_interval
        while True:
            await asyncio.sleep(interval)
            await self._probe(check)

    def snapshot(self) -> list[dict]:
        now = time.monotonic()
        return [
            {
                "proxy": s.label,
                "healthy": s.healthy,
                "available": s.available_now,
                "cooldown_left": max(0.0, s.cooldown_until - now),
                "success": s.success,
                "fail": s.fail,
                "rate_limited": s.rate_limited,
                "forbidden": s.forbidden,
                "errors": s.errors,
                "auth_failures": s.auth_failures,
                "latency_ema": round(s.latency_ema, 3),
            }
            for s in self.states
        ]
