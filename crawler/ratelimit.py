# 用途：异步令牌桶，控制请求节奏。
# 分类：下载核心；使用：内部
# 相关文件与阅读顺序：见同目录 README.md。

"""异步令牌桶：对单一 IP 做平滑限速。

为什么需要它：被封 IP 的根因不是"并发数"而是"单位时间请求密度"。
令牌桶把瞬时并发压缩成恒定速率，并允许少量突发（burst），
从而在给定时间内不超出发送节奏上限。
"""
from __future__ import annotations

import asyncio
import random
import time


class TokenBucket:
    def __init__(self, rate: float, burst: int, jitter: float = 0.0):
        assert rate > 0, "rate 必须大于 0"
        assert burst >= 1, "burst 至少为 1"
        self.rate = float(rate)
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.jitter = float(jitter)
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._updated
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self._updated = now

    @property
    def available(self) -> float:
        """当前可用令牌数（仅作"选最空闲代理"的启发式排序用，非严格线程安全）。"""
        # 代理池会用此值选择最空闲出口；若不先 refill，消费过一次的 lane
        # 会永久显示为 0，导致 max() 在全 0 时持续偏向第一个代理。
        self._refill()
        return self.tokens

    @property
    def wait_seconds(self) -> float:
        """Best-effort delay until one token is available."""
        self._refill()
        return max(0.0, (1.0 - self.tokens) / self.rate)

    async def try_acquire(self) -> bool:
        """Atomically reserve one token without sleeping."""
        async with self._lock:
            self._refill()
            if self.tokens < 1.0:
                return False
            self.tokens -= 1.0
            return True

    async def acquire(self) -> None:
        """阻塞直到取得一个令牌。"""
        while True:
            async with self._lock:
                self._refill()
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                need = 1.0 - self.tokens
                wait = need / self.rate
            # 锁外休眠，允许其它协程并发排队；加随机抖动模拟人工节奏
            if self.jitter > 0:
                wait += random.uniform(0.0, self.jitter)
            await asyncio.sleep(wait)
