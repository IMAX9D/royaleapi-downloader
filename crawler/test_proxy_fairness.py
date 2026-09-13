import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from .config import ProxyPoolConfig, RateLimitConfig
from .proxy_pool import ProxyPool


class SharedExitFairnessTests(unittest.IsolatedAsyncioTestCase):
    def pool(self):
        pool=ProxyPool(ProxyPoolConfig(proxies=['a','b','c']),RateLimitConfig(requests_per_second=1,burst=1,jitter=0))
        for s in pool.states:s.bucket=pool.states[0].bucket
        return pool

    async def test_every_session_gets_equal_turns_without_extra_tokens(self):
        pool=self.pool();bucket=pool.states[0].bucket
        with patch('crawler.ratelimit.time.monotonic',return_value=bucket._updated) as clock:
            assigned=[]
            for _ in range(9):
                assigned.append((await pool.acquire()).url)
                self.assertFalse(await bucket.try_acquire())
                clock.return_value+=1
        self.assertEqual(assigned,['a','b','c']*3)

    async def test_disabled_session_is_skipped_and_shared_cooldown_remains(self):
        pool=self.pool();pool.states[1].enabled=False
        self.assertEqual((await pool.pick()).url,'a')
        self.assertEqual((await pool.pick()).url,'c')
        pool.report_rate_limited(pool.states[0],120)
        self.assertFalse(any(s.available_now for s in pool.states))

    async def test_restart_keeps_counters_and_remaining_group_cooldown(self):
        from .expert_continuous import restore_proxy_progress
        pool=self.pool()
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'snapshot.json'
            path.write_text(json.dumps({'updated_at':100,'proxies':[{'proxy':'a','success':12,'fail':1,'cooldown_left':120}]}))
            with patch('crawler.expert_continuous.time.time',return_value=130),patch('crawler.proxy_pool.time.monotonic',return_value=10):
                restore_proxy_progress(pool,path)
                self.assertTrue(all(s.cooldown_until==100 for s in pool.states))
                self.assertEqual(pool.states[0].success,12)
                self.assertEqual(pool.states[1].success,0)


if __name__=='__main__':unittest.main()
