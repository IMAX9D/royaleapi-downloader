# 用途：恢复后的状态发布、Cookie 更新与429冷却保留。
# 分类：离线测试；使用：测试使用合成数据或模拟对象，不参与生产下载。
# 相关文件与阅读顺序：见同目录 README.md。

import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock,Mock

from .proxy_pool import ProxyState
from .ratelimit import TokenBucket
from .session_maintenance import SessionMaintenance


class SessionMaintenanceTests(unittest.IsolatedAsyncioTestCase):
    def setup_maintenance(self):
        state=ProxyState('http://local',TokenBucket(.15,1),cooldown_until=9999999)
        http=SimpleNamespace(_cookie_files={state.url:Path('session-02.json')},_reload_cookies=Mock())
        crawler=SimpleNamespace(_stop=asyncio.Event(),pool=SimpleNamespace(states=[state]),fetcher=http)
        return SessionMaintenance(crawler),state,http

    async def test_recovery_deduplicates_and_ignores_list_state(self):
        maintenance,state,http=self.setup_maintenance()
        maintenance._recover=AsyncMock()
        maintenance.schedule(ProxyState('http://other',TokenBucket(.15,1)),'challenge')
        self.assertEqual(len(maintenance._jobs),0)
        maintenance.schedule(state,'challenge');maintenance.schedule(state,'challenge')
        self.assertEqual(len(maintenance._jobs),1)
        self.assertEqual(maintenance._recover.call_count,1)
        await maintenance.aclose()

    async def test_success_clears_challenge_wait_but_preserves_new_429(self):
        maintenance,state,http=self.setup_maintenance()
        old=SimpleNamespace(close=AsyncMock());http._sessions={state.url:old}
        await maintenance._publish_ready(state,'session-02',http,0)
        self.assertEqual(state.cooldown_until,0)
        old.close.assert_awaited_once()
        self.assertNotIn(state.url,http._sessions)
        state.cooldown_until=9999999;state.rate_limited=1
        await maintenance._publish_ready(state,'session-02',http,0)
        self.assertEqual(state.cooldown_until,9999999)
        self.assertEqual(maintenance.succeeded,2)

    async def test_recovery_preserves_existing_rate_limit_deadline(self):
        maintenance,state,http=self.setup_maintenance()
        state.rate_limit_until=12345678
        await maintenance._publish_ready(state,'session-02',http,state.rate_limited)
        self.assertEqual(state.cooldown_until,12345678)


if __name__=='__main__':unittest.main()
