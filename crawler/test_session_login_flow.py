# 用途：模拟浏览器中的验证、登录和回放检查流程。
# 分类：离线测试；使用：测试使用合成数据或模拟对象，不参与生产下载。
# 相关文件与阅读顺序：见同目录 README.md。

"""Human challenge first, then authorized login: synthetic browser regression."""
import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch


@unittest.skipUnless(importlib.util.find_spec('ruyipage'), 'Optional browser runtime unavailable')
class LoginFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_login_steps_remain_available_after_human_challenge(self):
        await self._exercise_flow(False)

    async def test_existing_cf_handler_is_connected_before_google_login(self):
        await self._exercise_flow(True)

    async def _exercise_flow(self, auto_cf):
        from . import session_login
        class Page:
            stage='challenge'
            waits=0
            inputs=[]
            async def get(self,*args,**kwargs):pass
            async def wait(self,seconds):
                self.waits+=1
                if self.stage=='challenge' and self.waits>1:self.stage='email'
            async def get_url(self):
                return 'https://accounts.google.com/signin' if self.stage in ('email','password') else 'https://royaleapi.com/'
            async def run_js(self,script,*args,**kwargs):
                if 'document.title' in script:return self.stage=='challenge'
                return {'status':200,'text':json.dumps({'success':True})}
            async def ele(self,selector,**kwargs):
                page=self
                class Button:
                    async def click_self(self):
                        page.stage='password' if page.stage=='email' else 'finished'
                return Button()
            async def get_cookies(self,**kwargs):
                return [SimpleNamespace(domain='royaleapi.com',name='session',value='synthetic-value',path='/',http_only=True,secure=True,same_site='lax',expiry=None)]
            async def get_user_agent(self):return 'Mozilla/5.0 Firefox/155.0'
            async def quit(self,**kwargs):pass
        page=Page()
        async def visible(page,selector,**kwargs):
            wanted='email' if "type='email'" in selector else 'password' if "type='password'" in selector else None
            if page.stage!=wanted:return None
            class Input:
                async def input(self,value,**kwargs):
                    page.inputs.append((page.stage,value))
            return Input()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            with patch.object(session_login,'Path',side_effect=lambda value:root/value), \
                 patch.object(session_login,'load_config',return_value=SimpleNamespace(base_url='https://royaleapi.com')), \
                 patch.object(session_login,'launch',AsyncMock(return_value=page)), \
                 patch.object(session_login,'_visible_element',side_effect=visible), \
                 patch.object(session_login.asyncio,'sleep',side_effect=page.wait), \
                 patch('crawler.cf_recover.try_challenge_once',AsyncMock(return_value=True)) as cf, \
                 patch.dict(os.environ,{'GOOGLE_LOGIN_EMAIL':'synthetic@example.test','GOOGLE_LOGIN_PASSWORD':'synthetic-only'}):
                ok=await session_login.login('test','http://local.test',1,True,'https://royaleapi.com/data/replay?tag=TEST',auto_cf)
                if auto_cf:cf.assert_awaited_once_with(page)
                else:cf.assert_not_awaited()
            self.assertTrue(ok)
            self.assertEqual(page.inputs,[('email','synthetic@example.test'),('password','synthetic-only')])
            self.assertEqual(json.loads((root/'data/auth_sessions/test.login-status.json').read_text())['phase'],'ready')


if __name__=='__main__':unittest.main()
