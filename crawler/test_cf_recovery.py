import importlib.util
import unittest
import shutil,json,subprocess
from unittest.mock import AsyncMock


@unittest.skipUnless(importlib.util.find_spec('ruyipage'),'Optional browser runtime unavailable')
class CFRecoveryTests(unittest.IsolatedAsyncioTestCase):
    @unittest.skipUnless(shutil.which('node'),'JavaScript parser unavailable')
    async def test_real_javascript_parser_accepts_recovery_scripts(self):
        from .session_login import CF_DETECT_EXPRESSION
        from .cf_recover import FRAME_RECT_SCRIPT
        result=subprocess.run([shutil.which('node'),'-e',
            "const d=JSON.parse(require('fs').readFileSync(0,'utf8'));new Function('return '+d.expression);new Function(d.body);"],
            input=json.dumps({'expression':CF_DETECT_EXPRESSION,'body':FRAME_RECT_SCRIPT}),text=True,capture_output=True,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr)

    async def test_foreign_page_is_rejected_before_input(self):
        from .cf_recover import try_challenge_once
        page=AsyncMock();page.get_url.return_value='https://other.test/'
        with self.assertRaises(ValueError):await try_challenge_once(page)
        page.actions.click.assert_not_awaited()

    async def test_only_expected_challenge_frame_receives_existing_click(self):
        from .cf_recover import try_challenge_once
        page=AsyncMock();page.get_url.return_value='https://royaleapi.com/login/google'
        bad=AsyncMock();bad.get_src.return_value='https://cloudflare.other.test/frame'
        good=AsyncMock();good.get_src.return_value='https://challenges.cloudflare.com/turnstile'
        good.run_js.return_value={'x':100,'y':200,'w':300,'h':60}
        page.eles.return_value=[bad,good]
        self.assertTrue(await try_challenge_once(page))
        bad.run_js.assert_not_awaited()
        page.actions.move_to.assert_awaited_once_with((135,230),duration=450)
        page.actions.perform.assert_awaited_once()


if __name__=='__main__':unittest.main()
