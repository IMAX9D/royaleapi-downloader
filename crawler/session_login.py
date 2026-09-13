# 用途：打开登录浏览器，保存本地会话，并检查真实回放请求。
# 分类：会话、观测与诊断；使用：需要自己的浏览器环境与账号；可能需要人工操作
# 相关文件与阅读顺序：见同目录 README.md。

"""交互式创建独立 RoyaleAPI 登录会话；用户亲自完成 Google 登录。"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from urllib.parse import urlparse

from ruyipage.aio import Keys, launch

from .config import load_config

CF_DETECT_EXPRESSION = '''(
    /Just a moment|请稍候|请稍等|正在安全验证/i.test(document.title) ||
    /Performing\\s+security\\s+verification|Verif(?:y|ying)\\s+(?:that\\s+)?you\\s+are\\s+human|验证您是真人|验证您是人类/i.test(document.body?.innerText || '') ||
    Array.from(document.querySelectorAll('iframe')).some(frame => {
        try {
            const rect=frame.getBoundingClientRect();
            return new URL(frame.src,location.href).hostname==='challenges.cloudflare.com' && rect.width>0 && rect.height>0;
        } catch { return false; }
    })
)'''


async def _http_replay_probe(page, proxy, url, cfg):
    """Validate the exported browser session through the production HTTP transport.

    Avoid relying on a page-script fetch after navigating to a replay document.
    """
    from curl_cffi import requests
    from .curl_cffi_windows import install_curl_cffi_windows_workarounds
    install_curl_cffi_windows_workarounds()
    jar=requests.Cookies()
    for cookie in await asyncio.wait_for(page.get_cookies(all_info=True),15):
        domain=str(cookie.domain or '')
        if domain.lstrip('.').lower() not in ('royaleapi.com','www.royaleapi.com'):continue
        jar.set(cookie.name,cookie.value,domain=domain,path=cookie.path or '/',secure=bool(cookie.secure))
    agent=await asyncio.wait_for(page.get_user_agent(),10)
    async with requests.AsyncSession(impersonate='firefox147',cookies=jar,
            proxies={'http':proxy,'https':proxy},timeout=20,
            headers={'User-Agent':agent,'Accept':'application/json, text/plain, */*',
                     'Referer':cfg.referer,'X-Requested-With':'XMLHttpRequest'}) as client:
        response=await client.get(url)
        return {'status':response.status_code,'text':response.text,'headers':dict(response.headers)}


async def _visible_element(page, selector: str, timeout: float = 45):
    deadline = time.time() + timeout
    while time.time() < deadline:
        for element in await page.eles(selector, timeout=2):
            try:
                if await element.get_is_displayed():
                    return element
            except Exception:
                continue
        await page.wait(0.5)
    return None


def _sample_replay_url() -> str:
    path = Path("data/index.jsonl")
    for line in path.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        if row.get("kind") == "battle":
            return row["url"]
    raise RuntimeError("data/index.jsonl 中没有回放 URL")


async def login(name: str, proxy: str, timeout_minutes: int, auto_google: bool = False, replay_url: str | None = None, auto_cf: bool = False, reuse_only: bool = False) -> bool:
    from .storage import Storage
    diagnostic = bool(os.environ.get('CR_LOGIN_DIAGNOSTIC'))
    if diagnostic:
        import faulthandler
        faulthandler.dump_traceback_later(25,repeat=False)
    cfg = load_config("config.toml")
    root = Path("data/auth_sessions")
    root.mkdir(parents=True, exist_ok=True)
    profile = root / f"{name}-profile"
    output = root / f"{name}.json"
    status_path = root / f'{name}.login-status.json'
    cf_attempts = 0
    previous_phase = None
    last_replay_status = None
    last_replay_result = None
    http_forbidden_checks = 0
    def phase(value):
        nonlocal previous_phase
        Storage.atomic_text(status_path,json.dumps({'session':name,'phase':value,'previous_phase':previous_phase,'updated_at':time.time(),'pid':os.getpid(),'cf_attempts':cf_attempts,
            'last_replay_status':last_replay_status,'last_replay_result':last_replay_result}))
        previous_phase=value
    login_url = (
        cfg.base_url.rstrip("/")
        + "/login/google?r=https://royaleapi.com/player/PPPQQCJ02/battles"
    )
    replay_url = replay_url or _sample_replay_url()
    if urlparse(replay_url).netloc != urlparse(cfg.base_url).netloc or urlparse(replay_url).path != '/data/replay':
        raise ValueError('Replay check must use the configured RoyaleAPI endpoint')
    email_value = os.environ.pop('GOOGLE_LOGIN_EMAIL', None) if auto_google else None
    password_value = os.environ.pop('GOOGLE_LOGIN_PASSWORD', None) if auto_google else None
    if auto_google and (not email_value or not password_value):
        raise RuntimeError('Automatic login credentials were not provided in process memory')
    phase('opening_browser')
    page = await asyncio.wait_for(launch(
        proxy=proxy,
        user_dir=str(profile.resolve()),
        headless=False,
        close_on_exit=True,
        timeout_page_load=90,
        timeout_script=40,
    ),60)
    try:
        phase('browser_ready')
        if reuse_only and output.exists():
            rows=[]
            for row in json.loads(output.read_text(encoding='utf-8')).get('cookies',[]):
                if str(row.get('domain','')).lstrip('.').lower() not in ('royaleapi.com','www.royaleapi.com'):continue
                clean={k:row[k] for k in ('name','value','domain','path','secure','httpOnly') if k in row}
                if row.get('sameSite'):clean['sameSite']=row['sameSite'].lower()
                expiry=row.get('expiry',row.get('expires'))
                if isinstance(expiry,(int,float)) and expiry>0:clean['expiry']=expiry
                rows.append(clean)
            await asyncio.wait_for(page.set_cookies(rows),20)
            phase('restoring_saved_session')
        try:
            # The replay endpoint may have a different verification rule from
            # the homepage. Restore the session at the URL we actually need.
            await asyncio.wait_for(page.get(replay_url if reuse_only else login_url, wait="none", timeout=40),45)
        except asyncio.TimeoutError:
            phase('navigation_timeout')
        else:
            phase('login_page_loaded')
        email_sent = password_sent = False
        last_check = 0.0
        cf_attempts = 0
        last_cf = 0.0
        deadline = time.time() + timeout_minutes * 60
        js = """function(url){return fetch(url,{credentials:'include',headers:{
          'Accept':'application/json, text/plain, */*','X-Requested-With':'XMLHttpRequest'
        }}).then(async r=>({status:r.status,text:await r.text(),headers:Object.fromEntries(r.headers.entries())}));}"""
        while time.time() < deadline:
            await asyncio.sleep(3)
            phase('reading_location')
            current = await asyncio.wait_for(page.get_url(),15)
            parsed = urlparse(current)
            if parsed.hostname == 'accounts.google.com' and parsed.scheme == 'https':
                if reuse_only:
                    phase('google_login_required')
                    return False
                phase('google_login')
                if auto_google:
                    # Re-enter these steps after the user finishes a challenge;
                    # never exhaust a one-time wait while still on Cloudflare.
                    if not email_sent:
                        email = await _visible_element(page,"css:input[type='email']",timeout=1)
                        if email and urlparse(await page.get_url()).hostname == 'accounts.google.com':
                            await email.input(email_value,clear=True)
                            button = await page.ele('css:#identifierNext button',timeout=2)
                            if button: await button.click_self()
                            else: await page.actions.press(Keys.ENTER);await page.actions.perform()
                            email_sent=True
                            continue
                        account=await _visible_element(page,f'css:[data-identifier="{email_value}" i], [data-email="{email_value}" i]',timeout=1)
                        if account:
                            await account.click_self();email_sent=True;continue
                    if not password_sent:
                        password=await _visible_element(page,"css:input[type='password']",timeout=1)
                        if password and urlparse(await page.get_url()).hostname == 'accounts.google.com':
                            await password.input(password_value,clear=True)
                            button=await page.ele('css:#passwordNext button',timeout=2)
                            if button: await button.click_self()
                            else: await page.actions.press(Keys.ENTER);await page.actions.perform()
                            password_sent=True
                    else:
                        phase('waiting_google_confirmation')
                continue
            if parsed.hostname not in ("royaleapi.com", "www.royaleapi.com"):
                phase('waiting_login_navigation')
                continue
            try:
                phase('checking_cloudflare')
                challenge = await asyncio.wait_for(page.run_js(CF_DETECT_EXPRESSION,as_expr=True,timeout=10),12)
            except Exception as exc:
                phase('challenge_check_failed_'+type(exc).__name__)
                print('CHALLENGE_CHECK_ERROR '+type(exc).__name__+': '+str(exc)[:240],flush=True)
                continue
            if challenge:
                http_forbidden_checks = 0
                if auto_cf and cf_attempts < 3 and time.monotonic()-last_cf >= 30:
                    from .cf_recover import try_challenge_once
                    cf_attempts += 1
                    last_cf = time.monotonic()
                    phase('recovering_cloudflare')
                    try:
                        await asyncio.wait_for(try_challenge_once(page),30)
                    except Exception:
                        phase('waiting_human_verification')
                    continue
                if auto_cf and cf_attempts >= 3:
                    phase('waiting_human_verification')
                    continue
                phase('waiting_human_verification')
                continue
            if time.time()-last_check<15:
                continue
            last_check=time.time()
            phase('verifying_replay')
            try:
                if reuse_only:
                    result = await asyncio.wait_for(_http_replay_probe(page,proxy,replay_url,cfg),45)
                else:
                    result = await asyncio.wait_for(page.run_js(js, replay_url, timeout=40),45)
                last_replay_status = int(result.get('status',0))
                if last_replay_status == 429:
                    from .crawler import Crawler
                    last_replay_result='rate_limited'
                    phase('rate_limited')
                    retry_after=Crawler._parse_retry_after(result.get('headers',{}).get('retry-after'))
                    await asyncio.sleep(min(max(120,retry_after or 0),max(0,deadline-time.time())))
                    continue
                if last_replay_status == 403:
                    http_forbidden_checks+=1
                    last_replay_result='http_forbidden'
                    phase('http_verification_failed' if http_forbidden_checks>=2 else 'replay_http_forbidden')
                    if http_forbidden_checks>=2:return False
                    continue
                payload = json.loads(result.get("text", "{}"))
                last_replay_result = 'success' if isinstance(payload,dict) and payload.get('success') is True else 'unsuccessful_response'
            except Exception as exc:
                last_replay_result = type(exc).__name__
                phase('replay_request_failed')
                continue
            if int(result.get("status", 0)) != 200 or payload.get("success") is not True:
                if reuse_only and 'requires login' in str(payload.get('html','')).lower():
                    phase('saved_session_expired')
                    return False
                continue
            cookies = await page.get_cookies(all_info=True)
            saved = []
            for cookie in cookies:
                domain = str(cookie.domain or "")
                if "royaleapi.com" not in domain:
                    continue
                saved.append({
                    "name": cookie.name,
                    "value": cookie.value,
                    "domain": domain,
                    "path": cookie.path or "/",
                    "httpOnly": bool(cookie.http_only),
                    "secure": bool(cookie.secure),
                    "sameSite": cookie.same_site,
                    "expiry": cookie.expiry,
                })
            from .storage import Storage
            user_agent = await asyncio.wait_for(page.get_user_agent(),10)
            Storage.atomic_text(output,json.dumps({"name":name,"cookies":saved,
                'http_impersonate':'firefox147','http_user_agent':user_agent},ensure_ascii=False))
            phase('ready')
            print(f"SESSION_READY name={name} cookies={len(saved)}", flush=True)
            return True
        print(f"SESSION_TIMEOUT name={name}", flush=True)
        phase('timeout')
        return False
    except Exception as exc:
        phase('failed_'+type(exc).__name__)
        raise
    finally:
        if diagnostic:
            faulthandler.cancel_dump_traceback_later()
        try:
            await asyncio.wait_for(page.quit(force=True),10)
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--proxy", default="http://127.0.0.1:7897")
    parser.add_argument("--timeout-minutes", type=int, default=30)
    parser.add_argument("--auto-google", action="store_true")
    parser.add_argument("--replay-url", help="A current replay URL for login validation")
    parser.add_argument('--auto-cf',action='store_true',help='Use the existing bounded CF recovery strategy; success still requires replay verification')
    parser.add_argument('--reuse-only',action='store_true',help='Refresh CF around the existing site session without starting a Google login')
    args = parser.parse_args()
    try:
        return 0 if asyncio.run(login(
            args.name, args.proxy, args.timeout_minutes, args.auto_google, args.replay_url, args.auto_cf, args.reuse_only
        )) else 1
    except Exception as exc:
        # Credentials or DOM contents must not be interpolated into failure logs.
        print(f'SESSION_FAILED name={args.name} type={type(exc).__name__}',flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
