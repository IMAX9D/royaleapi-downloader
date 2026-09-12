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


async def login(name: str, proxy: str, timeout_minutes: int, auto_google: bool = False) -> bool:
    cfg = load_config("config.toml")
    root = Path("data/auth_sessions")
    root.mkdir(parents=True, exist_ok=True)
    profile = root / f"{name}-profile"
    output = root / f"{name}.json"
    login_url = (
        cfg.base_url.rstrip("/")
        + "/login/google?r=https://royaleapi.com/player/PPPQQCJ02/battles"
    )
    replay_url = _sample_replay_url()
    page = await launch(
        proxy=proxy,
        user_dir=str(profile.resolve()),
        headless=False,
        close_on_exit=True,
        timeout_page_load=90,
        timeout_script=40,
    )
    try:
        await page.get(login_url, wait="none", timeout=90)
        if auto_google:
            email_value = os.environ.get("GOOGLE_LOGIN_EMAIL")
            password_value = os.environ.get("GOOGLE_LOGIN_PASSWORD")
            if not email_value or not password_value:
                raise RuntimeError("自动登录需要临时 GOOGLE_LOGIN_EMAIL/GOOGLE_LOGIN_PASSWORD")
            email = await _visible_element(page, "css:input[type='email']", timeout=45)
            if email:
                await email.input(email_value, clear=True)
                next_button = await page.ele("css:#identifierNext button", timeout=5)
                if next_button:
                    await next_button.click_self()
                else:
                    await page.actions.press(Keys.ENTER)
                    await page.actions.perform()
                await page.wait(3)
            password = await _visible_element(page, "css:input[type='password']", timeout=45)
            if password:
                await password.input(password_value, clear=True)
                next_button = await page.ele("css:#passwordNext button", timeout=5)
                if next_button:
                    await next_button.click_self()
                else:
                    await page.actions.press(Keys.ENTER)
                    await page.actions.perform()
        deadline = time.time() + timeout_minutes * 60
        js = """function(url){return fetch(url,{credentials:'include',headers:{
          'Accept':'application/json, text/plain, */*','X-Requested-With':'XMLHttpRequest'
        }}).then(async r=>({status:r.status,text:await r.text()}));}"""
        while time.time() < deadline:
            await page.wait(3)
            current = await page.get_url()
            if urlparse(current).hostname not in ("royaleapi.com", "www.royaleapi.com"):
                continue
            try:
                result = await page.run_js(js, replay_url, timeout=40)
                payload = json.loads(result.get("text", "{}"))
            except Exception:
                continue
            if int(result.get("status", 0)) != 200 or payload.get("success") is not True:
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
            output.write_text(
                json.dumps({"name": name, "cookies": saved}, ensure_ascii=False),
                encoding="utf-8",
            )
            print(f"SESSION_READY name={name} cookies={len(saved)}", flush=True)
            return True
        print(f"SESSION_TIMEOUT name={name}", flush=True)
        return False
    finally:
        await page.quit(force=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--proxy", default="http://127.0.0.1:7897")
    parser.add_argument("--timeout-minutes", type=int, default=30)
    parser.add_argument("--auto-google", action="store_true")
    args = parser.parse_args()
    return 0 if asyncio.run(login(
        args.name, args.proxy, args.timeout_minutes, args.auto_google
    )) else 1


if __name__ == "__main__":
    raise SystemExit(main())
