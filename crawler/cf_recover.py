"""单 lane Cloudflare 后台恢复器：有头 RuyiPage 自动点击并验收。"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path

from ruyipage.aio import launch

from .config import load_config


async def recover(port: int) -> bool:
    cfg = load_config("config.toml")
    proxy = f"http://127.0.0.1:{port}"
    label = hashlib.sha256(proxy.encode("utf-8")).hexdigest()[:16]
    profile = Path(cfg.browser_profile_dir).parent / "ruyi_profiles" / label
    if cfg.ruyi_auth_map_file:
        try:
            mapping = json.loads(Path(cfg.ruyi_auth_map_file).read_text(encoding="utf-8"))
            if proxy in mapping:
                profile = Path(mapping[proxy])
        except Exception:
            pass
    page = await launch(
        proxy=proxy,
        user_dir=str(profile),
        headless=False,
        close_on_exit=True,
        timeout_page_load=60,
        timeout_script=40,
    )
    try:
        await page.get(cfg.base_url, wait="none", timeout=60)
        await page.wait(4)
        test_url = cfg.list_endpoint_template.format(
            base=cfg.base_url.rstrip("/"), tag="PPPQQCJ02"
        )
        js = """function(url){return fetch(url,{credentials:'include'}).then(
            async r=>({status:r.status,text:await r.text()}));}"""
        for _ in range(3):
            frames = await page.eles("css:iframe")
            clicked = False
            for frame in frames:
                src = await frame.get_src()
                if not src or not any(x in src for x in ("cloudflare", "turnstile", "cf-chl")):
                    continue
                rect = await frame.run_js(
                    "function(){const r=this.getBoundingClientRect();return {x:r.left,y:r.top,w:r.width,h:r.height};}"
                )
                if not rect or rect.get("w", 0) <= 0 or rect.get("h", 0) <= 0:
                    continue
                # checkbox 位于 iframe 左侧；使用父页面视口绝对坐标点击。
                x = int(rect["x"] + min(35, max(5, rect["w"] / 4)))
                y = int(rect["y"] + rect["h"] / 2)
                await page.actions.move_to((x, y), duration=450)
                await page.actions.click()
                await page.actions.perform()
                clicked = True
                break
            if not clicked:
                # 框架原生方法作为兜底，但缩短等待，避免后台恢复长期占用。
                await page.handle_cloudflare_challenge(timeout=20, check_interval=2)
            await page.wait(8)
            result = await page.run_js(js, test_url, timeout=40)
            if int(result.get("status", 0)) == 200 and "replay_button" in result.get("text", ""):
                return True
        return False
    finally:
        await page.quit(force=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    port = parser.parse_args(argv).port
    return 0 if asyncio.run(recover(port)) else 1


if __name__ == "__main__":
    raise SystemExit(main())
