"""Playwright 抓包脚本：在你本机通过 Cloudflare，抓出"二次点击"的真实接口与 cookie。

这是"混合方案"的第一步（只需跑一次），产出供 curl_cffi 批量爬虫使用：

产出（默认 captured/ 目录）：
- captured.json     cookie + 请求头 + 抓到的 JSON 接口清单（可直接设为 config 的 captured_file）
- bodies/*.json     每个 JSON 响应体（用来确认"列表"与"详情"的字段结构）

用法：
    python -m crawler.capture --url https://royaleapi.com/player/2P0LYQ/battles
    python -m crawler.capture --url https://royaleapi.com/player/2P0LYQ/battles --click --headless

前置：pip install playwright && python -m playwright install chromium
提示：首次跑建议不加 --headless，Cloudflare 挑战若未自动通过可手动点一下。
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

CHALLENGE_MARKERS = ("Just a moment", "Verify you are human", "challenge-platform")


def _fix_stdio() -> None:
    for s in (sys.stdout, sys.stderr):
        try:
            s.reconfigure(errors="replace")
        except Exception:
            pass


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RoyaleAPI 抓包：过 CF + 记录 JSON 接口 + 导出 cookie")
    p.add_argument("--url", required=True, help="目标 URL，如 https://royaleapi.com/player/2P0LYQ/battles")
    p.add_argument("--click", action="store_true", help="自动点击第一场对局，触发'二次点击'详情接口")
    p.add_argument("--headless", action="store_true", help="无头模式（可能过不了 CF，默认有头）")
    p.add_argument("--output", default="captured", help="输出目录（默认 captured）")
    p.add_argument("--wait", type=float, default=4.0, help="页面渲染后等待秒数，让 XHR 发完")
    p.add_argument("--timeout", type=float, default=120.0, help="等待 Cloudflare 通过的最长秒数")
    p.add_argument("--domain", default="royaleapi.com", help="导出 cookie 的域名")
    return p.parse_args(argv)


def _wait_cloudflare(page, timeout: float) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        title = page.title()
        content = ""
        try:
            content = page.content()
        except Exception:
            pass
        if not any(m in title or m in content for m in CHALLENGE_MARKERS):
            print("[ok] Cloudflare 已通过。")
            return
        print(f"[wait] Cloudflare 挑战中…({int(time.time() - t0)}s) 若卡住请在有头窗口里手动勾选/点击")
        time.sleep(3)
    print("[warn] 等待 Cloudflare 超时，可能仍在挑战页；若需人工处理请在窗口内完成。")


def _click_first_battle(page) -> bool:
    selectors = [
        'a[href*="battle"]',
        '[data-battle-id]',
        '[data-battle-time]',
        'tr.clickable',
        '.battle-item',
        '.battles tbody tr',
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.click()
                print(f"[click] 点击了第一场对局（selector={sel}）")
                return True
        except Exception as e:  # noqa: BLE001
            print(f"[click] selector={sel} 失败: {e}")
    print("[warn] 未找到对局入口，请手动在窗口里点一场对局，接口会自动被抓到。")
    return False


def _is_json_response(resp) -> bool:
    ctype = (resp.headers.get("content-type") or "").lower()
    if "json" in ctype:
        return True
    url = resp.url
    return bool(re.search(r"(/battle|/battles|/api/|battlelog)", url))


def _sanitize(name: str, limit: int = 120) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)[:limit]


def main(argv=None) -> int:
    _fix_stdio()
    args = parse_args(argv)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("缺少 playwright：pip install playwright && python -m playwright install chromium")
        return 1

    out = Path(args.output)
    bodies = out / "bodies"
    bodies.mkdir(parents=True, exist_ok=True)

    captured: list[dict] = []
    seen: set[str] = set()
    sample_headers: dict = {}

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=args.headless)
        except Exception as e:  # noqa: BLE001
            print(f"启动浏览器失败（可能未装内核）: {e}\n请执行: python -m playwright install chromium")
            return 1

        context = browser.new_context(viewport={"width": 1366, "height": 900})
        page = context.new_page()

        def on_response(resp) -> None:
            nonlocal sample_headers
            if not _is_json_response(resp):
                return
            url = resp.url
            if url in seen:
                return
            seen.add(url)
            entry = {"method": resp.request.method, "url": url, "status": resp.status}
            try:
                entry["request_headers"] = dict(resp.request.headers)
                if not sample_headers:
                    sample_headers = dict(resp.request.headers)
                body = resp.text()
                entry["body_bytes"] = len(body)
                try:
                    data = json.loads(body)
                    fname = f"{resp.status}_{_sanitize(url)}_{len(seen)}.json"
                    (bodies / fname).write_text(
                        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
                    )
                    entry["saved_to"] = str(bodies / fname)
                    entry["top_keys"] = list(data.keys())[:20] if isinstance(data, dict) else f"list[{len(data)}]"
                except Exception:
                    entry["saved_to"] = None
            except Exception:  # noqa: BLE001
                pass
            captured.append(entry)
            print(f"[captured] {entry['method']} {resp.status} {url}")

        page.on("response", on_response)

        print(f"打开 {args.url} ...")
        page.goto(args.url, wait_until="domcontentloaded", timeout=60000)
        _wait_cloudflare(page, args.timeout)
        time.sleep(args.wait)

        if args.click:
            _click_first_battle(page)
            time.sleep(args.wait)

        cookies = context.cookies(args.domain)
        result = {
            "domain": args.domain,
            "captured_at": time.time(),
            "source_url": args.url,
            "cookies": {c["name"]: c["value"] for c in cookies},
            "headers": sample_headers,
            "responses": captured,
            "note": (
                "把本文件路径填到 config.toml 的 captured_file；"
                "再根据下面 responses 里的真实 URL 改 list_endpoint_template / detail_endpoint_template。"
            ),
        }
        outfile = out / "captured.json"
        outfile.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        browser.close()

    print(f"\n已导出: {outfile}  （共 {len(captured)} 个 JSON 接口，响应体在 {bodies}/）")
    _print_hints(captured)
    return 0


def _print_hints(responses: list[dict]) -> None:
    if not responses:
        print("\n没有抓到 JSON 接口，可能是 CF 未过或页面尚未加载完，建议加 --wait 或去掉 --headless 重试。")
        return
    print("\n=== 接口线索（用于填 config.toml）===")
    for r in responses:
        keys = r.get("top_keys", "")
        print(f"  [{r['method']}] {r['status']} {r['url']}")
        if keys:
            print(f"        字段: {keys}")
    print("\n提示：通常一个是'列表'（含多场对局/数组），一个是'详情'（单场对局）。"
          "\n把列表 URL 里变动的 tag 换成 {tag}，详情 URL 里变动的 id 换成 {id}，填入两个端点模板。")


if __name__ == "__main__":
    raise SystemExit(main())
