"""HTTP 客户端：传输无关的 Fetcher 抽象 + curl_cffi 实现。

为什么用 curl_cffi：它能伪装真实浏览器的 TLS/JA3/JA4/HTTP2 指纹，
配合 capture.py 导出的 cf_clearance cookie，可在不触发 Cloudflare 的情况下批量请求。

设计：
- Fetcher 是抽象接口，crawler 只依赖 FetchResult，便于用 MockFetcher 做无网络自检。
- CurlCffiFetcher 为每个代理维护一个 AsyncSession（复用连接、独立 cookie/指纹）。
- 所有网络异常统一包装成 FetcherError，供重试逻辑识别。
"""
from __future__ import annotations

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Mapping, Optional

from .config import CrawlConfig

log = logging.getLogger("crawler.client")


class FetcherError(Exception):
    """网络层错误（超时/连接失败/传输异常），用于触发重试。"""


class FetchResult:
    __slots__ = ("url", "status_code", "text", "headers")

    def __init__(self, url: str, status_code: int, text: str, headers: Mapping[str, str]):
        self.url = url
        self.status_code = status_code
        self.text = text
        # 统一小写，方便无差别访问 retry-after 等头
        self.headers = {k.lower(): v for k, v in dict(headers).items()}

    def json(self):
        text = self.text.strip()
        if text.startswith("<"):
            # FlareSolverr 等浏览器方案对 JSON 端点可能返回 <pre>{...}</pre> 包裹
            import re
            m = re.search(r"<pre[^>]*>(.*?)</pre>", text, re.S)
            if m:
                text = m.group(1).strip()
            else:
                text = re.sub(r"<[^>]+>", "", text).strip()
        return json.loads(text)

    def __repr__(self) -> str:
        return f"FetchResult({self.status_code}, {len(self.text)} bytes)"


class Fetcher(ABC):
    @abstractmethod
    async def fetch(self, proxy_url: Optional[str], url: str) -> FetchResult: ...

    @abstractmethod
    async def check(self, proxy_url: Optional[str], url: str) -> bool: ...

    @abstractmethod
    async def aclose(self) -> None: ...


class CurlCffiFetcher(Fetcher):
    def __init__(self, cfg: CrawlConfig):
        from .curl_cffi_windows import install_curl_cffi_windows_workarounds
        install_curl_cffi_windows_workarounds()
        try:
            from curl_cffi import requests as curl_requests
        except ImportError as e:
            raise ImportError("缺少 curl_cffi，请执行: python -m pip install curl_cffi") from e
        self._cr = curl_requests
        self.cfg = cfg
        self._sessions: dict[Optional[str], object] = {}
        self._headers = {
            "User-Agent": cfg.user_agent,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": cfg.referer,
            "X-Requested-With": "XMLHttpRequest",
        }
        self._cookies = self._load_captured(cfg)

    def _load_captured(self, cfg: CrawlConfig) -> dict[str, str]:
        """从 capture.py 导出的 JSON 读取 cookie 与 headers。"""
        if not cfg.captured_file:
            return {}
        p = Path(cfg.captured_file)
        if not p.exists():
            log.warning("captured_file 不存在: %s", p)
            return {}
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:  # noqa: BLE001
            log.warning("解析 captured_file 失败: %s", e)
            return {}
        cookies = data.get("cookies", {})
        if isinstance(cookies, list):  # playwright 导出的是 cookie 对象列表
            cookies = {c.get("name"): c.get("value") for c in cookies if c.get("name")}
        extra_headers = data.get("headers", {})
        if isinstance(extra_headers, dict):
            for k, v in extra_headers.items():
                if v:
                    self._headers[k] = v
        log.info("已加载 captured cookie %d 个", len(cookies))
        return cookies

    def _session_for(self, proxy_url: Optional[str]):
        if proxy_url not in self._sessions:
            proxies = None
            if proxy_url:
                proxies = {"http": proxy_url, "https": proxy_url}
            self._sessions[proxy_url] = self._cr.AsyncSession(
                impersonate=self.cfg.impersonate,
                headers=self._headers,
                cookies=self._cookies,
                proxies=proxies,
                timeout=self.cfg.request_timeout,
            )
        return self._sessions[proxy_url]

    async def fetch(self, proxy_url: Optional[str], url: str) -> FetchResult:
        session = self._session_for(proxy_url)
        try:
            resp = await session.get(url, allow_redirects=True)
        except Exception as e:  # curl_cffi 的超时/连接/传输异常
            raise FetcherError(f"{type(e).__name__}: {e}") from e
        try:
            headers = dict(resp.headers)
        except Exception:  # noqa: BLE001
            headers = {}
        return FetchResult(str(resp.url), resp.status_code, resp.text, headers)

    async def check(self, proxy_url: Optional[str], url: str) -> bool:
        try:
            r = await self.fetch(proxy_url, url)
            return r.status_code < 500
        except FetcherError:
            return False

    async def aclose(self) -> None:
        for s in self._sessions.values():
            try:
                await s.close()
            except Exception:  # noqa: BLE001
                pass
        self._sessions.clear()


class PatchrightFetcher(Fetcher):
    """用 patchright（反检测 Chromium）作为后端：真实浏览器过 Cloudflare + 登录态。

    用持久化用户目录（browser_profile_dir）保存登录态，登录一次后长期复用。
    请求通过页面内 fetch 发出，自动携带 cf_clearance 与 royaleapi 登录 cookie。
    """

    def __init__(self, cfg: CrawlConfig):
        self.cfg = cfg
        self._pw = None
        self._pw_lock = asyncio.Lock()
        self._states_lock = asyncio.Lock()
        self._states: dict[Optional[str], dict] = {}

    async def _playwright(self):
        async with self._pw_lock:
            if self._pw is None:
                from patchright.async_api import async_playwright
                self._pw = await async_playwright().start()
        return self._pw

    def _profile_dir(self, proxy_url: Optional[str]) -> str:
        """直连沿用原 profile；每个代理使用独立 profile，避免 cookie/IP 串线。"""
        if not proxy_url:
            return self.cfg.browser_profile_dir
        import hashlib
        root = Path(self.cfg.browser_profile_dir)
        label = hashlib.sha256(proxy_url.encode("utf-8")).hexdigest()[:16]
        return str(root.parent / f"{root.name}_lanes" / label)

    async def _ensure(self, proxy_url: Optional[str]):
        async with self._states_lock:
            state = self._states.get(proxy_url)
            if state is None:
                state = {
                    "context": None,
                    "page": None,
                    "init_lock": asyncio.Lock(),
                    "request_lock": asyncio.Lock(),
                    "last_request_at": 0.0,
                }
                self._states[proxy_url] = state

        async with state["init_lock"]:
            if state["page"] is None:
                pw = await self._playwright()
                kwargs = {
                    "user_data_dir": self._profile_dir(proxy_url),
                    "headless": self.cfg.browser_headless,
                    "viewport": {"width": 1366, "height": 900},
                    "args": ["--disable-blink-features=AutomationControlled"],
                }
                if proxy_url:
                    kwargs["proxy"] = {"server": proxy_url}
                context = await pw.chromium.launch_persistent_context(**kwargs)
                pages = context.pages
                state["context"] = context
                state["page"] = pages[0] if pages else await context.new_page()
                await self._warm_up(state["page"])
        return state

    async def _warm_up(self, page) -> None:
        """页面导航到首页，触发 Cloudflare 挑战自动重新解决（刷新 cf_clearance）。"""
        try:
            await page.goto(self.cfg.base_url, wait_until="domcontentloaded", timeout=60000)
            for _ in range(30):
                try:
                    content = await page.content()
                except Exception:
                    content = ""
                head = content[:4000]
                if not any(m in head for m in ("Just a moment", "请稍候", "正在安全验证")):
                    return
                # Turnstile iframe 内部尺寸偶尔不可读；从父页面点击 iframe 左侧 checkbox。
                try:
                    iframe = page.locator(
                        'iframe[src*="challenges.cloudflare.com"], iframe[src*="turnstile"]'
                    ).first
                    box = await iframe.bounding_box(timeout=1000)
                    if box and box["width"] > 0 and box["height"] > 0:
                        await page.mouse.click(
                            box["x"] + min(35, box["width"] / 4),
                            box["y"] + box["height"] / 2,
                        )
                        await asyncio.sleep(3)
                except Exception:  # noqa: BLE001
                    pass
                await asyncio.sleep(2)
            log.warning("patchright 预热：等待 Cloudflare 超时")
        except Exception as e:  # noqa: BLE001
            log.warning("patchright 预热失败: %s", e)

    @staticmethod
    def _is_challenge(status: int, text: str) -> bool:
        return status == 403 or any(
            m in text[:4000] for m in ("Just a moment", "请稍候", "正在安全验证")
        )

    async def fetch(self, proxy_url: Optional[str], url: str) -> FetchResult:
        state = await self._ensure(proxy_url)
        page = state["page"]
        js = """async (url) => {
            const r = await fetch(url, {credentials:'include',
                headers:{'Accept':'application/json, text/plain, */*','X-Requested-With':'XMLHttpRequest'}});
            const t = await r.text();
            const h = {};
            r.headers.forEach((v,k) => h[k]=v);
            return {status: r.status, text: t, url: r.url, headers: h};
        }"""
        # 同一个浏览器 profile 串行请求；不同代理/profile 之间仍可并发。
        async with state["request_lock"]:
            spacing = 1.0 / max(self.cfg.list_requests_per_second, 1e-6)
            wait = spacing - (asyncio.get_running_loop().time() - state["last_request_at"])
            if wait > 0:
                await asyncio.sleep(wait)
            state["last_request_at"] = asyncio.get_running_loop().time()
            for attempt in range(2):
                try:
                    result = await page.evaluate(js, url)
                except Exception as e:  # noqa: BLE001 - 页面导航中断等
                    message = str(e).lower()
                    if any(x in message for x in (
                        "page crashed", "page has been closed", "context or browser has been closed",
                        "target page, context or browser has been closed",
                    )):
                        log.warning("Patchright 页面/上下文失效，销毁后由下一请求重建")
                        context = state.get("context")
                        if context is not None:
                            try:
                                await context.close()
                            except Exception:  # noqa: BLE001
                                pass
                        state["context"] = None
                        state["page"] = None
                        raise FetcherError("Patchright context crashed") from e
                    await self._warm_up(page)
                    continue
                status = result.get("status", 0)
                text = result.get("text", "")
                if self._is_challenge(status, text):
                    if attempt == 0:
                        log.warning("检测到 Cloudflare 挑战，重新导航解挑战…")
                        await self._warm_up(page)
                        continue
                    raise FetcherError("Cloudflare 挑战未解决（cf_clearance 无法刷新）")
                return FetchResult(result.get("url", url), status, text, result.get("headers") or {})
        raise FetcherError("patchright fetch 失败")

    async def check(self, proxy_url: Optional[str], url: str) -> bool:
        try:
            r = await self.fetch(proxy_url, url)
            return r.status_code < 500
        except FetcherError:
            return False

    async def aclose(self) -> None:
        for state in self._states.values():
            context = state.get("context")
            if context is None:
                continue
            try:
                await context.close()
            except Exception:  # noqa: BLE001
                pass
        if self._pw:
            try:
                await self._pw.stop()
            except Exception:  # noqa: BLE001
                pass
        self._states.clear()
        self._pw = None


class RuyiPageFetcher(Fetcher):
    """RuyiPage Firefox 后端：每个代理独立 profile，并迁移现有 RoyaleAPI 登录态。"""

    def __init__(self, cfg: CrawlConfig):
        self.cfg = cfg
        self._states: dict[Optional[str], dict] = {}
        self._states_lock = asyncio.Lock()
        self._auth_lock = asyncio.Lock()
        self._auth_cookies: Optional[list[dict]] = None
        self._manual_profiles: dict[str, str] = {}
        if cfg.ruyi_auth_map_file:
            try:
                mapping = json.loads(Path(cfg.ruyi_auth_map_file).read_text(encoding="utf-8"))
                self._manual_profiles = {
                    str(proxy): str(profile) for proxy, profile in mapping.items()
                }
            except Exception as exc:  # noqa: BLE001
                raise FetcherError(f"读取 ruyi_auth_map_file 失败: {exc}") from exc

    def _profile_dir(self, proxy_url: Optional[str]) -> str:
        if proxy_url and proxy_url in self._manual_profiles:
            return str(Path(self._manual_profiles[proxy_url]).resolve())
        import hashlib
        label = "direct" if not proxy_url else hashlib.sha256(
            proxy_url.encode("utf-8")
        ).hexdigest()[:16]
        root = Path(self.cfg.browser_profile_dir).parent / "ruyi_profiles"
        return str(root / label)

    async def _load_auth_cookies(self) -> list[dict]:
        """从用户已登录的 Chromium profile 读取 RoyaleAPI Cookie；绝不写日志。"""
        async with self._auth_lock:
            if self._auth_cookies is not None:
                return self._auth_cookies
            from patchright.async_api import async_playwright

            pw = await async_playwright().start()
            context = None
            try:
                context = await pw.chromium.launch_persistent_context(
                    user_data_dir=self.cfg.browser_profile_dir,
                    headless=False,
                )
                raw = await context.cookies([self.cfg.base_url])
            finally:
                if context is not None:
                    await context.close()
                await pw.stop()

            cookies: list[dict] = []
            for c in raw:
                # Cloudflare Cookie 往往与原出口绑定；仅迁移站点登录 Cookie。
                if c["name"].lower().startswith(("cf_", "__cf")):
                    continue
                item = {
                    "name": c["name"],
                    "value": c["value"],
                    "domain": c.get("domain", ".royaleapi.com"),
                    "path": c.get("path", "/"),
                    "secure": c.get("secure", True),
                    "httpOnly": c.get("httpOnly", False),
                }
                if c.get("expires", -1) > 0:
                    item["expiry"] = int(c["expires"])
                if c.get("sameSite"):
                    item["sameSite"] = c["sameSite"].lower()
                cookies.append(item)
            if not cookies:
                raise FetcherError("未从 browser_profile 读取到 RoyaleAPI 登录 Cookie")
            self._auth_cookies = cookies
            log.info("已向 RuyiPage 准备 %d 个 RoyaleAPI 登录 Cookie", len(cookies))
            return cookies

    async def prepare(self) -> None:
        """在 worker 启动前串行导出认证 Cookie，避免与列表 Chrome 抢 profile。"""
        from .proxy_pool import ProxyPool

        configured = {s.url for s in ProxyPool(self.cfg.proxy, self.cfg.rate_limit).states if s.url}
        if configured and configured.issubset(self._manual_profiles):
            return
        await self._load_auth_cookies()

    async def _ensure(self, proxy_url: Optional[str]):
        async with self._states_lock:
            state = self._states.get(proxy_url)
            if state is None:
                state = {
                    "page": None,
                    "init_lock": asyncio.Lock(),
                    "request_lock": asyncio.Lock(),
                    "last_request_at": 0.0,
                }
                self._states[proxy_url] = state

        async with state["init_lock"]:
            if state["page"] is None:
                from ruyipage.aio import launch

                cookies = None
                if not proxy_url or proxy_url not in self._manual_profiles:
                    cookies = await self._load_auth_cookies()
                page = await launch(
                    proxy=proxy_url,
                    user_dir=self._profile_dir(proxy_url),
                    headless=self.cfg.browser_headless,
                    close_on_exit=True,
                    timeout_page_load=60,
                    timeout_script=40,
                )
                try:
                    if cookies:
                        await page.set_cookies(cookies)
                    await page.get(self.cfg.base_url, wait="none", timeout=60)
                    await page.wait(2)
                except Exception:
                    await page.quit(force=True)
                    raise
                state["page"] = page
        return state

    @staticmethod
    def _is_challenge(status: int, text: str) -> bool:
        head = text[:5000].lower()
        return status == 403 or any(
            x in head for x in ("just a moment", "verify you are human", "security verification")
        )

    async def fetch(self, proxy_url: Optional[str], url: str) -> FetchResult:
        state = await self._ensure(proxy_url)
        page = state["page"]
        js = """function(url){return fetch(url,{credentials:'include',headers:{
            'Accept':'application/json, text/plain, */*',
            'X-Requested-With':'XMLHttpRequest'
        }}).then(async r=>({status:r.status,text:await r.text(),url:r.url,
            headers:Object.fromEntries(r.headers.entries())}));}"""
        async with state["request_lock"]:
            # 令牌必须在页面锁内部约束“真实请求启动时间”；否则初始化期间
            # 多个 worker 会提前拿令牌并在页面就绪后形成瞬时突发。
            spacing = 1.0 / max(self.cfg.rate_limit.requests_per_second, 1e-6)
            wait = spacing - (asyncio.get_running_loop().time() - state["last_request_at"])
            if wait > 0:
                await asyncio.sleep(wait)
            state["last_request_at"] = asyncio.get_running_loop().time()
            try:
                result = await page.run_js(js, url, timeout=max(40, self.cfg.request_timeout))
                for challenge_attempt in range(2):
                    if not self._is_challenge(
                        int(result.get("status", 0)), result.get("text", "")
                    ):
                        break
                    if "/data/replay" in url:
                        # 回放 403 不在主 worker 内同步等待 60-120 秒挑战。
                        # 关闭 profile，交给出口冷却和独立 lane watchdog 恢复/换线。
                        log.warning("RuyiPage 回放通道遇到 Cloudflare，关闭上下文并交给 watchdog")
                        try:
                            await page.quit(force=True)
                        except Exception:  # noqa: BLE001
                            pass
                        state["page"] = None
                        break
                    log.warning(
                        "RuyiPage 检测到 Cloudflare，自动点击挑战(attempt=%d)",
                        challenge_attempt + 1,
                    )
                    await page.handle_cloudflare_challenge(timeout=60, check_interval=2)
                    # Turnstile 点击后 cf_clearance/页面跳转可能延迟落地。
                    await page.wait(5)
                    result = await page.run_js(js, url, timeout=max(40, self.cfg.request_timeout))
            except Exception as e:  # noqa: BLE001
                message = str(e).lower()
                if any(x in message for x in (
                    "browsingcontext does no longer exist",
                    "discardedbrowsingcontext",
                    "no such frame",
                    "websocket connection is closed",
                )):
                    log.warning("RuyiPage 上下文失效，销毁后由下一请求重建")
                    try:
                        await page.quit(force=True)
                    except Exception:  # noqa: BLE001
                        pass
                    state["page"] = None
                raise FetcherError(f"RuyiPage: {type(e).__name__}: {e}") from e
        return FetchResult(
            result.get("url", url), int(result.get("status", 0)),
            result.get("text", ""), result.get("headers") or {},
        )

    async def check(self, proxy_url: Optional[str], url: str) -> bool:
        try:
            r = await self.fetch(proxy_url, url)
            return r.status_code < 500
        except FetcherError:
            return False

    async def aclose(self) -> None:
        for state in self._states.values():
            page = state.get("page")
            if page is None:
                continue
            try:
                await page.quit(force=True)
            except Exception:  # noqa: BLE001
                pass
        self._states.clear()


class SessionCurlFetcher(Fetcher):
    """轻量多会话后端：每个代理绑定独立 RoyaleAPI Cookie 文件。"""

    def __init__(self, cfg: CrawlConfig):
        from .curl_cffi_windows import install_curl_cffi_windows_workarounds
        install_curl_cffi_windows_workarounds()
        try:
            from curl_cffi import requests as curl_requests
        except ImportError as exc:
            raise ImportError("缺少 curl_cffi") from exc
        if not cfg.ruyi_auth_map_file:
            raise FetcherError("session_curl 需要 ruyi_auth_map_file")
        self.cfg = cfg
        self._cr = curl_requests
        self._sessions: dict[str, object] = {}
        mapping = json.loads(Path(cfg.ruyi_auth_map_file).read_text(encoding="utf-8"))
        self._cookies: dict[str, dict[str, str]] = {}
        for proxy, profile_value in mapping.items():
            profile = Path(profile_value)
            name = profile.name.removesuffix("-profile")
            cookie_file = profile.parent / f"{name}.json"
            if not cookie_file.exists():
                raise FetcherError(f"会话 Cookie 文件不存在: {cookie_file}")
            payload = json.loads(cookie_file.read_text(encoding="utf-8"))
            self._cookies[str(proxy)] = {
                row["name"]: row["value"] for row in payload.get("cookies", [])
            }

    def _session_for(self, proxy_url: Optional[str]):
        if not proxy_url or proxy_url not in self._cookies:
            raise FetcherError(f"没有为代理绑定独立会话: {proxy_url}")
        if proxy_url not in self._sessions:
            proxies = {"http": proxy_url, "https": proxy_url}
            self._sessions[proxy_url] = self._cr.AsyncSession(
                impersonate=self.cfg.impersonate,
                cookies=self._cookies[proxy_url],
                proxies=proxies,
                timeout=self.cfg.request_timeout,
                headers={
                    "User-Agent": self.cfg.user_agent,
                    "Accept": "application/json, text/plain, */*",
                    "Referer": self.cfg.referer,
                    "X-Requested-With": "XMLHttpRequest",
                },
            )
        return self._sessions[proxy_url]

    async def fetch(self, proxy_url: Optional[str], url: str) -> FetchResult:
        session = self._session_for(proxy_url)
        try:
            response = await session.get(url, allow_redirects=True)
        except Exception as exc:
            raise FetcherError(f"SessionCurl: {type(exc).__name__}: {exc}") from exc
        return FetchResult(
            str(response.url), response.status_code, response.text, dict(response.headers)
        )

    async def check(self, proxy_url: Optional[str], url: str) -> bool:
        try:
            response = await self.fetch(proxy_url, url)
            return response.status_code < 500
        except FetcherError:
            return False

    async def aclose(self) -> None:
        for session in self._sessions.values():
            try:
                await session.close()
            except Exception:
                pass
        self._sessions.clear()


class FlareSolverrFetcher(Fetcher):
    """把请求交给 FlareSolverr（无头浏览器自动解 Cloudflare 挑战）的 Fetcher。

    FlareSolverr 是一个本地代理服务（Docker），自动执行 JS 解掉「Just a moment」，
    并把解挑战后的响应（status + body + headers）原样返回。每个 proxy_url 映射到一个
    FlareSolverr session（独立浏览器上下文），cf_clearance 在该 session 内自动续期。

    启动方式：
        docker run -d --name flaresolverr -p 8191:8191 \
            -e LOG_LEVEL=info ghcr.io/flaresolverr/flaresolverr:latest
    """

    def __init__(self, cfg: CrawlConfig):
        from .curl_cffi_windows import install_curl_cffi_windows_workarounds
        install_curl_cffi_windows_workarounds()
        try:
            from curl_cffi import requests as curl_requests
        except ImportError as e:
            raise ImportError("缺少 curl_cffi，请执行: python -m pip install curl_cffi") from e
        self.cfg = cfg
        self.base = cfg.flaresolverr_url.rstrip("/")
        # FlareSolverr 解挑战可能需要几十秒，客户端超时放宽松
        self._client = curl_requests.AsyncSession(timeout=max(cfg.request_timeout, 60) + 30)
        self._session_ids: dict[Optional[str], str] = {}

    @staticmethod
    def _parse_solution(payload: dict) -> FetchResult:
        """把 FlareSolverr /v1 的响应解析成 FetchResult（纯函数，便于测试）。"""
        if payload.get("status") != "ok":
            raise FetcherError(f"FlareSolverr: {payload.get('message', 'unknown error')}")
        sol = payload.get("solution") or {}
        return FetchResult(
            sol.get("url", ""),
            int(sol.get("status", 200)),
            sol.get("response", ""),
            sol.get("headers") or {},
        )

    def _sid(self, proxy_url: Optional[str]) -> str:
        if proxy_url not in self._session_ids:
            import hashlib
            self._session_ids[proxy_url] = hashlib.md5(
                (proxy_url or "direct").encode("utf-8")
            ).hexdigest()[:12]
        return self._session_ids[proxy_url]

    async def fetch(self, proxy_url: Optional[str], url: str) -> FetchResult:
        payload = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": int(max(self.cfg.request_timeout, 60) * 1000),
            "session": self._sid(proxy_url),
        }
        try:
            r = await self._client.post(f"{self.base}/v1", json=payload)
        except Exception as e:  # noqa: BLE001
            raise FetcherError(f"FlareSolverr 不可达: {type(e).__name__}: {e}") from e
        try:
            data = r.json()
        except Exception as e:  # noqa: BLE001
            raise FetcherError(f"FlareSolverr 返回异常: {e}") from e
        return self._parse_solution(data)

    async def check(self, proxy_url: Optional[str], url: str) -> bool:
        try:
            r = await self.fetch(proxy_url, url)
            return r.status_code < 500
        except FetcherError:
            return False

    async def aclose(self) -> None:
        try:
            await self._client.close()
        except Exception:  # noqa: BLE001
            pass
