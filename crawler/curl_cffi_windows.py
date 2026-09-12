"""curl_cffi Windows 异步选择器的窄范围兼容修复。

curl_cffi 0.16.x 在连接重置/超时时有两个竞态：
1. libcurl 重复发送 CURL_POLL_REMOVE 时对 set.remove() 触发 KeyError；
2. Proactor 兼容选择器的唤醒 socket 已关闭时 send/recv 触发 WinError 10054。

这些异常发生在 CFFI 回调边界，可能由无控制台进程显示为 ``Python-CFFI error``
弹窗。这里只把对应数据结构和唤醒管道改为幂等，不隐藏请求层异常。
"""
from __future__ import annotations

import os
from typing import Any

_installed = False


class _DiscardingSocketSet(set[int]):
    def remove(self, element: int) -> None:
        self.discard(element)


def _safe_wake_selector(self: Any) -> None:
    if self._closed:
        return
    try:
        self._waker_w.send(b"a")
    except OSError:
        # 唤醒管道正在关闭/已被远端重置；下次 selector 周期会自然收敛。
        return


def _safe_consume_waker(self: Any) -> None:
    try:
        self._waker_r.recv(1024)
    except OSError:
        return


def install_curl_cffi_windows_workarounds() -> bool:
    """在第一个 AsyncCurl 实例创建前安装补丁；重复调用安全。"""
    global _installed
    if _installed or os.name != "nt":
        return _installed

    from curl_cffi.aio import AsyncCurl
    from curl_cffi import _asyncio_selector

    original_init = AsyncCurl.__init__
    if not getattr(original_init, "_royaleapi_windows_safe", False):
        def safe_init(self: Any, *args: Any, **kwargs: Any) -> None:
            original_init(self, *args, **kwargs)
            self._sockfds = _DiscardingSocketSet(self._sockfds)

        safe_init._royaleapi_windows_safe = True  # type: ignore[attr-defined]
        AsyncCurl.__init__ = safe_init  # type: ignore[method-assign]

    # 真正持有 socketpair 的是内部 SelectorThread；外层 event loop 只做代理。
    selector_thread = _asyncio_selector.SelectorThread
    selector_thread._wake_selector = _safe_wake_selector
    selector_thread._consume_waker = _safe_consume_waker
    _installed = True
    return True
