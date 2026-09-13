# 用途：检测可用内存，按暂停和恢复阈值保护采集。
# 分类：下载核心；使用：内部
# 相关文件与阅读顺序：见同目录 README.md。

"""Low-overhead resource guards used by the long-running collector."""
from __future__ import annotations

import ctypes
import os
import time
from collections.abc import Callable


GIB = 1024 ** 3


def available_physical_memory_bytes() -> int:
    """Return host available physical memory without adding a psutil dependency."""
    if os.name == "nt":
        class MemoryStatusEx(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatusEx()
        status.dwLength = ctypes.sizeof(status)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            raise OSError(ctypes.get_last_error(), "GlobalMemoryStatusEx failed")
        return int(status.ullAvailPhys)

    page_size = int(os.sysconf("SC_PAGE_SIZE"))
    available_pages = int(os.sysconf("SC_AVPHYS_PAGES"))
    return page_size * available_pages


class MemoryHysteresis:
    """Pause memory-heavy work below one threshold and resume above another.

    A gap between thresholds prevents browser/list work from oscillating when
    available memory hovers around the safety boundary.  Probe failures retain
    the previous state: an active pause therefore fails closed, while a machine
    without a supported probe is not made unusable at startup.
    """

    def __init__(
        self,
        *,
        pause_below_gib: float,
        resume_at_gib: float,
        check_interval: float = 2.0,
        probe: Callable[[], int] = available_physical_memory_bytes,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if pause_below_gib < 0:
            raise ValueError("pause_below_gib must be non-negative")
        if resume_at_gib <= pause_below_gib:
            raise ValueError("resume_at_gib must exceed pause_below_gib")
        if check_interval < 0:
            raise ValueError("check_interval must be non-negative")
        self.pause_bytes = int(pause_below_gib * GIB)
        self.resume_bytes = int(resume_at_gib * GIB)
        self.check_interval = float(check_interval)
        self._probe = probe
        self._clock = clock
        self.paused = False
        self.last_available_bytes: int | None = None
        self.last_error: str | None = None
        self.last_check = float("-inf")
        self.transitions = 0

    @property
    def enabled(self) -> bool:
        return self.pause_bytes > 0

    def refresh(self, *, force: bool = False) -> bool:
        if not self.enabled:
            self.paused = False
            return False
        now = self._clock()
        if not force and now - self.last_check < self.check_interval:
            return self.paused
        self.last_check = now
        try:
            available = int(self._probe())
            if available < 0:
                raise ValueError("available memory probe returned a negative value")
        except Exception as exc:  # resource telemetry must not crash collection
            self.last_error = f"{type(exc).__name__}: {exc}"
            return self.paused

        self.last_available_bytes = available
        self.last_error = None
        before = self.paused
        if self.paused:
            if available >= self.resume_bytes:
                self.paused = False
        elif available < self.pause_bytes:
            self.paused = True
        if before != self.paused:
            self.transitions += 1
        return self.paused

    def snapshot(self) -> dict:
        return {
            "enabled": self.enabled,
            "paused": self.paused,
            "available_gib": (
                round(self.last_available_bytes / GIB, 3)
                if self.last_available_bytes is not None else None
            ),
            "pause_below_gib": round(self.pause_bytes / GIB, 3),
            "resume_at_gib": round(self.resume_bytes / GIB, 3),
            "transitions": self.transitions,
            "probe_error": self.last_error,
        }
