# 用途：有界磁盘工作线程，避免文件写入阻塞网络任务。
# 分类：下载核心；使用：内部
# 相关文件与阅读顺序：见同目录 README.md。

"""Bounded serial disk worker; cancellation never interrupts a file write."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial


class FileIO:
    def __init__(self):
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="crawler-files")

    async def call(self, function, *args, **kwargs):
        return await asyncio.wrap_future(self._executor.submit(partial(function, *args, **kwargs)))

    def close(self):
        self._executor.shutdown(wait=True)


class PersistenceError(RuntimeError):
    """Stop collection without retrying already-committed replay work."""
