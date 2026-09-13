# 用途：读取玩家种子文件，将标签转换为列表 URL。
# 分类：下载核心；使用：内部
# 相关文件与阅读顺序：见同目录 README.md。

"""种子加载：把"玩家 tag / 完整 URL"转成任务 URL。

框架阶段只提供两种入口：
1. 种子文件：每行一个玩家 tag 或完整 URL（# 开头为注释）。
2. tags_to_urls：程序化地把 tag 列表按端点模板拼接。

后续"从排行榜/部落发现玩家 tag"的爬取逻辑，可在本模块新增一个
Discoverer 类，产出 (url, seed) 后交给 Crawler.submit() 动态入队即可。
"""
from __future__ import annotations

from pathlib import Path


def load_seed_file(path: str | Path) -> list[tuple[str, str]]:
    """读取种子文件，返回 [(值, 来源)]。

    规则：空行与 `//`、`;` 开头的行视为注释跳过；
    `#TAG` 开头的行是玩家 tag（# 是 tag 的一部分，由爬虫在拼接时去掉）。
    """
    out: list[tuple[str, str]] = []
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"种子文件不存在: {path}")
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("//") or line.startswith(";"):
            continue
        out.append((line, str(p)))
    return out


def is_url(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")


def tags_to_urls(tags: list[str], template: str, base: str) -> list[str]:
    """把玩家 tag 按模板拼接成 URL。模板可用 {tag} 与 {base} 占位。"""
    urls = []
    for tag in tags:
        tag = tag.strip().lstrip("#")
        if not tag:
            continue
        urls.append(template.format(base=base.rstrip("/"), tag=tag))
    return urls
