"""In-process source refresh using the existing crawler's fetch/queue budget."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlsplit

from selectolax.parser import HTMLParser

from .discovery import canonical_list_url
from .parsers import parse_player_links
from .seeds import load_seed_file

log = logging.getLogger('crawler.sources')
PLAYER_TAG = re.compile(r'[A-Z0-9]{3,32}\Z')


def source_url(value: str, base_url: str) -> str:
    url = canonical_list_url(urljoin(base_url.rstrip('/') + '/', value))
    parts, base = urlsplit(url), urlsplit(base_url)
    if parts.scheme not in ('http', 'https') or (parts.scheme, parts.netloc) != (base.scheme.lower(), base.netloc.lower()) or parts.username or parts.password:
        raise ValueError('玩家来源必须与配置 base_url 同源')
    if not parts.path.startswith('/players/'):
        raise ValueError('玩家来源必须是 /players/ 下的公开榜单/玩家列表')
    return url


def parse_source_page(html: str, base_url: str, max_players: int, max_sources: int) -> dict:
    tags = list(dict.fromkeys(tag.upper() for tag in parse_player_links(html) if PLAYER_TAG.fullmatch(tag.upper())))
    if not tags:
        raise ValueError('来源页没有可识别玩家，可能是登录/验证页或页面结构变更')
    if len(tags) > max_players:
        raise ValueError('来源页玩家数超过配置安全上限；未截断或假报成功')
    sources = []
    for node in HTMLParser(html).css('a[href]'):
        value = node.attributes.get('href', '')
        try:
            url = source_url(value, base_url)
        except ValueError:
            continue
        if re.fullmatch(r'/players/leaderboard/[a-z]{2}', urlsplit(url).path):
            sources.append(url)
    return {'tags': tags, 'source_urls': list(dict.fromkeys(sources))[:max_sources],
            'html_sha256': hashlib.sha256(html.encode()).hexdigest()}


def file_signature(path: str) -> tuple[int, int]:
    stat = Path(path).stat()
    return stat.st_mtime_ns, stat.st_size


def seed_snapshot(path: str) -> tuple[list[tuple[str, str]], tuple[int, int]]:
    before = file_signature(path)
    seeds = load_seed_file(path)
    after = file_signature(path)
    if before != after:
        raise OSError('种子文件正在写入，等待下一次稳定读取')
    return seeds, after


def cached_source_is_fresh(record: dict, url: str, now: float, ttl: float) -> bool:
    """Shared by the standalone refresh tool; malformed/old caches fail closed."""
    if not isinstance(record, dict) or record.get('url') != url:
        return False
    tags = record.get('tags')
    if not isinstance(tags, list) or not tags or not all(isinstance(tag, str) and PLAYER_TAG.fullmatch(tag.upper()) for tag in tags):
        return False
    try:
        fetched = datetime.fromisoformat(str(record['fetched_utc']).replace('Z', '+00:00'))
        if fetched.tzinfo is None:
            return False
        age = now - fetched.timestamp()
        return 0 <= age < ttl
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


class LiveSources:
    def __init__(self, crawler, *, clock=time.time):
        self.crawler = crawler
        self.cfg = crawler.cfg.source_refresh
        self.clock = clock
        self._seed_signature = None
        self._next_seed_poll = 0.0
        self.state = {'enabled': True, 'last_poll_at': None, 'paused_reason': None,
                      'seed_file_last_loaded_at': None, 'seed_file_new_tasks': 0,
                      'seed_file_error': None}

    async def initialize(self) -> None:
        urls = [source_url(value, self.crawler.cfg.base_url) for value in self.cfg.urls]
        await self.crawler.store.register_seed_sources(urls, self.cfg.max_sources)
        # Seed ingestion is idempotent. First poll rereads once, closing the
        # race where the file changed after the CLI's initial startup read.
        await self.tick()

    async def tick(self) -> None:
        c = self.crawler
        self.state['last_poll_at'] = self.clock()
        work = await c.store.work_state()
        if c._refresh_list_memory_guard():
            self.state['paused_reason'] = 'low_memory'
            return
        if c.cfg.detail_backlog_high and (work.get('ready_backlog', 0) >= c.cfg.detail_backlog_high or c._backlog_paused):
            self.state['paused_reason'] = 'replay_backlog'
            return
        self.state['paused_reason'] = None
        await self._poll_seed_file()
        await c.store.schedule_seed_sources(self.cfg.max_pending_tasks, self.cfg.error_retry_delay)
        await c.store.schedule_player_revisits(
            self.cfg.player_revisit_batch_size, self.cfg.player_revisit_pending_limit,
            self.cfg.fresh_player_low_watermark, self.cfg.player_revisit_max_interval,
        )

    async def loop(self) -> None:
        while not self.crawler._stop.is_set():
            await asyncio.sleep(self.cfg.poll_interval)
            if not self.crawler._stop.is_set():
                await self.tick()

    async def _poll_seed_file(self) -> None:
        path = self.crawler.cfg.seeds_file
        now = self.clock()
        if not path or now < self._next_seed_poll:
            return
        self._next_seed_poll = now + self.cfg.seed_file_poll_interval
        try:
            signature = await self.crawler._file_io.call(file_signature, path)
            if signature == self._seed_signature:
                return
            seeds, signature = await self.crawler._file_io.call(seed_snapshot, path)
            added = 0
            for offset in range(0, len(seeds), 500):
                added += await self.crawler._seed(seeds[offset:offset + 500])
            self._seed_signature = signature
            self.state.update(seed_file_last_loaded_at=now, seed_file_error=None)
            self.state['seed_file_new_tasks'] += added
            if added:
                log.info('种子文件热更新：新入队 %d 个入口', added)
        except (OSError, UnicodeError, ValueError) as exc:
            self.state['seed_file_error'] = str(exc)
            log.warning('种子文件暂不可用，将在下次轮询重试: %s', type(exc).__name__)

    async def process(self, task: dict) -> None:
        c = self.crawler
        try:
            result = await c._fetch_raw(task['url'], list_request=True)
            if result is None:
                raise ValueError('来源页面 404')
            parsed = await c._file_io.call(parse_source_page, result.text, c.cfg.base_url,
                                          self.cfg.max_players_per_page, self.cfg.max_sources)
        except (asyncio.CancelledError, KeyboardInterrupt):
            raise
        except Exception as exc:
            # Keep replay workers running; do not overwrite a last-good cache
            # or pretend that an empty/error page is a fresh source snapshot.
            await c.store.fail_seed_source(task['id'], task['url'],
                f'{type(exc).__name__}: {str(exc)[:250]}', self.cfg.error_retry_delay, self.cfg.max_interval)
            log.warning('来源刷新失败并退避：%s (%s)', task['url'], type(exc).__name__)
            return
        player_tasks = c._seed_to_tasks([(tag, task['url']) for tag in parsed['tags']])
        result = await c.store.finish_seed_source(
            task_id=task['id'], url=task['url'], tags=parsed['tags'], player_tasks=player_tasks,
            discovered_urls=parsed['source_urls'], html_sha256=parsed['html_sha256'],
            refresh_interval=self.cfg.refresh_interval, max_interval=self.cfg.max_interval,
            max_sources=self.cfg.max_sources,
        )
        log.info('来源刷新：%s，玩家 %d，新入口 %d', task['url'], len(parsed['tags']), result['new_players'])
