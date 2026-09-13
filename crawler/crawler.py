# 用途：主流水线：列表发现、回放下载、筛选、重试与停止。
# 分类：下载核心；使用：核心编排
# 相关文件与阅读顺序：见同目录 README.md。

"""爬虫编排：两级流水线（列表 → 对局详情）+ 限速 + 代理池 + 重试 + 断点续传。

流水线：
    种子(玩家 tag) → list 任务 → 抓"对局列表" → 解析出多个对局 id
                 → 动态入队 detail 任务 → 抓"单场对局回放 JSON"（二次点击）
"""
from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import logging
import math
import random
import signal
import time
import weakref
from collections import OrderedDict
from contextlib import ExitStack
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

from .client import CurlCffiFetcher, Fetcher, FetcherError
from .config import CrawlConfig
from .authoritative import (
    NativeContract,
    apply_native_contract_metadata,
    evaluate_native_eligibility,
    load_native_contract,
    resolve_king_tower_level_evidence,
    upgrade_exact_replay_body,
)
from .authoritative_manifest import prepare_upgrade_groups
from .parsers import (
    COORDINATE_TRANSFORM_ID,
    OUTPUT_SCHEMA_VERSION,
    ReplayCoordinateError,
    ReplayParseError,
    base_card_key,
    coordinate_provenance as replay_coordinate_provenance,
    derive_native_coordinates,
    parse_battles,
    is_normal_1v1,
    parse_next_battles_page,
    parse_player_tags,
    parse_replay_html,
)
from .proxy_pool import ProxyPool, ProxyState
from .ratelimit import TokenBucket
from .queue import TaskStore
from .list_observability import ListLaneTelemetry, PageHashCache, ParsedListPage
from .resource_guard import MemoryHysteresis
from .seeds import is_url, load_seed_file
from .storage import Storage
from .file_io import FileIO, PersistenceError
from .run_lock import RunLock
from .discovery import canonical_list_url, navigation_decision, page_fingerprint, players_from_battles
from .live_sources import LiveSources
from .season import battle_timestamp, load_roster, normalize_player

log = logging.getLogger("crawler")


class Crawler:
    def __init__(self, cfg: CrawlConfig, fetcher: Optional[Fetcher] = None):
        self._locks = ExitStack()
        self._file_io = FileIO()
        try:
            roots = {str(Path(cfg.output_dir).resolve())}
            if cfg.authoritative_target or cfg.authoritative_upgrade_manifest:
                roots.add(str(Path(cfg.authoritative_output_dir).resolve()))
            paths = {str(Path(cfg.db_path).resolve()) + ".run.lock"}
            paths.update(str(Path(root) / ".collector.run.lock") for root in roots)
            for path in sorted(paths):
                self._locks.enter_context(RunLock(path))
            self._initialize(cfg, fetcher)
        except BaseException:
            self.close()
            raise

    def _initialize(self, cfg: CrawlConfig, fetcher: Optional[Fetcher]) -> None:
        self.cfg = cfg
        self.season_roster = load_roster(cfg.season_roster_file, expected_season=cfg.season_id,
            expected_top=cfg.season_top_n) if cfg.season_roster_file else None
        self._season_ranks = self.season_roster.ranks if self.season_roster else {}
        self._season_rank_seasons = {tag: season for tag, season, _rank in self.season_roster.proofs} if self.season_roster else {}
        if self.season_roster and (
            cfg.discover_players or cfg.source_refresh.enabled or cfg.seeds_file
            or not cfg.adaptive_discovery
            or cfg.min_battle_timestamp != self.season_roster.start
            or cfg.max_battle_timestamp != self.season_roster.end or not cfg.campaign_id
        ):
            raise ValueError('赛季名单与运行配置不一致，禁止启动')
        self.store = TaskStore(cfg.db_path, cfg.retry.max_retries)
        self.storage = Storage(cfg.output_dir)
        self._authoritative_enabled = bool(
            cfg.authoritative_target or cfg.authoritative_upgrade_manifest
        )
        self.native_contract: Optional[NativeContract] = None
        self.authoritative_storage: Optional[Storage] = None
        if self._authoritative_enabled:
            if not cfg.authoritative_native_contract:
                raise ValueError(
                    "authoritative mode requires authoritative_native_contract"
                )
            if Path(cfg.output_dir).resolve() == Path(cfg.authoritative_output_dir).resolve():
                raise ValueError("authoritative_output_dir must differ from legacy output_dir")
            self.native_contract = load_native_contract(
                cfg.authoritative_native_contract,
                expected_game_version=cfg.authoritative_game_version,
            )
            self.store.assert_authoritative_contract(
                self.native_contract.contract_sha256
            )
            self.authoritative_storage = Storage(cfg.authoritative_output_dir)
        self.pool = ProxyPool(cfg.proxy, cfg.rate_limit)
        self.fetcher = fetcher or self._make_fetcher(cfg)
        self.list_fetcher: Optional[Fetcher] = None
        self._list_proxy: Optional[ProxyState] = None
        self._list_proxies: list[ProxyState] = []
        self._list_pick_lock = asyncio.Lock()
        self._list_started_labels: set[str] = set()
        self._list_telemetry = ListLaneTelemetry()
        self._upgrade_page_cache = PageHashCache(capacity=128)
        self._list_memory_guard = MemoryHysteresis(
            pause_below_gib=cfg.list_pause_free_memory_gb,
            resume_at_gib=cfg.list_resume_free_memory_gb,
            check_interval=cfg.list_memory_check_interval,
        )
        self._replay_global_bucket = (
            TokenBucket(cfg.replay_global_requests_per_second, 1, cfg.rate_limit.jitter)
            if cfg.replay_global_requests_per_second > 0 else None
        )
        if fetcher is None and cfg.backend in ("ruyipage", "session_curl"):
            from .client import PatchrightFetcher

            list_cfg = dataclasses.replace(
                cfg, browser_headless=cfg.list_browser_headless
            )
            self.list_fetcher = PatchrightFetcher(list_cfg)
            list_urls: list[Optional[str]] = [None]
            for proxy_url in cfg.list_proxy_urls:
                if proxy_url and proxy_url not in list_urls:
                    list_urls.append(proxy_url)
            self._list_proxies = [
                ProxyState(
                    url=proxy_url,
                    bucket=TokenBucket(
                        cfg.list_requests_per_second, 1, cfg.rate_limit.jitter
                    ),
                )
                for proxy_url in list_urls
            ]
            self._list_proxy = self._list_proxies[0]
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue(
            maxsize=cfg.queue_capacity
        )
        self._queue_sequence = 0
        self._fair_list_limit = max(1, cfg.global_concurrency // 5)
        self._fair_ready_limit = max(1, cfg.global_concurrency - self._fair_list_limit)
        self._all_done = asyncio.Event()
        self._stop = asyncio.Event()
        self._claimed_total = 0
        self._initial_done = 0
        self._initial_authoritative_accepted = 0
        self._metadata_page_locks = weakref.WeakValueDictionary()
        self._metadata_page_cache = OrderedDict()
        self._index_flush_lock = asyncio.Lock()
        self._phase = "initializing"
        self._fatal_error = None
        self._queue_state = {}
        self._backlog_paused = False
        self._navigation_stats = {}
        self._source_status = {}
        self._live_sources = LiveSources(self) if cfg.source_refresh.enabled else None
        self._upgrade_manifest_summary: Optional[dict] = None
        self._stats = {
            "ok": 0, "dead": 0, "retried": 0, "list_pages": 0,
            "filtered": 0, "list_candidates": 0, "list_eligible": 0,
            "list_normal_1v1": 0, "list_timestamp_current": 0,
            "list_timestamp_missing": 0, "list_timestamp_old": 0,
            "detail_enqueued": 0,
            "authoritative_accepted": 0,
            "authoritative_rejected": 0,
            "authoritative_reused": 0,
            "authoritative_downloaded": 0,
            "upgrade_list_enqueued": 0,
            "upgrade_enqueued": 0,
            "upgrade_dependency_pending": 0,
            "upgrade_list_confirmations": 0,
            "upgrade_list_deterministic_rejections": 0,
            "upgrade_list_page_cache_hits": 0,
        }

    # ---------------------------------------------------------------- 种子
    @staticmethod
    def _make_fetcher(cfg: CrawlConfig) -> Fetcher:
        if cfg.backend == "flaresolverr":
            from .client import FlareSolverrFetcher
            return FlareSolverrFetcher(cfg)
        if cfg.backend == "patchright":
            from .client import PatchrightFetcher
            return PatchrightFetcher(cfg)
        if cfg.backend == "ruyipage":
            from .client import RuyiPageFetcher
            return RuyiPageFetcher(cfg)
        if cfg.backend == "session_curl":
            from .client import SessionCurlFetcher
            return SessionCurlFetcher(cfg)
        return CurlCffiFetcher(cfg)

    def _seed_to_tasks(self, seeds: list[tuple[str, str]]) -> list[tuple[str, str, str, dict, str]]:
        """把 (值, 来源) 转成 (url, seed, kind, meta, dedup_key)。

        三重去重：
        - 玩家 tag → dedup_key = "player:{tag}"（同一玩家只爬一次列表）
        - 完整 URL → dedup_key = url（精确 URL 去重）
        """
        tasks: list[tuple[str, str, str, dict, str]] = []
        for value, source in seeds:
            v = value.strip()
            if is_url(v):
                parsed = urlparse(v)
                parts = parsed.path.strip('/').split('/')
                if len(parts) >= 2 and parts[0] == 'player' and (
                    len(parts) == 2 or parts[2] == 'battles'
                ):
                    tag = parts[1].upper()
                    if len(parts) == 2:
                        v = self.cfg.list_endpoint_template.format(base=self.cfg.base_url.rstrip('/'), tag=tag)
                    first_page = '/history' not in parsed.path
                    key = self._player_list_dedup_key(tag) if first_page and not parsed.query else self._list_page_dedup_key(v)
                    tasks.append((v, source, 'list', {'tag': tag, 'page': 1 if first_page or self.season_roster else 2}, key))
                else:
                    tag = parse_qs(parsed.query).get('tag', [''])[0]
                    key = 'battle:' + tag if tag and parsed.path == '/data/replay' else v
                    tasks.append((v, source, "detail", {"battle_tag": tag} if tag else {}, key))
            else:
                tag = v.lstrip("#").upper()
                if tag:
                    url = self.cfg.list_endpoint_template.format(
                        base=self.cfg.base_url.rstrip("/"), tag=tag
                    )
                    tasks.append((
                        url, source, "list", {"tag": tag, "page": 1},
                        self._player_list_dedup_key(tag),
                    ))
        if self.season_roster:
            for url, _source, kind, meta, _key in tasks:
                if kind != 'list' or not self._season_list_url_allowed(url, meta.get('tag')):
                    raise ValueError('固定赛季批次拒绝榜外种子、手工详情和非本站玩家列表')
        return tasks

    def _season_list_url_allowed(self, url: str, tag: str | None) -> bool:
        if not self.season_roster:
            return True
        parsed, base = urlparse(url), urlparse(self.cfg.base_url)
        return bool(tag in self._season_ranks and parsed.scheme == base.scheme
            and parsed.netloc == base.netloc and not parsed.username and not parsed.password
            and parsed.path in (f'/player/{tag}/battles', f'/player/{tag}/battles/history'))

    def _season_members(self, metadata: Optional[dict]) -> list[dict]:
        result = []
        for side in ('team', 'opponent'):
            for tag in (metadata or {}).get(side + '_tags') or []:
                try:
                    tag = normalize_player(tag)
                except ValueError:
                    continue
                if tag in self._season_ranks:
                    result.append({'side': side, 'tag': tag, 'rank': self._season_ranks[tag],
                                   'rank_season': self._season_rank_seasons.get(tag, self.season_roster.season_id)})
        return result

    def _player_list_dedup_key(self, tag: str) -> str:
        base = "player:" + tag
        epoch = str(self.cfg.list_refresh_epoch or "").strip()
        return f"{base}:refresh:{epoch}" if epoch else base

    def _list_page_dedup_key(self, url: str) -> str:
        return canonical_list_url(url) if self.cfg.adaptive_discovery else url

    async def _seed(self, seeds: list[tuple[str, str]]) -> int:
        return len(await self.store.add_many(self._seed_to_tasks(seeds)))

    def _eligible_metadata(self, metadata: Optional[dict]) -> bool:
        if not is_normal_1v1(metadata):
            return False
        if self.season_roster and not self._season_members(metadata):
            return False
        if self._authoritative_enabled:
            if not metadata or not metadata.get("authoritative_complete"):
                return False
            assert self.native_contract is not None
            if (
                metadata.get("numeric_game_mode_id")
                not in self.native_contract.source_numeric_game_mode_ids
            ):
                return False
            # Contract v3 accepts either an undamaged full-HP King anchor or
            # exact side-local Tower Troop level 16.  Requiring final King HP
            # to remain full here silently reinstated the old v2 rule and
            # discarded valid battles before replay download.
            if any(
                resolve_king_tower_level_evidence(
                    metadata, self.native_contract, side
                )
                is None
                for side in ("team", "opponent")
            ):
                return False
            for round_data in metadata.get("rounds") or []:
                for side in ("team", "opponent"):
                    for player in round_data.get(side) or []:
                        deck = {
                            str(card).strip().lower()
                            for card in player.get("full_deck") or []
                        }
                        if not deck.issubset(self.native_contract.allowed_card_tokens):
                            return False
                        if str(player.get("tower_troop") or "").lower() not in (
                            self.native_contract.allowed_tower_troops
                        ):
                            return False
        if self.cfg.min_battle_timestamp is None and self.cfg.max_battle_timestamp is None:
            return True
        timestamp = battle_timestamp(metadata)
        if timestamp is None:
            return False
        return ((self.cfg.min_battle_timestamp is None or timestamp >= self.cfg.min_battle_timestamp)
            and (self.cfg.max_battle_timestamp is None or timestamp < self.cfg.max_battle_timestamp))

    def _list_url_in_version_window(self, url: str) -> bool:
        if self.cfg.min_battle_timestamp is None:
            return True
        raw = (parse_qs(urlparse(url).query).get("before") or [None])[0]
        if raw is None:
            return True
        try:
            timestamp = int(raw)
            if timestamp > 10_000_000_000:
                timestamp //= 1000
        except (TypeError, ValueError):
            return False
        return timestamp > int(self.cfg.min_battle_timestamp) if self.season_roster else timestamp >= int(self.cfg.min_battle_timestamp)

    @staticmethod
    def _load_excluded_battle_tags(path: str) -> list[str]:
        manifest = Path(path).resolve(strict=True)
        tags: list[str] = []
        with manifest.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                value = json.loads(line)
                tag = str(value.get("battle_tag") or "").strip()
                if not tag:
                    raise ValueError(
                        f"excluded battle manifest missing tag at line {line_number}"
                    )
                tags.append(tag)
        if len(tags) != len(set(tags)):
            raise ValueError("excluded battle manifest contains duplicate tags")
        return tags

    async def queue_authoritative_upgrade_manifest(self, path: str) -> dict:
        if not self._authoritative_enabled:
            raise ValueError("authoritative upgrade manifest requires authoritative mode")
        manifest = Path(path).resolve(strict=True)
        stat = manifest.stat()
        if await self.store.authoritative_manifest_imported(
            str(manifest), size=stat.st_size, mtime_ns=stat.st_mtime_ns
        ):
            summary = {
                "manifest": str(manifest),
                "already_imported": True,
                "upgrade_list_enqueued": 0,
            }
            self._upgrade_manifest_summary = summary
            return summary

        prepared = prepare_upgrade_groups(manifest)
        all_candidates = [
            {**item, "contract_sha256": self.native_contract.contract_sha256}
            for candidates in prepared["groups"].values()
            for item in candidates
        ] + [
            {**item, "contract_sha256": self.native_contract.contract_sha256}
            for item in prepared["unaddressable"]
        ]
        queued_rows = await self.store.seed_authoritative_candidates(all_candidates)
        pages: list[tuple[str, dict, str]] = []
        for source_list_url, candidates in prepared["groups"].items():
            digest = hashlib.sha256(source_list_url.encode("utf-8")).hexdigest()
            pages.append((
                source_list_url,
                {"candidates": candidates, "phase": "authoritative_list_dependency"},
                "authoritative-list:" + digest,
            ))
        inserted = await self.store.add_authoritative_upgrade_pages(pages)
        for item in prepared["unaddressable"]:
            await self.store.record_authoritative_result(
                item["battle_tag"],
                status="rejected",
                tier="dependency_unresolved",
                reason=str(item.get("reason") or "upgrade address missing"),
                source_path=item.get("source_path"),
                source_schema_version=item.get("source_schema_version"),
                contract_sha256=(
                    self.native_contract.contract_sha256 if self.native_contract else None
                ),
            )
        await self.store.record_authoritative_manifest_import(
            str(manifest), size=stat.st_size, mtime_ns=stat.st_mtime_ns,
            candidates=prepared["unique_battles"],
        )
        self._stats["upgrade_list_enqueued"] += len(inserted)
        summary = {
            key: value for key, value in prepared.items()
            if key not in ("groups", "unaddressable")
        }
        summary.update({
            "already_imported": False,
            "upgrade_list_enqueued": len(inserted),
            "unaddressable_battles": len(prepared["unaddressable"]),
            "authoritative_rows_seeded": queued_rows,
        })
        list_lanes = max(1, len(self._list_proxies))
        list_rps = max(1e-9, list_lanes * self.cfg.list_requests_per_second)
        replay_rps = (
            self.cfg.replay_global_requests_per_second
            if self.cfg.replay_global_requests_per_second > 0
            else max(1e-9, self.pool.size * self.cfg.rate_limit.requests_per_second)
        )
        theoretical_seconds = (
            prepared["unique_source_list_urls"] / list_rps
            + prepared["replay_refetch_required"] / replay_rps
        )
        summary["theoretical_network_eta"] = {
            "list_requests": prepared["unique_source_list_urls"],
            "replay_requests": prepared["replay_refetch_required"],
            "list_requests_per_second": round(list_rps, 4),
            "replay_requests_per_second": round(replay_rps, 4),
            "hours_excluding_retries": round(theoretical_seconds / 3600, 2),
        }
        self._upgrade_manifest_summary = summary
        return summary

    def submit(self, task: dict) -> None:
        """把已从 SQLite 租约领取的任务加入有界内存队列。"""
        self._queue.put_nowait(self._queue_item(task))

    def _queue_item(self, task: dict) -> tuple[int, int, dict]:
        """保留持久层的 4:1 领取顺序，避免 PriorityQueue 再按旧 ID 重排。"""
        self._queue_sequence += 1
        priority = 2 if self.cfg.adaptive_discovery and task.get('kind') == 'list' else self._task_priority(task)
        return priority, self._queue_sequence, task

    @staticmethod
    def _task_priority(task: dict) -> int:
        if task.get("kind") in ("detail", "upgrade", "upgrade_list", "source"):
            return 0
        if "/history?before=" not in str(task.get("url", "")):
            return 2
        return 3

    # ---------------------------------------------------------------- 运行
    async def run(self, seeds: Optional[list[tuple[str, str]]] = None,
                  limit: Optional[int] = None) -> dict:
        try:
            await self._flush_index_outbox()
            result = await self._run_impl(seeds, limit)
            result['navigation'] = await self.store.navigation_stats()
            result['live_sources'] = await self.store.live_source_status()
            if self._fatal_error is not None:
                raise self._fatal_error
            self._phase = "stopped" if self._stop.is_set() else "completed"
            return result
        except BaseException as exc:
            self._phase = "failed"
            self._fatal_error = exc
            raise
        finally:
            try:
                try:
                    await self._flush_index_outbox()
                except BaseException:
                    self._phase = "failed"
                    raise
                finally:
                    await self._snapshot_progress(0.0)
            finally:
                await self._close_fetchers()

    async def _close_fetchers(self) -> None:
        fetchers = [self.fetcher]
        if self.list_fetcher is not None and self.list_fetcher is not self.fetcher:
            fetchers.append(self.list_fetcher)
        results = await asyncio.gather(*(asyncio.wait_for(item.aclose(), 30) for item in fetchers), return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                log.error('关闭下载后端失败，请检查残留会话: %s', result)

    async def _run_impl(self, seeds: Optional[list[tuple[str, str]]], limit: Optional[int]) -> dict:
        if self.cfg.campaign_id:
            contract = {
                'campaign_id': self.cfg.campaign_id,
                'base_url': self.cfg.base_url,
                'min_battle_timestamp': self.cfg.min_battle_timestamp,
                'require_complete_decks': self.cfg.require_complete_decks,
                'schema_version': OUTPUT_SCHEMA_VERSION,
                'authoritative_contract': self.native_contract.contract_sha256 if self.native_contract else None,
            }
            # Do not change the serialized contract of existing unbounded campaigns.
            if self.cfg.max_battle_timestamp is not None:
                contract['max_battle_timestamp_exclusive'] = self.cfg.max_battle_timestamp
            if self.season_roster:
                contract['season_roster'] = self.season_roster.summary()
                contract['discover_players'] = False
            await self.store.assert_campaign_contract(contract)
            if self.season_roster:
                await self._file_io.call(Storage.atomic_text,
                    Path(self.cfg.output_dir) / 'collection-roster.json',
                    json.dumps(self.season_roster.manifest(), ensure_ascii=False, indent=2))
        self._stats = {
            "ok": 0, "dead": 0, "retried": 0, "list_pages": 0,
            "filtered": 0, "list_candidates": 0, "list_eligible": 0,
            "list_normal_1v1": 0, "list_timestamp_current": 0,
            "list_timestamp_missing": 0, "list_timestamp_old": 0,
            "detail_enqueued": 0,
            "authoritative_accepted": 0,
            "authoritative_rejected": 0,
            "authoritative_reused": 0,
            "authoritative_downloaded": 0,
            "upgrade_list_enqueued": 0,
            "upgrade_enqueued": 0,
            "upgrade_dependency_pending": 0,
            "upgrade_list_confirmations": 0,
            "upgrade_list_deterministic_rejections": 0,
            "upgrade_list_page_cache_hits": 0,
        }
        self._all_done = asyncio.Event()
        self._stop = asyncio.Event()
        self._claimed_total = 0
        self._initial_authoritative_accepted = (
            await self.store.authoritative_accepted_count(
                self.native_contract.contract_sha256
            )
            if self._authoritative_enabled else 0
        )
        if (
            self._authoritative_enabled
            and self.cfg.authoritative_target
            and self._initial_authoritative_accepted >= self.cfg.authoritative_target
        ):
            log.info(
                "已达到 authoritative 目标 %d 场，不再导入或领取任务。",
                self.cfg.authoritative_target,
            )
            return self._summary()
        if self.cfg.excluded_battles_manifest:
            excluded_tags = self._load_excluded_battle_tags(
                self.cfg.excluded_battles_manifest
            )
            exclusion_result = await self.store.import_excluded_battles(
                excluded_tags, str(self.cfg.excluded_battles_manifest)
            )
            log.info(
                "训练集排除表 %d 条（本次新增 %d，跳过未完成任务 %d）",
                exclusion_result["total"], exclusion_result["inserted"],
                exclusion_result["unfinished_tasks_skipped"],
            )
        for source_db in self.cfg.excluded_battles_databases:
            if Path(source_db).resolve() == Path(self.cfg.db_path).resolve():
                raise ValueError('排除来源数据库不能是当前采集数据库')
            result = await self.store.import_completed_database(source_db)
            log.info('旧库已完成 ID 排除导入：新增 %d，总计 %d（不导入旧 pending）', result['inserted'], result['total'])
        if self.season_roster:
            if seeds:
                raise ValueError('固定赛季名单模式不接收外部 seeds')
            # RoyaleAPI's history cursor uses milliseconds; source battle
            # timestamps and our inclusion window use seconds.
            seeds = [(f'{self.cfg.base_url.rstrip("/")}/player/{tag}/battles/history?before={self.season_roster.end * 1000}',
                      f'season:{self.season_roster.season_id}') for _rank, tag in self.season_roster.players]
        elif seeds is None:
            seeds = load_seed_file(self.cfg.seeds_file) if self.cfg.seeds_file else []
        added = await self._seed(seeds)
        if self.cfg.authoritative_upgrade_manifest:
            await self.queue_authoritative_upgrade_manifest(
                self.cfg.authoritative_upgrade_manifest
            )

        requeued = await self.store.requeue_inflight()
        self._initial_done = await self.store.done_details()
        if (
            not self._authoritative_enabled
            and self.cfg.max_battles
            and self._initial_done >= self.cfg.max_battles
        ):
            log.info("已达到累计目标 %d 场，不再领取任务。", self.cfg.max_battles)
            return self._summary()
        if self._live_sources is not None:
            await self._live_sources.initialize()
        state = await self.store.work_state()
        total = int(state.get("pending", 0))
        log.info(
            "本轮新增 %d，恢复 inflight %d，待处理 %d，代理数 %d，全局并发 %d，后端 %s",
            added, requeued, total, self.pool.size, self.cfg.global_concurrency, self.cfg.backend,
        )

        if total == 0 and not state.get("inflight", 0) and self._live_sources is None:
            log.info("无待处理任务，直接退出。")
            return self._summary()

        prepare = getattr(self.fetcher, "prepare", None)
        if prepare is not None:
            await prepare()

        self._install_signal_handlers()
        self._phase = "running"
        loader = asyncio.create_task(self._loader_loop(limit), name="db-loader")
        health = asyncio.create_task(
            self.pool.health_loop(self._health_check), name="health-check"
        )
        progress = asyncio.create_task(self._progress_loop(), name="progress")
        workers = [
            asyncio.create_task(self._worker(i), name=f"worker-{i}")
            for i in range(self.cfg.global_concurrency)
        ]
        backgrounds = [loader, health, progress]
        if self._live_sources is not None:
            backgrounds.append(asyncio.create_task(self._live_sources.loop(), name='live-sources'))
        for background in (*backgrounds, *workers):
            background.add_done_callback(self._background_finished)

        try:
            await self._all_done.wait()
        except asyncio.CancelledError:
            self._stop.set()
        finally:
            for background in backgrounds:
                background.cancel()
            if self._stop.is_set():
                log.warning("收到中断信号，正在停止（已入队任务不再新增）…")
                for w in workers:
                    w.cancel()
            else:
                for _ in workers:
                    await self._queue.put((99, 0, None))
            await asyncio.gather(*workers, *backgrounds, return_exceptions=True)

        return self._summary()

    def _background_finished(self, task: asyncio.Task) -> None:
        if not task.cancelled() and task.exception() is not None:
            self._fatal_error = task.exception()
            self._request_stop()

    async def _flush_index_outbox(self) -> None:
        async with self._index_flush_lock:
            targets = {str(self.storage.root.resolve()): self.storage}
            if self.authoritative_storage is not None:
                targets[str(self.authoritative_storage.root.resolve())] = self.authoritative_storage
            try:
                while rows := await self.store.pending_index():
                    for row in rows:
                        target = targets[row['root']]
                        await self._file_io.call(target.append_index_idempotent, row['record'])
                        await self.store.index_published(row['task_id'])
            except Exception as exc:
                raise PersistenceError('索引发布失败；已保存 outbox，停止采集并保留现场') from exc

    async def _loader_loop(self, limit: Optional[int]) -> None:
        """小批量领取 SQLite 任务，避免百万 pending 一次性进入内存。"""
        while not self._stop.is_set():
            if limit is not None and self._claimed_total >= limit:
                state = await self.store.work_state()
                if not state.get("inflight", 0):
                    self._all_done.set()
                    return
                await asyncio.sleep(0.2)
                continue

            free = self.cfg.queue_capacity - self._queue.qsize()
            batch = min(self.cfg.claim_batch_size, max(0, free))
            if limit is not None:
                batch = min(batch, limit - self._claimed_total)
            list_work_paused = self._refresh_list_memory_guard()
            if self.cfg.detail_backlog_high:
                backlog = int((await self.store.work_state())['ready_backlog'])
                if backlog >= self.cfg.detail_backlog_high:
                    self._backlog_paused = True
                elif backlog <= self.cfg.detail_backlog_low:
                    self._backlog_paused = False
                list_work_paused = list_work_paused or self._backlog_paused
            if batch > 0:
                rows = await self.store.pending_authoritative_fair(
                    limit=batch,
                    ready_limit=self._fair_ready_limit,
                    list_limit=0 if list_work_paused else self._fair_list_limit,
                )
            else:
                rows = await self.store.pending(limit=batch) if batch > 0 else []
            if rows:
                self._phase = 'running'
                for task in rows:
                    await self._queue.put(self._queue_item(task))
                self._claimed_total += len(rows)
                continue

            state = await self.store.work_state()
            pending = int(state.get("pending", 0))
            inflight = int(state.get("inflight", 0))
            if pending == 0 and inflight == 0 and self._queue.empty():
                if self._live_sources is None:
                    self._all_done.set()
                    return
                self._phase = 'waiting_for_sources'
                await asyncio.sleep(min(5.0, self.cfg.source_refresh.poll_interval))
                continue
            delay = 0.5
            next_at = state.get("next_retry_at")
            if pending and not inflight and isinstance(next_at, (int, float)):
                delay = min(5.0, max(0.2, float(next_at) - time.time()))
            await asyncio.sleep(delay)

    async def _progress_loop(self, interval: float = 2.0) -> None:
        """每 interval 秒打印一次进度汇总（实时进度）。"""
        last_ok = 0
        last_time = time.monotonic()
        last_log = last_time
        await self._snapshot_progress(0.0)
        while True:
            await asyncio.sleep(interval)
            now = time.monotonic()
            dt = max(now - last_time, 1e-6)
            rate = (self._stats["ok"] - last_ok) / dt
            last_ok, last_time = self._stats["ok"], now
            await self._snapshot_progress(rate)
            if now - last_log < 60:
                continue
            last_log = now
            total_accepted = (
                self._initial_authoritative_accepted
                + self._stats["authoritative_accepted"]
                if self._authoritative_enabled
                else self._initial_done + self._stats["ok"]
            )
            log.info(
                "进度: accepted %d 场(本轮 %d) / %d 列表页 | 候选 %d / 标准1v1 %d / 日期通过 %d(缺失%d,过旧%d) / 门禁通过 %d / 新详情 %d | 拒绝 %d / 失败 %d / 重试 %d | 速度 %.1f 场/秒",
                total_accepted, self._stats["ok"], self._stats["list_pages"],
                self._stats["list_candidates"], self._stats["list_normal_1v1"],
                self._stats["list_timestamp_current"], self._stats["list_timestamp_missing"],
                self._stats["list_timestamp_old"], self._stats["list_eligible"],
                self._stats["detail_enqueued"], self._stats["authoritative_rejected"],
                self._stats["dead"],
                self._stats["retried"], rate,
            )

    async def _snapshot_progress(self, rate: float) -> None:
        self._queue_state = await self.store.work_state()
        self._navigation_stats = await self.store.navigation_stats()
        self._source_status = await self.store.live_source_status()
        self._persisted_done = (
            await self.store.authoritative_accepted_count(self.native_contract.contract_sha256)
            if self._authoritative_enabled else await self.store.done_details()
        )
        target = self.cfg.authoritative_target if self._authoritative_enabled else self.cfg.max_battles
        if self._phase in ('stopped', 'completed') and target and self._persisted_done >= target:
            self._phase = 'target_reached'
        # Snapshot event-loop-owned deques/dicts before dispatching disk work.
        await self._file_io.call(self._write_runtime_metrics, rate, self._runtime_metrics_payload(rate))

    def _runtime_metrics_payload(self, rate: float) -> dict:
        return {
            "updated_at": time.time(),
            "phase": self._phase,
            "list_backlog_paused": self._backlog_paused,
            "tasks": dict(self._queue_state),
            "stats": dict(self._stats),
            "target": self.cfg.authoritative_target if self._authoritative_enabled else self.cfg.max_battles,
            "error": str(self._fatal_error) if self._fatal_error else None,
            "battle_rate": rate,
            "total_done": getattr(self, '_persisted_done', 0),
            "authoritative": self._authoritative_enabled,
            "contract_sha256": self.native_contract.contract_sha256 if self.native_contract else None,
            "proxies": self.pool.snapshot(),
            "list_lanes": self._list_telemetry.snapshot(),
            "list_page_cache_entries": len(self._upgrade_page_cache),
            "list_memory_guard": self._list_memory_guard.snapshot(),
            "navigation": dict(self._navigation_stats),
            "live_sources": {
                **self._source_status,
                **(dict(self._live_sources.state) if self._live_sources is not None else {'enabled': False}),
            },
        }

    def _write_runtime_metrics(self, rate: float, payload: Optional[dict] = None) -> None:
        """给独立 lane watchdog 输出轻量状态，不阻塞采集。"""
        try:
            root = Path(self.cfg.output_dir) / "lanes"
            root.mkdir(parents=True, exist_ok=True)
            path = root / "crawler-proxies.json"
            temp = root / "crawler-proxies.json.tmp"
            if payload is None:
                payload = self._runtime_metrics_payload(rate)
            temp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            temp.replace(path)
        except Exception:  # noqa: BLE001 - 指标失败不能影响采集
            log.debug("写入 lane metrics 失败", exc_info=True)

    async def _health_check(self, proxy_state) -> bool:
        return await self.fetcher.check(proxy_state.url, self.cfg.proxy.health_check_url)

    async def _worker(self, idx: int) -> None:
        while True:
            _, _, task = await self._queue.get()
            if task is None:  # 停止信号
                self._queue.task_done()
                return
            try:
                await self._process(task)
            except PersistenceError:
                raise
            except OSError as exc:
                raise PersistenceError('本地文件/资源操作失败，停止采集并保留未完成任务') from exc
            except Exception:  # noqa: BLE001 - 单任务失败不拖垮 worker
                log.exception("任务处理异常: %s", task.get("url"))
                await self._retry_task(task, "unexpected exception")
            finally:
                self._queue.task_done()

    # ---------------------------------------------------------------- 任务处理
    async def _process(self, task: dict) -> None:
        if task.get('kind') == 'source':
            if self._live_sources is None:
                await self.store.mark_skipped(task['url'], 'live source refresh disabled', task_id=task['id'])
            else:
                await self._live_sources.process(task)
        elif task.get("kind") == "upgrade_list":
            await self._process_upgrade_list(task)
        elif task.get("kind") == "list":
            await self._process_list(task)
        elif task.get("kind") == "upgrade":
            await self._process_upgrade(task)
        else:
            await self._process_detail(task)

    async def _process_list(self, task: dict) -> None:
        url = task["url"]
        meta = task.get("meta") or {}
        tag = meta.get("tag")
        page = meta.get("page", 1)
        if not self._season_list_url_allowed(url, tag):
            raise ValueError('赛季任务出现榜外玩家/非法列表 URL；停止并保留现场')
        if not self._list_url_in_version_window(url):
            await self.store.mark_skipped(
                url, "list page before native balance window", task_id=task.get("id")
            )
            self._stats["filtered"] += 1
            return
        try:
            result = await self._fetch_raw(url, list_request=True)
        except Exception as e:
            await self._retry_task(task, str(e))
            log.error("列表任务本轮失败: %s (%s)", url, e)
            return
        if result is None:
            await self.store.mark_dead(url, "404 not found", task_id=task.get("id"))
            self._stats["dead"] += 1
            log.warning("列表 404 跳过: %s", url)
            return

        saved_path = ""
        if self.cfg.save_lists:
            saved_path = await self._file_io.call(self.storage.save_html, url, result.text)
            await self._file_io.call(self.storage.append_index,
                {"url": url, "kind": "list", "saved_path": saved_path, "fetched_at": time.time()}
            )

        battles = parse_battles(result.text, self.cfg.base_url, url)
        if self.season_roster and not battles:
            # A challenge/login/changed layout is not evidence of no matches.
            if 'battle_list_container' not in result.text and not parse_next_battles_page(result.text, self.cfg.base_url):
                raise ValueError('历史页没有可识别的对局列表结构；不能把验证页/结构变更当作采集完成')
            self._stats['empty_history_pages'] = self._stats.get('empty_history_pages', 0) + 1
        # dedup_key = battle:{对局tag}，同一场对局从双方视角只抓一次
        normal = [
            battle for battle in battles
            if is_normal_1v1(battle.get("metadata"))
        ]
        eligible = [
            battle for battle in normal
            if self._eligible_metadata(battle.get("metadata"))
        ]
        self._stats["list_candidates"] += len(battles)
        self._stats["list_normal_1v1"] += len(normal)
        self._stats["list_timestamp_current"] += len(eligible)
        for battle in normal:
            raw_timestamp = (battle.get("metadata") or {}).get("timestamp")
            try:
                parsed_timestamp = int(raw_timestamp)
                if parsed_timestamp > 10_000_000_000:
                    parsed_timestamp //= 1000
            except (TypeError, ValueError):
                self._stats["list_timestamp_missing"] += 1
                continue
            if (
                self.cfg.min_battle_timestamp is not None
                and parsed_timestamp < int(self.cfg.min_battle_timestamp)
            ):
                self._stats["list_timestamp_old"] += 1
            if self.cfg.max_battle_timestamp is not None and parsed_timestamp >= self.cfg.max_battle_timestamp:
                self._stats['list_timestamp_after_season'] = self._stats.get('list_timestamp_after_season', 0) + 1
            if self.season_roster and not self._season_members(battle.get('metadata')):
                self._stats['list_outside_player_pool'] = self._stats.get('list_outside_player_pool', 0) + 1
        self._stats["list_eligible"] += len(eligible)
        self._stats["filtered"] += len(battles) - len(eligible)
        inserted = await self.store.add_battles(
            [(b["url"], url, {"tag": tag, "battle_tag": b["tag"]}, "battle:" + b["tag"],
              b["tag"], b["metadata"])
             for b in eligible],
            source_task_id=int(task['id']) if self.cfg.adaptive_discovery else None,
        )
        self._stats["detail_enqueued"] += len(inserted)
        # 新任务只写入 SQLite，由有界 loader 分批领取，不能直接灌满内存。

        if self.cfg.adaptive_discovery:
            await self._expand_novel_frontier(task, result.text, battles, eligible, saved_path)
            self._stats['list_pages'] += 1
            return

        # 雪球式发现：从每场对局提取双方玩家 tag，自动入队新玩家
        discovered = 0
        if self.cfg.discover_players:
            for ptag in parse_player_tags(result.text):
                if ptag == tag:
                    continue
                list_url = self.cfg.list_endpoint_template.format(
                    base=self.cfg.base_url.rstrip("/"), tag=ptag
                )
                if await self.store.add(
                    list_url, url, "list", {"tag": ptag, "page": 1},
                    self._player_list_dedup_key(ptag),
                ):
                    discovered += 1

        # 翻页：抓这个玩家的更早对局（/battles/history?before=...），按 URL 去重
        if page < self.cfg.max_pages_per_player:
            next_url = parse_next_battles_page(result.text, self.cfg.base_url)
            if (
                next_url
                and self._list_url_in_version_window(next_url)
                and await self.store.add(
                    next_url, url, "list", {"tag": tag, "page": page + 1}, self._list_page_dedup_key(next_url)
                )
            ):
                pass

        await self.store.mark_done(url, saved_path, task_id=task.get("id"))
        self._stats["list_pages"] += 1
        log.debug("列表 %s[第%d页] -> %d 场对局(新增 %d)，发现新玩家 %d",
                  url, page, len(battles), len(inserted), discovered)

    async def _expand_novel_frontier(self, task: dict, html: str, battles: list[dict],
                                     eligible: list[dict], saved_path: str) -> None:
        meta = task.get('meta') or {}
        url = task['url']
        tag = str(meta.get('tag') or '').upper()
        page = int(meta.get('page', 1))
        eligible_tags = {battle['tag'] for battle in eligible}
        # This ledger commits with replay insertion, so a navigation retried
        # after a crash cannot misclassify its own newly queued IDs as overlap.
        new_tags = (await self.store.list_new_battle_tags(int(task['id']))) & eligible_tags
        fresh_players = players_from_battles(eligible, new_tags)
        decision = navigation_decision(
            eligible_count=len(eligible_tags), new_count=len(new_tags),
            previous_zero=int(meta.get('zero_new_chain', 0)),
            fingerprint=page_fingerprint(battle['tag'] for battle in battles),
            previous_fingerprint=meta.get('previous_page_fingerprint'),
            zero_limit=self.cfg.discovery_zero_page_limit,
        )
        if self.cfg.discover_players:
            items = []
            for player in dict.fromkeys(str(value).lstrip('#').upper() for value in parse_player_tags(html)):
                if not player or player == tag:
                    continue
                list_url = self.cfg.list_endpoint_template.format(base=self.cfg.base_url.rstrip('/'), tag=player)
                items.append((list_url, url, 'list', {
                    'tag': player, 'page': 1,
                    'discovery_band': 'fresh' if player in fresh_players else 'overlap',
                    'discovery_parent_task': task['id'],
                }, self._player_list_dedup_key(player)))
            # Keep overlap-only players for a bounded exploration share instead
            # of asserting that their unseen pages must also contain duplicates.
            await self.store.add_many(items)

        cutoff = False
        reason = None
        if page < self.cfg.max_pages_per_player:
            next_url = parse_next_battles_page(html, self.cfg.base_url)
            if next_url and self._list_url_in_version_window(next_url):
                if not self._season_list_url_allowed(next_url, tag):
                    raise ValueError('历史翻页跳到了榜外玩家/非本站 URL')
                next_key = self._list_page_dedup_key(next_url)
                same_page = next_key == self._list_page_dedup_key(url)
                if self.season_roster:
                    # Closed-season backfill must not stop at two duplicate
                    # pages: unseen older matches can still be in this season.
                    current_before = battle_timestamp({'timestamp': (parse_qs(urlparse(url).query).get('before') or [self.season_roster.end])[0]})
                    next_before = battle_timestamp({'timestamp': (parse_qs(urlparse(next_url).query).get('before') or [None])[0]})
                    if next_before is None or current_before is None or next_before >= current_before:
                        raise ValueError('赛季历史游标未严格向过去推进，停止而非循环请求')
                cutoff = bool((decision['stop_history'] and not self.season_roster) or same_page)
                reason = 'same_list_url' if same_page else decision['reason']
                if not cutoff:
                    await self.store.add(next_url, url, 'list', {
                        'tag': tag, 'page': page + 1,
                        'discovery_band': 'fresh' if new_tags else 'overlap',
                        'zero_new_chain': decision['zero_chain'],
                        'previous_page_fingerprint': decision['fingerprint'],
                    }, next_key)
        await self.store.commit_list_navigation(
            task_id=int(task['id']), url=url, saved_path=saved_path,
            fingerprint=decision['fingerprint'], candidates=len(eligible_tags),
            new_candidates=len(new_tags), history_cutoff=cutoff, reason=reason,
            player_refresh={
                'player_key': meta.get('player_refresh_key') or (
                    self._list_page_dedup_key(url) if urlparse(url).query else self._player_list_dedup_key(tag)
                ),
                'player_tag': tag, 'url': url,
                'interval': self.cfg.source_refresh.player_revisit_interval,
                'max_interval': self.cfg.source_refresh.player_revisit_max_interval,
            } if self._live_sources is not None and page == 1 and tag else None,
        )
        log.debug('导航收益 %s：合格候选 %d / 新增 %d / 已知 %d，停止翻页=%s',
                  url, len(eligible_tags), len(new_tags), len(eligible_tags) - len(new_tags), reason)

    async def _process_upgrade_list(self, task: dict) -> None:
        """Resolve metadata with one successful cross-lane confirmation at most.

        Transport/challenge failures retain the ordinary persistent retry path.
        Once a page has parsed successfully, however, resolved candidates are
        split out immediately and never held hostage by an absent/incomplete
        sibling.  Only the unresolved subset is persisted for confirmation.
        """
        url = task["url"]
        meta = dict(task.get("meta") or {})
        candidates = meta.get("candidates") or []
        confirmation = meta.get("list_confirmation_v1")
        if confirmation:
            await self._confirm_upgrade_list(task, candidates, confirmation)
            return
        try:
            result, lane = await self._fetch_raw_with_lane(url, list_request=True)
        except Exception as exc:  # noqa: BLE001
            status = await self._retry_task(task, f"upgrade list dependency: {exc}")
            if status == "dead":
                await self._reject_upgrade_candidates(
                    candidates, reason=f"source list fetch exhausted: {exc}"
                )
            return
        if result is None:
            await self.store.mark_dead(
                url, "authoritative source list 404", task_id=task.get("id")
            )
            self._stats["dead"] += 1
            await self._reject_upgrade_candidates(
                candidates, reason="authoritative source list 404"
            )
            return

        page = self._parse_upgrade_page(url, result.text, lane)
        if not page.valid:
            status = await self._retry_task(
                task, "upgrade list returned no parseable battle rows"
            )
            if status == "dead":
                await self._reject_upgrade_candidates(
                    candidates,
                    reason="source list parseable content exhausted",
                )
            return
        resolved, unresolved = self._partition_upgrade_candidates(
            candidates, page.by_tag
        )
        await self._enqueue_upgrade_candidates(url, resolved)
        if not unresolved:
            await self.store.mark_done(url, "", task_id=task.get("id"))
            self._stats["list_pages"] += 1
            return

        self._list_telemetry.missing(lane, len(unresolved))
        first_observations = {
            str(candidate.get("battle_tag") or ""): reason
            for candidate, reason in unresolved
        }
        for candidate, reason in unresolved:
            await self.store.record_authoritative_result(
                str(candidate.get("battle_tag") or ""),
                status="queued",
                tier="dependency_pending",
                reason=reason,
                source_path=candidate.get("source_path"),
                source_schema_version=candidate.get("source_schema_version"),
                contract_sha256=(
                    self.native_contract.contract_sha256 if self.native_contract else None
                ),
            )
        self._stats["upgrade_dependency_pending"] += len(unresolved)

        narrowed_meta = dict(meta)
        narrowed_meta["candidates"] = [candidate for candidate, _ in unresolved]
        narrowed_meta["list_confirmation_v1"] = {
            "state": "pending_cross_lane",
            "first_lane": lane,
            "first_page_sha256": page.sha256,
            "first_observations": first_observations,
        }
        # Persist the split before the second network operation.  A crash or a
        # transient confirmation failure resumes with only unresolved tags.
        await self.store.update_task_meta(int(task["id"]), narrowed_meta)
        task["meta"] = narrowed_meta
        await self._confirm_upgrade_list(
            task,
            narrowed_meta["candidates"],
            narrowed_meta["list_confirmation_v1"],
        )

    def _parse_upgrade_page(
        self, url: str, text: str, lane: str
    ) -> ParsedListPage:
        page = self._upgrade_page_cache.parse(
            url=url,
            text=text,
            parser=lambda body: parse_battles(body, self.cfg.base_url, url),
        )
        if page.valid:
            if page.cache_hit:
                self._stats["upgrade_list_page_cache_hits"] += 1
            # Missing counts are added after candidate partitioning.
            self._list_telemetry.parsed(lane, cache_hit=page.cache_hit)
        return page

    @staticmethod
    def _partition_upgrade_candidates(
        candidates: list[dict], by_tag: dict[str, dict]
    ) -> tuple[list[tuple[dict, dict]], list[tuple[dict, str]]]:
        resolved: list[tuple[dict, dict]] = []
        unresolved: list[tuple[dict, str]] = []
        for candidate in candidates:
            battle_tag = str(candidate.get("battle_tag") or "")
            battle = by_tag.get(battle_tag)
            metadata = battle.get("metadata") if battle else None
            if metadata and metadata.get("authoritative_complete"):
                resolved.append((candidate, metadata))
            else:
                unresolved.append((
                    candidate,
                    "battle tag absent from exact source list page"
                    if battle is None else "source list metadata incomplete",
                ))
        return resolved, unresolved

    async def _enqueue_upgrade_candidates(
        self, source_list_url: str, resolved: list[tuple[dict, dict]]
    ) -> None:
        upgrades: list[tuple[str, str, dict, str, Optional[dict]]] = []
        for candidate, metadata in resolved:
            battle_tag = str(candidate.get("battle_tag") or "")
            task_meta = dict(candidate)
            task_meta["battle_tag"] = battle_tag
            task_meta["contract_sha256"] = (
                self.native_contract.contract_sha256 if self.native_contract else None
            )
            upgrades.append((
                str(candidate["replay_url"]),
                source_list_url,
                task_meta,
                battle_tag,
                metadata,
            ))
        inserted = await self.store.add_authoritative_upgrades(upgrades)
        self._stats["upgrade_enqueued"] += len(inserted)

    def _has_cross_list_lane(self, excluded_lane: str) -> bool:
        return any(state.label != excluded_lane for state in self._list_proxies)

    async def _confirm_upgrade_list(
        self, task: dict, candidates: list[dict], confirmation: dict
    ) -> None:
        url = task["url"]
        first_lane = str(confirmation.get("first_lane") or "")
        if not self._has_cross_list_lane(first_lane):
            reason = (
                "deterministic metadata unresolved; cross-lane confirmation unavailable; "
                f"first_lane={first_lane};first_sha256="
                f"{confirmation.get('first_page_sha256')}"
            )
            await self._finalize_upgrade_confirmation(
                task, candidates, [], reason=reason,
                confirmation_update={"state": "confirmation_unavailable"},
            )
            return
        try:
            result, confirm_lane = await self._fetch_raw_with_lane(
                url, list_request=True, exclude_list_lane=first_lane
            )
        except Exception as exc:  # noqa: BLE001
            status = await self._retry_task(
                task, f"upgrade list cross-lane confirmation: {exc}"
            )
            if status == "dead":
                await self._reject_upgrade_candidates(
                    candidates,
                    reason=f"cross-lane confirmation fetch exhausted: {exc}",
                )
            return
        self._stats["upgrade_list_confirmations"] += 1
        if result is None:
            reason = (
                "deterministic metadata unresolved after cross-lane 404; "
                f"first_lane={first_lane};confirm_lane={confirm_lane};"
                f"first_sha256={confirmation.get('first_page_sha256')}"
            )
            await self._finalize_upgrade_confirmation(
                task, candidates, [], reason=reason,
                confirmation_update={
                    "state": "confirmed_404", "confirm_lane": confirm_lane,
                    "confirm_page_sha256": None,
                },
            )
            return
        page = self._parse_upgrade_page(url, result.text, confirm_lane)
        if not page.valid:
            # HTTP 200 without parseable battle rows is treated like an
            # interstitial/login/challenge failure, never deterministic absence.
            status = await self._retry_task(
                task, "cross-lane confirmation returned no parseable battle rows"
            )
            if status == "dead":
                await self._reject_upgrade_candidates(
                    candidates,
                    reason="cross-lane confirmation parseable content exhausted",
                )
            return
        resolved, unresolved = self._partition_upgrade_candidates(
            candidates, page.by_tag
        )
        self._list_telemetry.missing(confirm_lane, len(unresolved))
        confirm_observations = {
            str(candidate.get("battle_tag") or ""): observation
            for candidate, observation in unresolved
        }
        audit = {
            "state": "confirmed",
            "confirm_lane": confirm_lane,
            "confirm_page_sha256": page.sha256,
            "same_page_sha256": (
                page.sha256 == confirmation.get("first_page_sha256")
            ),
            "resolved_on_confirmation": len(resolved),
            "unresolved_after_confirmation": len(unresolved),
            "resolved_tags": [
                str(candidate.get("battle_tag") or "")
                for candidate, _metadata in resolved
            ],
            "confirm_observations": confirm_observations,
        }
        reason = (
            "deterministic metadata unresolved after one cross-lane confirmation; "
            f"first_lane={first_lane};confirm_lane={confirm_lane};"
            f"first_sha256={confirmation.get('first_page_sha256')};"
            f"confirm_sha256={page.sha256}"
        )
        await self._finalize_upgrade_confirmation(
            task,
            [candidate for candidate, _ in unresolved],
            resolved,
            reason=reason,
            confirmation_update=audit,
        )

    async def _finalize_upgrade_confirmation(
        self,
        task: dict,
        unresolved_candidates: list[dict],
        resolved: list[tuple[dict, dict]],
        *,
        reason: str,
        confirmation_update: dict,
    ) -> None:
        await self._enqueue_upgrade_candidates(task["url"], resolved)
        await self._reject_upgrade_candidates(unresolved_candidates, reason=reason)
        self._stats["upgrade_list_deterministic_rejections"] += len(
            unresolved_candidates
        )
        final_meta = dict(task.get("meta") or {})
        state = dict(final_meta.get("list_confirmation_v1") or {})
        state.update(confirmation_update)
        final_meta["list_confirmation_v1"] = state
        await self.store.update_task_meta(int(task["id"]), final_meta)
        task["meta"] = final_meta
        await self.store.mark_done(task["url"], "", task_id=task.get("id"))
        self._stats["list_pages"] += 1

    async def _reject_upgrade_candidates(
        self, candidates: list[dict], *, reason: str
    ) -> None:
        for candidate in candidates:
            await self.store.record_authoritative_result(
                str(candidate.get("battle_tag") or ""),
                status="rejected",
                tier="dependency_unresolved",
                reason=reason,
                source_path=candidate.get("source_path"),
                source_schema_version=candidate.get("source_schema_version"),
                contract_sha256=(
                    self.native_contract.contract_sha256 if self.native_contract else None
                ),
            )
            self._stats["authoritative_rejected"] += 1

    async def _process_detail(self, task: dict) -> None:
        url = task["url"]
        metadata = await self._ensure_battle_metadata(
            task, authoritative=self._authoritative_enabled
        )
        if self._authoritative_enabled and not (
            metadata and metadata.get("authoritative_complete")
        ):
            self._stats["upgrade_dependency_pending"] += 1
            status = await self._retry_task(
                task, "authoritative metadata dependency pending"
            )
            if status == "dead":
                await self.store.record_authoritative_result(
                    self._battle_tag(task),
                    status="rejected",
                    tier="dependency_unresolved",
                    reason="authoritative metadata dependency exhausted",
                    source_path=(task.get("meta") or {}).get("source_path"),
                    source_schema_version=(task.get("meta") or {}).get(
                        "source_schema_version"
                    ),
                    contract_sha256=self.native_contract.contract_sha256,
                )
            return
        if metadata and not self._eligible_metadata(metadata):
            await self.store.mark_skipped(
                url, "not eligible 1v1/version window", task_id=task.get("id")
            )
            await self.store.delete_battle_metadata(self._battle_tag(task))
            self._stats["filtered"] += 1
            log.debug("跳过非普通1v1或版本窗口外对局: %s", url)
            return
        if self.cfg.require_complete_decks and not (metadata and metadata.get("complete")):
            await self._retry_task(task, "complete deck metadata unavailable")
            log.warning("详情缺少完整卡组，暂不请求回放: %s", url)
            return
        data = await self._download_replay_payload(task)
        if data is not None:
            await self._finalize_replay(
                task, data, metadata, source_kind="downloaded"
            )

    async def _process_upgrade(self, task: dict) -> None:
        meta = task.get("meta") or {}
        if meta.get("local_schema5_contract_upgrade"):
            source_path = Path(str(meta.get("source_path") or ""))
            try:
                raw = source_path.read_bytes()
                expected_file_sha = str(meta.get("source_file_sha256") or "")
                if (
                    len(expected_file_sha) != 64
                    or hashlib.sha256(raw).hexdigest() != expected_file_sha
                ):
                    raise ValueError("schema5 reuse source SHA-256 mismatch")
                source_value = json.loads(raw)
                stamp = source_value.get("authoritative_native_contract")
                if (
                    not isinstance(stamp, dict)
                    or stamp.get("contract_sha256")
                    != meta.get("source_contract_sha256")
                    or stamp.get("contract_file_sha256")
                    != meta.get("source_contract_file_sha256")
                    or stamp.get("game_version")
                    != meta.get("source_contract_game_version")
                    or source_value.get("schema_version") != OUTPUT_SCHEMA_VERSION
                ):
                    raise ValueError("schema5 reuse source contract mismatch")
                source_value.pop("authoritative_native_contract", None)
                source_value.pop("authoritative_eligibility", None)
            except Exception as exc:  # noqa: BLE001 - fail closed, no refetch ambiguity
                await self.store.record_authoritative_result(
                    self._battle_tag(task),
                    status="rejected",
                    tier="contract_migration",
                    reason=str(exc),
                    source_path=str(source_path),
                    source_schema_version=OUTPUT_SCHEMA_VERSION,
                    contract_sha256=self.native_contract.contract_sha256,
                )
                await self.store.mark_skipped(
                    task["url"],
                    f"authoritative:contract_migration:{exc}",
                    task_id=task.get("id"),
                )
                self._stats["authoritative_rejected"] += 1
                return
            await self._finalize_replay(
                task,
                source_value,
                None,
                source_kind="reused_schema5_contract_v2",
                premerged=True,
            )
            return

        metadata = await self._ensure_battle_metadata(task, authoritative=True)
        if not metadata or not metadata.get("authoritative_complete"):
            self._stats["upgrade_dependency_pending"] += 1
            status = await self._retry_task(
                task, "authoritative metadata dependency pending"
            )
            if status == "dead":
                await self.store.record_authoritative_result(
                    self._battle_tag(task),
                    status="rejected",
                    tier="dependency_unresolved",
                    reason="authoritative metadata dependency exhausted",
                    source_path=(task.get("meta") or {}).get("source_path"),
                    source_schema_version=(task.get("meta") or {}).get(
                        "source_schema_version"
                    ),
                    contract_sha256=self.native_contract.contract_sha256,
                )
            return
        if (
            self.native_contract is not None
            and metadata.get("numeric_game_mode_id")
            not in self.native_contract.source_numeric_game_mode_ids
        ):
            battle_tag = self._battle_tag(task)
            await self.store.record_authoritative_result(
                battle_tag,
                status="rejected",
                tier="mode",
                reason="numeric_game_mode_not_allowed",
                source_path=(task.get("meta") or {}).get("source_path"),
                source_schema_version=(task.get("meta") or {}).get(
                    "source_schema_version"
                ),
                contract_sha256=self.native_contract.contract_sha256,
            )
            await self.store.mark_skipped(
                task["url"],
                "authoritative:mode:numeric_game_mode_not_allowed",
                task_id=task.get("id"),
            )
            await self.store.delete_battle_metadata(battle_tag)
            self._stats["authoritative_rejected"] += 1
            self._stats["filtered"] += 1
            return
        king_reasons = [
            f"{side}_king_tower_level_exact_evidence_missing"
            for side in ("team", "opponent")
            if resolve_king_tower_level_evidence(
                metadata, self.native_contract, side
            ) is None
        ]
        if king_reasons:
            battle_tag = self._battle_tag(task)
            reason = ";".join(king_reasons)
            await self.store.record_authoritative_result(
                battle_tag,
                status="rejected",
                tier="king_level",
                reason=reason,
                source_path=(task.get("meta") or {}).get("source_path"),
                source_schema_version=(task.get("meta") or {}).get(
                    "source_schema_version"
                ),
                contract_sha256=self.native_contract.contract_sha256,
            )
            await self.store.mark_skipped(
                task["url"],
                f"authoritative:king_level:{reason}",
                task_id=task.get("id"),
            )
            await self.store.delete_battle_metadata(battle_tag)
            self._stats["authoritative_rejected"] += 1
            self._stats["filtered"] += 1
            return
        if meta.get("local_exact_replay_body") and meta.get("source_path"):
            try:
                source_value = json.loads(
                    Path(str(meta["source_path"])).read_text(encoding="utf-8")
                )
                upgraded, _ = upgrade_exact_replay_body(source_value)
            except Exception:  # noqa: BLE001 - network refetch remains available
                upgraded = None
            if upgraded is not None:
                await self._finalize_replay(
                    task, upgraded, metadata, source_kind="reused_exact_legacy"
                )
                return
        data = await self._download_replay_payload(task)
        if data is not None:
            await self._finalize_replay(
                task, data, metadata, source_kind="upgrade_refetched"
            )

    async def _download_replay_payload(self, task: dict) -> Optional[dict]:
        url = task["url"]
        try:
            result = await self._fetch_raw(url)
        except Exception as e:
            await self._retry_task(task, str(e))
            log.error("详情任务本轮失败: %s (%s)", url, e)
            return
        if result is None:
            await self.store.mark_dead(url, "404 not found", task_id=task.get("id"))
            self._stats["dead"] += 1
            log.warning("详情 404 跳过: %s", url)
            return None
        try:
            data = result.json()
        except ValueError:
            await self._retry_task(task, "non-JSON response (可能被 Cloudflare 拦截)")
            log.warning("详情返回非 JSON，重新入队: %s", url)
            return None
        # /data/replay 返回 {"success":false,...} 表示限流/未登录等，视为失败
        if isinstance(data, dict) and data.get("success") is False:
            await self._retry_task(task, "success=false (限流或挑战)")
            log.warning("回放 success=false（限流/挑战），重新入队: %s", url)
            return None
        # /data/replay 返回 {"success":true,"html":"..."}，转成结构化格式（card_plays 等）
        if isinstance(data, dict) and data.get("html"):
            replay_html = str(data["html"])
            if "replay not found" in replay_html.lower():
                await self.store.mark_skipped(
                    url, "replay not found", task_id=task.get("id")
                )
                await self.store.delete_battle_metadata(self._battle_tag(task))
                self._stats["filtered"] += 1
                log.info("回放已过期，跳过: %s", url)
                return None
            try:
                data = parse_replay_html(replay_html)
            except ReplayParseError as exc:
                error = f"replay marker rejected: {exc}"
                await self._retry_task(task, error)
                log.warning("回放坐标无法无歧义派生，重新入队: %s (%s)", url, exc)
                return None
        if not isinstance(data, dict):
            await self._retry_task(task, "replay payload is not an object")
            return None
        return data

    async def _finalize_replay(
        self,
        task: dict,
        data: dict,
        metadata: Optional[dict],
        *,
        source_kind: str,
        premerged: bool = False,
    ) -> None:
        url = task["url"]
        if self.season_roster and not self._eligible_metadata(metadata):
            raise ValueError('回放落盘前赛季/玩家门禁不通过，未写入数据')
        if not premerged:
            data = self._merge_battle_metadata(data, metadata)
        if self.season_roster:
            data['collection_cohort'] = {**self.season_roster.summary(),
                                         'expert_sides': self._season_members(metadata)}
        if self._authoritative_enabled:
            assert self.native_contract is not None
            apply_native_contract_metadata(data, self.native_contract)
        validation_error = self._validate_replay_data(data, self._battle_tag(task))
        if validation_error:
            if validation_error == "replay has no card plays":
                await self.store.mark_skipped(
                    url, validation_error, task_id=task.get("id")
                )
                await self.store.delete_battle_metadata(self._battle_tag(task))
                self._stats["filtered"] += 1
                log.info("零出牌回放不适用于专家训练，跳过: %s", url)
                return
            if self._authoritative_enabled:
                battle_tag = self._battle_tag(task)
                await self.store.record_authoritative_result(
                    battle_tag,
                    status="rejected",
                    tier="schema",
                    reason=validation_error,
                    source_path=(task.get("meta") or {}).get("source_path"),
                    source_schema_version=(task.get("meta") or {}).get(
                        "source_schema_version"
                    ),
                    contract_sha256=(
                        self.native_contract.contract_sha256 if self.native_contract else None
                    ),
                )
                await self.store.mark_skipped(
                    url, f"authoritative:schema:{validation_error}",
                    task_id=task.get("id"),
                )
                self._stats["authoritative_rejected"] += 1
            else:
                await self._retry_task(task, validation_error)
                log.warning("回放数据验收失败，重新入队: %s (%s)", url, validation_error)
            return

        if self._authoritative_enabled:
            eligibility = evaluate_native_eligibility(
                data,
                min_battle_timestamp=self.cfg.min_battle_timestamp,
                native_contract=self.native_contract,
            )
            battle_tag = self._battle_tag(task)
            if not eligibility.accepted:
                reason = ";".join(eligibility.reasons)
                await self.store.record_authoritative_result(
                    battle_tag,
                    status="rejected",
                    tier=eligibility.tier,
                    reason=reason,
                    source_path=(task.get("meta") or {}).get("source_path"),
                    source_schema_version=(task.get("meta") or {}).get(
                        "source_schema_version"
                    ),
                    contract_sha256=self.native_contract.contract_sha256,
                )
                await self.store.mark_skipped(
                    url, f"authoritative:{eligibility.tier}:{reason}",
                    task_id=task.get("id"),
                )
                await self.store.delete_battle_metadata(battle_tag)
                self._stats["authoritative_rejected"] += 1
                self._stats["filtered"] += 1
                return
            data["authoritative_native_contract"] = {
                "game_version": self.native_contract.game_version,
                "contract_sha256": self.native_contract.contract_sha256,
                "contract_file_sha256": self.native_contract.file_sha256,
            }
            data["authoritative_eligibility"] = {
                "status": "accepted",
                "gate": "native_static_v2",
                "source_kind": source_kind,
            }
            assert self.authoritative_storage is not None
            saved = await self._file_io.call(self.authoritative_storage.save_json, url, data)
            index_record = {
                "url": url,
                "kind": "authoritative_battle",
                "battle_tag": battle_tag,
                "schema_version": OUTPUT_SCHEMA_VERSION,
                "contract_sha256": self.native_contract.contract_sha256,
                "saved_path": saved,
                "fetched_at": time.time(),
            }
            recorded = await self.store.commit_authoritative_acceptance(
                task_id=int(task["id"]),
                battle_tag=battle_tag,
                saved_path=saved,
                source_path=(task.get("meta") or {}).get("source_path"),
                source_schema_version=(task.get("meta") or {}).get(
                    "source_schema_version"
                ),
                contract_sha256=self.native_contract.contract_sha256,
                target=int(self.cfg.authoritative_target or 0),
                index_root=str(self.authoritative_storage.root),
                index_record=index_record,
            )
            await self._flush_index_outbox()
            await self.store.delete_battle_metadata(battle_tag)
            if recorded["newly_accepted"]:
                self._stats["ok"] += 1
                self._stats["authoritative_accepted"] += 1
                if source_kind in {
                    "reused_exact_legacy",
                    "reused_schema5_contract_v2",
                }:
                    self._stats["authoritative_reused"] += 1
                else:
                    self._stats["authoritative_downloaded"] += 1
            if (
                self.cfg.authoritative_target
                and int(recorded["accepted_total"]) >= self.cfg.authoritative_target
            ):
                log.info(
                    "已达到 authoritative 目标 %d 场，优雅停止…",
                    self.cfg.authoritative_target,
                )
                self._request_stop()
            return

        saved = await self._file_io.call(self.storage.save_json, url, data)
        recorded = await self.store.commit_replay(
            task_id=int(task['id']), saved_path=saved, target=self.cfg.max_battles,
            index_root=str(self.storage.root), index_record={
                "url": url, "kind": "battle", "battle_tag": self._battle_tag(task),
                "saved_path": saved, "fetched_at": time.time(),
            },
        )
        await self._flush_index_outbox()
        await self.store.delete_battle_metadata(self._battle_tag(task))
        self._stats["ok"] += int(recorded['newly_accepted'])
        log.debug("OK  %s -> %s", url, saved)
        if self.cfg.max_battles and recorded['accepted_total'] >= self.cfg.max_battles:
            log.info("已达到累计目标 %d 场，优雅停止…", self.cfg.max_battles)
            self._request_stop()

    @staticmethod
    def _battle_tag(task: dict) -> str:
        meta = task.get("meta") or {}
        if meta.get("battle_tag"):
            return str(meta["battle_tag"])
        return parse_qs(urlparse(task.get("url", "")).query).get("tag", [""])[0]

    async def _ensure_battle_metadata(
        self, task: dict, *, authoritative: bool = False
    ) -> Optional[dict]:
        battle_tag = self._battle_tag(task)
        if not battle_tag:
            return None
        metadata = await self.store.get_battle_metadata(battle_tag)
        completeness_key = "authoritative_complete" if authoritative else "complete"
        if metadata and metadata.get(completeness_key):
            return metadata
        source_url = task.get("seed")
        if not source_url or not str(source_url).startswith("http") or self.list_fetcher is None:
            return metadata
        lock = self._metadata_page_locks.setdefault(str(source_url), asyncio.Lock())
        async with lock:
            metadata = await self.store.get_battle_metadata(battle_tag)
            if metadata and metadata.get(completeness_key):
                return metadata
            if source_url not in self._metadata_page_cache:
                try:
                    result = await self._fetch_raw(str(source_url), list_request=True)
                    if result is not None:
                        battles = parse_battles(result.text, self.cfg.base_url, str(source_url))
                        self._metadata_page_cache[str(source_url)] = {
                            battle["tag"]: battle["metadata"] for battle in battles
                        }
                        while len(self._metadata_page_cache) > 128:
                            self._metadata_page_cache.popitem(last=False)
                except Exception as exc:  # noqa: BLE001
                    log.warning("补取卡组来源页失败: %s (%s)", source_url, exc)
            matched = self._metadata_page_cache.get(str(source_url), {}).get(battle_tag)
            if str(source_url) in self._metadata_page_cache:
                self._metadata_page_cache.move_to_end(str(source_url))
            if matched is not None:
                await self.store.upsert_battle_metadata([(battle_tag, matched)])
            return await self.store.get_battle_metadata(battle_tag)

    @staticmethod
    def _merge_battle_metadata(data: dict, metadata: Optional[dict]) -> dict:
        result = dict(data)
        result["schema_version"] = OUTPUT_SCHEMA_VERSION
        if not metadata:
            result["deck_metadata"] = {"complete": False, "schema_version": 1}
            return result
        result["battle_type"] = metadata.get("battle_type")
        result["game_mode"] = metadata.get("game_mode")
        result["numeric_game_mode_id"] = metadata.get("numeric_game_mode_id")
        result["numeric_game_mode_provenance"] = metadata.get(
            "numeric_game_mode_provenance"
        )
        result["battle_index"] = metadata.get("battle_index")
        result["battle_index_provenance"] = metadata.get("battle_index_provenance")
        result["matchup_players"] = metadata.get("matchup_players")
        result["draft"] = metadata.get("draft", False)
        result["timestamp"] = metadata.get("timestamp")
        result["version_timestamp"] = metadata.get("version_timestamp")
        result["version_timestamp_provenance"] = metadata.get(
            "version_timestamp_provenance"
        )
        result["battle_time_utc"] = metadata.get("battle_time_utc")
        result["battle_time_utc_provenance"] = metadata.get(
            "battle_time_utc_provenance"
        )
        result["team_tags"] = metadata.get("team_tags", [])
        result["opponent_tags"] = metadata.get("opponent_tags", [])
        result["team_crowns"] = metadata.get("team_crowns")
        result["opponent_crowns"] = metadata.get("opponent_crowns")
        result["rounds"] = metadata.get("rounds", [])
        result["final_tower_hp"] = metadata.get("final_tower_hp")
        result["normal_1v1"] = metadata.get("normal_1v1")
        result["deck_crosscheck_complete"] = metadata.get("deck_crosscheck_complete")
        result["deck_metadata"] = {
            "complete": bool(metadata.get("complete")),
            "authoritative_complete": bool(metadata.get("authoritative_complete")),
            "source": metadata.get("source"),
            "source_list_url": metadata.get("source_list_url"),
            "schema_version": metadata.get("schema_version", 1),
        }
        rounds = result["rounds"]
        if len(rounds) == 1 and len(rounds[0].get("team", [])) == 1 and len(rounds[0].get("opponent", [])) == 1:
            team_deck = rounds[0]["team"][0].get("full_deck", [])
            opponent_deck = rounds[0]["opponent"][0].get("full_deck", [])
            result["team_deck"] = team_deck
            result["opponent_deck"] = opponent_deck
            counts = result.get("card_counts", {})
            form_maps = {
                "team": {base_card_key(card): card for card in team_deck},
                "opponent": {base_card_key(card): card for card in opponent_deck},
            }
            for play in result.get("card_plays", []):
                base = base_card_key(play.get("card", ""))
                play["card_base"] = base
                play["card_form"] = form_maps.get(play.get("side"), {}).get(base)
            result["deck_validation"] = {
                "team_unplayed_cards": sorted(
                    set(form_maps["team"]) - {base_card_key(card) for card in counts.get("team", {})}
                ),
                "opponent_unplayed_cards": sorted(
                    set(form_maps["opponent"]) - {base_card_key(card) for card in counts.get("opponent", {})}
                ),
                "team_played_unknown": sorted(
                    {base_card_key(card) for card in counts.get("team", {})} - set(form_maps["team"])
                ),
                "opponent_played_unknown": sorted(
                    {base_card_key(card) for card in counts.get("opponent", {})} - set(form_maps["opponent"])
                ),
            }
        return result

    @staticmethod
    def _validate_replay_data(data: object, expected_tag: str) -> Optional[str]:
        if not isinstance(data, dict):
            return "replay payload is not an object"
        actual_tag = str(data.get("battle_tag") or "")
        if not actual_tag:
            return "replay battle_tag missing"
        if expected_tag and actual_tag != expected_tag:
            return f"replay battle_tag mismatch: {actual_tag} != {expected_tag}"
        plays = data.get("card_plays")
        if not isinstance(plays, list) or not plays:
            return "replay has no card plays"
        duration = data.get("duration_seconds")
        if not isinstance(duration, int) or isinstance(duration, bool) or not 0 < duration <= 360:
            return f"invalid replay duration: {duration}"
        abilities = data.get("ability_plays")
        if not isinstance(abilities, list):
            return "ability_plays missing"
        if data.get("schema_version") != OUTPUT_SCHEMA_VERSION:
            return f"unexpected replay schema_version: {data.get('schema_version')}"
        provenance = data.get("coordinate_provenance")
        if not isinstance(provenance, dict):
            return "coordinate_provenance missing"
        if provenance != replay_coordinate_provenance():
            return "coordinate_provenance contract mismatch"
        elixir = data.get("elixir_stats") or {}
        for side in ("team", "opponent"):
            side_plays = [item for item in plays if item.get("side") == side]
            total = (((elixir.get(side) or {}).get("Total") or {}).get("count"))
            if not isinstance(total, int) or total != len(side_plays):
                return (
                    f"card play count mismatch for {side}: "
                    f"markers={len(side_plays)}, table={total}"
                )
            expected = (
                (((elixir.get(side) or {}).get("Ability") or {}).get("count"))
                or 0
            )
            observed = sum(1 for item in abilities if item.get("side") == side)
            if int(expected) != observed:
                return (
                    f"ability count mismatch for {side}: "
                    f"markers={observed}, table={int(expected)}"
                )
        for play in plays:
            if not isinstance(play.get("time_raw"), int) or int(play["time_raw"]) < 0:
                return "invalid card play tick"
            data_i = play.get("data_i")
            x_raw = play.get("x_raw")
            y_raw = play.get("y_raw")
            if (
                isinstance(data_i, bool) or data_i not in (0, 1)
                or not isinstance(x_raw, int) or isinstance(x_raw, bool)
                or not isinstance(y_raw, int) or isinstance(y_raw, bool)
            ):
                return "invalid card play raw coordinate provenance"
            try:
                expected_x, expected_y, expected_transform = derive_native_coordinates(
                    x_raw, y_raw, data_i
                )
            except ReplayCoordinateError as exc:
                return f"invalid card play raw coordinate: {exc}"
            if play.get("coordinate_provenance") != COORDINATE_TRANSFORM_ID:
                return "card play coordinate_provenance mismatch"
            if play.get("coordinate_transform") != expected_transform:
                return "card play coordinate_transform mismatch"
            if play.get("x") != expected_x or play.get("y") != expected_y:
                return "card play native coordinate mismatch"
        for ability in abilities:
            if not isinstance(ability.get("time_raw"), int) or int(ability["time_raw"]) < 0:
                return "invalid ability tick"
            coordinate_status = ability.get("coordinate_status")
            if coordinate_status == "not_applicable":
                if ability.get("x_raw") is not None or ability.get("y_raw") is not None:
                    return "non-spatial ability carries partial raw coordinate"
                continue
            if coordinate_status != "resolved":
                return "invalid ability coordinate_status"
            try:
                expected_x, expected_y, expected_transform = derive_native_coordinates(
                    ability.get("x_raw"), ability.get("y_raw"), ability.get("data_i")
                )
            except (ReplayCoordinateError, TypeError) as exc:
                return f"invalid ability raw coordinate: {exc}"
            if ability.get("coordinate_provenance") != COORDINATE_TRANSFORM_ID:
                return "ability coordinate_provenance mismatch"
            if ability.get("coordinate_transform") != expected_transform:
                return "ability coordinate_transform mismatch"
            if ability.get("x") != expected_x or ability.get("y") != expected_y:
                return "ability native coordinate mismatch"
        return None

    async def _retry_task(self, task: dict, error: str) -> str:
        """把瞬时失败持久化回 pending；达到持久重试上限后才进入 dead。"""
        attempts = int(task.get("attempts", 0)) + 1
        delay = min(
            self.cfg.retry.max_delay * 5,
            self.cfg.persistent_retry_delay * (2 ** min(attempts - 1, 3)),
        )
        status = await self.store.mark_retry(
            task["url"], error, delay, task_id=task.get("id")
        )
        if status == "dead":
            self._stats["dead"] += 1
            log.error("任务达到持久重试上限，标记 dead: %s", task["url"])
        else:
            self._stats["retried"] += 1
            log.warning("任务 %.0fs 后重新入队(attempt=%d): %s", delay, attempts, task["url"])
        return status

    # ---------------------------------------------------------------- 抓取
    def _refresh_list_memory_guard(self) -> bool:
        before = self._list_memory_guard.paused
        paused = self._list_memory_guard.refresh()
        if paused != before:
            snapshot = self._list_memory_guard.snapshot()
            if paused:
                log.warning(
                    "可用内存 %.2f GiB < %.2f GiB：暂停领取新列表任务和扩展浏览器 profile；回放继续",
                    snapshot.get("available_gib") or 0.0,
                    snapshot["pause_below_gib"],
                )
            else:
                log.info(
                    "可用内存恢复到 %.2f GiB（恢复线 %.2f GiB）：恢复列表任务",
                    snapshot.get("available_gib") or 0.0,
                    snapshot["resume_at_gib"],
                )
        return paused

    async def _pick_list_proxy(
        self, *, exclude_lane: Optional[str] = None
    ) -> ProxyState:
        """原子选择并占用独立 Patchright profile 的令牌。"""
        while True:
            selected: ProxyState | None = None
            delay = 0.5
            async with self._list_pick_lock:
                states = self._list_proxies or ([self._list_proxy] if self._list_proxy else [])
                for state in states:
                    if not state.healthy and time.monotonic() >= state.cooldown_until:
                        state.healthy = True
                        state.consecutive_failures = 0
                candidates = [
                    state for state in states
                    if state.available_now and state.label != exclude_lane
                ]
                # Low-memory hysteresis never closes the six configured lanes;
                # it only prevents a not-yet-used profile from being launched.
                if self._refresh_list_memory_guard():
                    candidates = [
                        state for state in candidates
                        if state.label in self._list_started_labels
                    ]
                candidates.sort(
                    key=lambda state: state.bucket.available, reverse=True
                )
                for state in candidates:
                    if await state.bucket.try_acquire():
                        selected = state
                        break
                if candidates and selected is None:
                    delay = min(state.bucket.wait_seconds for state in candidates)
            if selected is not None:
                self._list_started_labels.add(selected.label)
                if selected.bucket.jitter > 0:
                    await asyncio.sleep(random.uniform(0.0, selected.bucket.jitter))
                return selected
            await asyncio.sleep(min(0.5, max(0.01, delay)))

    async def _fetch_raw(self, url: str, list_request: bool = False):
        """返回 200 的 FetchResult（原样，不解析内容）；404 返回 None；重试耗尽抛异常。"""
        result, _lane = await self._fetch_raw_with_lane(
            url, list_request=list_request
        )
        return result

    async def _fetch_raw_with_lane(
        self,
        url: str,
        *,
        list_request: bool = False,
        exclude_list_lane: Optional[str] = None,
    ):
        """Fetch raw data and retain the exact lane label for audit/metrics."""
        last_err: Optional[Exception] = None
        network_attempts = self.cfg.retry.network_attempts or self.cfg.retry.max_retries
        for attempt in range(network_attempts):
            if list_request and self.list_fetcher is not None and self._list_proxy is not None:
                proxy = await self._pick_list_proxy(exclude_lane=exclude_list_lane)
                active_fetcher = self.list_fetcher
            else:
                proxy = await self.pool.acquire()
                active_fetcher = self.fetcher
                if self._replay_global_bucket is not None:
                    await self._replay_global_bucket.acquire()
            # 其它 worker 可能在本 worker 等令牌期间触发了 429/403 冷却。
            # 必须重新选择，避免冷却中的同一出口被排队请求继续轰击。
            if not proxy.available_now:
                continue
            t0 = time.monotonic()
            if list_request:
                self._list_telemetry.request(proxy.label)
            try:
                result = await active_fetcher.fetch(proxy.url, url)
                latency = time.monotonic() - t0
            except FetcherError as e:
                latency = time.monotonic() - t0
                if list_request:
                    message = str(e).lower()
                    self._list_telemetry.error(
                        proxy.label,
                        latency,
                        challenge=any(marker in message for marker in (
                            "cloudflare", "challenge", "cf_clearance", "turnstile",
                        )),
                    )
                self.pool.report_error(proxy)
                last_err = e
                await self._backoff(attempt, e)
                continue

            status = result.status_code
            if list_request:
                self._list_telemetry.response(
                    proxy.label, latency, success=status == 200
                )

            if status == 200:
                # 回放端点会以 HTTP 200 包装业务限流 success=false。
                if "/data/replay" in url:
                    try:
                        payload = result.json()
                    except ValueError:
                        payload = None
                    if isinstance(payload, dict) and payload.get("success") is False:
                        response_html = str(payload.get("html", "")).lower()
                        if "requires login" in response_html or "需要登录" in response_html:
                            self.pool.report_auth_failure(proxy)
                            last_err = RuntimeError("replay requires login (会话失效)")
                        else:
                            self.pool.report_rate_limited(proxy)
                            last_err = RuntimeError("replay success=false (业务限流或挑战)")
                        extra = self.cfg.proxy.cooldown_429 if self.pool.size <= 1 else 0.0
                        await self._backoff(
                            attempt, last_err, extra=extra
                        )
                        continue
                self.pool.report_success(proxy, latency)
                return result, proxy.label

            if status == 404:
                if list_request:
                    self._list_telemetry.not_found(proxy.label)
                return None, proxy.label

            if status == 429:
                if list_request:
                    self._list_telemetry.rate_limited(proxy.label)
                retry_after = self._parse_retry_after(result.headers.get("retry-after"))
                self.pool.report_rate_limited(proxy, retry_after)
                last_err = RuntimeError(f"HTTP 429 Too Many Requests")
                extra = (retry_after or 0.0) if self.pool.size <= 1 else 0.0
                await self._backoff(attempt, last_err, extra=extra)
                continue

            if status == 403:
                if list_request:
                    self._list_telemetry.failure(proxy.label, challenge=True)
                self.pool.report_forbidden(proxy)
                last_err = RuntimeError(f"HTTP 403 Forbidden")
                extra = self.cfg.proxy.cooldown_403 if self.pool.size <= 1 else 0.0
                await self._backoff(attempt, last_err, extra=extra)
                continue

            if 500 <= status < 600:
                if list_request:
                    self._list_telemetry.failure(proxy.label)
                self.pool.report_error(proxy)
                last_err = RuntimeError(f"HTTP {status}")
                await self._backoff(attempt, last_err)
                continue

            if list_request:
                self._list_telemetry.failure(proxy.label)
            last_err = RuntimeError(f"unexpected status {status}")
            await self._backoff(attempt, last_err)

        raise last_err or RuntimeError("max retries exceeded")

    async def _backoff(self, attempt: int, err: Exception, extra: float = 0.0) -> None:
        base = self.cfg.retry.base_delay * (self.cfg.retry.backoff_factor ** attempt)
        delay = min(self.cfg.retry.max_delay, max(base, extra))
        jitter = random.uniform(0.0, delay * self.cfg.retry.jitter)
        self._stats["retried"] += 1
        log.warning("第 %d 次失败，%.2fs 后重试: %s", attempt + 1, delay + jitter, err)
        await asyncio.sleep(delay + jitter)

    @staticmethod
    def _parse_retry_after(value: Optional[str]) -> Optional[float]:
        if value is None:
            return None
        try:
            n = float(value)
            return n if n >= 0 and math.isfinite(n) else None
        except (ValueError, TypeError):
            try:
                return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
            except (TypeError, ValueError, OverflowError):
                return None

    # ---------------------------------------------------------------- 工具
    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._request_stop)
            except (NotImplementedError, RuntimeError):
                pass

    def _request_stop(self) -> None:
        self._stop.set()
        self._all_done.set()

    def _summary(self) -> dict:
        return {
            "stats": dict(self._stats),
            "proxies": self.pool.snapshot(),
            "list_lanes": self._list_telemetry.snapshot(),
            "list_page_cache_entries": len(self._upgrade_page_cache),
            "list_memory_guard": self._list_memory_guard.snapshot(),
            "authoritative": {
                "enabled": self._authoritative_enabled,
                "initial_accepted": self._initial_authoritative_accepted,
                "target": self.cfg.authoritative_target,
                "output_dir": (
                    self.cfg.authoritative_output_dir
                    if self._authoritative_enabled else None
                ),
                "contract_sha256": (
                    self.native_contract.contract_sha256 if self.native_contract else None
                ),
                "manifest_import": self._upgrade_manifest_summary,
            },
        }

    async def store_stats(self) -> dict[str, int]:
        return await self.store.stats()

    async def authoritative_stats(self) -> dict:
        return await self.store.authoritative_stats()

    def close(self) -> None:
        try:
            self._file_io.close()
            if hasattr(self, 'store'):
                self.store.close()
        finally:
            self._locks.close()
