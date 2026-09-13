# 用途：配置结构、TOML 加载、路径解析及参数校验。
# 分类：下载核心；使用：修改配置行为时阅读
# 相关文件与阅读顺序：见同目录 README.md。

"""配置模块：所有可调参数集中于此，支持 TOML 文件覆盖默认值。

参数按"每 IP 限速 / 重试 / 代理池 / 全局 + 两级流水线端点"分组。
"""
from __future__ import annotations

import dataclasses
import math
import tomllib
import types
from pathlib import Path
from typing import Optional, Union, get_args, get_origin, get_type_hints


@dataclasses.dataclass
class RateLimitConfig:
    """每个 IP（或每个代理）的限速参数。

    关键思想：总吞吐 ≈ 代理数 × requests_per_second，而不是单 IP 无限并发。
    单一 IP 的 requests_per_second 保持在保守值，即可规避"短暂封 IP"。
    """

    requests_per_second: float = 1.0  # 单 IP 持续速率（请求/秒），保守默认 1.0
    burst: int = 2                    # 令牌桶容量：允许的瞬时突发请求数
    jitter: float = 0.4               # 每次请求前附加的随机延迟上限（秒）


@dataclasses.dataclass
class RetryConfig:
    max_retries: int = 6              # 单个任务最大重试次数（含首次）
    base_delay: float = 1.0           # 首次退避延迟（秒）
    max_delay: float = 120.0          # 退避延迟上限（秒）
    backoff_factor: float = 2.0       # 指数退避底数：delay = base * factor^attempt
    jitter: float = 0.5               # 退避随机抖动比例
    network_attempts: Optional[int] = None  # 每次领取的网络尝试；None 保持旧行为


@dataclasses.dataclass
class ProxyPoolConfig:
    enabled: bool = True
    proxies: list[str] = dataclasses.field(default_factory=list)  # "http://user:pass@host:port"
    proxies_file: str = "proxies.txt"  # 每行一个代理的文本文件；留空则只用 proxies 列表
    health_check_url: str = "https://royaleapi.com/"
    health_check_interval: float = 60.0
    health_check_timeout: float = 10.0
    cooldown_429: float = 60.0
    cooldown_403: float = 300.0
    cooldown_error: float = 30.0
    max_consecutive_failures: int = 5


@dataclasses.dataclass
class SourceRefreshConfig:
    enabled: bool = False
    urls: list[str] = dataclasses.field(default_factory=lambda: ['/players/leaderboard', '/players/pro'])
    refresh_interval: float = 1800.0
    max_interval: float = 14400.0
    error_retry_delay: float = 300.0
    poll_interval: float = 30.0
    max_pending_tasks: int = 2
    max_sources: int = 256
    max_players_per_page: int = 20000
    seed_file_poll_interval: float = 60.0
    player_revisit_interval: float = 3600.0
    player_revisit_max_interval: float = 21600.0
    player_revisit_batch_size: int = 32
    player_revisit_pending_limit: int = 128
    fresh_player_low_watermark: int = 256


@dataclasses.dataclass
class CrawlConfig:
    base_url: str = "https://royaleapi.com"

    # --- 底层 HTTP 与 Cloudflare 绕过 ---
    backend: str = "curl_cffi"        # 底层：curl_cffi | patchright | ruyipage | session_curl | flaresolverr
    impersonate: str = "chrome"       # curl_cffi 指纹目标：chrome/chrome124/chrome131/safari...
    captured_file: Optional[str] = None  # capture.py 导出的 JSON（cookie/headers/端点）
    flaresolverr_url: str = "http://localhost:8191"  # backend=flaresolverr 时的服务地址
    browser_profile_dir: str = "data/browser_profile"  # backend=patchright 时的持久化用户目录（保存登录态）
    browser_headless: bool = False   # profile 完成挑战后可无头长期运行
    list_browser_headless: bool = False  # Patchright 列表浏览器需保留有头抗挑战能力

    global_concurrency: int = 8       # 全局同时在飞请求上限（跨所有代理）
    queue_capacity: int = 2000        # 内存有界队列容量；百万任务不能一次全部载入
    claim_batch_size: int = 500       # 每次从 SQLite 租约领取的任务数
    persistent_retry_delay: float = 300.0  # 一轮网络重试耗尽后重新入队的延迟
    list_requests_per_second: float = 0.3  # 每个列表浏览器出口的独立速率
    list_proxy_urls: list[str] = dataclasses.field(default_factory=list)  # 额外列表浏览器出口/profile
    # Browser profiles are the dominant RAM consumer.  New list work/profile
    # expansion pauses below 8 GiB and resumes only after 10 GiB, avoiding
    # threshold oscillation while replay work keeps draining normally.
    list_pause_free_memory_gb: float = 8.0
    list_resume_free_memory_gb: float = 10.0
    list_memory_check_interval: float = 2.0
    replay_global_requests_per_second: float = 0.3  # 单登录会话全局回放速率
    ruyi_auth_map_file: Optional[str] = None  # proxy -> 独立 Ruyi profile 的 JSON 映射
    require_complete_decks: bool = False  # 为 true 时不落盘缺少完整卡组的新回放
    request_timeout: float = 20.0
    connect_timeout: float = 10.0
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
    referer: str = "https://royaleapi.com/"
    output_dir: str = "data"
    db_path: str = "data/progress.sqlite3"
    seeds_file: Optional[str] = None
    # JSONL records containing battle_tag. Imported at startup so resetting the
    # progress DB cannot silently re-download the training corpus.
    excluded_battles_manifest: Optional[str] = None
    excluded_battles_databases: list[str] = dataclasses.field(default_factory=list)
    detail_backlog_high: int = 0     # 0 关闭背压；新批次推荐 20000
    detail_backlog_low: int = 0      # 高水位暂停发现，降到低水位后恢复
    adaptive_discovery: bool = False  # 新批次开启；旧配置保留原导航策略
    discovery_zero_page_limit: int = 2  # 连续多少页无新增后停止该历史翻页链
    # Optional inclusive lower bound for battle-list data-timestamp (Unix sec).
    # Use this to keep native-replay training data within one balance patch.
    min_battle_timestamp: Optional[int] = None
    # Exclusive upper bound; needed for CLOSED seasons, not just fresh replay collection.
    max_battle_timestamp: Optional[int] = None
    season_roster_file: Optional[str] = None
    season_id: Optional[str] = None
    season_top_n: int = 1000
    # Refresh each player's first page once per collection round while keeping
    # detail replay deduplication global by battle tag.
    list_refresh_epoch: Optional[str] = None
    campaign_id: Optional[str] = None  # 固定批次的数据选择条件，避免续跑时混入不同日期范围
    quality_monitor_state_file: Optional[str] = None

    # --- schema-5 authoritative native replay corpus ---
    # Only unique rows admitted by the native-static gate under this root count
    # toward the target. The legacy output_dir is never overwritten.
    authoritative_target: Optional[int] = None
    authoritative_output_dir: str = "data/authoritative-fresh"
    # JSONL manifest of historical replay files to reuse/upgrade. Schema 3/4
    # exact event bodies can be reused after list metadata refresh; schema 1/2
    # require a replay refetch because their ability ticks are not exact.
    authoritative_upgrade_manifest: Optional[str] = None
    # Mandatory frozen mapping contract exported by CR-Native-Core. It carries
    # its own canonical SHA and is the sole allowlist for game modes, card
    # base+forms, tower troops and active-ability sources.
    authoritative_native_contract: Optional[str] = None
    authoritative_game_version: str = "15.535.29"

    # --- 两级流水线 ---
    # 第一级：由玩家 tag 拿到"对局列表页"（服务端渲染 HTML）。
    # 第二级：解析列表页里每场对局的 replay 参数，拼 /data/replay 请求（见 parsers.py）。
    # 占位符：{base} {tag}
    list_endpoint_template: str = "{base}/player/{tag}/battles"
    # 是否保存第一级"列表页"原始 HTML（便于回放/去重/分析）
    save_lists: bool = True
    # 雪球式发现：从每场对局里提取双方玩家 tag，自动入队新玩家（大规模抓取必需）
    discover_players: bool = True
    # 抓满 N 场成功对局后自动停止（None=不限）
    max_battles: Optional[int] = None
    # 每个玩家最多翻多少页对局列表（每页约 25 场；翻页走 /battles/history?before=...）
    max_pages_per_player: int = 20

    rate_limit: RateLimitConfig = dataclasses.field(default_factory=RateLimitConfig)
    retry: RetryConfig = dataclasses.field(default_factory=RetryConfig)
    proxy: ProxyPoolConfig = dataclasses.field(default_factory=ProxyPoolConfig)
    source_refresh: SourceRefreshConfig = dataclasses.field(default_factory=SourceRefreshConfig)


def _matches(value, annotation) -> bool:
    origin = get_origin(annotation)
    if origin in (Union, types.UnionType):
        return any(_matches(value, item) for item in get_args(annotation))
    if origin is list:
        return isinstance(value, list) and all(_matches(item, get_args(annotation)[0]) for item in value)
    if annotation is float:
        return type(value) in (float, int) and math.isfinite(value)
    return type(value) is annotation


def validate_config(cfg: CrawlConfig) -> None:
    def check(obj, prefix=''):
        for name, annotation in get_type_hints(type(obj)).items():
            value = getattr(obj, name)
            if dataclasses.is_dataclass(value):
                check(value, prefix + name + '.')
            elif not _matches(value, annotation):
                raise ValueError(f'配置类型错误: {prefix}{name}')
    check(cfg)
    positive = (
        cfg.global_concurrency, cfg.queue_capacity, cfg.claim_batch_size,
        cfg.request_timeout, cfg.connect_timeout, cfg.max_pages_per_player,
        cfg.rate_limit.requests_per_second, cfg.rate_limit.burst,
        cfg.list_requests_per_second, cfg.retry.max_retries,
        cfg.retry.network_attempts if cfg.retry.network_attempts is not None else 1,
        cfg.max_battles if cfg.max_battles is not None else 1,
        cfg.authoritative_target if cfg.authoritative_target is not None else 1,
        cfg.proxy.health_check_interval, cfg.proxy.health_check_timeout,
        cfg.proxy.max_consecutive_failures, cfg.list_memory_check_interval,
        cfg.discovery_zero_page_limit,
        cfg.source_refresh.refresh_interval, cfg.source_refresh.max_interval,
        cfg.source_refresh.error_retry_delay, cfg.source_refresh.poll_interval,
        cfg.source_refresh.max_pending_tasks, cfg.source_refresh.max_sources,
        cfg.source_refresh.max_players_per_page, cfg.source_refresh.seed_file_poll_interval,
        cfg.source_refresh.player_revisit_interval, cfg.source_refresh.player_revisit_max_interval,
        cfg.source_refresh.player_revisit_batch_size, cfg.source_refresh.player_revisit_pending_limit,
        cfg.source_refresh.fresh_player_low_watermark,
    )
    if any(value <= 0 for value in positive):
        raise ValueError('并发、队列、超时、重试次数、速率和目标必须为正数')
    if cfg.backend not in {'curl_cffi', 'session_curl', 'patchright', 'ruyipage', 'flaresolverr'}:
        raise ValueError('未知 backend')
    if not 0 <= cfg.detail_backlog_low <= cfg.detail_backlog_high:
        raise ValueError('背压需要 0 <= detail_backlog_low <= detail_backlog_high')
    if not 0 <= cfg.list_pause_free_memory_gb < cfg.list_resume_free_memory_gb:
        raise ValueError('内存恢复线必须高于暂停线')
    if any(value < 0 for value in (
        cfg.rate_limit.jitter, cfg.retry.base_delay, cfg.retry.max_delay,
        cfg.retry.jitter, cfg.persistent_retry_delay, cfg.replay_global_requests_per_second,
        cfg.proxy.cooldown_429, cfg.proxy.cooldown_403, cfg.proxy.cooldown_error,
    )) or cfg.retry.backoff_factor < 1:
        raise ValueError('退避/冷却/抖动不能为负，退避倍数不能小于 1')
    if cfg.authoritative_upgrade_manifest and not cfg.authoritative_target:
        raise ValueError('历史权威升级必须指定 authoritative_target')
    if cfg.source_refresh.max_interval < cfg.source_refresh.refresh_interval:
        raise ValueError('source_refresh.max_interval 不能小于 refresh_interval')
    if cfg.source_refresh.player_revisit_max_interval < cfg.source_refresh.player_revisit_interval:
        raise ValueError('玩家最大回访间隔不能小于基础回访间隔')
    if cfg.max_battle_timestamp is not None and (
        cfg.min_battle_timestamp is None or cfg.max_battle_timestamp <= cfg.min_battle_timestamp
    ):
        raise ValueError('日期上限必须晚于日期下限；上限不包含在采集范围内')
    if not 1 <= cfg.season_top_n <= 10000:
        raise ValueError('赛季玩家数必须介于 1 和 10000')
    if cfg.season_roster_file:
        if not cfg.season_id or not cfg.campaign_id or cfg.max_battle_timestamp is None:
            raise ValueError('固定赛季名单必须绑定独立批次、赛季及明确时间上下限')
        if cfg.discover_players or cfg.source_refresh.enabled or cfg.seeds_file:
            raise ValueError('固定赛季批次禁止雪球扩散、动态来源及外部种子')
        if not cfg.adaptive_discovery:
            raise ValueError('固定赛季批次必须开启受控历史游标调度')
        if cfg.authoritative_target or cfg.authoritative_upgrade_manifest:
            raise ValueError('固定赛季批次不恢复旧 authoritative 升级任务')
    if cfg.source_refresh.enabled and not cfg.adaptive_discovery:
        raise ValueError('实时来源需要同时开启 adaptive_discovery')


def load_config(path: str | Path) -> CrawlConfig:
    """Fail fast on typos; all configured paths are relative to this TOML."""
    cfg = CrawlConfig()
    p = Path(path).resolve(strict=True)
    with p.open('rb') as handle:
        data = tomllib.load(handle)

    def apply(obj, values, prefix=''):
        fields = {field.name for field in dataclasses.fields(obj)}
        for name, value in values.items():
            if name not in fields:
                raise ValueError(f'未知配置项: {prefix}{name}')
            current = getattr(obj, name)
            if dataclasses.is_dataclass(current):
                if not isinstance(value, dict):
                    raise ValueError(f'配置项必须是表: {prefix}{name}')
                apply(current, value, prefix + name + '.')
            else:
                setattr(obj, name, value)
    apply(cfg, data)
    validate_config(cfg)

    def resolve(value):
        return str((p.parent / value).resolve()) if value else value
    for name in (
        'captured_file', 'browser_profile_dir', 'ruyi_auth_map_file', 'output_dir',
        'db_path', 'seeds_file', 'excluded_battles_manifest', 'quality_monitor_state_file', 'season_roster_file',
        'authoritative_output_dir', 'authoritative_upgrade_manifest', 'authoritative_native_contract',
    ):
        setattr(cfg, name, resolve(getattr(cfg, name)))
    cfg.proxy.proxies_file = resolve(cfg.proxy.proxies_file)
    cfg.excluded_battles_databases = [resolve(value) for value in cfg.excluded_battles_databases]
    return cfg
