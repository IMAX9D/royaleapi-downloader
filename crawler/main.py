# 用途：通用命令行入口，读取参数，选择普通、批次、赛季或高级采集模式。
# 分类：下载核心；使用：普通下载入口
# 相关文件与阅读顺序：见同目录 README.md。

"""命令行入口。

示例：
    python -m crawler.main --config config.toml --seeds seeds.txt
    python -m crawler.main --dry-run
    python -m crawler.main --selftest
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

from .config import CrawlConfig, load_config, validate_config
from .campaign import fresh_campaign, readonly_status, season_campaign
from .crawler import Crawler
from .seeds import load_seed_file


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="RoyaleAPI 对局 JSON 并行爬虫（限速 + 代理池 + 断点续传）"
    )
    p.add_argument("--config", default="config.toml", help="TOML 配置文件路径（默认 config.toml）")
    p.add_argument("--seeds", default=None, help="种子文件（每行一个 tag/URL，覆盖配置中的 seeds_file）")
    p.add_argument("--workers", type=int, default=None, help="覆盖全局并发数")
    p.add_argument("--backend", default=None, choices=["curl_cffi", "patchright", "ruyipage", "session_curl", "flaresolverr"],
                   help="覆盖底层：curl_cffi / patchright / ruyipage / session_curl / flaresolverr")
    p.add_argument("--limit", type=int, default=None, help="本轮最多处理多少任务（调试用）")
    p.add_argument("--max-battles", type=int, default=None, help="抓满 N 场对局后自动停止")
    p.add_argument("--authoritative-target", type=int, default=None,
                   help="schema-5 native-static accepted 唯一对局目标（不看旧文件总数）")
    p.add_argument("--authoritative-root", default=None,
                   help="独立 fresh authoritative 输出根目录；绝不覆盖 legacy output_dir")
    p.add_argument("--authoritative-contract", default=None,
                   help="CR-Native-Core 导出的带 SHA 冻结 native ingest contract")
    p.add_argument("--authoritative-upgrade-manifest", default=None,
                   help="启动时幂等导入历史 JSONL，两阶段刷新列表元数据后再升级")
    p.add_argument("--prepare-authoritative-upgrades", default=None, metavar="MANIFEST",
                   help="只离线建立升级/list依赖队列后退出，不发网络请求")
    p.add_argument("--authoritative-status", action="store_true",
                   help="只输出 authoritative accepted/queued/rejected 分层状态")
    p.add_argument("--seed-from", action="append", default=None, metavar="URL",
                   help="从榜单 URL 提取玩家 tag 追加到当前 seeds_file（可多次，如 Pro 玩家 /players/pro）")
    p.add_argument("--dry-run", action="store_true", help="仅打印配置与计划，不发起网络请求")
    p.add_argument("--selftest", action="store_true", help="运行无网络自检")
    p.add_argument('--campaign', help='独立新批次名称；不恢复旧库 pending，不修改旧配置')
    p.add_argument('--season', help='仅采集指定已结束赛季 YYYY-MM；必须配合 --season-roster')
    p.add_argument('--season-roster', help='带结算排名、精确起止时间的冻结玩家名单 JSON')
    p.add_argument('--season-top', type=int, default=1000, help='固定玩家池人数（默认 1000，不足拒绝启动）')
    p.add_argument('--since', help='新批次最早对局日期 YYYY-MM-DD（UTC）')
    p.add_argument('--exclude-db', action='append', default=[], help='只读导入旧库已完成 battle ID 以避免重复；可多次')
    p.add_argument('--status', action='store_true', help='只读查看计数、过滤原因和心跳新鲜度；不启动浏览器/迁移数据库')
    p.add_argument('--live-sources', action=argparse.BooleanOptionalAction, default=None,
                   help='启用/禁用进程内来源刷新、种子热加载和节制回访（新批次默认开启）')
    p.add_argument("-v", "--verbose", action="store_true", help="输出 DEBUG 日志")
    return p.parse_args(argv)


def _print_plan(cfg: CrawlConfig, seed_count: int) -> None:
    from .proxy_pool import ProxyPool
    ips = len(ProxyPool._load_proxies(cfg.proxy)) or 1
    print("=== 爬虫配置 ===")
    print(f"  目标站点     : {cfg.base_url}")
    if cfg.backend == "flaresolverr":
        print(f"  底层          : flaresolverr @ {cfg.flaresolverr_url}（自动过 Cloudflare）")
    else:
        print(f"  底层/指纹    : {cfg.backend} / impersonate={cfg.impersonate}")
    print(f"  列表端点     : {cfg.list_endpoint_template}")
    print(f"  列表浏览器   : {1 + len(cfg.list_proxy_urls)} 路 × {cfg.list_requests_per_second} req/s")
    print(f"  详情接口     : {cfg.base_url.rstrip('/')}/data/replay（由列表页 HTML 自动解析参数）")
    print("  模式门禁     : 仅普通、单轮、双方各 1 人的 1V1")
    print(f"  完整卡组     : {'强制双方 8+8 卡' if cfg.require_complete_decks else '不强制（不建议用于训练集）'}")
    print(f"  种子任务     : {cfg.season_top_n if cfg.season_roster_file else seed_count}")
    print(f"  全局并发     : {cfg.global_concurrency}")
    print(f"  有界队列     : {cfg.queue_capacity}（每批领取 {cfg.claim_batch_size}）")
    print(f"  代理数       : {ips}{'' if cfg.proxy.proxies else ' (直连单 IP 保守限速)'}")
    print(f"  单 IP 速率   : {cfg.rate_limit.requests_per_second} req/s "
          f"(burst={cfg.rate_limit.burst}, jitter={cfg.rate_limit.jitter}s)")
    print(f"  重试         : 最多 {cfg.retry.max_retries} 次，退避 {cfg.retry.base_delay}s→{cfg.retry.max_delay}s")
    print(f"  输出目录     : {cfg.output_dir}")
    if cfg.authoritative_target or cfg.authoritative_upgrade_manifest:
        print(f"  权威目标     : {cfg.authoritative_target}")
        print(f"  权威独立目录 : {cfg.authoritative_output_dir}")
        print(f"  Native契约   : {cfg.authoritative_native_contract}")
    print(f"  断点续传库   : {cfg.db_path}")
    print(f"  日期下限     : {cfg.min_battle_timestamp}（Unix UTC，缺失日期不通过）")
    print(f"  日期上限     : {cfg.max_battle_timestamp}（Unix UTC，不包含上限）")
    if cfg.season_roster_file:
        print(f"  固定赛季池   : {cfg.season_id} / {cfg.season_top_n} 人 / {cfg.season_roster_file}")
        print('  赛季采集     : 从结算时刻向前翻页；禁止榜外扩散；重复只跳过回放，不误截断历史')
    print(f"  已下载排除库 : {len(cfg.excluded_battles_databases)} 个（只导入 ID，不恢复旧任务）")
    print(f"  回放积压背压 : {cfg.detail_backlog_high} 暂停列表发现 / {cfg.detail_backlog_low} 恢复")
    discovery_plan = f'新入口优先 + 20%重复来源探索；连续 {cfg.discovery_zero_page_limit} 页无新增停止历史链' if cfg.adaptive_discovery else '兼容旧模式（未启用）'
    if cfg.season_roster_file:
        discovery_plan = '固定玩家池，历史游标严格递减，直至赛季起点/可用历史结束；不因重复页跳过更老的有效对局'
    print(f"  导航收益调度 : {discovery_plan}")
    print(f"  持续来源更新 : {'开启' if cfg.source_refresh.enabled else '关闭'}；榜单基础间隔 {cfg.source_refresh.refresh_interval:g}s，玩家回访基础间隔 {cfg.source_refresh.player_revisit_interval:g}s")
    print(f"  理论总吞吐   : ~{ips} IP × {cfg.rate_limit.requests_per_second} = "
          f"{ips * cfg.rate_limit.requests_per_second} req/s（不含重试退避）")


def _print_result(result: dict) -> None:
    stats = result.get("stats", {})
    store = result.get("store", {})
    print("\n=== 运行结果 ===")
    print(f"  列表页      : {stats.get('list_pages', 0)}")
    print(f"  对局成功    : {stats.get('ok', 0)}")
    print(f"  永久失败    : {stats.get('dead', 0)}")
    print(f"  重试次数    : {stats.get('retried', 0)}")
    print(f"  模式过滤    : {stats.get('filtered', 0)}")
    navigation = result.get('navigation') or {}
    if navigation.get('pages'):
        print(f"  列表收益    : 平均每页新增 {navigation['new_candidates_per_page']}，合格候选重复比例 {navigation['eligible_duplicate_ratio']}")
        print(f"  导航剪枝    : 零新增页 {navigation.get('zero_new_pages', 0)}，停止历史链 {navigation.get('history_cutoffs', 0)}")
    if store:
        print(f"  库内状态    : {store}")
    authoritative = result.get("authoritative") or {}
    if authoritative.get("enabled"):
        print(f"  权威目标    : {authoritative.get('target')}")
        print(f"  权威目录    : {authoritative.get('output_dir')}")
        print(f"  契约SHA     : {authoritative.get('contract_sha256')}")
        print(f"  权威分层    : {result.get('authoritative_store', {})}")
    print("\n=== 代理状态 ===")
    for row in result.get("proxies", []):
        print(f"  {row['proxy']:>12} healthy={row['healthy']} available={row['available']} "
              f"ok={row['success']} fail={row['fail']} latency={row['latency_ema']}s")


async def _run_seed_from(cfg: CrawlConfig, urls: list[str]) -> int:
    """从榜单/列表页 URL 提取玩家 tag，追加到 seeds.txt。"""
    from pathlib import Path

    from .crawler import Crawler
    from .parsers import parse_player_links

    fetcher = Crawler._make_fetcher(cfg)
    all_tags: list[str] = []
    try:
        for url in urls:
            print(f"抓取榜单: {url}")
            r = None
            errors: list[str] = []
            candidates = list(cfg.proxy.proxies) or [None]
            for proxy_url in candidates:
                try:
                    candidate = await fetcher.fetch(proxy_url, url)
                    if candidate.status_code == 200:
                        r = candidate
                        break
                    errors.append(f"{proxy_url}: HTTP {candidate.status_code}")
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{proxy_url}: {e}")
            if r is None:
                print(f"  失败: {' | '.join(errors[-3:])}")
                continue
            if r.status_code != 200:
                print(f"  [{r.status_code}] 获取失败")
                continue
            tags = parse_player_links(r.text)
            print(f"  提取 {len(tags)} 个玩家")
            all_tags.extend(tags)
    finally:
        await fetcher.aclose()

    seed_path = Path(cfg.seeds_file or 'seeds.txt')
    seed_path.parent.mkdir(parents=True, exist_ok=True)
    existing: set[str] = set()
    if seed_path.exists():
        for line in seed_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("//") and not line.startswith(";"):
                existing.add(line.lstrip("#"))
    new = [t for t in dict.fromkeys(all_tags) if t not in existing]
    with seed_path.open("a", encoding="utf-8") as f:
        for t in new:
            f.write(f"#{t}\n")
    print(f"新增 {len(new)} 个玩家到 {seed_path}（累计 {len(existing) + len(new)} 个）")
    return 0


async def _run_async(cfg: CrawlConfig, seeds: list[tuple[str, str]], limit: int | None) -> dict:
    crawler = Crawler(cfg)
    try:
        result = await crawler.run(seeds=seeds, limit=limit)
        result["store"] = await crawler.store_stats()
        result["authoritative_store"] = await crawler.authoritative_stats()
        return result
    finally:
        crawler.close()


async def _prepare_authoritative(cfg: CrawlConfig, manifest: str) -> dict:
    crawler = Crawler(cfg)
    try:
        result = await crawler.queue_authoritative_upgrade_manifest(manifest)
        result["authoritative_store"] = await crawler.authoritative_stats()
        result["tasks"] = await crawler.store_stats()
        return result
    finally:
        crawler.close()


async def _authoritative_status(cfg: CrawlConfig) -> dict:
    return await asyncio.to_thread(readonly_status, cfg)


def _fix_stdio() -> None:
    """确保任何控制台编码下都不会因个别字符不可编码而崩溃。"""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="replace")
        except Exception:
            pass


def main(argv: list[str] | None = None) -> int:
    _fix_stdio()
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )
    if not args.verbose:
        logging.getLogger("ruyipage").setLevel(logging.WARNING)

    if args.selftest:
        from .selftest import run_selftest

        return 0 if asyncio.run(run_selftest()) else 1

    cfg = load_config(args.config)
    if args.seeds:
        cfg.seeds_file = str(Path(args.seeds).resolve())
    if args.workers is not None:
        cfg.global_concurrency = args.workers
    if args.backend:
        cfg.backend = args.backend
    if args.max_battles is not None:
        cfg.max_battles = args.max_battles
    if args.authoritative_target is not None:
        if args.authoritative_target <= 0:
            raise SystemExit("--authoritative-target 必须是正整数")
        cfg.authoritative_target = args.authoritative_target
    if args.authoritative_root:
        cfg.authoritative_output_dir = args.authoritative_root
    if args.authoritative_contract:
        cfg.authoritative_native_contract = args.authoritative_contract
    if args.authoritative_upgrade_manifest:
        cfg.authoritative_upgrade_manifest = args.authoritative_upgrade_manifest
    if args.prepare_authoritative_upgrades:
        cfg.authoritative_upgrade_manifest = args.prepare_authoritative_upgrades

    if args.season or args.season_roster:
        if not args.season or not args.season_roster:
            raise SystemExit('--season 与 --season-roster 必须同时指定')
        if args.since or args.seeds or args.seed_from or args.live_sources or args.prepare_authoritative_upgrades:
            raise SystemExit('固定赛季批次不能同时指定动态来源、外部种子、since 或升级任务')
        cfg = season_campaign(cfg, args.campaign or f'season-{args.season}-top{args.season_top}',
                              args.season, args.season_roster, args.season_top, args.exclude_db)
        if args.max_battles is not None:
            cfg.max_battles = args.max_battles
    elif args.campaign:
        if not args.since or args.max_battles is None:
            raise SystemExit('--campaign 必须同时指定 --since YYYY-MM-DD 和 --max-battles N')
        cfg = fresh_campaign(cfg, args.campaign, args.since, args.max_battles, args.exclude_db)
    elif args.since or args.exclude_db:
        raise SystemExit('--since / --exclude-db 仅用于 --campaign，以免意外改变旧批次')
    if args.live_sources is not None:
        cfg.source_refresh.enabled = args.live_sources
        if args.live_sources:
            cfg.adaptive_discovery = True
    validate_config(cfg)
    if args.limit is not None and args.limit <= 0:
        raise SystemExit('--limit 必须为正数')

    if args.status:
        print(json.dumps(readonly_status(cfg), ensure_ascii=False, indent=2))
        return 0

    if args.authoritative_status:
        if not (cfg.authoritative_target or cfg.authoritative_upgrade_manifest):
            raise SystemExit("authoritative status 需要 target 或 upgrade manifest 配置")
        print(json.dumps(
            asyncio.run(_authoritative_status(cfg)), ensure_ascii=False, indent=2
        ))
        return 0

    if args.prepare_authoritative_upgrades:
        result = asyncio.run(_prepare_authoritative(
            cfg, args.prepare_authoritative_upgrades
        ))
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print("[prepare-only] 未发起网络请求；下次正常启动从 SQLite 断点队列继续。")
        return 0

    # 种子采集模式：从榜单 URL 提取玩家 tag，不做抓取
    if args.seed_from:
        return asyncio.run(_run_seed_from(cfg, args.seed_from))

    seeds: list[tuple[str, str]] = []
    if args.seeds:
        seeds = load_seed_file(args.seeds)
    elif cfg.seeds_file:
        seeds = load_seed_file(cfg.seeds_file)

    _print_plan(cfg, len(seeds))

    if args.dry_run:
        print("\n[dry-run] 未发起网络请求。")
        return 0

    result = asyncio.run(_run_async(cfg, seeds, args.limit))
    _print_result(result)

    print(f"\n提示：断点续传库 {cfg.db_path}，重跑同一命令会跳过已完成任务。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
