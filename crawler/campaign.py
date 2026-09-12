"""Isolated fresh collection campaigns and genuinely read-only status."""
from __future__ import annotations

import dataclasses
import datetime as dt
import json
import re
import sqlite3
import time
from pathlib import Path

from .config import CrawlConfig, validate_config
from .discovery import navigation_summary
from .source_store import read_live_source_status
from .season import load_roster


def season_campaign(cfg: CrawlConfig, name: str, season: str, roster_file: str,
                    top_n: int = 1000, excluded_databases: list[str] | None = None) -> CrawlConfig:
    """No network/DB writes: bind a complete ranked cohort before startup."""
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}', name):
        raise ValueError('批次名称只允许英文字母、数字、下划线、连字符，最长 64 字符')
    if cfg.authoritative_target or cfg.authoritative_upgrade_manifest:
        raise ValueError('赛季采集请基于普通配置，不导入旧权威升级队列')
    roster = load_roster(roster_file, expected_season=season, expected_top=top_n)
    root = Path(cfg.output_dir).resolve() / 'campaigns' / name
    result = dataclasses.replace(cfg, output_dir=str(root), db_path=str(root / 'progress.sqlite3'),
        campaign_id=name, list_refresh_epoch=name, min_battle_timestamp=roster.start,
        max_battle_timestamp=roster.end, season_id=season, season_top_n=top_n,
        season_roster_file=str(Path(roster_file).resolve(strict=True)), seeds_file=None,
        discover_players=False, require_complete_decks=True, adaptive_discovery=True,
        # Traverse the closed season instead of stopping at 1/20 pages. This is
        # a safety ceiling, not a claim that every player has 10,000 pages.
        max_pages_per_player=10000, max_battles=None,
        detail_backlog_high=20000, detail_backlog_low=10000,
        source_refresh=dataclasses.replace(cfg.source_refresh, enabled=False),
        retry=dataclasses.replace(cfg.retry, network_attempts=2),
        excluded_battles_databases=[str(Path(path).resolve(strict=True)) for path in dict.fromkeys(
            [*cfg.excluded_battles_databases, *(excluded_databases or [])])])
    validate_config(result)
    return result


def fresh_campaign(cfg: CrawlConfig, name: str, since: str, target: int,
                   excluded_databases: list[str] | None = None) -> CrawlConfig:
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}', name):
        raise ValueError('批次名称只允许英文字母、数字、下划线、连字符，最长 64 字符')
    if cfg.authoritative_target or cfg.authoritative_upgrade_manifest:
        raise ValueError('新通用批次请基于 config.toml，不要基于旧 authoritative 配置；严格门禁不会被自动放宽')
    timestamp = int(dt.datetime.combine(dt.date.fromisoformat(since), dt.time(), dt.timezone.utc).timestamp())
    root = Path(cfg.output_dir).resolve() / 'campaigns' / name
    result = dataclasses.replace(
        cfg, output_dir=str(root), db_path=str(root / 'progress.sqlite3'),
        max_battles=target, min_battle_timestamp=timestamp, list_refresh_epoch=name, campaign_id=name,
        require_complete_decks=True, max_pages_per_player=1,
        detail_backlog_high=20000, detail_backlog_low=10000,
        adaptive_discovery=True,
        source_refresh=dataclasses.replace(cfg.source_refresh, enabled=True),
        retry=dataclasses.replace(cfg.retry, network_attempts=2),
        excluded_battles_databases=[
            str(Path(path).resolve(strict=True)) for path in dict.fromkeys(
                [*cfg.excluded_battles_databases, *(excluded_databases or [])]
            )
        ],
    )
    validate_config(result)
    return result


def readonly_status(cfg: CrawlConfig) -> dict:
    path = Path(cfg.db_path).resolve()
    result = {
        'db_path': str(path), 'exists': path.exists(),
        'target': cfg.authoritative_target or cfg.max_battles,
        'output_dir': cfg.authoritative_output_dir if cfg.authoritative_target else cfg.output_dir,
        'tasks': {}, 'results': {'status': {}, 'tiers': {}},
    }
    if path.exists():
        conn = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)
        try:
            conn.execute('PRAGMA query_only=ON')
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if 'task_counts' in tables:
                rows = conn.execute('SELECT kind,status,total FROM task_counts WHERE total>0')
            else:
                rows = conn.execute('SELECT kind,status,COUNT(*) FROM tasks GROUP BY kind,status')
            for kind, status, count in rows:
                result['tasks'].setdefault(kind, {})[status] = count
            if 'authoritative_results' in tables:
                query = (
                    'SELECT status,tier,SUM(total) FROM authoritative_counts WHERE total>0 GROUP BY status,tier'
                    if 'authoritative_counts' in tables else
                    'SELECT status,tier,COUNT(*) FROM authoritative_results GROUP BY status,tier'
                )
                for status, tier, count in conn.execute(query):
                    groups = result['results']
                    groups['status'][status] = groups['status'].get(status, 0) + count
                    groups['tiers'][status + ':' + tier] = count
            result['pending_index_records'] = (
                conn.execute('SELECT COUNT(*) FROM index_outbox').fetchone()[0]
                if 'index_outbox' in tables else 0
            )
            if 'navigation_totals' in tables:
                result['navigation'] = navigation_summary(dict(conn.execute('SELECT name,total FROM navigation_totals')))
            if 'seed_sources' in tables:
                result['live_sources'] = read_live_source_status(conn)
        finally:
            conn.close()
    live = Path(cfg.output_dir) / 'lanes' / 'crawler-proxies.json'
    if live.exists():
        try:
            value = json.loads(live.read_text(encoding='utf-8'))
            result['runtime'] = {key: value.get(key) for key in (
                'phase', 'updated_at', 'battle_rate', 'total_done', 'target',
                'list_memory_guard', 'list_backlog_paused', 'error', 'stats', 'navigation', 'live_sources',
            )}
            age = max(0, time.time() - float(value.get('updated_at', 0)))
            result['runtime']['heartbeat_age_seconds'] = round(age, 1)
            result['runtime']['stale'] = age > 15
            # A stale metrics file is not proof of a running process.
            if age > 15 and result['runtime']['phase'] in (None, 'running', 'initializing', 'waiting_for_sources'):
                result['runtime']['phase'] = 'stale_unknown'
        except (OSError, ValueError, TypeError):
            result['runtime'] = {'phase': 'unreadable'}
    return result
