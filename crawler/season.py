# 用途：导入有排名来源和时间边界的名单，校验玩家资格。
# 分类：玩家发现、批次与持续采集；使用：固定赛季或历史高手名单
# 相关文件与阅读顺序：见同目录 README.md。

"""Closed-season ranked cohorts. Missing ranks never turn into random seeds.

Only parses data; never evaluates leaderboard JavaScript or starts a browser.
Season timestamps are explicit source-backed UTC instants, NOT calendar months.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from selectolax.parser import HTMLParser

from .storage import Storage

SEASON_ID = re.compile(r'20\d{2}-(?:0[1-9]|1[0-2])\Z')
PLAYER_TAG = re.compile(r'[A-Z0-9]{3,32}\Z')


def utc_timestamp(value: str) -> int:
    if not isinstance(value, str):
        raise ValueError('赛季边界必须是带时区的 ISO 时间')
    try:
        stamp = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise ValueError('赛季边界必须是带时区的 ISO 时间') from exc
    if stamp.tzinfo is None or stamp.microsecond:
        raise ValueError('赛季边界必须包含时区，且精确到整秒')
    return int(stamp.timestamp())


def battle_timestamp(metadata: dict | None) -> int | None:
    raw = (metadata or {}).get('timestamp')
    # Never truncate a float, accept bool, or guess from a display date.
    if type(raw) is int:
        stamp = raw
    elif isinstance(raw, str) and re.fullmatch(r'\d+', raw):
        stamp = int(raw)
    else:
        return None
    if stamp > 10_000_000_000:
        stamp //= 1000
    return stamp if stamp > 0 else None


def normalize_player(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError('玩家 Tag 必须是字符串')
    tag = value.strip().lstrip('#').upper()
    if not PLAYER_TAG.fullmatch(tag):
        raise ValueError('无效玩家 Tag')
    return tag


def ranked_players(rows: list[dict], top_n: int) -> tuple[tuple[int, str], ...]:
    if type(top_n) is not int or not 1 <= top_n <= 10000:
        raise ValueError('Top 人数必须介于 1 和 10000')
    if not isinstance(rows, list):
        raise ValueError('players 必须是排名记录列表')
    ranks: dict[int, str] = {}
    tags: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict) or type(row.get('rank')) is not int or row['rank'] < 1:
            raise ValueError('每条排名必须包含正整数 rank')
        rank, tag = row['rank'], normalize_player(row.get('tag'))
        if rank > top_n:
            continue
        if (rank in ranks and ranks[rank] != tag) or (tag in tags and tags[tag] != rank):
            raise ValueError('榜单排名或玩家冲突，不能拼接不同赛季/不同榜单')
        ranks[rank], tags[tag] = tag, rank
    missing = sorted(set(range(1, top_n + 1)) - ranks.keys())
    if missing:
        raise ValueError(f'Top {top_n} 名单不完整：只有 {len(ranks)} 人；缺失排名示例 {missing[:8]}。不会降级或用榜外玩家补数')
    return tuple(sorted(ranks.items()))


def historical_pool(rows: list[dict], pool_size: int, latest_season: str) -> tuple[tuple[str, str, int], ...]:
    """First qualified unique accounts, bounded at pool_size; record proof season."""
    if type(pool_size) is not int or not 1 <= pool_size <= 10000 or not isinstance(rows, list):
        raise ValueError('无效玩家池人数/记录')
    selected: dict[str, tuple[str, str, int]] = {}
    evidence: dict[tuple[str, str], int] = {}
    for row in rows:
        if not isinstance(row, dict) or type(row.get('rank')) is not int:
            raise ValueError('历史记录必须包含整数结算排名')
        season = row.get('rank_season')
        if not isinstance(season, str) or not SEASON_ID.fullmatch(season) or season > latest_season:
            raise ValueError('历史结算记录缺少有效已结束赛季，不能使用当前实时排名')
        tag, rank = normalize_player(row.get('tag')), row['rank']
        key = (tag, season)
        if key in evidence and evidence[key] != rank:
            raise ValueError('同一玩家同一赛季结算排名矛盾')
        evidence[key] = rank
        if 1 <= rank <= 10000 and tag not in selected and len(selected) < pool_size:
            selected[tag] = (tag, season, rank)
    if len(selected) != pool_size:
        raise ValueError(f'历史 Top 10000 合格玩家池不足：{len(selected)} / {pool_size}；不会混入未验证玩家')
    return tuple(selected.values())


@dataclass(frozen=True)
class SeasonRoster:
    season_id: str
    top_n: int
    start: int
    end: int
    players: tuple[tuple[int, str], ...]
    source_urls: tuple[str, ...]
    boundary_source: str
    proofs: tuple[tuple[str, str, int], ...] = ()

    @property
    def ranks(self) -> dict[str, int]:
        return {tag: rank for rank, tag in self.players}

    @property
    def contract(self) -> dict:
        return {'season_id': self.season_id, 'ranking_type': 'path_of_legends',
                'top_n': self.top_n, 'start_inclusive': self.start,
                'end_exclusive': self.end, 'players': self.players,
                'historical_proofs': self.proofs}

    @property
    def sha256(self) -> str:
        return hashlib.sha256(json.dumps(self.contract, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

    def summary(self) -> dict:
        return {'season_id': self.season_id, 'ranking_type': 'path_of_legends',
                'top_n': self.top_n, 'verified_player_count': len(self.players),
                'start_inclusive': self.start, 'end_exclusive': self.end,
                'roster_sha256': self.sha256, 'source_urls': self.source_urls,
                'boundary_source': self.boundary_source,
                'eligibility': 'historical_top10000' if self.proofs else 'season_top_n'}

    def rank_season(self, tag: str) -> str:
        return next((season for player, season, _rank in self.proofs if player == tag), self.season_id)

    def manifest(self) -> dict:
        iso = lambda stamp: dt.datetime.fromtimestamp(stamp, dt.timezone.utc).isoformat().replace('+00:00', 'Z')
        value = {'schema_version': 1, 'season_id': self.season_id,
                'ranking_type': 'path_of_legends', 'top_n': self.top_n,
                'start_utc': iso(self.start), 'end_utc': iso(self.end),
                'source_urls': list(self.source_urls), 'boundary_source': self.boundary_source,
                'players': [{'rank': rank, 'tag': tag} for rank, tag in self.players],
                'roster_sha256': self.sha256}
        if self.proofs:
            value['eligibility'] = 'historical_top10000'
            value['players'] = [{'tag': tag, 'rank_season': season, 'rank': rank} for tag, season, rank in self.proofs]
        return value


def parse_manifest(value: dict, *, expected_season: str | None = None,
                   expected_top: int | None = None, now: float | None = None) -> SeasonRoster:
    if not isinstance(value, dict) or type(value.get('schema_version')) is not int or value.get('schema_version') != 1:
        raise ValueError('需要 schema_version=1 的赛季名单')
    season = value.get('season_id', '')
    if not isinstance(season, str) or not SEASON_ID.fullmatch(season):
        raise ValueError('赛季 ID 必须是 YYYY-MM')
    if expected_season is not None and season != expected_season:
        raise ValueError('名单赛季与请求赛季不一致')
    if value.get('ranking_type') != 'path_of_legends':
        raise ValueError('必须是全球排位赛季最终榜，不接受天梯杯数榜/国家榜')
    if value.get('eligibility', 'season_top_n') not in ('historical_top10000', 'season_top_n'):
        raise ValueError('未知入池资格规则')
    top = value.get('top_n')
    if expected_top is not None and top != expected_top:
        raise ValueError('名单 Top 人数与请求不一致')
    proofs = historical_pool(value.get('players'), top, season) if value.get('eligibility') == 'historical_top10000' else ()
    players = tuple((rank, tag) for tag, _season, rank in proofs) if proofs else ranked_players(value.get('players'), top)
    start, end = utc_timestamp(value.get('start_utc')), utc_timestamp(value.get('end_utc'))
    if start <= 0 or not 0 < end - start <= 62 * 86400:
        raise ValueError('赛季时间范围无效')
    now = dt.datetime.now(dt.timezone.utc).timestamp() if now is None else now
    if end > now:
        raise ValueError('赛季尚未结束，不能当作已结束赛季最终榜')
    if dt.datetime.fromtimestamp(start, dt.timezone.utc).strftime('%Y-%m') != season:
        raise ValueError('赛季起始时间与赛季 ID 不一致')
    urls = value.get('source_urls')
    if not isinstance(urls, list) or not urls or not all(
        isinstance(url, str) and urlsplit(url).scheme == 'https' and urlsplit(url).netloc
        and not urlsplit(url).username and not urlsplit(url).password for url in urls
    ):
        raise ValueError('需要不含凭据的 HTTPS 榜单来源 URL')
    boundary = value.get('boundary_source')
    if not isinstance(boundary, str) or not boundary.strip():
        raise ValueError('必须记录精确赛季起止时间的来源，不能默认月初/月末')
    roster = SeasonRoster(season, top, start, end, players, tuple(urls), boundary, proofs)
    if value.get('roster_sha256') is not None and value['roster_sha256'] != roster.sha256:
        raise ValueError('名单 SHA256 不符')
    return roster


def load_roster(path: str, **kwargs) -> SeasonRoster:
    with Path(path).open('r', encoding='utf-8-sig') as handle:
        return parse_manifest(json.load(handle), **kwargs)


def parse_roster_html(html: str, season_id: str) -> list[dict]:
    """Parse the observed initRoster(table, JSON, ...) format, not JS eval."""
    tree = HTMLParser(html)
    heading = tree.css_first('h1')
    if heading is None or heading.text(strip=True) != f'Top Global Players for Season {season_id}':
        raise ValueError('页面不是指定赛季全球最终榜（可能是验证页/当季榜）')
    rows = []
    for script in tree.css('script'):
        content = script.text()
        for match in re.finditer(r'initRoster\(\s*\$\([\'\"]#roster[\'\"]\)\s*,\s*', content):
            try:
                value, _ = json.JSONDecoder().raw_decode(content[match.end():])
            except ValueError as exc:
                raise ValueError('榜单 JSON 无法解析') from exc
            if not isinstance(value, list):
                raise ValueError('榜单数据不是数组')
            if not all(isinstance(row, dict) for row in value):
                raise ValueError('榜单包含非对象记录')
            rows.extend(value)
    if not rows:
        raise ValueError('没有可解析的赛季排名；不从页面任意玩家链接凑名单')
    return [{'rank': row.get('rank'), 'tag': row.get('tag')} for row in rows]


def parse_profile_history_html(html: str, latest_season: str) -> dict | None:
    """Only the account's Ranked Seasons heatmap, not esport teammates/badges."""
    tree = HTMLParser(html)
    tags = [heading.text(strip=True) for heading in tree.css('h1,h2')
            if re.fullmatch(r'#[A-Z0-9]{3,32}', heading.text(strip=True))]
    if len(set(tags)) != 1:
        raise ValueError('玩家主页缺少明确自身 Tag（可能是验证页）')
    tag = normalize_player(tags[0])
    records = []
    for node in tree.css('.player__ladder_history_container [data-html]'):
        popup = HTMLParser(node.attributes['data-html'])
        rank_node, season_node = popup.css_first('.hist_heatmap__popup_rank'), popup.css_first('.hist_heatmap__popup_season')
        if rank_node is None or season_node is None:
            continue
        rank_text, season = rank_node.text(strip=True).replace(',', ''), season_node.text(strip=True)
        if rank_text.isdigit() and SEASON_ID.fullmatch(season) and season <= latest_season and 1 <= int(rank_text) <= 10000:
            records.append({'tag': tag, 'rank': int(rank_text), 'rank_season': season})
    return max(records, key=lambda row: (row['rank_season'], -row['rank'])) if records else None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='导入/核查固定赛季 Top 名单（不联网，不自动降级）')
    parser.add_argument('--check', metavar='MANIFEST')
    parser.add_argument('--html', action='append', default=[], help='同一赛季榜单的本地 HTML，可重复')
    parser.add_argument('--profile-html', action='append', default=[], help='缺额时用玩家主页 Ranked Seasons 结算记录补充')
    parser.add_argument('--historical-pool', action='store_true', help='按历史结算 Top 10000 资格组成最多 --top 人的固定池')
    parser.add_argument('--season', required=True)
    parser.add_argument('--top', type=int, default=1000)
    parser.add_argument('--start-utc')
    parser.add_argument('--end-utc')
    parser.add_argument('--boundary-source')
    parser.add_argument('--source-url', action='append', default=[])
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    try:
        if args.check:
            roster = load_roster(args.check, expected_season=args.season, expected_top=args.top)
        else:
            if not (args.html or args.profile_html) or not args.output:
                raise ValueError('导入需要 --html/--profile-html 与 --output，或使用 --check 核查现有名单')
            if args.profile_html and not args.historical_pool:
                raise ValueError('主页结算记录只能用于 --historical-pool')
            rows = []
            for path in args.html:
                rows.extend({**row, 'rank_season': args.season} for row in parse_roster_html(Path(path).read_text(encoding='utf-8-sig'), args.season))
            for path in args.profile_html:
                # Do not spend more CPU/files once the requested pool is full.
                if len({normalize_player(row['tag']) for row in rows if 1 <= row['rank'] <= 10000}) >= args.top:
                    break
                proof = parse_profile_history_html(Path(path).read_text(encoding='utf-8-sig'), args.season)
                if proof:
                    rows.append(proof)
            roster = parse_manifest({'schema_version': 1, 'season_id': args.season,
                'eligibility': 'historical_top10000' if args.historical_pool else 'season_top_n',
                'ranking_type': 'path_of_legends', 'top_n': args.top, 'players': rows,
                'start_utc': args.start_utc, 'end_utc': args.end_utc,
                'source_urls': args.source_url, 'boundary_source': args.boundary_source})
            output = Path(args.output)
            if output.exists():
                existing = load_roster(str(output), expected_season=args.season, expected_top=args.top)
                if existing.sha256 != roster.sha256:
                    raise ValueError('已有名单内容不同；使用新文件名，不覆盖冻结名单')
            else:
                output.parent.mkdir(parents=True, exist_ok=True)
                Storage.atomic_text(output, json.dumps(roster.manifest(), ensure_ascii=False, indent=2))
        print(json.dumps(roster.summary(), ensure_ascii=False, indent=2))
        return 0
    except (ValueError, OSError) as exc:
        print(json.dumps({'status': 'blocked', 'error': str(exc)}, ensure_ascii=False))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
