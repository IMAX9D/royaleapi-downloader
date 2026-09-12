"""无网络自检：验证令牌桶、断点续传、重试、两级流水线。

使用 MockFetcher（不联网）注入 canned 响应，覆盖核心逻辑。
"""
from __future__ import annotations

import asyncio
import json
import logging
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

from .client import FetchResult, Fetcher
from .config import CrawlConfig
from .crawler import Crawler
from .queue import TaskStore
from .ratelimit import TokenBucket


class MockFetcher(Fetcher):
    """按 URL 返回预设响应；每 URL 可给一串响应（依次消费，模拟 429→200）。"""

    def __init__(self, routes: dict[str, list[tuple[int, object]]]):
        self.routes = routes
        self.calls: list[str] = []
        self.proxy_calls: list[tuple[Optional[str], str]] = []

    async def fetch(self, proxy_url: Optional[str], url: str) -> FetchResult:
        self.calls.append(url)
        self.proxy_calls.append((proxy_url, url))
        seq = self.routes.get(url, [(404, {})])
        if len(seq) > 1:
            status, payload = seq.pop(0)
        else:
            status, payload = seq[0]
        if isinstance(payload, Exception):
            raise payload
        text = payload if isinstance(payload, str) else json.dumps(payload)
        return FetchResult(url, status, text, {})

    async def check(self, proxy_url: Optional[str], url: str) -> bool:
        return True

    async def aclose(self) -> None:
        pass


def _fast_cfg(tmp: str) -> CrawlConfig:
    cfg = CrawlConfig()
    cfg.retry.max_retries = 3
    cfg.retry.base_delay = 0.01
    cfg.retry.max_delay = 0.05
    cfg.retry.jitter = 0.0
    cfg.rate_limit.requests_per_second = 1000.0
    cfg.rate_limit.burst = 100
    cfg.rate_limit.jitter = 0.0
    cfg.replay_global_requests_per_second = 1000.0
    cfg.global_concurrency = 4
    cfg.discover_players = False  # 测试时关闭雪球，避免扩散
    cfg.db_path = f"{tmp}/p.sqlite3"
    cfg.output_dir = f"{tmp}/out"
    return cfg


async def _test_token_bucket() -> None:
    b = TokenBucket(rate=10.0, burst=2, jitter=0.0)
    t0 = time.monotonic()
    for _ in range(4):
        await b.acquire()
    dt = time.monotonic() - t0
    assert 0.15 <= dt <= 0.6, f"令牌桶限速异常: {dt:.3f}s"
    # available 必须实时 refill，供多代理调度器公平选择。
    b2 = TokenBucket(rate=20.0, burst=1, jitter=0.0)
    await b2.acquire()
    assert b2.available < 1.0
    deadline = time.monotonic() + 1.0
    while b2.available < 1.0 and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    assert b2.available >= 1.0
    print(f"[ok] TokenBucket：4 次 acquire 用时 {dt:.3f}s（预期约 0.2s）")


async def _test_store() -> None:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
        priority = TaskStore(f"{d}/priority.sqlite3", max_retries=3)
        for i in range(5):
            assert await priority.add(
                f"http://a/dependency/{i}", kind="upgrade_list",
                dedup_key=f"dependency:{i}",
            )
        for i in range(8):
            assert await priority.add(
                f"http://a/ready/{i}", kind="upgrade", dedup_key=f"upgrade:{i}"
            )
        leased = await priority.pending_authoritative_fair(
            limit=100, ready_limit=8, list_limit=2
        )
        assert [task["kind"] for task in leased] == [
            "upgrade", "upgrade", "upgrade", "upgrade", "upgrade_list",
            "upgrade", "upgrade", "upgrade", "upgrade", "upgrade_list",
        ], leased
        assert Crawler._task_priority({"kind": "upgrade"}) == Crawler._task_priority(
            {"kind": "upgrade_list"}
        )
        priority.close()

        cold = TaskStore(f"{d}/cold.sqlite3", max_retries=3)
        for i in range(20):
            assert await cold.add(
                f"http://a/cold/{i}", kind="upgrade_list", dedup_key=f"cold:{i}"
            )
        cold_leased = await cold.pending_authoritative_fair(
            limit=100, ready_limit=8, list_limit=2
        )
        assert len(cold_leased) == 2
        assert {task["kind"] for task in cold_leased} == {"upgrade_list"}
        assert await cold.add(
            "http://a/cold-ready", kind="upgrade", dedup_key="cold-ready:1"
        )
        next_leased = await cold.pending_authoritative_fair(
            limit=100, ready_limit=8, list_limit=2
        )
        assert [task["kind"] for task in next_leased] == ["upgrade"]
        cold.close()

        s = TaskStore(f"{d}/t.sqlite3", max_retries=3)
        assert await s.add("http://a/1", kind="detail", meta={"tag": "T"})
        assert not await s.add("http://a/1")  # URL 幂等去重
        inserted = await s.add_many([
            ("http://a/list", "s", "list", {"tag": "T"}, "player:T"),
            ("http://a/2", "s", "detail", {}, "battle:B2"),
        ])
        assert set(inserted) == {"http://a/list", "http://a/2"}
        # 三重去重：同一玩家 tag（不同 URL）、同一对局 tag（不同 URL）都应被忽略
        assert not await s.add("http://a/list?lang=en", "s", "list", {"tag": "T"}, "player:T")
        assert not await s.add("http://a/2/other", "s", "detail", {}, "battle:B2")
        p = await s.pending()
        assert len(p) == 3, p
        kinds = {x["kind"] for x in p}
        assert kinds == {"list", "detail"}
        await s.mark_done("http://a/1", "p.json")
        assert await s.requeue_inflight() == 2
        await s.upsert_battle_metadata([("BT", {"complete": False, "rounds": []})])
        await s.upsert_battle_metadata([("BT", {"complete": True, "rounds": [{"team": [], "opponent": []}]})])
        await s.upsert_battle_metadata([("BT", {"complete": False, "rounds": []})])
        md = await s.get_battle_metadata("BT")
        assert md and md["complete"] is True and len(md["rounds"]) == 1, md
        assert (await s.battle_metadata_stats())["complete"] == 1
        exclusion = await s.import_excluded_battles(["B2", "BLOCKED"], "selftest")
        assert exclusion == {
            "requested": 2, "inserted": 2, "total": 2,
            "unfinished_tasks_skipped": 1,
        }, exclusion
        assert not await s.add(
            "http://a/blocked", kind="detail", dedup_key="battle:BLOCKED"
        )
        assert not await s.add_battles([(
            "http://a/blocked2", "http://a/list", {}, "battle:BLOCKED",
            "BLOCKED", {"complete": True},
        )])
        assert await s.add(
            "http://a/allowed", kind="detail", dedup_key="battle:ALLOWED"
        )
        assert await s.excluded_battles_count() == 2
        s.close()
        print("[ok] TaskStore：三重去重 + 全局排除 + 状态流转 + 断点续传")


async def _test_retry() -> None:
    routes = {"http://x/b1": [(429, {}), (200, {"battleId": "b1"})]}
    fetcher = MockFetcher(routes)
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
        cfg = _fast_cfg(d)
        cfg.proxy.cooldown_429 = 0.0
        c = Crawler(cfg, fetcher=fetcher)
        result = await c._fetch_raw("http://x/b1")
        assert result.json() == {"battleId": "b1"}, result.json()
        assert fetcher.calls.count("http://x/b1") == 2
        await c.fetcher.aclose()
        c.close()
        print("[ok] 重试：429 -> 200 成功")


async def _test_two_stage() -> None:
    from .parsers import parse_battles_replay_urls

    def player(tag: str, prefix: str) -> str:
        cards = "".join(
            f'<div class="deck_card__four_wide"><img class="deck_card" data-card-key="{prefix}{i}">'
            f'<div class="card-level">Lvl {10+i}</div></div>' for i in range(8)
        )
        return (
            f'<a class="player_name_header" href="/player/{tag}/battles">p</a>'
            f'<div id="deck_{prefix}">{cards}</div>'
            '<div class="deck_tower_card__container"><img class="deck_card deck_card_key_tower-princess"></div>'
        )

    def battle(tag: str, opponent: str, prefix: str) -> str:
        return (
            '<div class="battle_list_battle" data-battle-type="trail">'
            '<h4 class="game_mode_header">1v1</h4>'
            '<div class="battle-team-segment-container">'
            + player("T", "t" + prefix) + player(opponent, "o" + prefix) + '</div>'
            + f'<button class="replay_button" data-replay="{tag}" data-team-tags="T" '
              f'data-opponent-tags="{opponent}" data-team-crowns="1" '
              'data-opponent-crowns="0" data-draft="0"></button></div>'
        )

    special_2v2 = (
        '<div class="battle_list_battle" data-battle-type="trail">'
        '<h4 class="game_mode_header">2v2 Battle</h4>'
        '<div class="battle-team-segment-container">'
        + player("T", "t3") + player("F", "f3") + player("G", "g3") + player("H", "h3") + '</div>'
        + '<button class="replay_button" data-replay="BATTLE3" data-team-tags="T,F" '
          'data-opponent-tags="G,H" data-team-crowns="1" data-opponent-crowns="0" '
          'data-draft="0"></button></div>'
    )
    LIST_HTML = battle("BATTLE1", "D", "1") + battle("BATTLE2", "E", "2") + special_2v2
    detail_urls = parse_battles_replay_urls(LIST_HTML, "http://x", "http://x/player/T/battles")
    assert len(detail_urls) == 3, detail_urls

    routes = {"http://x/player/T/battles": [(200, LIST_HTML)]}
    for i, u in enumerate(detail_urls[:2], 1):
        routes[u] = [(200, {"success": True, "html": (
            f'<div class="battle_replay" data-tag="BATTLE{i}">'
            f'<div class="blue marker" data-x="8499" data-y="500" data-i="0" data-c="knight" '
            f'data-t="203" data-s="t"></div>'
            '<table class="replay_elixir_table"><tr><td class="title">Total</td>'
            '<td class="count">1</td><td class="elixir">3</td></tr></table>'
            '<table class="replay_elixir_table"><tr><td class="title">Total</td>'
            '<td class="count">0</td><td class="elixir">0</td></tr></table>'
            '<div class="marker">0:30</div>'
            f'</div>'
        )})]

    fetcher = MockFetcher(routes)
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
        cfg = _fast_cfg(d)
        cfg.base_url = "http://x"
        cfg.list_endpoint_template = "{base}/player/{tag}/battles"
        c = Crawler(cfg, fetcher=fetcher)

        result = await c.run(seeds=[("T", "test")])
        assert result["stats"]["ok"] == 2, result["stats"]
        assert result["stats"]["list_pages"] == 1, result["stats"]
        assert result["stats"]["filtered"] == 1, result["stats"]
        assert detail_urls[2] not in fetcher.calls, fetcher.calls

        battles = list(Path(cfg.output_dir, "raw", "battles").glob("**/*.json"))
        lists = list(Path(cfg.output_dir, "raw", "lists").glob("*.html"))
        assert len(battles) == 2, battles
        assert len(lists) == 1, lists

        # 详情已解析成结构化格式（card_plays 等）
        import json as _json
        sample = _json.loads(battles[0].read_text(encoding="utf-8"))
        assert sample.get("battle_tag") and sample.get("card_plays"), sample
        assert sample.get("schema_version") == 5 and len(sample.get("team_deck", [])) == 8, sample
        assert len(sample.get("opponent_deck", [])) == 8 and sample["deck_metadata"]["complete"], sample
        assert (await c.store.battle_metadata_stats())["total"] == 0

        # 断点续传：重跑应无新任务
        result2 = await c.run(seeds=[("T", "test")])
        assert result2["stats"]["ok"] == 0, result2["stats"]
        c.close()
        print("[ok] 两级流水线：普通 1v1 入队，2v2 在回放请求前过滤 + 断点续传")


async def _test_legacy_detail_metadata_recovery() -> None:
    """旧 pending 详情没有卡组元数据时，先补取来源列表页，再请求回放。"""
    from .parsers import parse_battles_replay_urls
    from .proxy_pool import ProxyState

    cards_a = "".join(
        f'<div><img class="deck_card" data-card-key="a{i}"><span class="card-level">Lvl 14</span></div>'
        for i in range(8)
    )
    cards_b = "".join(
        f'<div><img class="deck_card" data-card-key="b{i}"><span class="card-level">Lvl 14</span></div>'
        for i in range(8)
    )
    source_url = "http://x/player/T/battles/history?before=1"
    list_html = (
        '<div class="battle_list_battle" data-battle-type="PvP">'
        '<h4 class="game_mode_header">Ladder</h4><div class="battle-team-segment-container">'
        '<a class="player_name_header" href="/player/T/battles">T</a>'
        f'<div id="deck_a">{cards_a}</div>'
        '<a class="player_name_header" href="/player/O/battles">O</a>'
        f'<div id="deck_b">{cards_b}</div></div>'
        '<button class="replay_button" data-replay="LEGACY1" data-team-tags="T" '
        'data-opponent-tags="O" data-team-crowns="1" data-opponent-crowns="0" '
        'data-draft="0"></button></div>'
    )
    detail_url = parse_battles_replay_urls(list_html, "http://x", source_url)[0]
    routes = {
        source_url: [(200, list_html)],
        detail_url: [(200, {"success": True, "html": (
            '<div class="battle_replay" data-tag="LEGACY1">'
            '<div class="blue marker" data-x="8499" data-y="500" data-i="0" data-c="a0" '
            'data-t="203" data-s="t"></div>'
            '<table class="replay_elixir_table"><tr><td class="title">Total</td>'
            '<td class="count">1</td><td class="elixir">3</td></tr></table>'
            '<table class="replay_elixir_table"><tr><td class="title">Total</td>'
            '<td class="count">0</td><td class="elixir">0</td></tr></table>'
            '<div class="marker">0:30</div></div>'
        )})],
    }
    fetcher = MockFetcher(routes)
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
        cfg = _fast_cfg(d)
        cfg.base_url = "http://x"
        cfg.require_complete_decks = True
        c = Crawler(cfg, fetcher=fetcher)
        c.list_fetcher = fetcher
        c._list_proxy = ProxyState(
            url=None, bucket=TokenBucket(rate=1000.0, burst=10, jitter=0.0)
        )
        assert await c.store.add(
            detail_url, source_url, "detail", {"battle_tag": "LEGACY1"}, "battle:LEGACY1"
        )
        result = await c.run(seeds=[])
        assert result["stats"]["ok"] == 1, result
        assert fetcher.calls.index(source_url) < fetcher.calls.index(detail_url), fetcher.calls
        saved = list(Path(cfg.output_dir, "raw", "battles").glob("**/*.json"))
        payload = json.loads(saved[0].read_text(encoding="utf-8"))
        assert len(payload["team_deck"]) == 8 and len(payload["opponent_deck"]) == 8, payload
        assert (await c.store.battle_metadata_stats())["total"] == 0
        c.close()
        print("[ok] 旧 pending 详情：先从来源列表页补齐双方 8 卡，再抓回放")


def _test_flaresolverr() -> None:
    from .client import FetcherError, FlareSolverrFetcher

    ok = {
        "status": "ok",
        "solution": {
            "url": "https://x/b",
            "status": 200,
            "response": '{"battleId": "1"}',
            "headers": {"Content-Type": "application/json"},
        },
    }
    r = FlareSolverrFetcher._parse_solution(ok)
    assert r.status_code == 200 and r.json() == {"battleId": "1"}, r

    try:
        FlareSolverrFetcher._parse_solution({"status": "error", "message": "challenge timeout"})
        raise AssertionError("应抛 FetcherError")
    except FetcherError:
        pass
    print("[ok] FlareSolverr 响应解析（ok / error 两条路径）")


def _test_parse_replay() -> None:
    from .parsers import parse_replay_html

    html = (
        '<div class="battle_replay" data-tag="BTAG123">'
        '<div class="blue marker" data-x="8499" data-y="500" data-i="0" data-c="knight" data-t="203" data-s="t"></div>'
        '<div class="blue marker" data-x="8000" data-y="600" data-i="1" data-c="archers" data-t="220" data-s="t"></div>'
        '<div class="red marker" data-x="9500" data-y="8500" data-i="1" data-c="bats" data-t="301" data-s="o"></div>'
        '<div class="blue marker" data-x="None" data-y="None" data-c="_invalid" data-t="0" data-s="t"></div>'
        '<table class="replay_elixir_table"><tbody>'
        '<tr><td class="title">Total</td><td class="right aligned count">2</td><td class="right aligned elixir">6</td></tr>'
        '<tr><td class="title">Ability</td><td class="right aligned count">1</td><td class="right aligned elixir">0</td></tr>'
        '</tbody></table>'
        '<table class="replay_elixir_table"><tbody>'
        '<tr><td class="title">Total</td><td class="right aligned count">1</td><td class="right aligned elixir">2</td></tr>'
        '</tbody></table>'
        '<div class="marker end_time" style="left:600px;">0:30</div>'
        '<div class="unrelated-clock">12:24</div>'
        '</div>'
    )
    d = parse_replay_html(html)
    assert d["battle_tag"] == "BTAG123"
    assert len(d["card_plays"]) == 3, d["card_plays"]
    assert d["ability_plays"] == [{
        "time": 0.0, "time_raw": 0, "side": "team", "color": "blue",
        "ability_id": None, "resolution_status": "unresolved",
        "marker_index": 3, "data_i": None, "x_raw": None, "y_raw": None,
        "coordinate_status": "not_applicable",
    }]
    p0 = d["card_plays"][0]
    assert p0["card"] == "knight" and p0["side"] == "team" and p0["color"] == "blue"
    assert p0["x_raw"] == 8499 and p0["x"] == 9501      # 18000 - 8499
    assert p0["y_raw"] == 500 and p0["y"] == 31500      # 32000 - 500
    assert p0["data_i"] == 0 and p0["coordinate_transform"] == "rotate_180"
    p1 = d["card_plays"][1]
    assert p1["x_raw"] == 8000 and p1["x"] == 8000       # data-i=1: identity
    assert p1["y_raw"] == 600 and p1["y"] == 600
    assert p1["data_i"] == 1 and p1["coordinate_transform"] == "identity"
    assert d["coordinate_provenance"]["transform_id"] == "royaleapi_data_i_to_libg_native_v1"
    assert p0["time_raw"] == 203 and p0["time"] == 10.15
    assert d["duration_seconds"] == 30
    assert d["elixir_stats"]["team"]["Total"] == {"count": 2, "elixir": 6.0}
    assert d["elixir_stats"]["opponent"]["Total"] == {"count": 1, "elixir": 2.0}
    assert d["card_counts"]["team"]["knight"] == 1

    # Deployment markers without a valid data-i are rejected. New downloads
    # must never silently inherit the legacy unconditional rotation.
    for bad_data_i in ("", "2", "x"):
        bad_attr = f' data-i="{bad_data_i}"' if bad_data_i else ""
        bad_html = (
            '<div class="battle_replay" data-tag="BAD">'
            f'<div class="blue marker" data-x="8499" data-y="500"{bad_attr} '
            'data-c="knight" data-t="203" data-s="t"></div></div>'
        )
        try:
            parse_replay_html(bad_html)
            raise AssertionError(f"invalid data-i should fail: {bad_data_i!r}")
        except ValueError as exc:
            assert "data-i" in str(exc), exc
    print("[ok] 回放 HTML 解析：部署坐标 + 技能时点 + elixir 统计")


def _authoritative_fixture() -> tuple[str, str, str]:
    team = [f"a{i}" for i in range(8)]
    opponent = [f"b{i}" for i in range(8)]

    def player(tag: str, cards: list[str], tower_level: int) -> str:
        deck = "".join(
            '<div class="deck_card__four_wide">'
            f'<img class="deck_card" data-card-key="{card}">'
            f'<div class="card-level">Lvl {11 + slot}</div></div>'
            for slot, card in enumerate(cards)
        )
        return (
            '<div class="team-segment">'
            f'<a class="player_name_header" href="https://royaleapi.com/player/{tag}/battles">p</a>'
            f'<div id="deck_{tag}">{deck}</div>'
            '<div class="deck_tower_card__container">'
            '<img class="deck_card deck_card_key_tower-princess">'
            f'<div class="level"><div>Tower Princess</div><div>Lvl {tower_level}</div></div>'
            '</div></div>'
        )

    index = "1787218979"
    list_html = (
        f'<div class="battle_list_battle" data-battle-type="trail" data-timestamp="{index}.0">'
        '<h4 class="game_mode_header">1v1</h4>'
        '<div class="battle-team-segment-container">'
        + player("TEAM", team, 14) + player("OPPO", opponent, 14) + '</div>'
        '<div class="battle-timestamp-popup" data-content="2026-08-20 09:42:59 UTC"></div>'
        '<span class="hp-both-popup" data-team-king="7728" data-team-princess0="3052" '
        'data-team-princess1="2000" data-team-total="12780" data-oppo-king="7728" '
        'data-oppo-princess0="1000" data-oppo-princess1="0" data-oppo-total="8728"></span>'
        f'<button class="matchup_button" data-index="{index}" data-players="1v1" '
        f'data-team-deck="{",".join(team)}" data-opponent-deck="{",".join(opponent)}" '
        'data-game-mode-id="72000450"></button>'
        f'<button class="replay_button" data-index="{index}" data-replay="AUTH1" '
        'data-team-tags="TEAM" data-opponent-tags="OPPO" data-draft="0" '
        'data-team-crowns="1" data-opponent-crowns="0"></button></div>'
    )

    markers: list[str] = []
    marker_index = 0
    for slot in range(8):
        tick = 20 + slot
        markers.append(
            f'<div class="blue marker" data-x="{1000 + slot}" data-y="{2000 + slot}" '
            f'data-i="1" data-c="a{slot}" data-t="{tick}" data-s="t"></div>'
        )
        marker_index += 1
        markers.append(
            f'<div class="red marker" data-x="{3000 + slot}" data-y="{4000 + slot}" '
            f'data-i="1" data-c="b{slot}" data-t="{tick}" data-s="o"></div>'
        )
        marker_index += 1
    replay_html = (
        '<div class="battle_replay" data-tag="AUTH1">'
        + "".join(markers)
        + '<table class="replay_elixir_table"><tr><td class="title">Total</td>'
          '<td class="count">8</td><td class="elixir">24</td></tr></table>'
          '<table class="replay_elixir_table"><tr><td class="title">Total</td>'
          '<td class="count">8</td><td class="elixir">24</td></tr></table>'
          '<div class="marker end_time">0:30</div></div>'
    )
    return list_html, replay_html, "https://royaleapi.com/player/TEAM/battles"


def _write_test_native_contract(path: Path) -> str:
    from .authoritative import native_contract_payload_sha256

    value = {
        "schema_version": 3,
        "kind": "cr_native_authoritative_contract_v3",
        "game_version": "15.535.29",
        "allowed_card_tokens": [
            *[f"a{i}" for i in range(8)],
            *[f"b{i}" for i in range(8)],
        ],
        "allowed_tower_troops": ["tower-princess"],
        "ability_source_tokens": ["a0"],
        "source_numeric_game_mode_ids": [72000006, 72000450, 72000464],
        "native_execution_mode_by_source": {
            "72000006": 72000006,
            "72000450": 72000006,
            "72000464": 72000006,
        },
        "king_tower_max_hp_by_level": {
            "11": 4824,
            "15": 7032,
            "16": 7728,
        },
        "king_tower_level_evidence": {
            "schema_version": 1,
            "scope": "side_local_ranked_template",
            "ranked_template_level_cap": 16,
            "resolved_king_tower_level": 16,
            "precedence": ["tower_troop_level", "final_king_hp"],
            "accepted_provenances": [
                "ranked_template_cap16_and_tower_troop_level16_v1",
                "ranked_template_cap16_and_full_king_hp_v1",
            ],
            "tower_troop_level": {
                "required_value": 16,
                "inference": (
                    "tower_troop_level<=king_tower_level and "
                    "ranked_template_cap=16"
                ),
                "provenance": (
                    "ranked_template_cap16_and_tower_troop_level16_v1"
                ),
                "official_sources": [
                    "https://support.supercell.com/clash-royale/en/articles/king-tower-level.html",
                    "https://support.supercell.com/clash-royale/en/articles/tower-troops-4.html",
                ],
            },
            "final_king_hp": {
                "required_value": 7728,
                "provenance": "ranked_template_cap16_and_full_king_hp_v1",
            },
            "forbidden_inference_fields": ["card_levels", "deck_cards.level"],
        },
    }
    value["contract_sha256"] = native_contract_payload_sha256(value)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    return value["contract_sha256"]


def _test_authoritative_schema_and_gate() -> None:
    from .authoritative import (
        apply_native_contract_metadata,
        evaluate_native_eligibility,
        load_native_contract,
    )
    from .parsers import parse_battles, parse_replay_html

    list_html, replay_html, source_url = _authoritative_fixture()
    battle = parse_battles(list_html, "https://royaleapi.com", source_url)[0]
    metadata = battle["metadata"]
    assert metadata["authoritative_complete"] is True, metadata
    assert metadata["battle_index"] == 1787218979
    assert metadata["numeric_game_mode_id"] == 72000450
    assert metadata["matchup_players"] == "1v1"
    assert metadata["version_timestamp"] == 1787218979
    assert metadata["battle_time_utc"] == "2026-08-20T09:42:59Z"
    assert metadata["final_tower_hp"]["team"] == {
        "king": 7728, "princess0": 3052,
        "princess1": 2000, "total": 12780,
    }
    assert [
        player["tower_troop_level"]
        for side in ("team", "opponent")
        for player in metadata["rounds"][0][side]
    ] == [14, 14]

    data = Crawler._merge_battle_metadata(parse_replay_html(replay_html), metadata)
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
        contract_path = Path(directory, "contract.json")
        expected_sha = _write_test_native_contract(contract_path)
        contract = load_native_contract(
            contract_path, expected_game_version="15.535.29"
        )
        assert contract.contract_sha256 == expected_sha
        tampered = json.loads(contract_path.read_text(encoding="utf-8"))
        tampered["source_numeric_game_mode_ids"] = [72000007]
        contract_path.write_text(json.dumps(tampered), encoding="utf-8")
        try:
            load_native_contract(contract_path, expected_game_version="15.535.29")
            raise AssertionError("tampered native contract should fail")
        except ValueError as exc:
            assert "SHA-256 mismatch" in str(exc), exc

        semantic_contract_failures = {
            "execution map coverage": lambda value: value[
                "native_execution_mode_by_source"
            ].pop("72000464"),
            "level-16 King HP anchor": lambda value: value[
                "king_tower_max_hp_by_level"
            ].__setitem__("16", 7727),
            "legacy v1 schema": lambda value: value.update({
                "schema_version": 1,
                "kind": "cr_native_authoritative_contract_v1",
            }),
        }
        from .authoritative import native_contract_payload_sha256
        for name, mutate in semantic_contract_failures.items():
            _write_test_native_contract(contract_path)
            invalid_contract = json.loads(contract_path.read_text(encoding="utf-8"))
            mutate(invalid_contract)
            invalid_contract["contract_sha256"] = native_contract_payload_sha256(
                invalid_contract
            )
            contract_path.write_text(
                json.dumps(invalid_contract, ensure_ascii=False), encoding="utf-8"
            )
            try:
                load_native_contract(
                    contract_path, expected_game_version="15.535.29"
                )
                raise AssertionError(f"semantic contract should fail: {name}")
            except ValueError:
                pass
        _write_test_native_contract(contract_path)
        apply_native_contract_metadata(data, contract)
        assert data["numeric_game_mode_id"] == 72000450
        assert data["native_execution_game_mode_id"] == 72000006
        assert data["native_execution_game_mode_provenance"] == (
            "frozen_native_ingest_contract_mode_map_v1"
        )
        for source_mode in (72000006, 72000450, 72000464):
            mapped = json.loads(json.dumps(data))
            mapped["numeric_game_mode_id"] = source_mode
            apply_native_contract_metadata(mapped, contract)
            assert mapped["numeric_game_mode_id"] == source_mode
            assert mapped["native_execution_game_mode_id"] == 72000006
            mapped_result = evaluate_native_eligibility(
                mapped,
                min_battle_timestamp=1785801600,
                native_contract=contract,
            )
            assert mapped_result.accepted, (source_mode, mapped_result)
            assert mapped_result.tier == "native_static_v2", mapped_result
        for side in ("team", "opponent"):
            player = data["rounds"][0][side][0]
            assert player["king_tower_level"] == 16
            assert player["king_tower_level_provenance"] == (
                "ranked_template_cap16_and_full_king_hp_v1"
            )
        result = evaluate_native_eligibility(
            data, min_battle_timestamp=1785801600, native_contract=contract
        )
        assert result.accepted, result

        damaged_king = json.loads(json.dumps(data))
        damaged_king["final_tower_hp"]["opponent"]["king"] = 7727
        damaged_king["final_tower_hp"]["opponent"]["total"] -= 1
        damaged_king["rounds"][0]["opponent"][0]["tower_troop_level"] = 16
        apply_native_contract_metadata(damaged_king, contract)
        recovered = evaluate_native_eligibility(
            damaged_king,
            min_battle_timestamp=1785801600,
            native_contract=contract,
        )
        assert recovered.accepted, recovered
        assert damaged_king["rounds"][0]["opponent"][0][
            "king_tower_level_provenance"
        ] == "ranked_template_cap16_and_tower_troop_level16_v1"
        damaged_metadata = json.loads(json.dumps(metadata))
        damaged_metadata["final_tower_hp"]["opponent"]["king"] -= 1
        damaged_metadata["final_tower_hp"]["opponent"]["total"] -= 1
        damaged_metadata["rounds"][0]["opponent"][0][
            "tower_troop_level"
        ] = 16
        metadata_gate = object.__new__(Crawler)
        metadata_gate.cfg = SimpleNamespace(min_battle_timestamp=1785801600, max_battle_timestamp=None)
        metadata_gate.season_roster = None
        metadata_gate._authoritative_enabled = True
        metadata_gate.native_contract = contract
        assert metadata_gate._eligible_metadata(damaged_metadata)

        unproven_damaged_king = json.loads(json.dumps(data))
        unproven_damaged_king["final_tower_hp"]["opponent"]["king"] = 7727
        unproven_damaged_king["final_tower_hp"]["opponent"]["total"] -= 1
        apply_native_contract_metadata(unproven_damaged_king, contract)
        rejected = evaluate_native_eligibility(
            unproven_damaged_king,
            min_battle_timestamp=1785801600,
            native_contract=contract,
        )
        assert not rejected.accepted and rejected.tier == "king_level", rejected
        assert (
            "opponent_king_tower_level_exact_evidence_missing" in rejected.reasons
        )
        unproven_metadata = json.loads(json.dumps(metadata))
        unproven_metadata["final_tower_hp"]["opponent"]["king"] -= 1
        unproven_metadata["final_tower_hp"]["opponent"]["total"] -= 1
        assert not metadata_gate._eligible_metadata(unproven_metadata)

        # Cross-side commands at one tick are a valid joint action.
        assert data["card_plays"][0]["time_raw"] == data["card_plays"][1]["time_raw"]

        collision = json.loads(json.dumps(data))
        collision["card_plays"][2]["time_raw"] = collision["card_plays"][0]["time_raw"]
        rejected = evaluate_native_eligibility(
            collision, min_battle_timestamp=1785801600, native_contract=contract
        )
        assert not rejected.accepted
        assert "team_multiple_commands_same_tick" in rejected.reasons, rejected

        deploy_ability_collision = json.loads(json.dumps(data))
        last_team_tick = deploy_ability_collision["card_plays"][-2]["time_raw"]
        deploy_ability_collision["ability_plays"].append({
            "time": round(last_team_tick / 20, 2),
            "time_raw": last_team_tick,
            "side": "team",
            "color": "blue",
            "ability_id": None,
            "resolution_status": "unresolved",
            "marker_index": 16,
            "data_i": 1,
            "x_raw": None,
            "y_raw": None,
            "coordinate_status": "not_applicable",
        })
        deploy_ability_collision["elixir_stats"]["team"]["Ability"] = {
            "count": 1, "elixir": 0.0,
        }
        rejected = evaluate_native_eligibility(
            deploy_ability_collision,
            min_battle_timestamp=1785801600,
            native_contract=contract,
        )
        assert not rejected.accepted
        assert "team_multiple_commands_same_tick" in rejected.reasons, rejected

        unsupported = json.loads(json.dumps(data))
        unsupported["team_deck"][0] = "party-hut"
        unsupported["rounds"][0]["team"][0]["full_deck"][0] = "party-hut"
        unsupported["rounds"][0]["team"][0]["deck_cards"][0].update({
            "slug": "party-hut", "base_slug": "party-hut", "form": "base",
        })
        levels = unsupported["rounds"][0]["team"][0]["card_levels"]
        levels["party-hut"] = levels.pop("a0")
        rejected = evaluate_native_eligibility(
            unsupported, min_battle_timestamp=1785801600, native_contract=contract
        )
        assert not rejected.accepted
        assert any("native_card_mapping_missing" in reason for reason in rejected.reasons)
    print("[ok] schema5 contract v3 模式映射 + King16 双证据 + 同方Tick占用门禁")


async def _test_authoritative_two_phase_resume() -> None:
    from .parsers import parse_replay_html
    from .storage import Storage

    list_html, replay_html, source_list_url = _authoritative_fixture()
    replay_url = "http://x/data/replay?tag=AUTH1"
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
        root = Path(directory)
        contract_path = root / "contract.json"
        contract_sha = _write_test_native_contract(contract_path)
        source_path = root / "schema3.json"
        source_value = parse_replay_html(replay_html)
        source_value["schema_version"] = 3
        source_path.write_text(
            json.dumps(source_value, ensure_ascii=False), encoding="utf-8"
        )
        cfg = _fast_cfg(directory)
        cfg.base_url = "http://x"
        cfg.output_dir = str(root / "legacy")
        cfg.db_path = str(root / "progress.sqlite3")
        cfg.authoritative_target = 1
        cfg.authoritative_output_dir = str(root / "fresh")
        cfg.authoritative_native_contract = str(contract_path)
        cfg.authoritative_game_version = "15.535.29"
        cfg.min_battle_timestamp = 1785801600
        cfg.require_complete_decks = True

        candidate = {
            "battle_tag": "AUTH1",
            "source_path": str(source_path),
            "source_schema_version": 3,
            "replay_url": replay_url,
            "source_list_url": source_list_url,
            "local_exact_replay_body": True,
            "upgrade_tier": "list_metadata_only",
        }

        # Crash/restart after phase-one list dependency was leased. No replay
        # task exists yet, so detail priority cannot outrun metadata.
        first = Crawler(cfg, fetcher=MockFetcher({}))
        await first.store.add_authoritative_upgrade_pages([(
            source_list_url,
            {"candidates": [candidate], "phase": "authoritative_list_dependency"},
            "authoritative-list:test",
        )])
        leased = await first.store.pending(limit=1)
        assert len(leased) == 1 and leased[0]["kind"] == "upgrade_list", leased
        assert not any(
            row[0] == "upgrade"
            for row in first.store.conn.execute("SELECT kind FROM tasks")
        )
        first.close()

        fetcher = MockFetcher({source_list_url: [(200, list_html)]})
        second = Crawler(cfg, fetcher=fetcher)
        result = await second.run(seeds=[])
        stats = await second.authoritative_stats()
        assert stats["status"] == {"accepted": 1}, stats
        assert result["stats"]["authoritative_reused"] == 1, result
        assert fetcher.calls == [source_list_url], fetcher.calls
        files = list((root / "fresh" / "raw" / "battles").glob("**/*.json"))
        assert len(files) == 1, files
        saved = json.loads(files[0].read_text(encoding="utf-8"))
        assert saved["schema_version"] == 5
        assert saved["authoritative_native_contract"]["contract_sha256"] == contract_sha
        assert saved["numeric_game_mode_id"] == 72000450
        assert saved["native_execution_game_mode_id"] == 72000006
        assert saved["native_execution_game_mode_provenance"] == (
            "frozen_native_ingest_contract_mode_map_v1"
        )
        assert saved["authoritative_eligibility"]["gate"] == "native_static_v2"
        assert all(
            saved["rounds"][0][side][0]["king_tower_level"] == 16
            for side in ("team", "opponent")
        )
        assert stats["tiers"] == {"accepted:native_static_v2": 1}, stats
        index_lines = (root / "fresh" / "index.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
        assert len(index_lines) == 1, index_lines
        second.close()

        # An unknown source mode must be rejected from exact list metadata
        # without spending a replay request.
        mode_root = root / "mode"
        mode_cfg = _fast_cfg(str(mode_root))
        mode_cfg.base_url = "http://x"
        mode_cfg.output_dir = str(mode_root / "legacy")
        mode_cfg.db_path = str(mode_root / "progress.sqlite3")
        mode_cfg.authoritative_target = 1
        mode_cfg.authoritative_output_dir = str(mode_root / "fresh")
        mode_cfg.authoritative_native_contract = str(contract_path)
        mode_cfg.authoritative_game_version = "15.535.29"
        mode_cfg.min_battle_timestamp = 1785801600
        mode_fetcher = MockFetcher({})
        mode_crawler = Crawler(mode_cfg, fetcher=mode_fetcher)
        from .parsers import parse_battles
        mode_metadata = parse_battles(
            list_html, "https://royaleapi.com", source_list_url
        )[0]["metadata"]
        mode_metadata["numeric_game_mode_id"] = 72000007
        mode_candidate = {
            **candidate,
            "battle_tag": "MODE1",
            "contract_sha256": contract_sha,
        }
        await mode_crawler.store.add_authoritative_upgrades([(
            "http://x/data/replay?tag=MODE1",
            source_list_url,
            mode_candidate,
            "MODE1",
            mode_metadata,
        )])
        await mode_crawler.run(seeds=[])
        mode_stats = await mode_crawler.authoritative_stats()
        assert mode_stats["tiers"] == {"rejected:mode": 1}, mode_stats
        assert mode_fetcher.calls == [], mode_fetcher.calls
        mode_crawler.close()

        # A damaged/unanchored King is also rejected from list metadata before
        # any replay bandwidth is spent.
        king_root = root / "king-level"
        king_cfg = _fast_cfg(str(king_root))
        king_cfg.base_url = "http://x"
        king_cfg.output_dir = str(king_root / "legacy")
        king_cfg.db_path = str(king_root / "progress.sqlite3")
        king_cfg.authoritative_target = 1
        king_cfg.authoritative_output_dir = str(king_root / "fresh")
        king_cfg.authoritative_native_contract = str(contract_path)
        king_cfg.authoritative_game_version = "15.535.29"
        king_cfg.min_battle_timestamp = 1785801600
        king_fetcher = MockFetcher({})
        king_crawler = Crawler(king_cfg, fetcher=king_fetcher)
        king_metadata = parse_battles(
            list_html, "https://royaleapi.com", source_list_url
        )[0]["metadata"]
        king_metadata["final_tower_hp"]["opponent"]["king"] -= 1
        king_metadata["final_tower_hp"]["opponent"]["total"] -= 1
        king_candidate = {
            **candidate,
            "battle_tag": "KING1",
            "contract_sha256": contract_sha,
        }
        await king_crawler.store.add_authoritative_upgrades([(
            "http://x/data/replay?tag=KING1",
            source_list_url,
            king_candidate,
            "KING1",
            king_metadata,
        )])
        await king_crawler.run(seeds=[])
        king_stats = await king_crawler.authoritative_stats()
        assert king_stats["tiers"] == {"rejected:king_level": 1}, king_stats
        assert king_fetcher.calls == [], king_fetcher.calls
        king_crawler.close()

        # Crash window: file+durable idempotent index may exist, but accepted
        # membership is unpublished until task done + accepted commit together.
        crash_db = root / "crash.sqlite3"
        crash_store = TaskStore(str(crash_db), 3)
        assert await crash_store.add(
            "http://x/data/replay?tag=CRASH", kind="detail",
            dedup_key="battle:CRASH",
        )
        crash_task = (await crash_store.pending(limit=1))[0]
        crash_files = Storage(root / "crash-fresh")
        crash_saved = crash_files.save_json(
            crash_task["url"], {"battle_tag": "CRASH"}
        )
        record = {
            "kind": "authoritative_battle",
            "battle_tag": "CRASH",
            "saved_path": crash_saved,
        }
        assert crash_files.append_index_idempotent(record) is True
        assert await crash_store.authoritative_accepted_count() == 0
        crash_store.close()

        recovered = TaskStore(str(crash_db), 3)
        assert await recovered.requeue_inflight() == 1
        recovered_task = (await recovered.pending(limit=1))[0]
        assert recovered_task["id"] == crash_task["id"]
        assert Storage(root / "crash-fresh").append_index_idempotent(record) is False
        committed = await recovered.commit_authoritative_acceptance(
            task_id=recovered_task["id"],
            battle_tag="CRASH",
            saved_path=crash_saved,
            source_path=None,
            source_schema_version=5,
            contract_sha256=contract_sha,
            target=1,
        )
        assert committed["accepted_total"] == 1
        accepted_tier = recovered.conn.execute(
            "SELECT tier FROM authoritative_results WHERE battle_tag='CRASH'"
        ).fetchone()[0]
        assert accepted_tier == "native_static_v2", accepted_tier
        task_status = recovered.conn.execute(
            "SELECT status FROM tasks WHERE id=?", (recovered_task["id"],)
        ).fetchone()[0]
        assert task_status == "done"

        assert await recovered.add(
            "https://example.invalid/replay/CAP2",
            kind="upgrade",
            dedup_key="authoritative:CAP2",
        )
        cap_task = (await recovered.pending(limit=1))[0]
        capped = await recovered.commit_authoritative_acceptance(
            task_id=cap_task["id"],
            battle_tag="CAP2",
            saved_path=str(root / "crash-fresh" / "CAP2.json"),
            source_path=None,
            source_schema_version=5,
            contract_sha256=contract_sha,
            target=1,
        )
        assert capped == {
            "accepted": False,
            "newly_accepted": False,
            "cap_rejected": True,
            "accepted_total": 1,
        }, capped
        assert await recovered.authoritative_accepted_count(contract_sha) == 1
        cap_status = recovered.conn.execute(
            "SELECT status FROM tasks WHERE id=?", (cap_task["id"],)
        ).fetchone()[0]
        assert cap_status == "skipped", cap_status
        recovered.close()

        # A list dependency that remains absent at retry exhaustion becomes an
        # explicit dependency_unresolved rejection, never a taskless queued row.
        missing_cfg = _fast_cfg(str(root / "missing"))
        missing_cfg.base_url = "http://x"
        missing_cfg.output_dir = str(root / "missing-legacy")
        missing_cfg.db_path = str(root / "missing.sqlite3")
        missing_cfg.authoritative_target = 1
        missing_cfg.authoritative_output_dir = str(root / "missing-fresh")
        missing_cfg.authoritative_native_contract = str(contract_path)
        missing_cfg.authoritative_game_version = "15.535.29"
        missing_cfg.retry.max_retries = 1
        missing = Crawler(
            missing_cfg,
            fetcher=MockFetcher({source_list_url: [(200, list_html)]}),
        )
        absent = {**candidate, "battle_tag": "ABSENT"}
        await missing.store.add_authoritative_upgrade_pages([(
            source_list_url,
            {"candidates": [absent], "phase": "authoritative_list_dependency"},
            "authoritative-list:absent",
        )])
        await missing.run(seeds=[])
        missing_stats = await missing.authoritative_stats()
        assert missing_stats["status"] == {"rejected": 1}, missing_stats
        assert missing_stats["tiers"] == {
            "rejected:dependency_unresolved": 1
        }, missing_stats
        missing.close()
    print("[ok] authoritative 两阶段依赖断点恢复 + accepted 原子发布崩溃窗")


def _install_test_list_lanes(crawler: Crawler, fetcher: MockFetcher) -> None:
    from .proxy_pool import ProxyState
    from .resource_guard import GIB, MemoryHysteresis

    crawler.list_fetcher = fetcher
    crawler._list_proxies = [
        ProxyState(None, TokenBucket(1000.0, 10, 0.0)),
        ProxyState("http://list-lane-b", TokenBucket(1000.0, 10, 0.0)),
    ]
    crawler._list_proxy = crawler._list_proxies[0]
    crawler._list_memory_guard = MemoryHysteresis(
        pause_below_gib=8.0,
        resume_at_gib=10.0,
        check_interval=0.0,
        probe=lambda: 32 * GIB,
    )


async def _test_upgrade_list_cross_lane_confirmation() -> None:
    """Parsed absence gets one confirmation, not eight full-page retries."""
    list_html, _replay_html, source_url = _authoritative_fixture()
    partitioned, incomplete = Crawler._partition_upgrade_candidates(
        [{"battle_tag": "INCOMPLETE"}],
        {"INCOMPLETE": {"metadata": {"authoritative_complete": False}}},
    )
    assert not partitioned and incomplete[0][1] == "source list metadata incomplete"
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
        root = Path(directory)
        contract_path = root / "contract.json"
        _write_test_native_contract(contract_path)
        cfg = _fast_cfg(directory)
        cfg.base_url = "http://x"
        cfg.db_path = str(root / "progress.sqlite3")
        cfg.output_dir = str(root / "legacy")
        cfg.authoritative_output_dir = str(root / "fresh")
        cfg.authoritative_target = 2
        cfg.authoritative_native_contract = str(contract_path)
        cfg.authoritative_game_version = "15.535.29"
        crawler = Crawler(cfg, fetcher=MockFetcher({}))
        list_fetcher = MockFetcher({
            source_url: [(200, list_html), (200, list_html)],
        })
        _install_test_list_lanes(crawler, list_fetcher)
        candidates = [
            {
                "battle_tag": "AUTH1", "replay_url": "http://x/replay/AUTH1",
                "source_path": str(root / "auth1.json"),
                "source_schema_version": 3, "upgrade_tier": "list_metadata_only",
            },
            {
                "battle_tag": "ABSENT", "replay_url": "http://x/replay/ABSENT",
                "source_path": str(root / "absent.json"),
                "source_schema_version": 3, "upgrade_tier": "list_metadata_only",
            },
        ]
        assert await crawler.store.add_authoritative_upgrade_pages([(
            source_url,
            {"candidates": candidates, "phase": "authoritative_list_dependency"},
            "authoritative-list:cross-lane",
        )])
        task = (await crawler.store.pending(limit=1))[0]
        await crawler._process_upgrade_list(task)

        assert len(list_fetcher.proxy_calls) == 2, list_fetcher.proxy_calls
        assert list_fetcher.proxy_calls[0][0] is None
        assert list_fetcher.proxy_calls[1][0] == "http://list-lane-b"
        upgrade_count = crawler.store.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE kind='upgrade'"
        ).fetchone()[0]
        assert upgrade_count == 1, upgrade_count
        row = crawler.store.conn.execute(
            "SELECT status,attempts,meta FROM tasks WHERE kind='upgrade_list'"
        ).fetchone()
        assert row["status"] == "done" and row["attempts"] == 0, dict(row)
        audit = json.loads(row["meta"])["list_confirmation_v1"]
        assert audit["state"] == "confirmed", audit
        assert audit["same_page_sha256"] is True, audit
        assert audit["confirm_observations"] == {
            "ABSENT": "battle tag absent from exact source list page"
        }, audit
        result_rows = {
            battle_tag: (status, tier, reason)
            for battle_tag, status, tier, reason in crawler.store.conn.execute(
                "SELECT battle_tag,status,tier,reason FROM authoritative_results"
            )
        }
        assert result_rows["AUTH1"][:2] == ("queued", "list_metadata_only")
        assert result_rows["ABSENT"][:2] == (
            "rejected", "dependency_unresolved"
        )
        assert "first_sha256=" in result_rows["ABSENT"][2]
        assert "confirm_sha256=" in result_rows["ABSENT"][2]
        assert crawler._stats["upgrade_list_confirmations"] == 1
        assert crawler._stats["upgrade_list_deterministic_rejections"] == 1
        assert crawler._stats["upgrade_list_page_cache_hits"] == 1
        metrics = {row["lane"]: row for row in crawler._list_telemetry.snapshot()}
        assert metrics["direct"]["requests"] == 1
        assert metrics["direct"]["missing_candidates"] == 1
        assert metrics["http://list-lane-b"]["requests"] == 1
        assert metrics["http://list-lane-b"]["missing_candidates"] == 1
        assert metrics["http://list-lane-b"]["cache_hits"] == 1
        await list_fetcher.aclose()
        crawler.close()

        # If the confirmation transport exhausts its in-call retries, the DB
        # task retains only unresolved candidates and resumes confirmation; the
        # already parsed source page and candidate are never fetched again.
        retry_root = root / "retry"
        retry_contract = retry_root / "contract.json"
        retry_root.mkdir()
        _write_test_native_contract(retry_contract)
        retry_cfg = _fast_cfg(str(retry_root))
        retry_cfg.base_url = "http://x"
        retry_cfg.db_path = str(retry_root / "progress.sqlite3")
        retry_cfg.output_dir = str(retry_root / "legacy")
        retry_cfg.authoritative_output_dir = str(retry_root / "fresh")
        retry_cfg.authoritative_target = 2
        retry_cfg.authoritative_native_contract = str(retry_contract)
        retry_cfg.authoritative_game_version = "15.535.29"
        retry_cfg.retry.max_retries = 2
        retry_cfg.persistent_retry_delay = 0.0
        retry_cfg.proxy.cooldown_error = 0.0
        retry_crawler = Crawler(retry_cfg, fetcher=MockFetcher({}))
        retry_fetcher = MockFetcher({
            source_url: [(200, list_html), (500, ""), (500, "")],
        })
        _install_test_list_lanes(retry_crawler, retry_fetcher)
        assert await retry_crawler.store.add_authoritative_upgrade_pages([(
            source_url,
            {"candidates": candidates, "phase": "authoritative_list_dependency"},
            "authoritative-list:retry-cross-lane",
        )])
        retry_task = (await retry_crawler.store.pending(limit=1))[0]
        await retry_crawler._process_upgrade_list(retry_task)
        pending_row = retry_crawler.store.conn.execute(
            "SELECT id,url,kind,meta,attempts,status FROM tasks WHERE kind='upgrade_list'"
        ).fetchone()
        pending_meta = json.loads(pending_row["meta"])
        assert pending_row["status"] == "pending" and pending_row["attempts"] == 1
        assert [c["battle_tag"] for c in pending_meta["candidates"]] == ["ABSENT"]
        assert pending_meta["list_confirmation_v1"]["state"] == "pending_cross_lane"
        assert len(retry_fetcher.proxy_calls) == 3, retry_fetcher.proxy_calls
        retry_fetcher.routes[source_url] = [(200, list_html)]
        resumed = dict(pending_row)
        resumed["meta"] = pending_meta
        await retry_crawler._process_upgrade_list(resumed)
        assert len(retry_fetcher.proxy_calls) == 4, retry_fetcher.proxy_calls
        final_row = retry_crawler.store.conn.execute(
            "SELECT status,attempts FROM tasks WHERE kind='upgrade_list'"
        ).fetchone()
        assert tuple(final_row) == ("done", 1), tuple(final_row)
        assert retry_crawler.store.conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE kind='upgrade'"
        ).fetchone()[0] == 1
        await retry_fetcher.aclose()
        retry_crawler.close()
    print("[ok] upgrade_list：候选拆分 + 单次跨lane确认 + 网络故障仅续确认")


async def _test_list_resource_guard_and_observability() -> None:
    from .list_observability import ListLaneTelemetry, PageHashCache
    from .resource_guard import GIB, MemoryHysteresis

    available = iter((12 * GIB, 7 * GIB, 9 * GIB, 10 * GIB))
    guard = MemoryHysteresis(
        pause_below_gib=8.0, resume_at_gib=10.0,
        check_interval=0.0, probe=lambda: next(available),
    )
    assert guard.refresh(force=True) is False
    assert guard.refresh(force=True) is True
    assert guard.refresh(force=True) is True  # 9 GiB stays paused: hysteresis
    assert guard.refresh(force=True) is False
    assert guard.transitions == 2

    parsed_calls = 0

    def parser(_text: str) -> list[dict]:
        nonlocal parsed_calls
        parsed_calls += 1
        return [{"tag": "A", "metadata": {"authoritative_complete": True}}]

    cache = PageHashCache(capacity=2)
    first = cache.parse(url="http://x/list", text="same", parser=parser)
    second = cache.parse(url="http://x/list", text="same", parser=parser)
    assert first.cache_hit is False and second.cache_hit is True
    assert first.sha256 == second.sha256 and parsed_calls == 1

    telemetry = ListLaneTelemetry()
    telemetry.request("lane-a")
    telemetry.response("lane-a", 0.010, success=True)
    telemetry.parsed("lane-a", cache_hit=True)
    telemetry.missing("lane-a", 2)
    telemetry.request("lane-b")
    telemetry.error("lane-b", 0.020, challenge=True)
    rows = {row["lane"]: row for row in telemetry.snapshot()}
    assert rows["lane-a"]["success"] == 1
    assert rows["lane-a"]["missing_candidates"] == 2
    assert rows["lane-a"]["latency_p95_ms"] == 10.0
    assert rows["lane-b"]["challenges"] == 1

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
        store = TaskStore(str(Path(directory) / "guard.sqlite3"), max_retries=3)
        assert await store.add(
            "http://x/ready", kind="upgrade", dedup_key="guard:ready"
        )
        assert await store.add(
            "http://x/list", kind="upgrade_list", dedup_key="guard:list"
        )
        leased = await store.pending_authoritative_fair(
            limit=10, ready_limit=8, list_limit=0
        )
        assert [row["kind"] for row in leased] == ["upgrade"], leased
        list_status = store.conn.execute(
            "SELECT status FROM tasks WHERE kind='upgrade_list'"
        ).fetchone()[0]
        assert list_status == "pending"
        store.close()
    print("[ok] list资源门：8→10GiB滞回 + replay继续 + SHA缓存/逐lane指标")


def _test_parse_player_tags() -> None:
    from .parsers import parse_battles, parse_next_battles_page, parse_player_links, parse_player_tags
    html = (
        '<button class="replay_button" data-team-tags="T,A" data-opponent-tags="B,C"></button>'
        '<button class="replay_button" data-team-tags="T" data-opponent-tags="D"></button>'
    )
    tags = parse_player_tags(html)
    assert tags == ["T", "A", "B", "C", "D"], tags

    # 对局 tag 去重：同一场对局双方视角只算一次
    dup = (
        '<button class="replay_button" data-replay="BT1" data-team-tags="A" data-opponent-tags="B" '
        'data-team-crowns="1" data-opponent-crowns="0"></button>'
        '<button class="replay_button" data-replay="BT1" data-team-tags="B" data-opponent-tags="A" '
        'data-team-crowns="0" data-opponent-crowns="1"></button>'
    )
    bs = parse_battles(dup, "https://x", "ref")
    assert len(bs) == 1 and bs[0]["tag"] == "BT1", bs

    # 翻页
    paged = (
        '<a class="item disabled" href="#&"><i class="angle left"></i></a>'
        '<a class="item" href="/player/T/battles/history?before=1787445810000&&"><i class="angle right"></i></a>'
    )
    assert parse_next_battles_page(paged, "https://royaleapi.com") == \
        "https://royaleapi.com/player/T/battles/history?before=1787445810000"
    assert parse_next_battles_page('<a class="item disabled" href="#&"></a>', "https://royaleapi.com") is None

    # 榜单玩家链接
    lb = (
        '<a href="/player/ABC123">x</a><a href="/clan/QQQ">c</a>'
        '<a href="/player/DEF456">y</a>'
        'initRoster($("#r"), [{"rank":"1","tag":"ROSTER9"}])'
    )
    assert parse_player_links(lb) == ["ABC123", "DEF456", "ROSTER9"]
    print("[ok] 玩家 tag 提取 / 对局tag去重 / 翻页链接 / 榜单链接解析")


def _test_parse_full_decks() -> None:
    from .parsers import is_normal_1v1, parse_battles

    def player(tag: str, prefix: str) -> str:
        cards = "".join(
            f'<div class="deck_card__four_wide"><img class="deck_card" data-card-key="{prefix}{i}">'
            f'<div class="card-level">Lvl {10+i}</div></div>' for i in range(8)
        )
        return (
            '<div class="team-segment">'
            f'<a class="player_name_header" href="/player/{tag}/battles">p</a>'
            f'<div id="deck_{prefix}">{cards}</div>'
            '<div class="deck_tower_card__container"><img class="deck_card deck_card_key_tower-princess"></div>'
            '</div>'
        )

    def button(tag: str, team: str, opponent: str) -> str:
        return (
            f'<button class="replay_button" data-replay="{tag}" data-team-tags="{team}" '
            f'data-opponent-tags="{opponent}" data-team-crowns="1" data-opponent-crowns="0" data-draft="0"></button>'
        )

    one_v_one = (
        '<div class="battle_list_battle" data-battle-type="trail" data-timestamp="1">'
        '<h4 class="game_mode_header">1v1</h4><div class="battle-team-segment-container">'
        + player("A", "a") + player("B", "b") + '</div>' + button("B1", "A", "B") + '</div>'
    )
    two_v_two = (
        '<div class="battle_list_battle" data-battle-type="trail" data-timestamp="2">'
        '<h4 class="game_mode_header">2v2</h4><div class="battle-team-segment-container">'
        + player("A", "a") + player("C", "c") + player("B", "b") + player("D", "d")
        + '</div>' + button("B2", "A,C", "B,D") + '</div>'
    )
    duel_round = '<div class="battle-team-segment-container">' + player("A", "a") + player("B", "b") + '</div>'
    duel_round2 = '<div class="battle-team-segment-container">' + player("A", "e") + player("B", "f") + '</div>'
    duel = (
        '<div class="battle_list_battle" data-battle-type="riverRaceDuel" data-timestamp="3">'
        '<h4 class="game_mode_header">Duel</h4>' + duel_round + duel_round2
        + button("B3", "A", "B") + '</div>'
    )
    rows = parse_battles(one_v_one + two_v_two + duel, "https://x", "https://x/list")
    assert len(rows) == 3
    md1, md2, md3 = [row["metadata"] for row in rows]
    assert md1["complete"] and len(md1["rounds"]) == 1
    assert md1["timestamp"] == 1
    assert md1["team_crowns"] == 1 and md1["opponent_crowns"] == 0
    assert is_normal_1v1(md1)
    assert len(md1["rounds"][0]["team"][0]["full_deck"]) == 8
    assert md1["rounds"][0]["team"][0]["tower_troop"] == "tower-princess"
    assert md2["complete"] and len(md2["rounds"][0]["team"]) == 2 and len(md2["rounds"][0]["opponent"]) == 2
    assert md3["complete"] and len(md3["rounds"]) == 2 and md3["battle_type"] == "riverRaceDuel"
    assert not is_normal_1v1(md2) and not is_normal_1v1(md3)
    popup_timestamp = one_v_one.replace(
        ' data-timestamp="1"', ''
    ).replace(
        '<h4 class="game_mode_header">',
        '<div class="battle-timestamp-popup" data-content="2026-08-26 01:02:03 UTC"></div>'
        '<h4 class="game_mode_header">',
        1,
    )
    popup_md = parse_battles(popup_timestamp, "https://x", "https://x/list")[0]["metadata"]
    assert popup_md["timestamp"] == 1787706123, popup_md["timestamp"]
    merged = Crawler._merge_battle_metadata(
        {"battle_tag": "B1", "card_counts": {"team": {"a0": 1}, "opponent": {"b0": 1}}}, md1
    )
    assert merged["schema_version"] == 5
    assert len(merged["team_deck"]) == 8 and len(merged["opponent_deck"]) == 8
    assert "a1" in merged["deck_validation"]["team_unplayed_cards"]
    print("[ok] 完整卡组解析：1v1 / 2v2 / Duel rounds + 塔兵 + 等级")


def _test_curl_cffi_windows_workaround() -> None:
    import os
    if os.name != "nt":
        return
    from .curl_cffi_windows import (
        _DiscardingSocketSet,
        _safe_consume_waker,
        _safe_wake_selector,
        install_curl_cffi_windows_workarounds,
    )

    sockets = _DiscardingSocketSet({7})
    sockets.remove(7)
    sockets.remove(7)  # 重复 CURL_POLL_REMOVE 必须幂等

    class BrokenWaker:
        def send(self, _value):
            raise ConnectionResetError(10054, "reset")

        def recv(self, _size):
            raise ConnectionResetError(10054, "reset")

    fake = type("FakeSelector", (), {
        "_closed": False, "_waker_w": BrokenWaker(), "_waker_r": BrokenWaker(),
    })()
    _safe_wake_selector(fake)
    _safe_consume_waker(fake)
    assert install_curl_cffi_windows_workarounds()
    print("[ok] curl_cffi Windows：重复移除与 WinError 10054 唤醒竞态已幂等处理")


async def _test_proxy_pool() -> None:
    from .config import ProxyPoolConfig, RateLimitConfig
    from .proxy_pool import ProxyPool

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
        pf = Path(d) / "p.txt"
        pf.write_text("http://a:1\nhttp://b:2\n# 注释\n", encoding="utf-8")
        cfg = ProxyPoolConfig(proxies=["http://c:3"], proxies_file=str(pf))
        rate = RateLimitConfig(requests_per_second=1000.0, burst=100, jitter=0.0)
        pool = ProxyPool(cfg, rate)
        # config 1 个 + 文件 2 个 = 3 个代理
        assert pool.size == 3, pool.size
        labels = sorted(s.label for s in pool.states)
        assert labels == ["http://a:1", "http://b:2", "http://c:3"], labels

        # Concurrent reservations must spread across full buckets instead of
        # all queueing behind the first lane.
        reserved = await asyncio.gather(*(pool.acquire() for _ in range(3)))
        assert len({state.label for state in reserved}) == 3, [
            state.label for state in reserved
        ]

        # 封禁后冷却不可用
        s0 = pool.states[0]
        pool.report_forbidden(s0)
        assert not s0.available_now

        # pick 只返回可用的代理
        for _ in range(5):
            p = await pool.pick()
            assert p is not s0 and p.url is not None

        # 无代理 → 直连单 IP 兜底
        pool2 = ProxyPool(ProxyPoolConfig(proxies=[], proxies_file=""), rate)
        assert pool2.size == 1 and pool2.states[0].url is None
        print("[ok] 代理池：proxies.txt 加载 / 每代理独立 / 封禁冷却 / 轮换 / 直连兜底")


async def run_selftest() -> bool:
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        await _test_token_bucket()
        await _test_proxy_pool()
        await _test_store()
        await _test_retry()
        await _test_two_stage()
        await _test_legacy_detail_metadata_recovery()
        _test_flaresolverr()
        _test_parse_replay()
        _test_authoritative_schema_and_gate()
        await _test_authoritative_two_phase_resume()
        await _test_upgrade_list_cross_lane_confirmation()
        await _test_list_resource_guard_and_observability()
        _test_parse_player_tags()
        _test_parse_full_decks()
        _test_curl_cffi_windows_workaround()
        print("全部自检通过 [PASS]")
        return True
    except AssertionError as e:
        print(f"自检失败: {e}")
        import traceback
        traceback.print_exc()
        return False
