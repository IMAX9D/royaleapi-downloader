"""royaleapi 对局列表页 HTML 解析：提取每场对局的 /data/replay 参数并拼出回放 URL。

从浏览器抓包确认的真实结构：
    列表页 /player/{tag}/battles 是服务端渲染 HTML，每场可回放的对局有一个
        <button class="... replay_button ..."
                data-replay="{对局tag}"
                data-team-tags="A,B" data-opponent-tags="C,D"
                data-team-crowns="1" data-opponent-crowns="0" ...>
    二次点击时前端请求：
        /data/replay?tag={对局tag}&team_tags=A,B&opponent_tags=C,D
            &team_crowns=1&opponent_crowns=0&referrer_path={列表页URL}
    返回 {"success":true,"html":"<div class='battle_replay'>..."}
"""
from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Optional

from selectolax.parser import HTMLParser

_BUTTON_RE = re.compile(r"<button[^>]*\breplay_button\b[^>]*>", re.S)
NORMAL_1V1_GAME_MODES = {
    "Ranked", "Ladder", "1v1", "1v1 Battle", "1v1 Showdown",
    "Classic Challenge", "Grand Challenge", "Normal Battle",
}
FORM_SUFFIX_RE = re.compile(r"-(?:ev\d+|hero)$")

# schema_version=5 is the complete authoritative replay contract: schema 4
# introduced explicit data-i-aware coordinates, while schema 5 additionally
# requires the joined numeric mode, battle index, exact timestamp, six final
# tower HP values, tower troop levels and native-static eligibility metadata.
OUTPUT_SCHEMA_VERSION = 5
COORDINATE_SCHEMA_VERSION = 1
COORDINATE_TRANSFORM_ID = "royaleapi_data_i_to_libg_native_v1"
NATIVE_ARENA_WIDTH = 18_000
NATIVE_ARENA_HEIGHT = 32_000


class ReplayParseError(ValueError):
    """A replay marker cannot be preserved exactly."""


class ReplayCoordinateError(ReplayParseError):
    """A replay marker cannot be mapped to native coordinates unambiguously."""


def coordinate_provenance() -> dict:
    """Return the explicit coordinate contract embedded in every new replay."""
    return {
        "schema_version": COORDINATE_SCHEMA_VERSION,
        "transform_id": COORDINATE_TRANSFORM_ID,
        "source": "royaleapi_replay_marker",
        "source_fields": ["data-x", "data-y", "data-i"],
        "target": "libg_native_arena",
        "arena_width": NATIVE_ARENA_WIDTH,
        "arena_height": NATIVE_ARENA_HEIGHT,
        "data_i_0": "rotate_180",
        "data_i_1": "identity",
        "missing_or_invalid_data_i": "reject",
    }


def derive_native_coordinates(
    x_raw: int,
    y_raw: int,
    data_i: int,
    *,
    marker_index: Optional[int] = None,
) -> tuple[int, int, str]:
    """Map one RoyaleAPI marker into libg native arena coordinates.

    RoyaleAPI's display layer rotates markers whose ``data-i`` is 1. The raw
    marker coordinates therefore need the inverse display treatment when they
    are written as native training actions:

    * data-i=0 -> rotate 180 degrees
    * data-i=1 -> identity

    Missing/invalid orientation is deliberately rejected; guessing would make
    new JSON indistinguishable from the legacy unconditional transform.
    """
    where = f" marker_index={marker_index}" if marker_index is not None else ""
    if isinstance(data_i, bool) or data_i not in (0, 1):
        raise ReplayCoordinateError(f"invalid or missing data-i={data_i!r}{where}")
    if not (0 <= x_raw <= NATIVE_ARENA_WIDTH and 0 <= y_raw <= NATIVE_ARENA_HEIGHT):
        raise ReplayCoordinateError(
            f"raw coordinate out of native arena bounds: ({x_raw},{y_raw}){where}"
        )
    if data_i == 0:
        return (
            NATIVE_ARENA_WIDTH - x_raw,
            NATIVE_ARENA_HEIGHT - y_raw,
            "rotate_180",
        )
    return x_raw, y_raw, "identity"


def base_card_key(value: str) -> str:
    return FORM_SUFFIX_RE.sub("", str(value).lower())


def card_form(value: str) -> str:
    slug = str(value).strip().lower()
    if slug.endswith("-hero"):
        return "hero"
    match = re.search(r"-(ev\d+)$", slug)
    return match.group(1) if match else "base"


def _attr(tag: str, name: str) -> str:
    m = re.search(name + r'="([^"]*)"', tag)
    return m.group(1) if m else ""


def build_replay_url(
    base: str,
    tag: str,
    team_tags: str,
    opponent_tags: str,
    team_crowns: str,
    opponent_crowns: str,
    referrer_path: str,
) -> str:
    """拼出 /data/replay 请求 URL。与前端一致：只对 tags 里的逗号做 %2C 编码，referrer 原样。"""
    tt = team_tags.replace(",", "%2C")
    ot = opponent_tags.replace(",", "%2C")
    return (
        f"{base.rstrip('/')}/data/replay?tag={tag}"
        f"&team_tags={tt}&opponent_tags={ot}"
        f"&team_crowns={team_crowns}&opponent_crowns={opponent_crowns}"
        f"&referrer_path={referrer_path}"
    )


def _tower_key(node) -> Optional[str]:
    if node is None:
        return None
    for value in str(node.attributes.get("class", "")).split():
        if value.startswith("deck_card_key_"):
            return value.removeprefix("deck_card_key_")
    return None


def _tower_level(node) -> Optional[int]:
    if node is None or node.parent is None:
        return None
    level_node = node.parent.css_first(".level")
    if level_node is None:
        return None
    match = re.search(r"Lvl\s*(\d+)\b", level_node.text(strip=True))
    return int(match.group(1)) if match else None


def _battle_timestamp(block) -> Optional[int]:
    raw = block.attributes.get("data-timestamp")
    if raw:
        try:
            numeric = float(raw)
            if not numeric.is_integer():
                raise ValueError("fractional battle timestamp")
            value = int(numeric)
            return value // 1000 if value > 10_000_000_000 else value
        except ValueError:
            pass
    popup = block.css_first(".battle-timestamp-popup")
    content = popup.attributes.get("data-content") if popup is not None else None
    if content:
        for pattern in ("%Y-%m-%d %H:%M:%S UTC", "%Y-%m-%d %H:%M UTC"):
            try:
                return int(datetime.strptime(content.strip(), pattern).replace(
                    tzinfo=timezone.utc
                ).timestamp())
            except ValueError:
                continue
    return None


def _battle_time_utc(block) -> Optional[str]:
    popup = block.css_first(".battle-timestamp-popup")
    content = popup.attributes.get("data-content") if popup is not None else None
    if not content:
        return None
    value = content.strip()
    for pattern in ("%Y-%m-%d %H:%M:%S UTC", "%Y-%m-%d %H:%M UTC"):
        try:
            parsed = datetime.strptime(value, pattern).replace(tzinfo=timezone.utc)
            return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    return None


def _deck_record(deck_node, player_tag: str, tower_node=None) -> dict:
    cards: list[str] = []
    levels: dict[str, int] = {}
    for image in deck_node.css("img.deck_card[data-card-key]"):
        key = image.attributes.get("data-card-key")
        if not key:
            continue
        cards.append(key)
        try:
            container = image.parent.parent
            level_node = container.css_first(".card-level") if container else None
            if level_node:
                match = re.search(r"\d+", level_node.text(strip=True))
                if match:
                    levels[key] = int(match.group(0))
        except Exception:
            pass
    complete = (
        len(cards) == 8
        and len(set(cards)) == 8
        and len({base_card_key(card) for card in cards}) == 8
        and all(card and card != "_invalid" for card in cards)
        and len(levels) == 8
        and all(isinstance(levels.get(card), int) for card in cards)
    )
    deck_cards = [
        {
            "slot": index,
            "slug": slug,
            "base_slug": base_card_key(slug),
            "form": card_form(slug),
            "level": levels.get(slug),
        }
        for index, slug in enumerate(cards)
    ]
    return {
        "player_tag": player_tag,
        "full_deck": cards,
        "deck_cards": deck_cards,
        "tower_troop": _tower_key(tower_node),
        "tower_troop_level": _tower_level(tower_node),
        "card_levels": levels,
        "complete": complete,
    }


def _parse_final_tower_hp(block) -> Optional[dict]:
    node = block.css_first(".hp-both-popup")
    if node is None:
        return None
    attrs = node.attributes

    def side(prefix: str) -> Optional[dict]:
        names = {
            "king": f"data-{prefix}-king",
            "princess0": f"data-{prefix}-princess0",
            "princess1": f"data-{prefix}-princess1",
            "total": f"data-{prefix}-total",
        }
        if any(not str(attrs.get(source, "")).isdigit() for source in names.values()):
            return None
        result = {key: int(attrs[source]) for key, source in names.items()}
        if result["total"] != (
            result["king"] + result["princess0"] + result["princess1"]
        ):
            return None
        return result

    team = side("team")
    opponent = side("oppo")
    if team is None or opponent is None:
        return None
    return {
        "team": team,
        "opponent": opponent,
        "provenance": "list_hp_both_popup",
        "slot_mapping_provenance": "source_slots_unmapped",
        "raw_princess_mapping": {
            "princess0": "data-*-princess0",
            "princess1": "data-*-princess1",
        },
    }


def _battle_metadata(block, button, matchup_button, referrer_path: str) -> dict:
    attrs = button.attributes
    matchup_attrs = matchup_button.attributes if matchup_button is not None else {}
    team_tags = [x for x in attrs.get("data-team-tags", "").split(",") if x]
    opponent_tags = [x for x in attrs.get("data-opponent-tags", "").split(",") if x]
    rounds: list[dict] = []

    for segment in block.css(".battle-team-segment-container"):
        players = []
        for link in segment.css("a.player_name_header"):
            match = re.search(r"/player/([^/]+)/battles", link.attributes.get("href", ""))
            if match:
                players.append(match.group(1))
        decks = segment.css('div[id^="deck_"]')
        towers = segment.css(".deck_tower_card__container img.deck_card")
        records = [
            _deck_record(deck, players[index] if index < len(players) else "", towers[index] if index < len(towers) else None)
            for index, deck in enumerate(decks)
        ]
        by_tag = {row["player_tag"]: row for row in records if row["player_tag"]}
        round_data = {
            "team": [by_tag[tag] for tag in team_tags if tag in by_tag],
            "opponent": [by_tag[tag] for tag in opponent_tags if tag in by_tag],
        }
        if round_data["team"] or round_data["opponent"]:
            rounds.append(round_data)

    expected = len(team_tags) + len(opponent_tags)
    complete = bool(rounds) and all(
        len(round_data["team"]) == len(team_tags)
        and len(round_data["opponent"]) == len(opponent_tags)
        and all(player.get("complete") for side in ("team", "opponent") for player in round_data[side])
        and len(round_data["team"]) + len(round_data["opponent"]) == expected
        for round_data in rounds
    )
    mode = block.css_first(".game_mode_header")
    game_mode = mode.text(strip=True) if mode else None
    replay_index_raw = attrs.get("data-index", "")
    matchup_index_raw = matchup_attrs.get("data-index", "")
    indexes_match = bool(replay_index_raw) and replay_index_raw == matchup_index_raw
    battle_index = int(replay_index_raw) if replay_index_raw.isdigit() else None
    numeric_mode_raw = matchup_attrs.get("data-game-mode-id", "") if indexes_match else ""
    numeric_game_mode_id = int(numeric_mode_raw) if numeric_mode_raw.isdigit() else None
    matchup_team_deck = [
        value.strip() for value in matchup_attrs.get("data-team-deck", "").split(",")
        if value.strip()
    ]
    matchup_opponent_deck = [
        value.strip() for value in matchup_attrs.get("data-opponent-deck", "").split(",")
        if value.strip()
    ]
    embedded_team_deck = (
        rounds[0]["team"][0].get("full_deck", [])
        if len(rounds) == 1 and len(rounds[0].get("team", [])) == 1 else []
    )
    embedded_opponent_deck = (
        rounds[0]["opponent"][0].get("full_deck", [])
        if len(rounds) == 1 and len(rounds[0].get("opponent", [])) == 1 else []
    )
    deck_crosscheck_complete = (
        len(matchup_team_deck) == 8
        and len(matchup_opponent_deck) == 8
        and {base_card_key(value) for value in matchup_team_deck}
        == {base_card_key(value) for value in embedded_team_deck}
        and {base_card_key(value) for value in matchup_opponent_deck}
        == {base_card_key(value) for value in embedded_opponent_deck}
    )
    timestamp = _battle_timestamp(block)
    battle_time_utc = _battle_time_utc(block)
    battle_time_utc_provenance = "battle_timestamp_popup_utc" if battle_time_utc else None
    if battle_time_utc is None and timestamp is not None:
        battle_time_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        battle_time_utc_provenance = "derived_from_exact_list_timestamp"
    final_tower_hp = _parse_final_tower_hp(block)
    tower_metadata_complete = bool(rounds) and all(
        isinstance(player.get("tower_troop"), str)
        and bool(player.get("tower_troop"))
        and isinstance(player.get("tower_troop_level"), int)
        and player["tower_troop_level"] > 0
        for round_data in rounds
        for side in ("team", "opponent")
        for player in round_data.get(side, [])
    )
    metadata = {
        "schema_version": 2,
        "battle_type": block.attributes.get("data-battle-type"),
        "game_mode": game_mode,
        "numeric_game_mode_id": numeric_game_mode_id,
        "numeric_game_mode_provenance": (
            "list_matchup_button_joined_by_data_index" if numeric_game_mode_id is not None else None
        ),
        "battle_index": battle_index,
        "battle_index_provenance": (
            "list_replay_and_matchup_button_data_index" if indexes_match else None
        ),
        "matchup_players": matchup_attrs.get("data-players") if indexes_match else None,
        "draft": attrs.get("data-draft") == "1",
        "timestamp": timestamp,
        "version_timestamp": timestamp,
        "version_timestamp_provenance": "battle_list_exact_timestamp",
        "battle_time_utc": battle_time_utc,
        "battle_time_utc_provenance": battle_time_utc_provenance,
        "team_tags": team_tags,
        "opponent_tags": opponent_tags,
        "team_crowns": int(attrs["data-team-crowns"])
        if attrs.get("data-team-crowns", "").isdigit() else None,
        "opponent_crowns": int(attrs["data-opponent-crowns"])
        if attrs.get("data-opponent-crowns", "").isdigit() else None,
        "rounds": rounds,
        "matchup_team_deck": matchup_team_deck,
        "matchup_opponent_deck": matchup_opponent_deck,
        "deck_crosscheck_complete": deck_crosscheck_complete,
        "final_tower_hp": final_tower_hp,
        "complete": complete,
        "authoritative_complete": bool(
            complete
            and indexes_match
            and battle_index is not None
            and numeric_game_mode_id is not None
            and matchup_attrs.get("data-players") == "1v1"
            and timestamp is not None
            and battle_time_utc is not None
            and deck_crosscheck_complete
            and tower_metadata_complete
            and final_tower_hp is not None
        ),
        "source": "battle_list_html",
        "source_list_url": referrer_path,
    }
    metadata["normal_1v1"] = is_normal_1v1(metadata)
    return metadata


def is_normal_1v1(metadata: Optional[dict]) -> bool:
    if not metadata or not metadata.get("complete") or metadata.get("draft"):
        return False
    rounds = metadata.get("rounds") or []
    if len(rounds) != 1:
        return False
    round_data = rounds[0]
    if len(round_data.get("team", [])) != 1 or len(round_data.get("opponent", [])) != 1:
        return False
    for side in ("team", "opponent"):
        player = round_data[side][0]
        deck = player.get("full_deck") or []
        levels = player.get("card_levels") or {}
        if (
            len(deck) != 8
            or len(set(deck)) != 8
            or any(not card or card == "_invalid" for card in deck)
            or len(levels) != 8
            or any(not isinstance(levels.get(card), int) for card in deck)
        ):
            return False
    if metadata.get("battle_type") == "riverRaceDuel":
        return False
    return metadata.get("game_mode") in NORMAL_1V1_GAME_MODES


def parse_battles(html: str, base: str, referrer_path: str) -> list[dict]:
    """从列表页 HTML 解析每场对局：返回 [{"url":..., "tag":...}, ...]，按对局 tag 去重。

    同一场对局会从双方玩家视角各出现一次（team_tags/opponent_tags 不同），
    但对局 tag（data-replay）相同，这里按 tag 去重，避免重复抓取。
    """
    out: list[dict] = []
    seen: set[str] = set()
    tree = HTMLParser(html)
    for block in tree.css(".battle_list_battle"):
        matchup_by_index = {
            node.attributes.get("data-index", ""): node
            for node in block.css(".matchup_button")
            if node.attributes.get("data-index")
        }
        for button in block.css(".replay_button"):
            attrs = button.attributes
            tag = attrs.get("data-replay", "")
            if not tag or tag in seen:
                continue
            seen.add(tag)
            url = build_replay_url(
                base, tag,
                attrs.get("data-team-tags", ""), attrs.get("data-opponent-tags", ""),
                attrs.get("data-team-crowns", ""), attrs.get("data-opponent-crowns", ""),
                referrer_path,
            )
            out.append({
                "url": url,
                "tag": tag,
                "metadata": _battle_metadata(
                    block,
                    button,
                    matchup_by_index.get(attrs.get("data-index", "")),
                    referrer_path,
                ),
            })

    # 简化测试 fixture / 旧 HTML 的兼容回退。
    for btn in _BUTTON_RE.findall(html):
        tag = _attr(btn, "data-replay")
        if not tag or tag in seen:
            continue
        seen.add(tag)
        url = build_replay_url(
            base,
            tag,
            _attr(btn, "data-team-tags"),
            _attr(btn, "data-opponent-tags"),
            _attr(btn, "data-team-crowns"),
            _attr(btn, "data-opponent-crowns"),
            referrer_path,
        )
        out.append({
            "url": url,
            "tag": tag,
            "metadata": {
                "schema_version": 2,
                "rounds": [],
                "complete": False,
                "authoritative_complete": False,
                "source": "battle_list_html",
                "source_list_url": referrer_path,
            },
        })
    return out


def parse_battles_replay_urls(html: str, base: str, referrer_path: str) -> list[str]:
    """只返回对局 URL 列表（不含 tag），兼容旧用法。"""
    return [b["url"] for b in parse_battles(html, base, referrer_path)]


def parse_player_tags(html: str) -> list[str]:
    """从列表页 HTML 提取所有玩家 tag（team_tags + opponent_tags，去重保序）。

    用于"雪球式发现"：每场对局会暴露双方 2~4 个玩家 tag，据此不断发现新玩家。
    """
    tags: list[str] = []
    seen: set[str] = set()
    for btn in _BUTTON_RE.findall(html):
        for name in ("data-team-tags", "data-opponent-tags"):
            for t in _attr(btn, name).split(","):
                t = t.strip()
                if t and t not in seen:
                    seen.add(t)
                    tags.append(t)
    return tags


def parse_next_battles_page(html: str, base: str) -> Optional[str]:
    """从对局列表页底部分页里取"下一页"URL；没有下一页返回 None。

    翻页机制：/player/{tag}/battles 是第一页，下一页是
    /player/{tag}/battles/history?before={毫秒时间戳}；最后一页的下一页是 disabled。
    """
    from urllib.parse import urljoin
    candidates=[]
    for node in HTMLParser(html).css('a[href]'):
        href=node.attributes.get('href','')
        if 'history?before=' not in href or 'disabled' in node.attributes.get('class','').split():continue
        if node.css_first('.angle.left.icon') is not None:continue
        candidate=urljoin(base.rstrip('/')+'/',href.rstrip('&'))
        if node.css_first('.angle.right.icon') is not None or 'next' in node.attributes.get('rel','').split():
            return candidate
        candidates.append(candidate)
    candidates=list(dict.fromkeys(candidates))
    if len(candidates)>1:raise ValueError('ambiguous history pagination direction')
    return candidates[0] if candidates else None


_PLAYER_HREF_RE = re.compile(r'href="/player/([A-Za-z0-9]{3,})"')
_ROSTER_PLAYER_TAG_RE = re.compile(
    r'"rank"\s*:\s*"?\d+"?\s*,\s*"tag"\s*:\s*"([A-Za-z0-9]{3,})"'
)


def parse_player_links(html: str) -> list[str]:
    """从任意榜单/列表页提取玩家 tag（匹配 /player/{tag} 链接，去重保序）。

    用于给爬虫喂种子，例如 Pro 玩家榜单 https://royaleapi.com/players/pro。
    """
    tags: list[str] = []
    seen: set[str] = set()
    for m in _PLAYER_HREF_RE.finditer(html):
        t = m.group(1)
        if t not in seen:
            seen.add(t)
            tags.append(t)
    # Current leaderboard pages render player rows from an embedded initRoster
    # JSON payload instead of /player/{tag} anchors.
    for m in _ROSTER_PLAYER_TAG_RE.finditer(html):
        t = m.group(1)
        if t not in seen:
            seen.add(t)
            tags.append(t)
    return tags


# ---------------------------------------------------------------------------
# 回放 HTML → 结构化 JSON（对齐之前的格式）
# 坐标规则（按 RoyaleAPI marker 的 data-i 显式派生）：
#   data-i=0: x = 18000 - x_raw, y = 32000 - y_raw
#   data-i=1: x = x_raw,         y = y_raw
#   tile_x = x/1000,    tile_y = y/1000
#   x_norm = x/18000,   y_norm = y/32000
#   time   = data-t / 20
# ---------------------------------------------------------------------------

_MARKER_RE = re.compile(r'<div class="(blue|red) marker"[^>]*>')


def _parse_elixir_table(table_html: str) -> dict:
    tbl: dict = {}
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.S):
        t = re.search(r'class="title">\s*([^<]+)', tr)
        if not t:
            continue
        title = t.group(1).strip()
        c = re.search(r'class="[^"]*count[^"]*">\s*(\d+)', tr)
        e = re.search(r'class="[^"]*elixir[^"]*"[^>]*>\s*([\d.]*)\s*<', tr)
        tbl[title] = {
            "count": int(c.group(1)) if c else None,
            "elixir": float(e.group(1)) if (e and e.group(1).strip()) else None,
        }
    return tbl


def parse_replay_html(html: str) -> dict:
    """把 /data/replay 返回的 HTML 解析成结构化 JSON。"""
    # battle_tag
    m = re.search(r'class="battle_replay"\s+data-tag="([^"]+)"', html)
    battle_tag = m.group(1) if m else None

    # card_plays：解析 arena markers（含坐标的每张卡牌）
    plays: list[dict] = []
    ability_plays: list[dict] = []
    for marker_index, mm in enumerate(_MARKER_RE.finditer(html)):
        tag = mm.group(0)
        color = mm.group(1)
        card = _attr(tag, "data-c")
        if not card:
            raise ReplayParseError(
                f"missing data-c marker_index={marker_index}"
            )
        x_raw_s = _attr(tag, "data-x")
        y_raw_s = _attr(tag, "data-y")
        t_s = _attr(tag, "data-t")
        if not t_s.isdigit():
            raise ReplayParseError(
                f"invalid data-t={t_s!r} marker_index={marker_index}"
            )
        t = int(t_s)
        s = _attr(tag, "data-s") or "o"
        side = "team" if s == "t" else "opponent"
        data_i_s = _attr(tag, "data-i")
        if data_i_s not in ("", "0", "1"):
            raise ReplayCoordinateError(
                f"invalid data-i={data_i_s!r} marker_index={marker_index}"
            )
        data_i = int(data_i_s) if data_i_s in ("0", "1") else None
        if card == "_invalid":
            x_raw = int(x_raw_s) if x_raw_s.isdigit() else None
            y_raw = int(y_raw_s) if y_raw_s.isdigit() else None
            ability: dict = {
                "time": round(t / 20, 2),
                "time_raw": t,
                "side": side,
                "color": color,
                "ability_id": None,
                "resolution_status": "unresolved",
                "marker_index": marker_index,
                "data_i": data_i,
                "x_raw": x_raw,
                "y_raw": y_raw,
            }
            if x_raw is None and y_raw is None:
                # Ability markers normally carry no spatial action. Missing
                # data-i is explicit and safe only in this non-spatial case.
                ability["coordinate_status"] = "not_applicable"
            elif x_raw is None or y_raw is None:
                raise ReplayCoordinateError(
                    f"partial ability coordinate marker_index={marker_index}"
                )
            else:
                x, y, transform = derive_native_coordinates(
                    x_raw, y_raw, data_i, marker_index=marker_index
                )
                ability.update({
                    "coordinate_status": "resolved",
                    "coordinate_provenance": COORDINATE_TRANSFORM_ID,
                    "coordinate_transform": transform,
                    "x": x,
                    "y": y,
                })
            ability_plays.append(ability)
            continue
        if not (x_raw_s.isdigit() and y_raw_s.isdigit()):
            raise ReplayCoordinateError(
                f"missing deployment coordinate marker_index={marker_index}"
            )
        x_raw = int(x_raw_s)
        y_raw = int(y_raw_s)
        x, y, transform = derive_native_coordinates(
            x_raw, y_raw, data_i, marker_index=marker_index
        )
        plays.append({
            "time": round(t / 20, 2),
            "time_raw": t,
            "side": side,
            "color": color,
            "card": card,
            "marker_index": marker_index,
            "data_i": data_i,
            "x_raw": x_raw,
            "y_raw": y_raw,
            "coordinate_provenance": COORDINATE_TRANSFORM_ID,
            "coordinate_transform": transform,
            "x": x,
            "y": y,
            "x_norm": round(x / NATIVE_ARENA_WIDTH, 4),
            "y_norm": round(y / NATIVE_ARENA_HEIGHT, 4),
            "tile_x": round(x / 1000, 1),
            "tile_y": round(y / 1000, 1),
        })

    team_plays = sum(1 for p in plays if p["side"] == "team")
    opp_plays = sum(1 for p in plays if p["side"] == "opponent")

    # elixir 表（两张：team / opponent），用 Total.count 与 play 数交叉匹配
    tables = [_parse_elixir_table(tb)
              for tb in re.findall(r'<table[^>]*replay_elixir_table[^>]*>.*?</table>', html, re.S)]
    team_tbl = opp_tbl = None
    for tbl in tables:
        total = (tbl.get("Total") or {}).get("count")
        if team_tbl is None and total == team_plays:
            team_tbl = tbl
        elif opp_tbl is None and total == opp_plays:
            opp_tbl = tbl
    if team_tbl is None and tables:
        team_tbl = tables[0]
    if opp_tbl is None and len(tables) >= 2:
        opp_tbl = tables[1]

    # card_counts：由 plays 派生
    card_counts = {"team": {}, "opponent": {}}
    for p in plays:
        bucket = card_counts[p["side"]]
        bucket[p["card"]] = bucket.get(p["card"], 0) + 1

    # duration：只信 replay timeline 的 end_time；页面其他区域可能含本地时钟。
    end_time = re.search(
        r'class="[^"]*marker[^"]*\bend_time\b[^"]*"[^>]*>\s*(\d+):(\d{2})',
        html,
        re.S,
    )
    if end_time:
        duration = int(end_time.group(1)) * 60 + int(end_time.group(2))
    else:
        # 兼容旧 fixture/旧站点，但生产页面应始终走 end_time。
        times = re.findall(r"(\d+):(\d{2})", html)
        duration = max((int(mm) * 60 + int(ss)) for mm, ss in times) if times else None

    return {
        "coordinate_provenance": coordinate_provenance(),
        "card_plays": plays,
        "ability_plays": ability_plays,
        "elixir_stats": {"team": team_tbl or {}, "opponent": opp_tbl or {}},
        "duration_seconds": duration,
        "card_counts": card_counts,
        "battle_tag": battle_tag,
    }
