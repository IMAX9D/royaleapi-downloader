# 用途：离线升级回放 schema，并按外部 native contract 检查字段和准入条件。
# 分类：高级数据校验；使用：了解目标格式与 contract
# 相关文件与阅读顺序：见同目录 README.md。

"""Authoritative replay schema upgrades and native-static eligibility gates.

This module is deliberately offline.  It never calls RoyaleAPI or libg: it
only upgrades exact schema-3/4 replay markers into schema 5 and decides whether
an assembled replay has every field required by the native replay pipeline.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Optional

from .parsers import (
    COORDINATE_TRANSFORM_ID,
    OUTPUT_SCHEMA_VERSION,
    ReplayCoordinateError,
    base_card_key,
    card_form,
    coordinate_provenance,
    derive_native_coordinates,
)


@dataclass(frozen=True)
class EligibilityResult:
    accepted: bool
    tier: str
    reasons: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "tier": self.tier,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class NativeContract:
    schema_version: int
    game_version: str
    contract_sha256: str
    file_sha256: str
    allowed_card_tokens: frozenset[str]
    allowed_tower_troops: frozenset[str]
    ability_source_tokens: frozenset[str]
    source_numeric_game_mode_ids: frozenset[int]
    native_execution_mode_by_source: Mapping[int, int]
    king_tower_max_hp_by_level: Mapping[int, int]
    source_path: str


NATIVE_EXECUTION_GAME_MODE_PROVENANCE = (
    "frozen_native_ingest_contract_mode_map_v1"
)
KING_TOWER_LEVEL = 16
KING_TOWER_MAX_HP = 7_728
KING_TOWER_LEVEL_PROVENANCE = (
    "ranked_template_cap16_and_full_king_hp_v1"
)
KING_TOWER_LEVEL_TOWER_TROOP_PROVENANCE = (
    "ranked_template_cap16_and_tower_troop_level16_v1"
)
KING_TOWER_LEVEL_PROVENANCES = frozenset({
    KING_TOWER_LEVEL_TOWER_TROOP_PROVENANCE,
    KING_TOWER_LEVEL_PROVENANCE,
})
KING_TOWER_LEVEL_EVIDENCE_SCHEMA = {
    "schema_version": 1,
    "scope": "side_local_ranked_template",
    "ranked_template_level_cap": KING_TOWER_LEVEL,
    "resolved_king_tower_level": KING_TOWER_LEVEL,
    "precedence": ["tower_troop_level", "final_king_hp"],
    "accepted_provenances": [
        KING_TOWER_LEVEL_TOWER_TROOP_PROVENANCE,
        KING_TOWER_LEVEL_PROVENANCE,
    ],
    "tower_troop_level": {
        "required_value": KING_TOWER_LEVEL,
        "inference": (
            "tower_troop_level<=king_tower_level and ranked_template_cap=16"
        ),
        "provenance": KING_TOWER_LEVEL_TOWER_TROOP_PROVENANCE,
        "official_sources": [
            "https://support.supercell.com/clash-royale/en/articles/king-tower-level.html",
            "https://support.supercell.com/clash-royale/en/articles/tower-troops-4.html",
        ],
    },
    "final_king_hp": {
        "required_value": KING_TOWER_MAX_HP,
        "provenance": KING_TOWER_LEVEL_PROVENANCE,
    },
    "forbidden_inference_fields": ["card_levels", "deck_cards.level"],
}


def native_contract_payload_sha256(value: Mapping[str, Any]) -> str:
    payload = {key: item for key, item in value.items() if key != "contract_sha256"}
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_native_contract(
    path: str | Path,
    *,
    expected_game_version: str,
) -> NativeContract:
    source = Path(path).resolve(strict=True)
    raw = source.read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("native contract root is not an object")
    identity = (value.get("schema_version"), value.get("kind"))
    if identity not in {
        (2, "cr_native_authoritative_contract_v2"),
        (3, "cr_native_authoritative_contract_v3"),
    }:
        raise ValueError("unsupported native contract schema/kind")
    if value.get("game_version") != expected_game_version:
        raise ValueError(
            f"native contract game version {value.get('game_version')!r} "
            f"!= expected {expected_game_version!r}"
        )
    claimed = str(value.get("contract_sha256") or "")
    actual = native_contract_payload_sha256(value)
    if len(claimed) != 64 or claimed != actual:
        raise ValueError("native contract canonical SHA-256 mismatch")

    def string_set(name: str) -> frozenset[str]:
        raw_values = value.get(name)
        if not isinstance(raw_values, list) or not raw_values:
            raise ValueError(f"native contract {name} missing")
        normalized = [str(item).strip().lower() for item in raw_values]
        if any(not item for item in normalized) or len(normalized) != len(set(normalized)):
            raise ValueError(f"native contract {name} contains invalid/duplicate values")
        return frozenset(normalized)

    raw_modes = value.get("source_numeric_game_mode_ids")
    if not isinstance(raw_modes, list) or not raw_modes:
        raise ValueError("native contract source_numeric_game_mode_ids missing")
    if any(not _strict_int(item, minimum=1) for item in raw_modes):
        raise ValueError("native contract source_numeric_game_mode_ids invalid")
    if len(raw_modes) != len(set(raw_modes)):
        raise ValueError(
            "native contract source_numeric_game_mode_ids contains duplicates"
        )

    raw_execution_modes = value.get("native_execution_mode_by_source")
    expected_mode_keys = {str(item) for item in raw_modes}
    if (
        not isinstance(raw_execution_modes, dict)
        or set(raw_execution_modes) != expected_mode_keys
        or any(
            not _strict_int(execution_mode, minimum=1)
            for execution_mode in raw_execution_modes.values()
        )
    ):
        raise ValueError(
            "native contract native_execution_mode_by_source must exactly cover "
            "source_numeric_game_mode_ids"
        )
    execution_modes = {
        int(source_mode): int(execution_mode)
        for source_mode, execution_mode in raw_execution_modes.items()
    }

    raw_king_hp = value.get("king_tower_max_hp_by_level")
    if not isinstance(raw_king_hp, dict) or not raw_king_hp:
        raise ValueError("native contract king_tower_max_hp_by_level missing")
    if any(
        not isinstance(level, str)
        or not level.isdigit()
        or str(int(level)) != level
        or not _strict_int(max_hp, minimum=1)
        for level, max_hp in raw_king_hp.items()
    ):
        raise ValueError("native contract king_tower_max_hp_by_level invalid")
    king_hp = {int(level): int(max_hp) for level, max_hp in raw_king_hp.items()}
    if king_hp.get(KING_TOWER_LEVEL) != KING_TOWER_MAX_HP:
        raise ValueError(
            "native contract level-16 King Tower max HP anchor must be 7728"
        )
    evidence = value.get("king_tower_level_evidence")
    if identity == (3, "cr_native_authoritative_contract_v3"):
        if evidence != KING_TOWER_LEVEL_EVIDENCE_SCHEMA:
            raise ValueError("native contract King Tower evidence schema mismatch")
    elif evidence is not None:
        raise ValueError("legacy native contract unexpectedly declares v3 evidence")

    allowed_cards = string_set("allowed_card_tokens")
    ability_sources = string_set("ability_source_tokens")
    if not ability_sources.issubset(allowed_cards):
        raise ValueError("native contract ability sources are not allowed card tokens")
    return NativeContract(
        schema_version=int(value["schema_version"]),
        game_version=expected_game_version,
        contract_sha256=actual,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        allowed_card_tokens=allowed_cards,
        allowed_tower_troops=string_set("allowed_tower_troops"),
        ability_source_tokens=ability_sources,
        source_numeric_game_mode_ids=frozenset(int(item) for item in raw_modes),
        native_execution_mode_by_source=execution_modes,
        king_tower_max_hp_by_level=king_hp,
        source_path=str(source),
    )


def resolve_king_tower_level_evidence(
    value: Mapping[str, Any],
    native_contract: NativeContract,
    side: str,
) -> Optional[str]:
    """Return one exact side-local King-16 provenance, or ``None``.

    Contract v3 prefers Tower Troop level 16.  Because a Tower Troop cannot
    exceed its King Tower and this Ranked template caps both at 16, this is an
    exact equality proof even when the King took damage.  Contract v2 retains
    the historical full-HP-only behavior during the migration window.
    """
    if side not in ("team", "opponent"):
        raise ValueError(f"invalid battle side: {side!r}")
    if value.get("numeric_game_mode_id") not in (
        native_contract.source_numeric_game_mode_ids
    ):
        return None
    rounds = value.get("rounds")
    players = (
        rounds[0].get(side)
        if isinstance(rounds, list)
        and len(rounds) == 1
        and isinstance(rounds[0], Mapping)
        else None
    )
    player = players[0] if isinstance(players, list) and len(players) == 1 else None
    if (
        native_contract.schema_version >= 3
        and isinstance(player, Mapping)
        and player.get("tower_troop_level") == KING_TOWER_LEVEL
    ):
        return KING_TOWER_LEVEL_TOWER_TROOP_PROVENANCE
    tower_hp = value.get("final_tower_hp")
    side_hp = tower_hp.get(side) if isinstance(tower_hp, Mapping) else None
    if (
        isinstance(side_hp, Mapping)
        and side_hp.get("king")
        == native_contract.king_tower_max_hp_by_level[KING_TOWER_LEVEL]
    ):
        return KING_TOWER_LEVEL_PROVENANCE
    return None


def apply_native_contract_metadata(
    value: dict[str, Any], native_contract: NativeContract,
) -> None:
    """Materialize contract-derived fields without guessing hidden state.

    The source numeric mode remains untouched.  Its native execution mode is
    an exact contract lookup.  King Tower evidence is resolved independently
    per side, with Tower Troop level 16 taking precedence over the full-HP
    fallback in contract v3.  No card/deck level participates.
    """
    value.pop("native_execution_game_mode_id", None)
    value.pop("native_execution_game_mode_provenance", None)
    source_mode = value.get("numeric_game_mode_id")
    if (
        _strict_int(source_mode, minimum=1)
        and source_mode in native_contract.source_numeric_game_mode_ids
    ):
        value["native_execution_game_mode_id"] = (
            native_contract.native_execution_mode_by_source[source_mode]
        )
        value["native_execution_game_mode_provenance"] = (
            NATIVE_EXECUTION_GAME_MODE_PROVENANCE
        )

    rounds = value.get("rounds") or []
    for round_data in rounds:
        if not isinstance(round_data, dict):
            continue
        for side in ("team", "opponent"):
            for player in round_data.get(side) or []:
                if isinstance(player, dict):
                    player.pop("king_tower_level", None)
                    player.pop("king_tower_level_provenance", None)

    for round_data in rounds:
        if not isinstance(round_data, dict):
            continue
        for side in ("team", "opponent"):
            provenance = resolve_king_tower_level_evidence(
                value, native_contract, side
            )
            if provenance is None:
                continue
            for player in round_data.get(side) or []:
                if isinstance(player, dict):
                    player["king_tower_level"] = KING_TOWER_LEVEL
                    player["king_tower_level_provenance"] = provenance


def _failure(tier: str, *reasons: str) -> EligibilityResult:
    return EligibilityResult(False, tier, tuple(dict.fromkeys(reasons)))


def _strict_int(value: Any, *, minimum: Optional[int] = None) -> bool:
    if not isinstance(value, int) or isinstance(value, bool):
        return False
    return minimum is None or value >= minimum


def upgrade_exact_replay_body(value: dict[str, Any]) -> tuple[Optional[dict], Optional[str]]:
    """Upgrade a schema-3/4 replay body without inventing any event.

    Schema 3/4 is reusable only when every deployment still carries raw X/Y,
    data-i and the original 20 Hz tick, and ability markers already have exact
    ticks. Schema 1/2 are intentionally not accepted by this path. Schema 4
    still needs the schema-5 list metadata contract before admission.
    """
    source_schema = int(value.get("schema_version") or 0)
    if source_schema not in (3, 4):
        return None, "source_schema_not_exact_v3_or_v4"
    plays = value.get("card_plays")
    abilities = value.get("ability_plays")
    if not isinstance(plays, list) or not isinstance(abilities, list):
        return None, "exact_event_arrays_missing"

    upgraded = copy.deepcopy(value)
    try:
        for play in upgraded["card_plays"]:
            if not _strict_int(play.get("time_raw"), minimum=0):
                return None, "deployment_exact_tick_missing"
            if not _strict_int(play.get("marker_index"), minimum=0):
                return None, "deployment_marker_index_missing"
            x, y, transform = derive_native_coordinates(
                play.get("x_raw"), play.get("y_raw"), play.get("data_i"),
                marker_index=play.get("marker_index"),
            )
            play.update({
                "coordinate_provenance": COORDINATE_TRANSFORM_ID,
                "coordinate_transform": transform,
                "x": x,
                "y": y,
                "x_norm": round(x / 18_000, 4),
                "y_norm": round(y / 32_000, 4),
                "tile_x": round(x / 1000, 1),
                "tile_y": round(y / 1000, 1),
            })
        for ability in upgraded["ability_plays"]:
            if not _strict_int(ability.get("time_raw"), minimum=0):
                return None, "ability_exact_tick_missing"
            if not _strict_int(ability.get("marker_index"), minimum=0):
                return None, "ability_marker_index_missing"
            x_raw = ability.get("x_raw")
            y_raw = ability.get("y_raw")
            if x_raw is None and y_raw is None:
                ability["coordinate_status"] = "not_applicable"
            elif x_raw is None or y_raw is None:
                return None, "ability_partial_raw_coordinate"
            else:
                x, y, transform = derive_native_coordinates(
                    x_raw, y_raw, ability.get("data_i"),
                    marker_index=ability.get("marker_index"),
                )
                ability.update({
                    "coordinate_status": "resolved",
                    "coordinate_provenance": COORDINATE_TRANSFORM_ID,
                    "coordinate_transform": transform,
                    "x": x,
                    "y": y,
                })
    except (ReplayCoordinateError, TypeError) as exc:
        return None, f"raw_coordinate_upgrade_failed:{exc}"

    upgraded["coordinate_provenance"] = coordinate_provenance()
    upgraded["schema_version"] = OUTPUT_SCHEMA_VERSION
    upgraded["source_schema_version"] = source_schema
    upgraded["authoritative_upgrade"] = {
        "mode": "local_exact_legacy_replay_body",
        "source_schema_version": source_schema,
        "events_modified": False,
        "coordinates_rederived_from_raw_data_i": True,
    }
    return upgraded, None


def evaluate_native_eligibility(
    value: Any,
    *,
    min_battle_timestamp: Optional[int] = None,
    native_contract: Optional[NativeContract] = None,
) -> EligibilityResult:
    """Apply the fail-closed static gate before authoritative admission."""
    if not isinstance(value, dict):
        return _failure("schema", "root_not_object")
    if value.get("schema_version") != OUTPUT_SCHEMA_VERSION:
        return _failure("schema", "schema_version_not_authoritative")
    if value.get("coordinate_provenance") != coordinate_provenance():
        return _failure("schema", "coordinate_contract_missing_or_mismatched")
    if not str(value.get("battle_tag") or "").strip():
        return _failure("schema", "battle_tag_missing")

    mode_reasons: list[str] = []
    rounds = value.get("rounds") or []
    if value.get("draft") is not False:
        mode_reasons.append("draft_or_unknown")
    if value.get("matchup_players") != "1v1":
        mode_reasons.append("matchup_players_not_1v1")
    if len(rounds) != 1 or not isinstance(rounds[0], dict):
        mode_reasons.append("not_single_round")
    elif len(rounds[0].get("team") or []) != 1 or len(rounds[0].get("opponent") or []) != 1:
        mode_reasons.append("not_one_player_per_side")
    if value.get("battle_type") == "riverRaceDuel":
        mode_reasons.append("river_race_duel")
    if native_contract is None:
        return _failure("mapping", "native_contract_missing")
    source_mode = value.get("numeric_game_mode_id")
    if not _strict_int(source_mode, minimum=1):
        mode_reasons.append("numeric_game_mode_id_missing")
    elif source_mode not in native_contract.source_numeric_game_mode_ids:
        mode_reasons.append("numeric_game_mode_not_allowed")
    else:
        expected_execution_mode = (
            native_contract.native_execution_mode_by_source[source_mode]
        )
        if value.get("native_execution_game_mode_id") != expected_execution_mode:
            mode_reasons.append("native_execution_game_mode_mismatch")
        if (
            value.get("native_execution_game_mode_provenance")
            != NATIVE_EXECUTION_GAME_MODE_PROVENANCE
        ):
            mode_reasons.append("native_execution_game_mode_provenance_invalid")
    if value.get("numeric_game_mode_provenance") != "list_matchup_button_joined_by_data_index":
        mode_reasons.append("numeric_game_mode_provenance_missing")
    if not _strict_int(value.get("battle_index"), minimum=1):
        mode_reasons.append("battle_index_missing")
    if value.get("battle_index_provenance") != "list_replay_and_matchup_button_data_index":
        mode_reasons.append("battle_index_provenance_missing")
    if mode_reasons:
        return _failure("mode", *mode_reasons)

    timestamp = value.get("version_timestamp")
    version_reasons: list[str] = []
    if not _strict_int(timestamp, minimum=1):
        version_reasons.append("exact_version_timestamp_missing")
    elif min_battle_timestamp is not None and timestamp < int(min_battle_timestamp):
        version_reasons.append("before_native_version_window")
    if value.get("version_timestamp_provenance") != "battle_list_exact_timestamp":
        version_reasons.append("version_timestamp_provenance_missing")
    if not isinstance(value.get("battle_time_utc"), str) or not value["battle_time_utc"].endswith("Z"):
        version_reasons.append("battle_time_utc_missing")
    if value.get("battle_time_utc_provenance") not in (
        "battle_timestamp_popup_utc",
        "derived_from_exact_list_timestamp",
    ):
        version_reasons.append("battle_time_utc_provenance_missing")
    if version_reasons:
        return _failure("version", *version_reasons)

    tower_hp = value.get("final_tower_hp")
    terminal_reasons: list[str] = []
    if not isinstance(tower_hp, dict) or tower_hp.get("provenance") != "list_hp_both_popup":
        terminal_reasons.append("final_tower_hp_missing")
    else:
        for side in ("team", "opponent"):
            hp = tower_hp.get(side)
            if not isinstance(hp, dict):
                terminal_reasons.append(f"{side}_tower_hp_missing")
                continue
            keys = ("king", "princess0", "princess1", "total")
            if any(not _strict_int(hp.get(key), minimum=0) for key in keys):
                terminal_reasons.append(f"{side}_tower_hp_invalid")
            elif hp["total"] != hp["king"] + hp["princess0"] + hp["princess1"]:
                terminal_reasons.append(f"{side}_tower_hp_total_mismatch")
    for key in ("team_crowns", "opponent_crowns"):
        if not _strict_int(value.get(key), minimum=0) or value[key] > 3:
            terminal_reasons.append(f"{key}_invalid")
    if terminal_reasons:
        return _failure("terminal", *terminal_reasons)

    king_level_reasons: list[str] = []
    for side in ("team", "opponent"):
        players = rounds[0].get(side) or []
        if len(players) != 1:
            king_level_reasons.append(f"{side}_king_tower_player_missing")
            continue
        player = players[0]
        if not isinstance(player, dict):
            king_level_reasons.append(f"{side}_king_tower_player_invalid")
            continue
        if player.get("king_tower_level") != KING_TOWER_LEVEL:
            king_level_reasons.append(f"{side}_king_tower_level_missing")
        expected_provenance = resolve_king_tower_level_evidence(
            value, native_contract, side
        )
        if expected_provenance is None:
            king_level_reasons.append(
                f"{side}_king_tower_level_exact_evidence_missing"
            )
        elif player.get("king_tower_level_provenance") != expected_provenance:
            king_level_reasons.append(
                f"{side}_king_tower_level_provenance_invalid"
            )
    if king_level_reasons:
        return _failure("king_level", *king_level_reasons)

    deck_forms: dict[str, dict[str, str]] = {}
    deck_reasons: list[str] = []
    for side in ("team", "opponent"):
        deck = value.get(f"{side}_deck")
        if not isinstance(deck, list) or len(deck) != 8:
            deck_reasons.append(f"{side}_deck_not_8")
            continue
        slugs = [str(card).strip().lower() for card in deck]
        bases = [base_card_key(card) for card in slugs]
        if any(not card or card == "_invalid" for card in slugs):
            deck_reasons.append(f"{side}_deck_invalid_slug")
        if len(set(slugs)) != 8 or len(set(bases)) != 8:
            deck_reasons.append(f"{side}_deck_not_unique_cycle")
        unsupported = sorted(set(slugs) - native_contract.allowed_card_tokens)
        if unsupported:
            deck_reasons.append(f"{side}_native_card_mapping_missing:{','.join(unsupported)}")
        deck_forms[side] = dict(zip(bases, slugs, strict=True))

        players = rounds[0].get(side) or []
        if len(players) != 1:
            continue
        player = players[0]
        if player.get("full_deck") != deck:
            deck_reasons.append(f"{side}_round_deck_mismatch")
        levels = player.get("card_levels")
        if not isinstance(levels, dict) or set(levels) != set(slugs):
            deck_reasons.append(f"{side}_card_levels_missing")
        elif any(not _strict_int(levels.get(slug), minimum=1) for slug in slugs):
            deck_reasons.append(f"{side}_card_level_invalid")
        deck_cards = player.get("deck_cards")
        if not isinstance(deck_cards, list) or len(deck_cards) != 8:
            deck_reasons.append(f"{side}_deck_cards_missing")
        else:
            for slot, (slug, item) in enumerate(zip(slugs, deck_cards, strict=True)):
                if not isinstance(item, dict) or (
                    item.get("slot") != slot
                    or item.get("slug") != slug
                    or item.get("base_slug") != base_card_key(slug)
                    or item.get("form") != card_form(slug)
                    or not _strict_int(item.get("level"), minimum=1)
                ):
                    deck_reasons.append(f"{side}_deck_card_{slot}_invalid")
        if not isinstance(player.get("tower_troop"), str) or not player["tower_troop"]:
            deck_reasons.append(f"{side}_tower_troop_missing")
        elif player["tower_troop"].lower() not in native_contract.allowed_tower_troops:
            deck_reasons.append(f"{side}_native_tower_troop_mapping_missing")
        if not _strict_int(player.get("tower_troop_level"), minimum=1):
            deck_reasons.append(f"{side}_tower_troop_level_missing")
    if deck_reasons:
        return _failure("deck", *deck_reasons)

    plays = value.get("card_plays")
    abilities = value.get("ability_plays")
    if not isinstance(plays, list) or not plays:
        return _failure("events", "card_plays_missing")
    if not isinstance(abilities, list):
        return _failure("events", "ability_plays_missing")
    duration = value.get("duration_seconds")
    if not _strict_int(duration, minimum=1) or duration > 360:
        return _failure("events", "duration_invalid")

    event_reasons: list[str] = []
    observed = {"team": set(), "opponent": set()}
    indexed_events: list[tuple[int, int]] = []
    for play in plays:
        if not isinstance(play, dict):
            event_reasons.append("deployment_not_object")
            continue
        side = play.get("side")
        if side not in observed:
            event_reasons.append("deployment_side_invalid")
            continue
        tick = play.get("time_raw")
        marker_index = play.get("marker_index")
        if not _strict_int(tick, minimum=0):
            event_reasons.append("deployment_exact_tick_missing")
        if not _strict_int(marker_index, minimum=0):
            event_reasons.append("deployment_marker_index_missing")
        else:
            indexed_events.append((marker_index, tick if isinstance(tick, int) else -1))
        card_base = base_card_key(play.get("card", ""))
        if not card_base or card_base not in deck_forms[side]:
            event_reasons.append(f"{side}_played_card_not_in_deck")
        else:
            observed[side].add(card_base)
            if play.get("card_form") != deck_forms[side][card_base]:
                event_reasons.append(f"{side}_played_card_form_unmapped")
        try:
            x, y, transform = derive_native_coordinates(
                play.get("x_raw"), play.get("y_raw"), play.get("data_i"),
                marker_index=marker_index if isinstance(marker_index, int) else None,
            )
            if (
                play.get("coordinate_provenance") != COORDINATE_TRANSFORM_ID
                or play.get("coordinate_transform") != transform
                or play.get("x") != x
                or play.get("y") != y
            ):
                event_reasons.append("deployment_coordinate_mismatch")
        except (ReplayCoordinateError, TypeError):
            event_reasons.append("deployment_raw_coordinate_invalid")

    for ability in abilities:
        if not isinstance(ability, dict):
            event_reasons.append("ability_not_object")
            continue
        side = ability.get("side")
        tick = ability.get("time_raw")
        marker_index = ability.get("marker_index")
        if side not in observed:
            event_reasons.append("ability_side_invalid")
        if not _strict_int(tick, minimum=0):
            event_reasons.append("ability_exact_tick_missing")
        if not _strict_int(marker_index, minimum=0):
            event_reasons.append("ability_marker_index_missing")
        else:
            indexed_events.append((marker_index, tick if isinstance(tick, int) else -1))
        if ability.get("coordinate_status") not in ("not_applicable", "resolved"):
            event_reasons.append("ability_coordinate_status_missing")

    for side in ("team", "opponent"):
        if observed[side] != set(deck_forms[side]):
            event_reasons.append(f"{side}_incomplete_observed_8_card_cycle")
        total_count = (((value.get("elixir_stats") or {}).get(side) or {}).get("Total") or {}).get("count")
        ability_count = (((value.get("elixir_stats") or {}).get(side) or {}).get("Ability") or {}).get("count") or 0
        if total_count != sum(1 for play in plays if play.get("side") == side):
            event_reasons.append(f"{side}_deployment_count_mismatch")
        if ability_count != sum(1 for ability in abilities if ability.get("side") == side):
            event_reasons.append(f"{side}_ability_count_mismatch")
        if ability_count and not (
            set(deck_forms[side].values()) & native_contract.ability_source_tokens
        ):
            event_reasons.append(f"{side}_native_ability_source_mapping_missing")

    # libg accepts at most one command from one side at a native tick. A joint
    # blue/red tick is valid, but two deployments or deployment+ability for the
    # same side are source-ambiguous and must not enter the target corpus.
    command_occupancy: set[tuple[str, int]] = set()
    for event in [*plays, *abilities]:
        side, tick = event.get("side"), event.get("time_raw")
        if side not in ("team", "opponent") or not _strict_int(tick, minimum=0):
            continue
        key = (side, tick)
        if key in command_occupancy:
            event_reasons.append(f"{side}_multiple_commands_same_tick")
        command_occupancy.add(key)

    indexes = [item[0] for item in indexed_events]
    if len(indexes) != len(set(indexes)):
        event_reasons.append("marker_index_duplicate")
    elif indexes and sorted(indexes) != list(range(len(indexes))):
        event_reasons.append("marker_index_not_contiguous")
    ordered = sorted(indexed_events)
    if any(tick < 0 for _, tick in ordered) or any(
        ordered[index][1] > ordered[index + 1][1]
        for index in range(len(ordered) - 1)
    ):
        event_reasons.append("marker_tick_order_invalid")
    if event_reasons:
        return _failure("events", *event_reasons)

    return EligibilityResult(True, "native_static_v2")
