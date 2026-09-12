"""Novelty-guided navigation, not a promise to know unseen page contents."""
from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def canonical_list_url(url: str) -> str:
    parts = urlsplit(url)
    path = parts.path.rstrip('/')
    segments = path.split('/')
    if len(segments) >= 3 and segments[1] == 'player':
        segments[2] = segments[2].upper()
    # Preserve every query parameter; only ordering/fragment/path spelling are
    # normalized. Do not merge different mode/filter/cursor values.
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), '/'.join(segments),
                      urlencode(sorted(parse_qsl(parts.query, keep_blank_values=True))), ''))


def page_fingerprint(battle_tags) -> str:
    return hashlib.sha256('\n'.join(sorted(set(battle_tags))).encode()).hexdigest()


def navigation_summary(totals: dict) -> dict:
    result = dict(totals)
    candidates = int(result.get('eligible_candidates', 0))
    pages = int(result.get('pages', 0))
    result['eligible_duplicate_ratio'] = round(result.get('duplicate_candidates', 0) / candidates, 4) if candidates else None
    result['new_candidates_per_page'] = round(result.get('new_candidates', 0) / pages, 3) if pages else None
    return result


def players_from_battles(battles: list[dict], wanted_tags: set[str]) -> set[str]:
    result = set()
    for battle in battles:
        if battle['tag'] not in wanted_tags:
            continue
        metadata = battle.get('metadata') or {}
        for side in ('team_tags', 'opponent_tags'):
            result.update(str(tag).lstrip('#').upper() for tag in metadata.get(side, []) if tag)
    return result


def navigation_decision(*, eligible_count: int, new_count: int, previous_zero: int,
                        fingerprint: str, previous_fingerprint: str | None,
                        zero_limit: int) -> dict:
    zero_chain = previous_zero + 1 if new_count == 0 else 0
    repeated_page = bool(eligible_count and previous_fingerprint == fingerprint)
    stop = repeated_page or zero_chain >= zero_limit
    return {
        'zero_chain': zero_chain, 'fingerprint': fingerprint,
        'stop_history': stop,
        'reason': 'same_battle_page' if repeated_page else ('consecutive_zero_new' if stop else None),
    }
