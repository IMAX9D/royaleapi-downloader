"""独立低频 lane watchdog：隔离检测、热替换坏出口，不阻塞主爬虫。"""
from __future__ import annotations

import concurrent.futures
import json
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

from curl_cffi import requests

from .lane_manager import (
    CONTROLLER,
    LANE_ROOT,
    PRODUCTION_LANES,
    TEST_LANE,
    TEST_PORT,
    _controller,
    _exit_hash,
    _select,
    controller_ready,
)
from .config import load_config

METRICS = LANE_ROOT / "crawler-proxies.json"
STATE = LANE_ROOT / "watchdog-state.json"
RESTART_REQUEST = LANE_ROOT / "restart-crawler.request"
AUTH_MAP = Path("data/auth_sessions/active_map.json")
SESSION_POOL = Path("data/auth_sessions/session_pool.json")
USABLE_NODES = LANE_ROOT / "usable-nodes.json"
INTERVAL = 120
SESSION_CURL_MODE = load_config("config.toml").backend == "session_curl"


def _connect_ok(port: int, timeout: float = 8.0) -> bool:
    proxy = f"http://127.0.0.1:{port}"
    try:
        response = requests.get(
            "https://www.gstatic.com/generate_204",
            proxies={"http": proxy, "https": proxy},
            timeout=timeout,
            impersonate="chrome",
        )
        return response.status_code == 204
    except Exception:
        return False


def _load_metrics() -> dict:
    try:
        return json.loads(METRICS.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_state() -> dict:
    try:
        return json.loads(STATE.read_text(encoding="utf-8"))
    except Exception:
        return {"network_failures": {}, "handled_forbidden": {}, "quarantine": []}


def _save_state(state: dict) -> None:
    temp = STATE.with_suffix(".tmp")
    temp.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    temp.replace(STATE)


def _lane_number(port: int) -> int:
    for lane, lane_port in PRODUCTION_LANES:
        if lane_port == port:
            return lane
    raise KeyError(port)


def _replay_verified_node_ips() -> dict[str, str]:
    """读取真实 /data/replay 扫描通过的节点及出口 IP。"""
    try:
        payload = json.loads(USABLE_NODES.read_text(encoding="utf-8"))
        return {
            str(row["node"]): str(row.get("ip") or "")
            for row in payload.get("results", [])
            if row.get("status") == "usable" and row.get("node")
        }
    except Exception:
        return {}


def _find_spare(bad_lane: int, bad_port: int, quarantine: set[str]) -> str | None:
    proxies = _controller("/proxies")["proxies"]
    active_names = {
        proxies[f"lane-{lane:02d}"].get("now") for lane, _ in PRODUCTION_LANES
    }
    providers = _controller("/providers/proxies")["providers"]
    verified_ips = _replay_verified_node_ips()
    active_ips = {
        verified_ips.get(str(name), "") for name in active_names if name
    }
    active_ips.discard("")
    candidates: list[str] = []
    for provider_name in ("subscription-1", "subscription-2", "subscription-3"):
        candidates.extend(
            row["name"] for row in providers.get(provider_name, {}).get("proxies", [])
            if row.get("alive")
            and (not verified_ips or row["name"] in verified_ips)
            and (not verified_ips.get(row["name"]) or verified_ips[row["name"]] not in active_ips)
            and row["name"] not in active_names
            and row["name"] not in quarantine
        )
    # 在线故障恢复必须有时间预算：最多测试 3 个 provider 已标记 alive 的节点。
    # 离线启动/维护阶段再做完整真实出口去重。
    for node in candidates[:3]:
        try:
            _select(TEST_LANE, node)
            if _connect_ok(TEST_PORT, timeout=5):
                return node
        except Exception:
            continue
    return None


def _recover_cloudflare(port: int) -> bool:
    try:
        result = subprocess.run(
            [sys.executable, "-m", "crawler.cf_recover", "--port", str(port)],
            timeout=130,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return result.returncode == 0
    except Exception:
        return False


def _rotate_session(proxy_url: str, reason: str) -> bool:
    try:
        mapping = json.loads(AUTH_MAP.read_text(encoding="utf-8"))
        pool = json.loads(SESSION_POOL.read_text(encoding="utf-8"))
    except Exception:
        return False
    now = time.time()
    cooldown = pool.setdefault("cooldown", [])
    still_cooling = []
    for row in cooldown:
        if float(row.get("available_after", 0)) <= now and row.get("reason") != "auth":
            pool.setdefault("spares", []).append({
                "name": row["name"], "profile": row["profile"]
            })
        else:
            still_cooling.append(row)
    pool["cooldown"] = still_cooling
    spares = pool.setdefault("spares", [])
    if not spares or proxy_url not in mapping:
        return False
    replacement = spares.pop(0)
    old_name = pool.setdefault("active", {}).get(proxy_url, "unknown")
    old_profile = mapping[proxy_url]
    mapping[proxy_url] = replacement["profile"]
    pool["active"][proxy_url] = replacement["name"]
    pool["cooldown"].append({
        "name": old_name,
        "profile": old_profile,
        "reason": reason,
        "available_after": now + (86400 if reason == "auth" else 300),
    })
    map_tmp = AUTH_MAP.with_suffix(".tmp")
    pool_tmp = SESSION_POOL.with_suffix(".tmp")
    map_tmp.write_text(json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8")
    pool_tmp.write_text(json.dumps(pool, ensure_ascii=False, indent=2), encoding="utf-8")
    map_tmp.replace(AUTH_MAP)
    pool_tmp.replace(SESSION_POOL)
    return True


def run_cycle(state: dict) -> dict:
    if not controller_ready():
        print("lane-watchdog: controller unavailable; supervisor will recover", flush=True)
        return state
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(PRODUCTION_LANES)) as pool:
        checks = dict(zip(
            [port for _, port in PRODUCTION_LANES],
            pool.map(_connect_ok, [port for _, port in PRODUCTION_LANES]),
        ))

    metrics = _load_metrics()
    if not metrics or time.time() - float(metrics.get("updated_at", 0)) > 90:
        # 子进程刚重启时不能使用上一代进程的失败计数，否则会误恢复已打开的 profile。
        state["updated_at"] = time.time()
        _save_state(state)
        return state
    proxy_rows = {
        int(row["proxy"].rsplit(":", 1)[-1]): row
        for row in metrics.get("proxies", [])
        if row.get("proxy", "").startswith("http://127.0.0.1:")
    }
    network_failures = state.setdefault("network_failures", {})
    handled = state.setdefault("handled_forbidden", {})
    handled_rate = state.setdefault("handled_rate_limited", {})
    handled_auth = state.setdefault("handled_auth_failures", {})
    quarantine = set(state.setdefault("quarantine", []))

    bad_lanes: list[tuple[int, int, str]] = []
    session_changed = False
    controller_proxies = _controller("/proxies")["proxies"]
    for lane, port in PRODUCTION_LANES:
        key = str(port)
        network_failures[key] = 0 if checks.get(port) else int(network_failures.get(key, 0)) + 1
        row = proxy_rows.get(port, {})
        proxy_url = f"http://127.0.0.1:{port}"
        rate_limited = int(row.get("rate_limited", 0))
        auth_failures = int(row.get("auth_failures", 0))
        prior_rate = int(handled_rate.get(key, 0))
        prior_auth = int(handled_auth.get(key, 0))
        if rate_limited < prior_rate:
            prior_rate = 0
        if auth_failures < prior_auth:
            prior_auth = 0
        session_reason = None
        if auth_failures - prior_auth >= 1:
            session_reason = "auth"
        elif rate_limited - prior_rate >= 2:
            session_reason = "rate"
        if session_reason and _rotate_session(proxy_url, session_reason):
            handled_rate[key] = rate_limited
            handled_auth[key] = auth_failures
            session_changed = True
            print(f"lane-watchdog: lane-{lane:02d} session rotated ({session_reason})", flush=True)
            continue
        forbidden = int(row.get("forbidden", 0))
        prior = int(handled.get(key, 0))
        target_bad = (
            (forbidden - prior >= 1)
            or (forbidden > prior and float(row.get("cooldown_left", 0)) > 240)
            or row.get("healthy") is False
        )
        if network_failures[key] >= 3 or target_bad:
            old = controller_proxies[f"lane-{lane:02d}"].get("now")
            bad_lanes.append((lane, port, old))
            handled[key] = forbidden

    changed = False
    batch = bad_lanes[:3]
    recoveries: list[bool] = []
    if batch and not SESSION_CURL_MODE:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(batch)) as pool:
            recoveries = list(pool.map(lambda row: _recover_cloudflare(row[1]), batch))
    elif batch:
        recoveries = [False] * len(batch)
    for (lane, port, old), recovered in zip(batch, recoveries):
        if recovered:
            network_failures[str(port)] = 0
            changed = True
            print(f"lane-watchdog: lane-{lane:02d} Cloudflare recovered", flush=True)
            continue
        if old:
            quarantine.add(old)
        replacement = _find_spare(lane, port, quarantine)
        if replacement:
            _select(lane, replacement)
            network_failures[str(port)] = 0
            changed = True
            print(f"lane-watchdog: lane-{lane:02d} hot-swapped", flush=True)
        else:
            print(f"lane-watchdog: lane-{lane:02d} unhealthy; no unique spare", flush=True)
    if changed or session_changed:
        RESTART_REQUEST.write_text(str(time.time()), encoding="ascii")
    state["quarantine"] = list(quarantine)
    state["updated_at"] = time.time()
    _save_state(state)
    return state


def main() -> int:
    state = _load_state()
    # 测试 lane 当前选择不直接进入候选，避免立即选回最近淘汰节点。
    try:
        test_now = _controller("/proxies")["proxies"]["lane-04"].get("now")
        if test_now and not state.get("quarantine"):
            state.setdefault("quarantine", []).append(test_now)
    except Exception:
        pass
    while True:
        try:
            state = run_cycle(state)
        except Exception as exc:  # noqa: BLE001
            print(f"lane-watchdog: cycle error {type(exc).__name__}: {exc}", flush=True)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    raise SystemExit(main())
