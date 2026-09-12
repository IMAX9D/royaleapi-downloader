"""Mihomo 多入口管理：确保服务运行，并按真实出口去重分配 26 条 lane。"""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import logging
import os
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

from curl_cffi import requests

log = logging.getLogger("crawler.lanes")

WORKSPACE = Path(__file__).resolve().parent.parent
LANE_ROOT = WORKSPACE / "data" / "lanes"
CONFIG = WORKSPACE / "mihomo-lanes.yaml"
USABLE_NODES = LANE_ROOT / "usable-nodes.json"
MIHOMO = Path(r"C:\Program Files\Clash Verge\verge-mihomo.exe")
CONTROLLER = "http://127.0.0.1:19090"
LANE_PORTS = list(range(18080, 18106))
PRODUCTION_LANES = [(lane, 18079 + lane) for lane in range(1, 27)]
TEST_LANE = 27
TEST_PORT = 18106


def _controller(path: str, method: str = "GET", body: dict | None = None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        CONTROLLER + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        raw = response.read()
        return json.loads(raw) if raw else None


def controller_ready() -> bool:
    try:
        _controller("/version")
        return True
    except Exception:
        return False


def ensure_mihomo() -> None:
    if controller_ready():
        return
    if not MIHOMO.exists():
        raise RuntimeError(f"Mihomo 不存在: {MIHOMO}")
    LANE_ROOT.mkdir(parents=True, exist_ok=True)
    (LANE_ROOT / "providers").mkdir(parents=True, exist_ok=True)
    stdout = (LANE_ROOT / "mihomo.stdout.log").open("ab")
    stderr = (LANE_ROOT / "mihomo.stderr.log").open("ab")
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    process = subprocess.Popen(
        [str(MIHOMO), "-d", str(LANE_ROOT), "-f", str(CONFIG)],
        cwd=str(WORKSPACE),
        stdout=stdout,
        stderr=stderr,
        creationflags=flags,
    )
    (LANE_ROOT / "mihomo.pid").write_text(str(process.pid), encoding="ascii")
    for _ in range(60):
        if controller_ready():
            return
        if process.poll() is not None:
            raise RuntimeError("Mihomo 启动失败，请查看 data/lanes/mihomo.stderr.log")
        time.sleep(0.5)
    raise RuntimeError("Mihomo 控制端口 19090 启动超时")


def _select(lane: int, node: str) -> None:
    name = f"lane-{lane:02d}"
    _controller(
        "/proxies/" + urllib.parse.quote(name, safe=""),
        "PUT",
        {"name": node},
    )


def _exit_hash(port: int, timeout: float = 12.0) -> str | None:
    proxy = f"http://127.0.0.1:{port}"
    proxies = {"http": proxy, "https": proxy}
    try:
        check = requests.get(
            "https://www.gstatic.com/generate_204",
            proxies=proxies,
            timeout=timeout,
            impersonate="chrome",
        )
        if check.status_code != 204:
            return None
        value = requests.get(
            "https://api.ipify.org",
            proxies=proxies,
            timeout=timeout,
            impersonate="chrome",
        ).text.strip()
        return hashlib.sha256(value.encode("utf-8")).hexdigest() if value else None
    except Exception:
        return None


def _current_unique_count(required: int | None = None, attempts: int = 1) -> int:
    """Return the best concurrent exit check, tolerating brief endpoint jitter."""
    lanes = PRODUCTION_LANES[:required] if required is not None else PRODUCTION_LANES
    best = 0
    for attempt in range(max(1, attempts)):
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(lanes)) as pool:
            hashes = list(pool.map(_exit_hash, [port for _, port in lanes]))
        best = max(best, len({value for value in hashes if value}))
        if required is not None and best >= required:
            break
        if attempt + 1 < attempts:
            time.sleep(1.0)
    return best


def _replay_verified_nodes() -> set[str]:
    try:
        payload = json.loads(USABLE_NODES.read_text(encoding="utf-8"))
        return {
            str(row["node"]) for row in payload.get("results", [])
            if row.get("status") == "usable" and row.get("node")
        }
    except Exception:
        return set()


def assign_distinct_lanes(required: int | None = None) -> int:
    """保持已合格分配；不足时遍历健康节点并按真实出口去重。"""
    required = required or len(PRODUCTION_LANES)
    if _current_unique_count(required=required, attempts=3) >= required:
        return required

    providers = _controller("/providers/proxies")["providers"]
    verified = _replay_verified_nodes()
    candidates: list[str] = []
    for provider_name in ("subscription-1", "subscription-2", "subscription-3"):
        provider = providers.get(provider_name) or {}
        candidates.extend(
            p["name"] for p in provider.get("proxies", [])
            if p.get("alive") and (not verified or p["name"] in verified)
        )

    unique_nodes: list[str] = []
    seen: set[str] = set()
    for node in candidates:
        try:
            # Never borrow a production lane for discovery: a failed scan must
            # leave the last known-good production assignment untouched.
            _select(TEST_LANE, node)
            key = _exit_hash(TEST_PORT, timeout=12)
            if key is None:
                key = _exit_hash(TEST_PORT, timeout=18)
        except Exception:
            key = None
        if key and key not in seen:
            seen.add(key)
            unique_nodes.append(node)
            if len(unique_nodes) >= required:
                break

    if len(unique_nodes) < required:
        raise RuntimeError(
            f"仅筛出 {len(unique_nodes)} 个不同健康出口，生产要求 {required} 个"
        )
    for (lane, _), node in zip(PRODUCTION_LANES[:required], unique_nodes[:required]):
        _select(lane, node)
    verified_count = _current_unique_count(required=required, attempts=3)
    if verified_count < required:
        # Each candidate was already proven unique through the isolated test
        # lane. Missing concurrent replies here mean transient availability,
        # not evidence that two lanes share an exit; the watchdog can replace
        # unavailable lanes without blocking the whole supervisor.
        log.warning(
            "已分配并逐条验证 %d 个不同出口；并行复核暂时响应 %d 个",
            required,
            verified_count,
        )
    return required


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="管理 2M 采集的 Mihomo lanes")
    parser.add_argument("--no-assign", action="store_true")
    args = parser.parse_args(argv)
    ensure_mihomo()
    count = len(PRODUCTION_LANES)
    if not args.no_assign:
        count = assign_distinct_lanes()
    print(f"Mihomo lanes ready: {count} distinct exits")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
