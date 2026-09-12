"""20 天 / 200 万场生产任务的后台启动、停止和状态查询。"""
from __future__ import annotations

import argparse
import ctypes
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from .lane_manager import (
    PRODUCTION_LANES,
    WORKSPACE,
    assign_distinct_lanes,
    controller_ready,
    ensure_mihomo,
)

LOG_DIR = WORKSPACE / "logs"
LOCK = LOG_DIR / "production.lock"
STATE = LOG_DIR / "production.state.json"
STDOUT = LOG_DIR / "production.stdout.log"
STDERR = LOG_DIR / "production.stderr.log"
WATCHDOG_LOG = LOG_DIR / "lane-watchdog.log"
RESTART_REQUEST = WORKSPACE / "data" / "lanes" / "restart-crawler.request"
DB = WORKSPACE / "data" / "progress.sqlite3"


def _configured_target() -> int:
    from .config import load_config

    cfg = load_config(WORKSPACE / "config.toml")
    value = cfg.authoritative_target or cfg.max_battles
    return int(value) if value else 2_000_000


def _authoritative_mode() -> bool:
    from .config import load_config

    cfg = load_config(WORKSPACE / "config.toml")
    return bool(cfg.authoritative_target or cfg.authoritative_upgrade_manifest)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name != "nt":
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
    handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
    if not handle:
        return False
    ctypes.windll.kernel32.CloseHandle(handle)
    return True


def _read_lock() -> dict:
    try:
        return json.loads(LOCK.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _done_details() -> int:
    if not DB.exists():
        return 0
    conn = sqlite3.connect(DB, timeout=10)
    try:
        if _authoritative_mode():
            try:
                row = conn.execute(
                    "SELECT COUNT(*) FROM authoritative_results WHERE status='accepted'"
                ).fetchone()
            except sqlite3.OperationalError:
                return 0
        else:
            row = conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE status='done' AND kind='detail'"
            ).fetchone()
        return int(row[0])
    finally:
        conn.close()


def _db_stats() -> dict[str, int]:
    if not DB.exists():
        return {}
    conn = sqlite3.connect(DB, timeout=10)
    try:
        result = {
            f"{status}:{kind}": int(n)
            for status, kind, n in conn.execute(
                "SELECT status,kind,COUNT(*) FROM tasks GROUP BY status,kind"
            )
        }
        try:
            for status, tier, count in conn.execute(
                "SELECT status,tier,COUNT(*) FROM authoritative_results "
                "GROUP BY status,tier"
            ):
                result[f"authoritative:{status}:{tier}"] = int(count)
        except sqlite3.OperationalError:
            pass
        return result
    finally:
        conn.close()


def start() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    old = _read_lock()
    if _pid_alive(int(old.get("pid", 0))):
        print(f"生产采集已在运行，PID={old['pid']}")
        return 0
    target = _configured_target()
    done = _done_details()
    if done >= target:
        print(f"已达到停止目标：{done:,} / {target:,}，不再启动采集。")
        return 0

    ensure_mihomo()
    # 节点选择由 Mihomo cache.db 持久化；生产启动不做慢速 IP 回显扫描。
    # 业务健康由爬虫自身的 403/429/NetworkError 冷却与重试负责。
    lanes = len(PRODUCTION_LANES)
    stdout = STDOUT.open("ab")
    stderr = STDERR.open("ab")
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) | getattr(
        subprocess, "CREATE_NEW_PROCESS_GROUP", 0
    )
    process = subprocess.Popen(
        [sys.executable, "-m", "crawler.production", "run"],
        cwd=str(WORKSPACE),
        stdout=stdout,
        stderr=stderr,
        creationflags=flags,
    )
    time.sleep(2)
    if process.poll() is not None:
        raise RuntimeError("生产进程启动失败，请查看 logs/production.stderr.log")
    print(f"生产采集已后台启动：PID={process.pid}，lanes={lanes}")
    return 0


def run() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    started = time.time()
    baseline = _done_details()
    target = _configured_target()
    payload = {"pid": os.getpid(), "started_at": started, "baseline_done": baseline}
    LOCK.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    STATE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    child: subprocess.Popen | None = None
    watchdog: subprocess.Popen | None = None
    try:
        while True:
            ensure_mihomo()
            flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) | getattr(
                subprocess, "CREATE_NEW_PROCESS_GROUP", 0
            )
            env = os.environ.copy()
            env["PYTHONUTF8"] = "1"
            if watchdog is None or watchdog.poll() is not None:
                watchdog_log = WATCHDOG_LOG.open("ab")
                watchdog = subprocess.Popen(
                    [sys.executable, "-m", "crawler.lane_watchdog"],
                    cwd=str(WORKSPACE),
                    env=env,
                    stdout=watchdog_log,
                    stderr=watchdog_log,
                    creationflags=flags,
                )
                print(f"supervisor: lane watchdog started pid={watchdog.pid}", flush=True)
            child_stdout = STDOUT.open("ab")
            child_stderr = STDERR.open("ab")
            metrics_path = WORKSPACE / "data" / "lanes" / "crawler-proxies.json"
            try:
                metrics_path.unlink()
            except FileNotFoundError:
                pass
            child = subprocess.Popen(
                [sys.executable, "-m", "crawler.main", "--config", "config.toml"],
                cwd=str(WORKSPACE),
                env=env,
                stdout=child_stdout,
                stderr=child_stderr,
                creationflags=flags,
            )
            print(f"supervisor: crawler child started pid={child.pid}", flush=True)

            controller_failed = False
            while child.poll() is None:
                time.sleep(30)
                if watchdog.poll() is not None:
                    watchdog_log = WATCHDOG_LOG.open("ab")
                    watchdog = subprocess.Popen(
                        [sys.executable, "-m", "crawler.lane_watchdog"],
                        cwd=str(WORKSPACE), env=env,
                        stdout=watchdog_log, stderr=watchdog_log,
                        creationflags=flags,
                    )
                    print(f"supervisor: lane watchdog restarted pid={watchdog.pid}", flush=True)
                if RESTART_REQUEST.exists():
                    try:
                        RESTART_REQUEST.unlink()
                    except FileNotFoundError:
                        pass
                    print("supervisor: lane recovery completed; restarting crawler child", flush=True)
                    subprocess.run(
                        ["taskkill", "/PID", str(child.pid), "/T", "/F"],
                        check=False,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    break
                done = _done_details()
                elapsed = max(1.0, time.time() - started)
                snapshot = {
                    **payload,
                    "child_pid": child.pid,
                    "done_battles": done,
                    "run_produced": max(0, done - baseline),
                    "projected_per_day": round(max(0, done - baseline) / elapsed * 86400),
                    "updated_at": time.time(),
                }
                STATE.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
                if done >= target:
                    child.wait(timeout=120)
                    return 0
                if not controller_ready():
                    controller_failed = True
                    print("supervisor: Mihomo unavailable; restarting lane service", flush=True)
                    subprocess.run(
                        ["taskkill", "/PID", str(child.pid), "/T", "/F"],
                        check=False,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    break

            code = child.poll()
            if _done_details() >= target:
                return 0
            if controller_failed:
                ensure_mihomo()
            print(f"supervisor: crawler exited code={code}; restart in 15s", flush=True)
            time.sleep(15)
    finally:
        if watchdog is not None and watchdog.poll() is None:
            watchdog.terminate()
        try:
            LOCK.unlink()
        except FileNotFoundError:
            pass


def status() -> int:
    lock = _read_lock()
    pid = int(lock.get("pid", 0))
    active = _pid_alive(pid)
    done = _done_details()
    target = _configured_target()
    started = float(lock.get("started_at", 0) or 0)
    baseline = int(lock.get("baseline_done", done))
    elapsed = max(0.0, time.time() - started) if started else 0.0
    produced = max(0, done - baseline)
    daily = produced / elapsed * 86400 if elapsed > 0 else 0.0
    short_target = 100_000
    short_remaining = max(0, short_target - done)
    short_eta_hours = short_remaining / daily * 24 if daily > 0 else None
    quality: dict = {}
    contract_sha256 = None
    try:
        from .config import load_config
        cfg = load_config(WORKSPACE / "config.toml")
        quality_path = cfg.quality_monitor_state_file
        if quality_path:
            quality = json.loads(Path(quality_path).read_text(encoding="utf-8"))
        if _authoritative_mode() and cfg.authoritative_native_contract:
            from .authoritative import load_native_contract
            contract_sha256 = load_native_contract(
                cfg.authoritative_native_contract,
                expected_game_version=cfg.authoritative_game_version,
            ).contract_sha256
    except Exception:
        quality = {}
    print(json.dumps({
        "active": active,
        "pid": pid if active else None,
        "accepted_battles": done if _authoritative_mode() else None,
        "done_battles": done,
        "authoritative_mode": _authoritative_mode(),
        "contract_sha256": contract_sha256,
        "target": target,
        "run_produced": produced,
        "elapsed_hours": round(elapsed / 3600, 2),
        "projected_per_day": round(daily),
        "acceptance_per_day": 100_000,
        "short_target": short_target,
        "short_remaining": short_remaining,
        "short_eta_hours": round(short_eta_hours, 2) if short_eta_hours is not None else None,
        "db": _db_stats(),
        "quality": quality,
    }, ensure_ascii=False, indent=2))
    return 0 if active else 1


def stop() -> int:
    lock = _read_lock()
    pid = int(lock.get("pid", 0))
    if not _pid_alive(pid):
        print("生产采集未运行。")
        try:
            LOCK.unlink()
        except FileNotFoundError:
            pass
        return 0
    subprocess.run(
        ["taskkill", "/PID", str(pid), "/T", "/F"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        LOCK.unlink()
    except FileNotFoundError:
        pass
    print(f"已停止生产采集 PID={pid}；inflight 任务会在下次启动时恢复。")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="2M 生产采集控制器")
    parser.add_argument("action", choices=["start", "run", "status", "stop"])
    action = parser.parse_args(argv).action
    return {"start": start, "run": run, "status": status, "stop": stop}[action]()


if __name__ == "__main__":
    raise SystemExit(main())
