"""Dedicated background controller for the schema-5 authoritative corpus."""
from __future__ import annotations

import argparse
import ctypes
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

from .authoritative import load_native_contract
from .config import CrawlConfig, load_config
from .lane_manager import (
    WORKSPACE,
    assign_distinct_lanes,
    controller_ready,
    ensure_mihomo,
)


LOG_DIR = WORKSPACE / "logs"
LOCK = LOG_DIR / "authoritative-production.lock"
STATE = LOG_DIR / "authoritative-production.state.json"
STDOUT = LOG_DIR / "authoritative-production.stdout.log"
STDERR = LOG_DIR / "authoritative-production.stderr.log"
WATCHDOG_LOG = LOG_DIR / "authoritative-lane-watchdog.log"
RESTART_REQUEST = WORKSPACE / "data" / "lanes" / "restart-crawler.request"
DEFAULT_CONFIG = WORKSPACE / "config.authoritative.toml"


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


def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _validated_config(path: str | Path) -> tuple[Path, CrawlConfig, str]:
    config_path = Path(path).resolve(strict=True)
    cfg = load_config(config_path)
    if not cfg.authoritative_target or cfg.authoritative_target <= 0:
        raise ValueError("authoritative production requires a positive target")
    if not cfg.authoritative_native_contract:
        raise ValueError("authoritative production requires a native contract")
    contract = load_native_contract(
        cfg.authoritative_native_contract,
        expected_game_version=cfg.authoritative_game_version,
    )
    if Path(cfg.output_dir).resolve() == Path(cfg.authoritative_output_dir).resolve():
        raise ValueError("authoritative output must be independent from legacy output")
    return config_path, cfg, contract.contract_sha256


def _accepted(cfg: CrawlConfig, contract_sha256: str) -> int:
    db = Path(cfg.db_path)
    if not db.exists():
        return 0
    conn = sqlite3.connect(db, timeout=10)
    try:
        return int(conn.execute(
            "SELECT COUNT(*) FROM authoritative_results "
            "WHERE status='accepted' AND tier='native_static_v2' "
            "AND contract_sha256=?",
            (contract_sha256,),
        ).fetchone()[0])
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()


def _ensure_authoritative_lanes(cfg: CrawlConfig) -> int:
    """Restore the configured number of distinct exits after a cold boot."""
    required = len(cfg.proxy.proxies) if cfg.proxy.enabled else 0
    ensure_mihomo()
    if required <= 0:
        return 0
    return assign_distinct_lanes(required)


def _db_stats(cfg: CrawlConfig) -> dict:
    db = Path(cfg.db_path)
    if not db.exists():
        return {"tasks": {}, "authoritative": {}}
    conn = sqlite3.connect(db, timeout=10)
    try:
        tasks = {
            f"{status}:{kind}": int(count)
            for status, kind, count in conn.execute(
                "SELECT status,kind,COUNT(*) FROM tasks GROUP BY status,kind"
            )
        }
        try:
            authoritative = {
                f"{status}:{tier}": int(count)
                for status, tier, count in conn.execute(
                    "SELECT status,tier,COUNT(*) FROM authoritative_results "
                    "GROUP BY status,tier"
                )
            }
        except sqlite3.OperationalError:
            authoritative = {}
        return {"tasks": tasks, "authoritative": authoritative}
    finally:
        conn.close()


def _kill_tree(pid: int) -> None:
    if pid <= 0:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"], check=False,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    else:
        try:
            os.kill(pid, 15)
        except OSError:
            pass


def start(config: str | Path) -> int:
    config_path, cfg, contract_sha = _validated_config(config)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    old = _read_json(LOCK)
    if _pid_alive(int(old.get("pid", 0))):
        print(f"authoritative 采集已运行，PID={old['pid']}")
        return 0
    accepted = _accepted(cfg, contract_sha)
    if accepted >= int(cfg.authoritative_target):
        print(f"已达到 authoritative 目标：{accepted:,}/{cfg.authoritative_target:,}")
        return 0
    _ensure_authoritative_lanes(cfg)
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) | getattr(
        subprocess, "CREATE_NEW_PROCESS_GROUP", 0
    )
    with STDOUT.open("ab") as stdout, STDERR.open("ab") as stderr:
        process = subprocess.Popen(
            [
                sys.executable, "-m", "crawler.authoritative_production", "run",
                "--config", str(config_path),
            ],
            cwd=str(WORKSPACE), stdout=stdout, stderr=stderr,
            creationflags=flags,
        )
    time.sleep(2)
    if process.poll() is not None:
        raise RuntimeError("authoritative supervisor 启动失败，请查看专用日志")
    print(
        f"authoritative 采集已启动 PID={process.pid} "
        f"accepted={accepted:,}/{cfg.authoritative_target:,} contract={contract_sha}"
    )
    return 0


def run(config: str | Path) -> int:
    config_path, cfg, contract_sha = _validated_config(config)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    started = time.time()
    baseline = _accepted(cfg, contract_sha)
    payload = {
        "pid": os.getpid(),
        "started_at": started,
        "baseline_accepted": baseline,
        "config": str(config_path),
        "contract_sha256": contract_sha,
    }
    LOCK.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    STATE.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    child: subprocess.Popen | None = None
    watchdog: subprocess.Popen | None = None
    try:
        while _accepted(cfg, contract_sha) < int(cfg.authoritative_target):
            _ensure_authoritative_lanes(cfg)
            flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) | getattr(
                subprocess, "CREATE_NEW_PROCESS_GROUP", 0
            )
            env = os.environ.copy()
            env["PYTHONUTF8"] = "1"
            if watchdog is None or watchdog.poll() is not None:
                with WATCHDOG_LOG.open("ab") as log:
                    watchdog = subprocess.Popen(
                        [sys.executable, "-m", "crawler.lane_watchdog"],
                        cwd=str(WORKSPACE), env=env, stdout=log, stderr=log,
                        creationflags=flags,
                    )
            with STDOUT.open("ab") as stdout, STDERR.open("ab") as stderr:
                child = subprocess.Popen(
                    [sys.executable, "-m", "crawler.main", "--config", str(config_path)],
                    cwd=str(WORKSPACE), env=env, stdout=stdout, stderr=stderr,
                    creationflags=flags,
                )
            while child.poll() is None:
                time.sleep(30)
                accepted = _accepted(cfg, contract_sha)
                elapsed = max(1.0, time.time() - started)
                STATE.write_text(json.dumps({
                    **payload,
                    "child_pid": child.pid,
                    "accepted": accepted,
                    "target": cfg.authoritative_target,
                    "run_accepted": max(0, accepted - baseline),
                    "accepted_per_day": round(
                        max(0, accepted - baseline) / elapsed * 86400
                    ),
                    "updated_at": time.time(),
                }, ensure_ascii=False), encoding="utf-8")
                if accepted >= int(cfg.authoritative_target):
                    _kill_tree(child.pid)
                    return 0
                if RESTART_REQUEST.exists():
                    try:
                        RESTART_REQUEST.unlink()
                    except FileNotFoundError:
                        pass
                    _kill_tree(child.pid)
                    break
                if not controller_ready():
                    _kill_tree(child.pid)
                    _ensure_authoritative_lanes(cfg)
                    break
            if _accepted(cfg, contract_sha) >= int(cfg.authoritative_target):
                return 0
            time.sleep(15)
        return 0
    finally:
        if child is not None and child.poll() is None:
            _kill_tree(child.pid)
        if watchdog is not None and watchdog.poll() is None:
            watchdog.terminate()
        try:
            LOCK.unlink()
        except FileNotFoundError:
            pass


def status(config: str | Path) -> int:
    config_path, cfg, contract_sha = _validated_config(config)
    lock = _read_json(LOCK)
    pid = int(lock.get("pid", 0))
    active = _pid_alive(pid)
    accepted = _accepted(cfg, contract_sha)
    started = float(lock.get("started_at", 0) or 0)
    elapsed = max(0.0, time.time() - started) if started else 0.0
    baseline = int(lock.get("baseline_accepted", accepted))
    produced = max(0, accepted - baseline)
    daily = produced / elapsed * 86400 if elapsed else 0.0
    remaining = max(0, int(cfg.authoritative_target) - accepted)
    print(json.dumps({
        "active": active,
        "pid": pid if active else None,
        "config": str(config_path),
        "accepted": accepted,
        "target": cfg.authoritative_target,
        "remaining": remaining,
        "run_accepted": produced,
        "accepted_per_day": round(daily),
        "eta_hours": round(remaining / daily * 24, 2) if daily else None,
        "authoritative_root": cfg.authoritative_output_dir,
        "db": cfg.db_path,
        "contract_sha256": contract_sha,
        **_db_stats(cfg),
    }, ensure_ascii=False, indent=2))
    return 0 if active else 1


def stop(config: str | Path) -> int:
    # Stopping must remain available even if the contract/config later becomes
    # unreadable; the dedicated lock is sufficient to identify the process.
    lock = _read_json(LOCK)
    pid = int(lock.get("pid", 0))
    if not _pid_alive(pid):
        print("authoritative 采集未运行。")
        try:
            LOCK.unlink()
        except FileNotFoundError:
            pass
        return 0
    _kill_tree(pid)
    try:
        LOCK.unlink()
    except FileNotFoundError:
        pass
    print(f"已停止 authoritative 采集 PID={pid}；inflight 下次自动恢复。")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="schema-5 authoritative 生产控制器")
    parser.add_argument("action", choices=("start", "run", "status", "stop"))
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    args = parser.parse_args(argv)
    return {
        "start": start,
        "run": run,
        "status": status,
        "stop": stop,
    }[args.action](args.config)


if __name__ == "__main__":
    raise SystemExit(main())
