"""Schema5 权威 10 万场采集进度面板（Tkinter，无额外依赖）。"""
from __future__ import annotations

import ctypes
import json
import os
import shutil
import sqlite3
import subprocess
import time
import tkinter as tk
import tomllib
from pathlib import Path
from tkinter import ttk

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "config.authoritative.toml"
DB = ROOT / "data" / "authoritative-progress.sqlite3"
LOCK = ROOT / "logs" / "authoritative-production.lock"
METRICS = ROOT / "data" / "lanes" / "crawler-proxies.json"
LOG = ROOT / "logs" / "authoritative-production.stderr.log"


def authoritative_settings() -> tuple[int, Path]:
    value = tomllib.loads(CONFIG.read_text(encoding="utf-8-sig"))
    target = int(value.get("authoritative_target") or 100_000)
    output = Path(str(value.get("authoritative_output_dir") or ""))
    if not output.is_absolute():
        output = ROOT / output
    return target, output.resolve()


TARGET, AUTHORITATIVE_OUTPUT = authoritative_settings()


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
    if not handle:
        return False
    ctypes.windll.kernel32.CloseHandle(handle)
    return True


def read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def db_snapshot(contract_sha256: str | None = None) -> dict:
    if not DB.exists():
        return {}
    connection = sqlite3.connect(f"file:{DB.as_posix()}?mode=ro", uri=True, timeout=3)
    try:
        rows = connection.execute(
            "SELECT status,kind,COUNT(*) FROM tasks GROUP BY status,kind"
        ).fetchall()
        out = {f"{status}:{kind}": int(n) for status, kind, n in rows}
        out["total"] = sum(out.values())
        if contract_sha256:
            out["accepted"] = int(connection.execute(
                "SELECT COUNT(*) FROM authoritative_results "
                "WHERE status='accepted' AND tier='native_static_v2' "
                "AND contract_sha256=?",
                (contract_sha256,),
            ).fetchone()[0])
        else:
            out["accepted"] = int(connection.execute(
                "SELECT COUNT(*) FROM authoritative_results "
                "WHERE status='accepted' AND tier='native_static_v2'"
            ).fetchone()[0])
        return out
    finally:
        connection.close()


def tail_text(path: Path, max_bytes: int = 32_000, max_lines: int = 18) -> str:
    try:
        with path.open("rb") as stream:
            stream.seek(0, 2)
            size = stream.tell()
            stream.seek(max(0, size - max_bytes))
            text = stream.read().decode("utf-8", "replace")
        lines = [line for line in text.splitlines() if "WARNING" in line or "ERROR" in line or "进度:" in line]
        return "\n".join(lines[-max_lines:])
    except Exception:
        return ""


def fmt_number(value: float | int) -> str:
    return f"{int(value):,}"


def fmt_duration(hours: float | None) -> str:
    if hours is None or hours < 0:
        return "--"
    days, remainder = divmod(hours, 24)
    if days >= 1:
        return f"{int(days)}天 {remainder:.1f}小时"
    return f"{hours:.1f}小时"


class ProgressGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("RoyaleAPI 权威 Schema5 采集进度")
        self.geometry("980x720")
        self.minsize(900, 650)
        self.configure(bg="#111827")
        self._last_done = None
        self._last_time = None
        self._build_style()
        self._build_ui()
        self.after(100, self.refresh)

    def _build_style(self):
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("TFrame", background="#111827")
        style.configure("Card.TFrame", background="#1f2937")
        style.configure("TLabel", background="#111827", foreground="#e5e7eb", font=("Microsoft YaHei UI", 10))
        style.configure("Title.TLabel", font=("Microsoft YaHei UI", 20, "bold"), foreground="#f9fafb")
        style.configure("Big.TLabel", background="#1f2937", font=("Microsoft YaHei UI", 18, "bold"), foreground="#60a5fa")
        style.configure("Card.TLabel", background="#1f2937", foreground="#d1d5db")
        style.configure("Green.Horizontal.TProgressbar", troughcolor="#374151", background="#22c55e")
        style.configure("Blue.Horizontal.TProgressbar", troughcolor="#374151", background="#3b82f6")
        style.configure("Treeview", background="#111827", fieldbackground="#111827", foreground="#e5e7eb", rowheight=25)
        style.configure("Treeview.Heading", background="#374151", foreground="#f9fafb")

    def _build_ui(self):
        top = ttk.Frame(self)
        top.pack(fill="x", padx=18, pady=(14, 8))
        ttk.Label(top, text="RoyaleAPI 对局采集", style="Title.TLabel").pack(side="left")
        self.status = ttk.Label(top, text="读取中…", font=("Microsoft YaHei UI", 11, "bold"))
        self.status.pack(side="right")

        cards = ttk.Frame(self)
        cards.pack(fill="x", padx=18, pady=6)
        self.card_vars = {}
        for key, title in (("done", "权威已验收"), ("rate", "实时速度"), ("daily", "预计日产"), ("eta", "10万 ETA")):
            card = ttk.Frame(cards, style="Card.TFrame", padding=12)
            card.pack(side="left", fill="x", expand=True, padx=5)
            ttk.Label(card, text=title, style="Card.TLabel").pack(anchor="w")
            var = tk.StringVar(value="--")
            ttk.Label(card, textvariable=var, style="Big.TLabel").pack(anchor="w", pady=(5, 0))
            self.card_vars[key] = var

        progress = ttk.Frame(self, style="Card.TFrame", padding=14)
        progress.pack(fill="x", padx=23, pady=8)
        self.short_label = ttk.Label(progress, text="权威 10 万场", style="Card.TLabel")
        self.short_label.pack(anchor="w")
        self.short_bar = ttk.Progressbar(progress, style="Green.Horizontal.TProgressbar", maximum=TARGET)
        self.short_bar.pack(fill="x", pady=(3, 0))

        info = ttk.Frame(self)
        info.pack(fill="x", padx=18, pady=5)
        self.info_var = tk.StringVar(value="")
        ttk.Label(info, textvariable=self.info_var).pack(side="left")
        ttk.Button(info, text="打开数据目录", command=lambda: os.startfile(AUTHORITATIVE_OUTPUT)).pack(side="right", padx=4)
        ttk.Button(info, text="打开日志目录", command=lambda: os.startfile(ROOT / "logs")).pack(side="right", padx=4)
        ttk.Button(info, text="立即刷新", command=self.refresh).pack(side="right", padx=4)

        lane_frame = ttk.Frame(self, style="Card.TFrame", padding=10)
        lane_frame.pack(fill="both", expand=True, padx=23, pady=7)
        ttk.Label(lane_frame, text="生产 lanes", style="Card.TLabel", font=("Microsoft YaHei UI", 11, "bold")).pack(anchor="w", pady=(0, 5))
        columns = ("proxy", "health", "available", "success", "fail", "latency", "cooldown")
        tree_wrap = ttk.Frame(lane_frame, style="Card.TFrame")
        tree_wrap.pack(fill="both", expand=True)
        self.tree = ttk.Treeview(tree_wrap, columns=columns, show="headings", height=10)
        headings = {"proxy":"入口", "health":"健康", "available":"可用", "success":"成功", "fail":"失败", "latency":"延迟", "cooldown":"冷却"}
        widths = {"proxy":190, "health":65, "available":65, "success":70, "fail":60, "latency":80, "cooldown":75}
        for column in columns:
            self.tree.heading(column, text=headings[column])
            self.tree.column(column, width=widths[column], anchor="center")
        scrollbar = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        log_frame = ttk.Frame(self, style="Card.TFrame", padding=10)
        log_frame.pack(fill="both", expand=True, padx=23, pady=(2, 14))
        ttk.Label(log_frame, text="最近进度 / 异常", style="Card.TLabel", font=("Microsoft YaHei UI", 11, "bold")).pack(anchor="w")
        self.log_text = tk.Text(log_frame, height=8, bg="#0b1220", fg="#cbd5e1", insertbackground="white", relief="flat", font=("Consolas", 9))
        self.log_text.pack(fill="both", expand=True, pady=(5, 0))

    def refresh(self):
        try:
            lock = read_json(LOCK, {})
            active = pid_alive(int(lock.get("pid", 0) or 0))
            stats = db_snapshot(str(lock.get("contract_sha256") or "") or None)
            metrics = read_json(METRICS, {})
            done = int(stats.get("accepted", 0))
            now = time.time()
            instant = float(metrics.get("battle_rate", 0) or 0)
            if instant <= 0 and self._last_done is not None and self._last_time:
                instant = max(0, done - self._last_done) / max(0.01, now - self._last_time)
            daily = instant * 86400
            eta = (TARGET - done) / daily * 24 if daily > 0 and done < TARGET else 0 if done >= TARGET else None
            self._last_done, self._last_time = done, now

            self.status.configure(text="● 运行中" if active else "● 已停止", foreground="#22c55e" if active else "#ef4444")
            self.card_vars["done"].set(fmt_number(done))
            self.card_vars["rate"].set(f"{instant:.3f} 场/秒")
            self.card_vars["daily"].set(f"{fmt_number(daily)} 场")
            self.card_vars["eta"].set(fmt_duration(eta))
            self.short_bar["value"] = min(done, TARGET)
            self.short_label.configure(text=f"权威目标：{done / TARGET * 100:.2f}%  ({fmt_number(done)} / {fmt_number(TARGET)})")
            disk = shutil.disk_usage(ROOT)
            self.info_var.set(
                f"列表完成 {fmt_number(stats.get('done:list', 0) + stats.get('done:upgrade_list', 0))}　"
                f"详情 pending {fmt_number(stats.get('pending:detail', 0) + stats.get('pending:upgrade', 0))}　"
                f"inflight {fmt_number(stats.get('inflight:detail', 0) + stats.get('inflight:list', 0) + stats.get('inflight:upgrade', 0) + stats.get('inflight:upgrade_list', 0))}　"
                f"dead {fmt_number(stats.get('dead:detail', 0) + stats.get('dead:list', 0) + stats.get('dead:upgrade', 0) + stats.get('dead:upgrade_list', 0))}　"
                f"skipped {fmt_number(stats.get('skipped:detail', 0) + stats.get('skipped:upgrade', 0))}　"
                f"磁盘剩余 {disk.free / 1024**3:.1f} GB"
            )

            for item in self.tree.get_children():
                self.tree.delete(item)
            for row in metrics.get("proxies", []):
                self.tree.insert("", "end", values=(
                    row.get("proxy", ""),
                    "✓" if row.get("healthy") else "✗",
                    "✓" if row.get("available") else "—",
                    row.get("success", 0), row.get("fail", 0),
                    f"{row.get('latency_ema', 0):.2f}s",
                    f"{row.get('cooldown_left', 0):.0f}s",
                ))
            self.log_text.delete("1.0", "end")
            self.log_text.insert("end", tail_text(LOG))
            self.log_text.see("end")
        except Exception as exc:
            self.status.configure(text=f"读取失败：{exc}", foreground="#f59e0b")
        self.after(2000, self.refresh)


def main():
    os.chdir(ROOT)
    ProgressGUI().mainloop()


if __name__ == "__main__":
    main()
