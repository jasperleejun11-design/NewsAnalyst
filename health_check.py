#!/usr/bin/env python3
"""
NewsAnalyst 监听链 health-check — 防止文档声明 vs 实际部署 gap 再发生。

检查项:
  1. systemd: news-alert-monitor.service active
  2. systemd: news-price-monitor.service active
  3. 进程: daemon.py 在 ps 里
  4. 日志心跳: alert_monitor.log mtime ≤ 5 min ago(2min 轮询 + 余量)
  5. 日志心跳: price_monitor.log mtime ≤ 2 min ago(30s tick + 余量)
  6. 日志心跳: daemon.log mtime ≤ 6 h ago(L2 是 6h 周期)

任意失败 → 一条 ntfy 推送列全部失败项。
去重: 同一失败集 1 小时内不重复推(state 文件)。
"""
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

NEWS_HOME = Path(__file__).resolve().parent
LOG_DIR = NEWS_HOME / "logs"
STATE_FILE = LOG_DIR / ".health_state.json"
NTFY_PUSH = NEWS_HOME / "ntfy_push.py"

DEDUP_SEC = 60 * 60  # 同 fingerprint 1h 不重推

CHECKS = [
    ("svc/news-alert-monitor", "systemd", "news-alert-monitor.service"),
    ("svc/news-price-monitor", "systemd", "news-price-monitor.service"),
    ("proc/daemon.py",         "process", "daemon.py"),
    ("log/alert_monitor",      "log_mtime", ("alert_monitor.log", 5 * 60)),
    ("log/price_monitor",      "log_mtime", ("price_monitor.log", 2 * 60)),
    ("log/daemon",             "log_mtime", ("daemon.log", 6 * 60 * 60)),
]


def check_systemd(unit: str) -> tuple[bool, str]:
    try:
        out = subprocess.run(
            ["systemctl", "is-active", unit],
            capture_output=True, text=True, timeout=10,
        )
        state = out.stdout.strip() or out.stderr.strip()
        return (state == "active", state)
    except Exception as e:
        return (False, f"exec err: {e}")


def check_process(pattern: str) -> tuple[bool, str]:
    try:
        out = subprocess.run(
            ["pgrep", "-f", pattern],
            capture_output=True, text=True, timeout=5,
        )
        pids = out.stdout.strip().split()
        if pids:
            return (True, f"pid {pids[0]}")
        return (False, "no process")
    except Exception as e:
        return (False, f"exec err: {e}")


def check_log_mtime(log_name: str, max_age_s: int) -> tuple[bool, str]:
    path = LOG_DIR / log_name
    if not path.exists():
        return (False, "missing")
    age = time.time() - path.stat().st_mtime
    if age > max_age_s:
        return (False, f"stale {int(age)}s (>{max_age_s}s)")
    return (True, f"fresh {int(age)}s")


def run_all() -> list[tuple[str, bool, str]]:
    results = []
    for name, kind, arg in CHECKS:
        if kind == "systemd":
            ok, msg = check_systemd(arg)
        elif kind == "process":
            ok, msg = check_process(arg)
        elif kind == "log_mtime":
            log_name, max_age = arg
            ok, msg = check_log_mtime(log_name, max_age)
        else:
            ok, msg = False, "unknown kind"
        results.append((name, ok, msg))
    return results


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state: dict) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def push_alert(failures: list[tuple[str, str]]) -> None:
    lines = ["🛠 NewsAnalyst 监听链 drift 告警", ""]
    for name, msg in failures:
        lines.append(f"❌ {name}: {msg}")
    lines.append("")
    lines.append("→ ssh va-ecs; cd OpusWorkspace/NewsAnalyst")
    lines.append("→ sudo systemctl status / tail logs/*.log")
    body = "\n".join(lines)
    try:
        subprocess.run(
            [sys.executable, str(NTFY_PUSH), "[health-check] ⚠️ 监听链 drift", body],
            timeout=15, check=False,
        )
    except Exception as e:
        print(f"ntfy push failed: {e}", file=sys.stderr)


def main() -> int:
    results = run_all()
    failures = [(n, m) for n, ok, m in results if not ok]
    summary_line = " ".join(
        f"{n}={'OK' if ok else 'FAIL'}" for n, ok, _ in results
    )
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {summary_line}")
    if not failures:
        return 0

    fingerprint = hashlib.sha1(
        "|".join(sorted(n for n, _ in failures)).encode()
    ).hexdigest()[:12]

    state = load_state()
    last = state.get(fingerprint, 0)
    now = time.time()
    if now - last < DEDUP_SEC:
        print(f"  (drift fingerprint={fingerprint} 已推过,dedup 剩 {int(DEDUP_SEC - (now - last))}s)")
        return 1

    push_alert(failures)
    state[fingerprint] = now
    # 清理超过 24h 的旧记录
    cutoff = now - 24 * 3600
    state = {k: v for k, v in state.items() if v >= cutoff}
    save_state(state)
    return 1


if __name__ == "__main__":
    sys.exit(main())
