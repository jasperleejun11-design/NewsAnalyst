#!/usr/bin/env python3
"""
XAUUSD 价格突变监听器 — 价格源来自 win-tester MT5(不在 VA 跑 MT5 调用)。

架构:
  win-tester 上常驻 tick_streamer.py(MT5 python),30s 输出 1 行 JSON tick;
  VA 这边用单条 ssh 长连消费 stdout,做 5/15 min 滑窗判断;
  触发后写 .urgent_trigger 给 daemon 拾取(格式兼容 alert_monitor)。

触发阈值:
  5min 绝对变动 ≥ 0.30%
  15min 绝对变动 ≥ 0.50%
触发后 10min 冷静期防抖。
"""
import json
import logging
import os
import signal
import subprocess
import sys
import time
import atexit
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

NEWS_HOME = Path(__file__).resolve().parent
LOG_DIR = NEWS_HOME / "logs"
TRIGGER_FILE = LOG_DIR / ".urgent_trigger"
PID_FILE = LOG_DIR / ".price_monitor.pid"

REMOTE_HOST = "win-tester"
REMOTE_CMD = "python C:/Users/12965/tick_streamer.py"

POLL_INTERVAL = 30
WINDOW_5MIN_S = 5 * 60
WINDOW_15MIN_S = 15 * 60
MAX_HISTORY_S = 20 * 60

THRESH_5MIN_PCT = 0.30
THRESH_15MIN_PCT = 0.50
COOLDOWN_SECONDS = 600

LOG_DIR.mkdir(parents=True, exist_ok=True)


def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("price_monitor")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("[%(asctime)s] [价格监控] %(message)s", "%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.FileHandler(LOG_DIR / "price_monitor.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


logger = _setup_logging()
_stop = False


def _unlink_quiet(p: Path):
    try:
        p.unlink()
    except Exception:
        pass


def write_trigger(now_price: float, ref_price: float, minutes: int, delta_pct: float):
    delta_usd = round(now_price - ref_price, 2)
    direction = "spike" if delta_pct > 0 else "plunge"
    headline = (
        f"XAUUSD {minutes}-min price {direction}: "
        f"${now_price:.2f} vs ${ref_price:.2f} ({delta_usd:+.2f} / {delta_pct:+.2f}%)"
    )
    trigger = {
        "time": datetime.now(timezone.utc).isoformat(),
        "headline": headline,
        "source": "MT5 Price Monitor (win-tester)",
        "score": 15,
        "keywords": [f"{minutes}min price {direction}", f"{delta_pct:+.2f}%"],
        "price_data": {
            "now": now_price,
            "ref": ref_price,
            "delta_usd": delta_usd,
            "delta_pct": delta_pct,
            "window_min": minutes,
        },
    }
    tmp = TRIGGER_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(trigger, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(str(tmp), str(TRIGGER_FILE))
    logger.warning(f"🚨 {headline}")


def prune(history: deque, now_ts: float):
    cutoff = now_ts - MAX_HISTORY_S
    while history and history[0][0] < cutoff:
        history.popleft()


def window_ref_price(history: deque, now_ts: float, window_s: int):
    target = now_ts - window_s
    for ts, mid in history:
        if ts >= target:
            return mid
    return None


def evaluate(history: deque, now_ts: float, price: float, state: dict):
    if (now_ts - state["last_trigger_ts"]) < COOLDOWN_SECONDS:
        remain = int(COOLDOWN_SECONDS - (now_ts - state["last_trigger_ts"]))
        return f"mid=${price:.2f} 样本={len(history)} (冷静期剩 {remain}s)"
    if TRIGGER_FILE.exists():
        return f"mid=${price:.2f} 样本={len(history)} (trigger 待处理,跳过)"

    summary = f"mid=${price:.2f} 样本={len(history)}"

    ref5 = window_ref_price(history, now_ts, WINDOW_5MIN_S)
    if ref5 is not None:
        chg5 = (price - ref5) / ref5 * 100.0
        summary += f" 5m={chg5:+.2f}%"
        if abs(chg5) >= THRESH_5MIN_PCT:
            write_trigger(price, ref5, 5, chg5)
            state["last_trigger_ts"] = now_ts
            return summary

    ref15 = window_ref_price(history, now_ts, WINDOW_15MIN_S)
    if ref15 is not None:
        chg15 = (price - ref15) / ref15 * 100.0
        summary += f" 15m={chg15:+.2f}%"
        if abs(chg15) >= THRESH_15MIN_PCT:
            write_trigger(price, ref15, 15, chg15)
            state["last_trigger_ts"] = now_ts

    return summary


def run_stream(history: deque, state: dict) -> int:
    logger.info(f"启动 ssh stream: {REMOTE_HOST} -> {REMOTE_CMD}")
    proc = subprocess.Popen(
        ["ssh", "-o", "ServerAliveInterval=30", "-o", "ServerAliveCountMax=3",
         "-o", "ConnectTimeout=10", REMOTE_HOST, REMOTE_CMD],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1,
    )
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            if _stop:
                break
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                logger.warning(f"非 JSON 行: {line[:200]}")
                continue
            if "err" in payload:
                logger.error(f"streamer 错误: {payload}")
                continue
            if payload.get("event") == "ready":
                logger.info(f"streamer ready: mt5_ver={payload.get('mt5_ver')}")
                continue

            ts = float(payload["ts"])
            mid = float(payload["mid"])
            history.append((ts, mid))
            prune(history, ts)
            logger.info(evaluate(history, ts, mid, state))
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        if proc.stderr:
            err = proc.stderr.read().strip()
            if err:
                logger.warning(f"ssh stderr: {err[:400]}")
    return proc.returncode or 0


def main():
    my_pid = os.getpid()
    if PID_FILE.exists():
        try:
            old_pid = int(PID_FILE.read_text().strip())
            os.kill(old_pid, 0)
            logger.error(f"已有实例运行 (PID {old_pid}),退出")
            return
        except (ProcessLookupError, ValueError):
            pass
        except Exception:
            pass
    PID_FILE.write_text(str(my_pid))
    atexit.register(lambda: _unlink_quiet(PID_FILE))

    def _on_signal(sig, _frame):
        global _stop
        logger.info(f"收到信号 {sig},退出")
        _stop = True
        sys.exit(0)
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    logger.info(
        f"启动 (PID={my_pid}) source={REMOTE_HOST} "
        f"5min 阈值 {THRESH_5MIN_PCT}% / 15min 阈值 {THRESH_15MIN_PCT}% "
        f"冷静期 {COOLDOWN_SECONDS}s"
    )

    history: deque = deque()
    state = {"last_trigger_ts": 0.0}
    backoff = 5

    while not _stop:
        try:
            rc = run_stream(history, state)
            logger.info(f"ssh stream 结束 rc={rc}")
        except Exception as e:
            logger.exception(f"stream 异常: {e}")
        if _stop:
            break
        logger.info(f"{backoff}s 后重连")
        time.sleep(backoff)
        backoff = min(backoff * 2, 120)


if __name__ == "__main__":
    main()
