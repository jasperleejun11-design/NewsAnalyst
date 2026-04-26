#!/usr/bin/env python3
"""
XAUUSD 价格突变监听器 — 每 30s 取 MT5 实时价，维护 5/15 分钟滑动窗口。
当 5-min delta > 0.3% 或 15-min delta > 0.5% 时，写 .urgent_trigger 供 daemon 拾取。

这是 alert_monitor.py 的**价格维度补充**：
- alert_monitor 依赖新闻标题（有延迟）
- price_monitor 直接监听市场自身（0 延迟）
- 两者共用 .urgent_trigger 文件协议

注意：必须用 python3.11（mt5linux 依赖）。
"""
import json
import logging
import os
import signal
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

# MT5 shim path
MT5_MODULE_PATH = NEWS_HOME.parent / "mt5"
sys.path.insert(0, str(MT5_MODULE_PATH))

POLL_INTERVAL = 30                    # 秒：每 30s 取一次价
WINDOW_5MIN_SAMPLES = 10              # 5 分钟 / 30s = 10 个样本
WINDOW_15MIN_SAMPLES = 30             # 15 分钟 / 30s = 30 个样本
MAX_HISTORY = 40                      # 保留 20 分钟历史

# 突发阈值（相对百分比）
THRESH_5MIN_PCT = 0.30                # 5 分钟内变动 > 0.3% → 触发（约 $14 at $4,800）
THRESH_15MIN_PCT = 0.50               # 15 分钟内变动 > 0.5% → 触发（约 $24 at $4,800）
COOLDOWN_SECONDS = 600                # 触发后 10 分钟冷静期，不再重复触发

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


def _unlink_quiet(p: Path):
    try:
        p.unlink()
    except Exception:
        pass


def fetch_mid_price():
    """返回 (mid_price, ts_utc) 或 (None, err)。"""
    try:
        from mt5_compat import connect
        client, err = connect()
        if err:
            return None, f"connect failed: {err}"
        try:
            tick = client.symbol_info_tick("XAUUSD")
            if tick is None:
                return None, "tick None (symbol missing or market closed)"
            mid = (float(tick.bid) + float(tick.ask)) / 2
            return round(mid, 2), datetime.now(timezone.utc)
        finally:
            try:
                client.shutdown()
            except Exception:
                pass
    except Exception as e:
        return None, str(e)


def write_trigger(now_price: float, ref_price: float, minutes: int, delta_pct: float):
    """价格突变触发：写 .urgent_trigger 让 daemon 跑 BREAKING_PROMPT。

    headline 被构造成英文短句，这样 daemon 现有流程无需改动。
    """
    delta_usd = round(now_price - ref_price, 2)
    direction = "spike" if delta_pct > 0 else "plunge"
    headline = (
        f"XAUUSD {minutes}-min price {direction}: "
        f"${now_price:.2f} vs ${ref_price:.2f} ({delta_usd:+.2f} / {delta_pct:+.2f}%)"
    )
    trigger = {
        "time": datetime.now(timezone.utc).isoformat(),
        "headline": headline,
        "source": "MT5 Price Monitor",
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


def main():
    my_pid = os.getpid()
    if PID_FILE.exists():
        try:
            old_pid = int(PID_FILE.read_text().strip())
            os.kill(old_pid, 0)
            logger.error(f"已有实例运行 (PID {old_pid})，退出")
            return
        except (ProcessLookupError, ValueError):
            pass
        except Exception:
            pass
    PID_FILE.write_text(str(my_pid))
    atexit.register(lambda: _unlink_quiet(PID_FILE))

    def _on_signal(sig, frame):
        logger.info(f"收到信号 {sig}，退出")
        sys.exit(0)
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    logger.info(
        f"启动 (PID={my_pid})，轮询 {POLL_INTERVAL}s，"
        f"5min 阈值 {THRESH_5MIN_PCT}%，15min 阈值 {THRESH_15MIN_PCT}%，"
        f"冷静期 {COOLDOWN_SECONDS}s"
    )

    history = deque(maxlen=MAX_HISTORY)
    last_trigger_ts = 0.0

    while True:
        price, ts_or_err = fetch_mid_price()
        if price is None:
            logger.warning(f"取价失败: {ts_or_err}")
            time.sleep(POLL_INTERVAL)
            continue

        history.append((ts_or_err.timestamp(), price))

        # 只在有足够样本时判断
        now_ts = ts_or_err.timestamp()
        in_cooldown = (now_ts - last_trigger_ts) < COOLDOWN_SECONDS
        trigger_file_busy = TRIGGER_FILE.exists()

        summary = f"mid=${price:.2f} 样本={len(history)}"

        if len(history) >= WINDOW_5MIN_SAMPLES and not in_cooldown and not trigger_file_busy:
            # 5 分钟参考价：最早的 (len - WINDOW_5MIN_SAMPLES) 索引
            ref_5m = history[len(history) - WINDOW_5MIN_SAMPLES][1]
            delta_5m_pct = (price - ref_5m) / ref_5m * 100
            summary += f" 5m={delta_5m_pct:+.2f}%"

            if abs(delta_5m_pct) >= THRESH_5MIN_PCT:
                write_trigger(price, ref_5m, minutes=5, delta_pct=delta_5m_pct)
                last_trigger_ts = now_ts
                time.sleep(POLL_INTERVAL)
                continue

            # 15 分钟窗口
            if len(history) >= WINDOW_15MIN_SAMPLES:
                ref_15m = history[len(history) - WINDOW_15MIN_SAMPLES][1]
                delta_15m_pct = (price - ref_15m) / ref_15m * 100
                summary += f" 15m={delta_15m_pct:+.2f}%"
                if abs(delta_15m_pct) >= THRESH_15MIN_PCT:
                    write_trigger(price, ref_15m, minutes=15, delta_pct=delta_15m_pct)
                    last_trigger_ts = now_ts
                    time.sleep(POLL_INTERVAL)
                    continue
        elif trigger_file_busy:
            summary += " (trigger 文件待处理，跳过判断)"
        elif in_cooldown:
            remain = int(COOLDOWN_SECONDS - (now_ts - last_trigger_ts))
            summary += f" (冷静期剩 {remain}s)"

        logger.info(summary)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
