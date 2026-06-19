#!/usr/bin/env python3
"""
tick_streamer.py — deployed on win-tester.
Reads XAUUSD tick from local MT5 every 30s, emits one JSON line to stdout.
Consumer (price_monitor.py on VA) reads via persistent ssh subprocess.
"""
import json
import sys
import time

import MetaTrader5 as mt5

SYMBOL = "XAUUSD"
INTERVAL_S = 30


def emit(payload: dict) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def main() -> None:
    if not mt5.initialize():
        emit({"err": "INIT_FAIL", "detail": str(mt5.last_error())})
        sys.exit(2)
    if not mt5.symbol_select(SYMBOL, True):
        emit({"err": "SYMBOL_SELECT_FAIL", "symbol": SYMBOL, "detail": str(mt5.last_error())})
    emit({"event": "ready", "mt5_ver": mt5.__version__, "symbol": SYMBOL})
    try:
        while True:
            t = mt5.symbol_info_tick(SYMBOL)
            if t:
                emit({
                    "ts": int(t.time),
                    "bid": float(t.bid),
                    "ask": float(t.ask),
                    "mid": round((t.bid + t.ask) / 2.0, 4),
                    "spread": round(t.ask - t.bid, 4),
                })
            else:
                emit({"err": "NO_TICK"})
            time.sleep(INTERVAL_S)
    finally:
        mt5.shutdown()


if __name__ == "__main__":
    main()
