#!/usr/bin/env python3
"""Fetch current XAUUSD spot price from MT5 bridge.

Usage:
  python get_price.py            # prints one-line summary: "XAUUSD $4,760.23 (bid/ask 4760.15/4760.31, spread 0.16) @ 2026-04-20 14:03:11 UTC"
  python get_price.py --json     # machine-readable JSON
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

MT5_MODULE_PATH = Path(__file__).resolve().parent.parent / "mt5"
sys.path.insert(0, str(MT5_MODULE_PATH))


def fetch_xauusd():
    from mt5_compat import connect
    client, err = connect()
    if err:
        return None, f"connect failed: {err}"
    try:
        tick = client.symbol_info_tick("XAUUSD")
        if tick is None:
            return None, "symbol_info_tick returned None (symbol not found or market closed)"
        return {
            "symbol": "XAUUSD",
            "bid": round(float(tick.bid), 2),
            "ask": round(float(tick.ask), 2),
            "spread": round(float(tick.ask) - float(tick.bid), 2),
            "mid": round((float(tick.bid) + float(tick.ask)) / 2, 2),
            "broker_time_epoch": int(tick.time),
            "broker_time_iso": datetime.fromtimestamp(int(tick.time), tz=timezone.utc).isoformat(),
            "fetched_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }, None
    finally:
        try:
            client.shutdown()
        except Exception:
            pass


def main():
    data, err = fetch_xauusd()
    as_json = "--json" in sys.argv
    if err:
        payload = {"error": err, "fetched_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        print(json.dumps(payload) if as_json else f"ERROR: {err}", file=sys.stderr)
        sys.exit(1)
    if as_json:
        print(json.dumps(data, ensure_ascii=False))
    else:
        print(
            f"XAUUSD ${data['mid']:.2f} "
            f"(bid/ask {data['bid']}/{data['ask']}, spread {data['spread']}) "
            f"@ {data['fetched_utc']}"
        )


if __name__ == "__main__":
    main()
