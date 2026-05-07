#!/usr/bin/env python3
"""Fetch current XAUUSD spot price from MT5 bridge.

Usage:
  python get_price.py            # 一行简版 (只 bid/ask)
  python get_price.py --json     # JSON (兼容旧调用方)
  python get_price.py --full     # 多行版: 现价 + 今日开盘锚点 + 1h/4h/24h move + spread
                                  # daemon 前置钩子用此版, 让 LLM 有"今日"统一锚, 不再编 +$XX
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


def fetch_full_anchors():
    """Pull today_open + multi-tf moves from V5 MT5 bars cache.

    Reads V5's xau_bars.json (60s 刷新). 不直接连 MT5 (避免和 mt5_compat connect 抢 RPC).
    Returns dict with today_open, today_change_dollar/_pct, change_1h/4h/24h, or None on failure.
    """
    cache_path = Path("/home/admin/OpusWorkspace/AITraderV5/data/cockpit/xau_bars.json")
    if not cache_path.exists():
        return None
    try:
        j = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    bars = j.get("bars", {})
    d1 = bars.get("D1") or []
    m1 = bars.get("M1") or []
    if not d1 or not m1:
        return None

    today_d1 = d1[-1]
    today_open = today_d1.get("open")
    today_high = today_d1.get("high")
    today_low = today_d1.get("low")
    today_open_ts = today_d1.get("ts_utc")
    last_close = m1[-1].get("close")

    if today_open is None or last_close is None:
        return None

    today_change = round(last_close - today_open, 2)
    today_pct = round((last_close - today_open) / today_open * 100, 3) if today_open else None

    def _change_n_min(n_min: int):
        if not m1:
            return None
        last_ts = m1[-1].get("ts_utc")
        if not last_ts:
            return None
        try:
            cutoff = datetime.fromisoformat(last_ts.replace("Z", "+00:00")) - \
                     __import__("datetime").timedelta(minutes=n_min)
        except Exception:
            return None
        for b in reversed(m1):
            try:
                bt = datetime.fromisoformat(b["ts_utc"].replace("Z", "+00:00"))
            except Exception:
                continue
            if bt <= cutoff:
                d = round(last_close - b["close"], 2)
                p = round(d / b["close"] * 100, 3) if b["close"] else None
                return {"dollar": d, "pct": p}
        return None

    return {
        "today_open": round(today_open, 2),
        "today_open_ts_utc": today_open_ts,
        "today_high": round(today_high, 2) if today_high else None,
        "today_low": round(today_low, 2) if today_low else None,
        "today_change_dollar": today_change,
        "today_change_pct": today_pct,
        "change_1h":  _change_n_min(60),
        "change_4h":  _change_n_min(240),
        "change_24h": _change_n_min(60 * 24),
        "source": "MT5 broker D1 bar (V5 cache)",
        "cache_ts_utc": j.get("ts_utc"),
    }


def format_full(tick: dict, anchors: dict | None) -> str:
    lines = [
        f"XAUUSD ${tick['mid']:.2f}  (bid {tick['bid']:.2f} / ask {tick['ask']:.2f} · 点差 ${tick['spread']:.2f})",
        f"取价时刻: {tick['fetched_utc']}",
    ]
    if anchors:
        lines.append("")
        lines.append(f"今日开盘锚 (broker D1 open): ${anchors['today_open']:.2f}  · 时间: {anchors['today_open_ts_utc']}")
        sign = "+" if (anchors['today_change_dollar'] or 0) >= 0 else ""
        lines.append(f"今日涨跌 (vs broker D1 开): {sign}${anchors['today_change_dollar']:.2f}  ({anchors['today_change_pct']:+.2f}%)")
        if anchors.get("today_high") and anchors.get("today_low"):
            lines.append(f"今日 D1 区间: ${anchors['today_low']:.2f} ~ ${anchors['today_high']:.2f}")
        for label, key in [("1h", "change_1h"), ("4h", "change_4h"), ("24h", "change_24h")]:
            ch = anchors.get(key)
            if ch and ch.get("dollar") is not None:
                s = "+" if ch["dollar"] >= 0 else ""
                lines.append(f"近 {label} 移动: {s}${ch['dollar']:.2f}  ({ch['pct']:+.2f}%)")
        lines.append(f"数据源: {anchors['source']} · cache: {anchors.get('cache_ts_utc','?')}")
    else:
        lines.append("")
        lines.append("⚠️ 今日开盘锚不可用 (V5 MT5 cache 未就绪) — 不要在推送里写'今日±$XX', 改用相对锚 (如'近 4h ±$XX')")
    return "\n".join(lines)


def main():
    data, err = fetch_xauusd()
    as_json = "--json" in sys.argv
    full = "--full" in sys.argv
    if err:
        payload = {"error": err, "fetched_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        print(json.dumps(payload) if as_json else f"ERROR: {err}", file=sys.stderr)
        sys.exit(1)

    if full:
        anchors = fetch_full_anchors()
        if as_json:
            payload = {**data, "anchors": anchors}
            print(json.dumps(payload, ensure_ascii=False))
        else:
            print(format_full(data, anchors))
        return

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
