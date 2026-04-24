#!/usr/bin/env python3
"""
Phase 0: Parse macro.md → write news_signal.csv for MT5 EA consumption.

Output path: MT5 Common/Files (FILE_COMMON flag reads from here)
Format:      timestamp,direction,strength,mute_until,confidence
             timestamp  = ISO-8601 UTC when signal was written
             direction  = +1 (bullish) / -1 (bearish) / 0 (neutral)
             strength   = 0.0-1.0  (弱=0.30, 中=0.50, 强=0.80)
             mute_until = Unix timestamp (int) of mute window end, 0 = no active mute
             confidence = 0.0-1.0  source credibility score from ★ count
"""
import re
import time
from datetime import datetime, timezone
from pathlib import Path

MACRO_FILE  = Path(__file__).parent / ".trader" / "macro.md"
SIGNAL_FILE = Path(
    "/data/mt5/data/.wine/drive_c/users/abc/AppData/Roaming/"
    "MetaQuotes/Terminal/Common/Files/news_signal.csv"
)


def _parse_direction_and_strength(text: str) -> tuple:
    for line in text.splitlines():
        if "方向偏向" not in line:
            continue
        # First arrow emoji by character index wins (short-term view)
        positions = []
        for emoji, val in [("📈", +1), ("📉", -1), ("➡️", 0)]:
            idx = line.find(emoji)
            if idx >= 0:
                positions.append((idx, val))
        direction = min(positions, key=lambda x: x[0])[1] if positions else 0

        strength = 0.30
        if "偏强度" in line:
            after = line.split("偏强度", 1)[1]
            if "强" in after and "弱" not in after:
                strength = 0.80
            elif "中" in after:
                strength = 0.50
        return direction, strength
    return 0, 0.30


def _parse_mute_until(text: str) -> int:
    """Return Unix timestamp of the mute window end, or 0."""
    today = datetime.now(timezone.utc).date()
    for line in text.splitlines():
        if "哑区" not in line or "⚠️" not in line:
            continue
        if "今日无特殊" in line:
            return 0
        # Match patterns like "13:15-13:45 UTC" or "（13:15-13:45 UTC）"
        m = re.search(r'(\d{1,2}:\d{2})\s*[-–]\s*(\d{1,2}:\d{2})\s*UTC', line)
        if m:
            try:
                h, mi = map(int, m.group(2).split(":"))
                end_dt = datetime(today.year, today.month, today.day, h, mi,
                                  tzinfo=timezone.utc)
                ts = int(end_dt.timestamp())
                return ts if ts > int(time.time()) else 0
            except (ValueError, OverflowError):
                pass
    return 0


def _parse_confidence(text: str) -> float:
    """Confidence score from source credibility (★) distribution."""
    three = text.count("★★★")
    two   = text.count("★★") - three        # deduplicate
    one   = text.count("★")  - three - two  # deduplicate
    total = max(1, three + two + one)
    score = (three * 1.0 + two * 0.5 + one * 0.1) / total
    return round(min(1.0, max(0.0, score)), 3)


def write_news_signal() -> tuple | None:
    """Parse macro.md and write news_signal.csv. Returns parsed tuple or None."""
    if not MACRO_FILE.exists():
        return None
    text = MACRO_FILE.read_text(encoding="utf-8").strip()
    if not text:
        return None

    direction, strength = _parse_direction_and_strength(text)
    mute_until          = _parse_mute_until(text)
    confidence          = _parse_confidence(text)
    timestamp           = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    SIGNAL_FILE.parent.mkdir(parents=True, exist_ok=True)
    content = (
        "timestamp,direction,strength,mute_until,confidence\n"
        f"{timestamp},{direction:+d},{strength:.3f},{mute_until},{confidence:.3f}\n"
    )
    tmp = SIGNAL_FILE.with_suffix(".csv.tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(SIGNAL_FILE)

    return direction, strength, mute_until, confidence


if __name__ == "__main__":
    result = write_news_signal()
    if result:
        d, s, m, c = result
        mute_str = datetime.fromtimestamp(m, tz=timezone.utc).strftime("%H:%MZ") if m else "none"
        print(f"[news_signal] dir={d:+d} str={s:.2f} mute_until={mute_str} conf={c:.2f}")
        print(f"  → {SIGNAL_FILE}")
    else:
        print("[news_signal] skipped (macro.md missing or empty)")
