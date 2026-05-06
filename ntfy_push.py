#!/usr/bin/env python3
"""Send ntfy push notification; auto-fallback to email if ntfy is unreachable.

Usage: python ntfy_push.py <title> <message>

Self-evolution: 每次推送自动 append 到 logs/push_tracker.jsonl 用于 hit rate 评估。
"""
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

TOPIC = "jasperli-zhh-xauusd"
HERE = Path(__file__).resolve().parent
TOKEN_FILE = HERE / ".ntfy_token"
PUSH_TRACKER = HERE / "logs" / "push_tracker.jsonl"
# ntfy.sh Pro（2026-04-22 回切）
BASE_URL = os.environ.get("NTFY_BASE_URL", "https://ntfy.sh")


def log_push_tracker(title: str, message: str, channel: str, status: str) -> None:
    """记录 push 到 tracker 用于自我进化评估（hit rate / surprise tracking）。"""
    try:
        PUSH_TRACKER.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).isoformat()
        # 提取价格 — 优先抓 "📍 $X,XXX" 标记位（金价行），其次抓首个 4 位以上数字
        price = None
        m = re.search(r"📍\s*\$([\d,]+(?:\.\d+)?)", message)
        if not m:
            # fallback: 抓首个 4+ 位数字（金价范围 $1,XXX-$9,XXX）
            m = re.search(r"\$\s?([1-9][,\d]{3,}(?:\.\d+)?)", message)
        if m:
            try:
                price = float(m.group(1).replace(",", ""))
            except Exception:
                pass
        # 推断方向
        direction = "unknown"
        head = message[:300]
        if any(k in head for k in ["多头", "↑↑", "利多", "偏多", "看多"]):
            direction = "long"
        elif any(k in head for k in ["空头", "↓↓", "利空", "偏空", "看空"]):
            direction = "short"
        elif any(k in head for k in ["震荡", "横盘", "中性", "夹压", "持平"]):
            direction = "neutral"
        # 提取 ⭐ 数（最大值）
        stars = 0
        for star_match in re.finditer(r"⭐+", message):
            stars = max(stars, len(star_match.group()))
        # 时间窗标签
        time_window = "unknown"
        for tw in ["SPIKE", "INTRADAY", "SWING", "POSITION"]:
            if tw in message[:200]:
                time_window = tw
                break
        # 叙事级别
        narrative = "unknown"
        if "A 新叙事" in head or "新叙事" in title:
            narrative = "A"
        elif "B 旧叙事" in head or "旧叙事变量" in head:
            narrative = "B"
        elif "C 噪音" in head:
            narrative = "C"
        entry = {
            "ts": ts,
            "title": title[:200],
            "channel": channel,
            "status": status,
            "direction": direction,
            "stars": stars,
            "time_window": time_window,
            "narrative": narrative,
            "price_at_push": price,
            "message_first_500": message[:500],
        }
        with open(PUSH_TRACKER, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass  # 不影响主推送流程


def send_ntfy(title: str, message: str) -> tuple[bool, str]:
    token = os.environ.get("NTFY_TOKEN", "").strip()
    if not token and TOKEN_FILE.exists():
        token = TOKEN_FILE.read_text(encoding="utf-8").strip()

    payload = json.dumps({
        "topic": TOPIC, "title": title, "message": message,
    }, ensure_ascii=False).encode("utf-8")

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "newsanalyst-ntfy/1.0",  # CF WAF blocks default Python UA
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        req = Request(BASE_URL, data=payload, method="POST", headers=headers)
        resp = urlopen(req, timeout=10)
        if 200 <= resp.status < 300:
            return True, f"OK {resp.status}"
        return False, f"HTTP {resp.status}"
    except URLError as e:
        return False, f"URLError: {e}"
    except Exception as e:
        return False, f"Exception: {e}"


def send_email_fallback(title: str, message: str) -> tuple[bool, str]:
    try:
        sys.path.insert(0, "/home/admin/OpusWorkspace/configEng")
        from mail import send_html, email_shell, md_to_mobile_html  # type: ignore
        subject = f"[ntfy-fallback] {title}"
        body_md = f"## {title}\n\n{message}\n\n---\n*（ntfy 不可达，经邮件通道补发）*"
        html = email_shell(md_to_mobile_html(body_md), subject_title=title)
        send_html(subject=subject, html=html)
        return True, "email sent"
    except Exception as e:
        return False, f"email fallback failed: {e}"


def _try_p1_enrich(title: str, message: str) -> tuple[str, str]:
    """Lazy-import V5 P1 coupler. Silent fallback if missing/errors —
    NewsAnalyst push pipeline must never break on V5 failure."""
    try:
        v5_path = Path("/home/admin/OpusWorkspace/AITraderV5/agents")
        if str(v5_path) not in sys.path:
            sys.path.insert(0, str(v5_path))
        from news_price_coupler import enrich_with_price_context  # type: ignore
        return enrich_with_price_context(title, message)
    except Exception:
        return title, message


def _spawn_p1plus_analyst(title: str, message: str) -> None:
    """P1+: fire-and-forget V5 顶级 trader 点评 LLM. Non-blocking — original
    push completes immediately, analyst pushes its own ntfy ~30s later. Only
    fires for ⭐⭐⭐ (gated inside news_analyst). Fully silent on any failure."""
    try:
        analyst_script = Path("/home/admin/OpusWorkspace/AITraderV5/agents/news_analyst.py")
        if not analyst_script.exists():
            return
        # detach: stdout/stderr to log, no wait
        log_path = Path("/home/admin/OpusWorkspace/AITraderV5/data/news_analyst")
        log_path.mkdir(parents=True, exist_ok=True)
        runner_log = log_path / "runner.log"
        with open(runner_log, "ab") as f:
            import subprocess as sp
            sp.Popen(
                ["python3.11", str(analyst_script), title, message],
                stdout=f, stderr=sp.STDOUT,
                start_new_session=True,
            )
    except Exception:
        pass


def main() -> int:
    if len(sys.argv) < 3:
        print("Usage: python ntfy_push.py <title> <message>", file=sys.stderr)
        return 1

    title, message = sys.argv[1], sys.argv[2]

    # P1: enrich high-impact news with XAU price context (synchronous, fast)
    title, message = _try_p1_enrich(title, message)

    ok, info = send_ntfy(title, message)
    if ok:
        log_push_tracker(title, message, "ntfy", info)
        print(info)
        # P1+: fire-and-forget LLM 顶级 trader 点评 (gated to ⭐⭐⭐ inside analyst)
        _spawn_p1plus_analyst(title, message)
        return 0

    print(f"ntfy failed: {info}", file=sys.stderr)
    ok2, info2 = send_email_fallback(title, message)
    if ok2:
        log_push_tracker(title, message, "email", info2)
        print(f"ntfy down → email fallback: {info2}")
        return 0

    log_push_tracker(title, message, "failed", f"ntfy:{info} | email:{info2}")
    print(f"both channels failed: {info2}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
