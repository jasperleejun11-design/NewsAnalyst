#!/usr/bin/env python3.11
"""NewsAnalyst — 发每 4h 研究员推演邮件（TL;DR 置顶）。

邮件结构：
    ┌ TL;DR 块（来自 .trader/macro.md 的【一行结论】，每 6h daemon 刷新）
    ├ 正文：.trader/forecast.md（Opus 深度推演，每日 00:10 UTC 覆盖 + 增量追加）
    └ 页脚：来源 + 时间戳

渲染用 configEng/mail.py 的共享 helpers（email_shell + md_to_mobile_html），
移动端 + CJK 友好。

用法：
    python3.11 send_news_email.py            # 默认: forecast.md + macro.md TL;DR
    python3.11 send_news_email.py --to a@x.com
    python3.11 send_news_email.py --dry-run

cron：每 4h（00:25 / 04:25 / 08:25 / 12:25 / 16:25 / 20:25 UTC）
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
FORECAST_MD = HERE / ".trader" / "forecast.md"
MACRO_MD = HERE / ".trader" / "macro.md"

sys.path.insert(0, "/home/admin/OpusWorkspace/configEng")
from mail import send_html, DEFAULT_TO, email_shell, md_to_mobile_html  # noqa: E402


def extract_tldr(macro_text: str) -> str | None:
    """抽 macro.md 的 `## 【一行结论】` 块到下一个 ## 前。返回 markdown 片段。"""
    m = re.search(
        r"(^|\n)## 【一行结论】\s*\n(.+?)(?=\n## |\Z)",
        macro_text, flags=re.DOTALL,
    )
    if not m:
        return None
    return m.group(2).strip()


def extract_price_anchor(forecast_text: str) -> str | None:
    """从 forecast.md 顶部抓`现价锚:` 行。"""
    for line in forecast_text.splitlines()[:15]:
        if line.lstrip().startswith(("现价锚", "现价:", "现价：")):
            return line.strip()
    return None


def extract_core_driver(tldr_md: str) -> str:
    """从 TL;DR 内抓`核心驱动:` 这一行做邮件主题后缀。"""
    for line in tldr_md.splitlines():
        m = re.match(r"^\s*核心驱动\s*[:：]\s*(.+)$", line)
        if m:
            s = m.group(1).strip()
            # 去掉 markdown 强调符，截断
            s = re.sub(r"[*_`]+", "", s)
            return s[:60]
    return ""


def build_body(forecast_text: str, macro_text: str | None) -> str:
    """拼邮件 inner HTML：TL;DR banner + 正文。"""
    tldr_md = extract_tldr(macro_text) if macro_text else None
    price_line = extract_price_anchor(forecast_text)

    parts: list[str] = []

    if tldr_md:
        tldr_html = md_to_mobile_html(tldr_md)
        price_row = (
            f'<p class="muted" style="margin:4px 0">{price_line}</p>'
            if price_line else ""
        )
        parts.append(
            '<div style="background:#fff8e1;border-left:4px solid #f59e0b;'
            'padding:12px 14px;border-radius:6px;margin-bottom:18px">'
            '<h3 style="margin:0 0 6px;color:#92400e">📌 TL;DR · 结论 + 依据</h3>'
            f'{price_row}{tldr_html}'
            '</div>'
        )
    elif price_line:
        parts.append(f'<p class="muted">{price_line}</p>')

    parts.append('<hr>')
    parts.append('<h3>🔬 完整推演正文</h3>')
    parts.append(md_to_mobile_html(forecast_text))
    return "".join(parts)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--forecast", default=str(FORECAST_MD),
                    help=f"正文 markdown（缺省 {FORECAST_MD.name}）")
    ap.add_argument("--macro", default=str(MACRO_MD),
                    help=f"TL;DR 来源（缺省 {MACRO_MD.name}）")
    ap.add_argument("--to", help="收件人，逗号分隔；缺省 mail.DEFAULT_TO")
    ap.add_argument("--dry-run", action="store_true", help="只打 HTML 到 stdout")
    args = ap.parse_args()

    src = Path(args.forecast)
    if not src.is_absolute():
        src = (HERE / src).resolve()
    if not src.is_file():
        print(f"ERROR: {src} 不存在", file=sys.stderr)
        return 2

    forecast_text = src.read_text(encoding="utf-8")

    macro_src = Path(args.macro)
    if not macro_src.is_absolute():
        macro_src = (HERE / macro_src).resolve()
    macro_text = macro_src.read_text(encoding="utf-8") if macro_src.is_file() else None

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")

    inner = build_body(forecast_text, macro_text)
    inner += (
        f'<p class="muted" style="margin-top:22px">'
        f'NewsAnalyst 自动发送 · {ts} · '
        f'source: <code>{src.name}</code>'
        + (f' + <code>{macro_src.name}</code>' if macro_text else '')
        + '</p>'
    )
    subject_title = f"每日黄金推演 · {date_str} UTC"
    html = email_shell(inner, subject_title=subject_title)

    # 主题：拉核心驱动作为后缀
    core = extract_core_driver(extract_tldr(macro_text) or "") if macro_text else ""
    subject = f"[XAUUSD] {date_str} UTC · 推演"
    if core:
        subject += f" — {core}"

    if args.dry_run:
        sys.stdout.write(html)
        print(f"\n\n-- dry-run: to={args.to or DEFAULT_TO} subject={subject!r}", file=sys.stderr)
        return 0

    to = [s.strip() for s in args.to.split(",")] if args.to else None
    send_html(subject=subject, html=html, to=to)
    print(f"OK: sent {subject!r} → {to or DEFAULT_TO}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
