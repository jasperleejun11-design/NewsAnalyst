#!/usr/bin/env python3
"""Send ntfy push notification; auto-fallback to email if ntfy is unreachable.

Usage: python ntfy_push.py <title> <message>
"""
import json
import os
import sys
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

TOPIC = "jasperli-zhh-xauusd"
HERE = Path(__file__).resolve().parent
TOKEN_FILE = HERE / ".ntfy_token"
# ntfy.sh Pro（2026-04-22 回切）
BASE_URL = os.environ.get("NTFY_BASE_URL", "https://ntfy.sh")


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


def main() -> int:
    if len(sys.argv) < 3:
        print("Usage: python ntfy_push.py <title> <message>", file=sys.stderr)
        return 1

    title, message = sys.argv[1], sys.argv[2]

    ok, info = send_ntfy(title, message)
    if ok:
        print(info)
        return 0

    print(f"ntfy failed: {info}", file=sys.stderr)
    ok2, info2 = send_email_fallback(title, message)
    if ok2:
        print(f"ntfy down → email fallback: {info2}")
        return 0

    print(f"both channels failed: {info2}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
