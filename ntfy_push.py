#!/usr/bin/env python3
"""Send ntfy push notification. Usage: python ntfy_push.py "title" "message" """
import json, sys
from urllib.request import urlopen, Request

TOPIC = "jasperli-zhh-xauusd"

if len(sys.argv) < 3:
    print("Usage: python ntfy_push.py <title> <message>")
    sys.exit(1)

title = sys.argv[1]
message = sys.argv[2]

payload = json.dumps({
    "topic": TOPIC,
    "title": title,
    "message": message,
}, ensure_ascii=False).encode("utf-8")

req = Request(
    "https://ntfy.sh",
    data=payload, method="POST",
    headers={"Content-Type": "application/json; charset=utf-8"})
resp = urlopen(req, timeout=15)
print(f"OK {resp.status}")
