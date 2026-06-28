#!/usr/bin/env python3.11
"""一键关停 Shot-MGC(给 NewsAnalyst 调用)。

调用场景:NewsAnalyst 检测到风险信号(news 反转 / VIX 爆 / 异常波动)
判断当前 Shot-MGC 持仓应该立刻退出 → 调用此脚本。

操作链(原子):
  1. 通过 shot_switch web (127.0.0.1:18096) POST disarm
       /api/mgc/master?on=false   →  daemon 进入 slow-poll, 阻止新 grab
       /api/mgc/enabled?on=false  →  即使 daemon 错误 fast-poll 也不会 fire
     这一步写 hk-ecs config file 自动通过 shot_switch 的 ssh fanout。

  2. 默认 --close-position(可关): ssh hk-ecs 调
       scripts.mgc_shot.force_close --mode <live|paper|shadow> --reason <text>
     该 helper 用 coordinated_close 强平任何 shot_mgc 仓位, 处理 broker
     FIFO drift + 部分平仓 + opposite working order 取消。

  3. 推 ntfy 报告(钉钉/手机)— 给 trader 立刻知道发生了什么。

  4. 写 NewsAnalyst log + 退出码反映成败。

Flags:
  --no-close-position  : 只 disarm (web POST), 不动 broker 仓位
  --mode {live|paper|shadow} : ssh hk-ecs force_close 传入 mode (default live)
  --reason "<text>"   : 写入 ntfy + audit log, NewsAnalyst 必须给原因

Idempotent: shot_mgc 没仓位时 force_close 是 no-op。多次调用安全。

退出码:
  0 = 全成功(disarm + close 都 OK, 或 close=noop)
  1 = disarm OK 但 force_close 失败 / 部分失败
  2 = disarm 失败(web POST 错)— shot daemon 可能仍在武装状态!

例子(NewsAnalyst LLM tool 调用):
  python3.11 /home/admin/OpusWorkspace/NewsAnalyst/mgc_shot_close.py \\
    --reason "Iran deal signed → gold reversal expected"

紧急但只想 disarm 不想动钱:
  python3.11 /home/admin/OpusWorkspace/NewsAnalyst/mgc_shot_close.py \\
    --no-close-position --reason "monitoring, not yet committed to close"
"""
from __future__ import annotations
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ── Paths / endpoints ──────────────────────────────────────────────────────

# shot_switch service runs on this host (new-va) at port 18096 (uvicorn direct
# loopback; no nginx basic auth needed for 127.0.0.1).
SHOT_SWITCH_URL = "http://127.0.0.1:18096"

# hk-ecs reachable via ssh alias
HK_ECS_SSH       = "hk-ecs"
HK_ECS_PY        = "/home/admin/HK_Research/.venv/bin/python"
HK_ECS_FORCE_CLOSE_MODULE = "scripts.mgc_shot.force_close"

# Audit log for NewsAnalyst (matches existing iran_*_action.py pattern)
HERE        = Path(__file__).resolve().parent
LOG_DIR     = HERE / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_LOG   = LOG_DIR / "mgc_shot_close.jsonl"

# ntfy topic (matches MGC convention)
NTFY_TOPIC  = "jasperli-zhh-MGC"


def _log(rec: dict) -> None:
    """Append one JSONL line to audit log; failure non-fatal."""
    rec = {"ts": datetime.now(timezone.utc).isoformat(), **rec}
    try:
        with open(AUDIT_LOG, "a") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[mgc_shot_close] audit log write failed: {e}", file=sys.stderr)


def _push_ntfy(title: str, body: str, priority: str = "urgent") -> None:
    """Push notification — mirrors existing iran_*_action.py pattern."""
    try:
        subprocess.run(
            ["curl", "-fsS", "-X", "POST",
             f"https://ntfy.sh/{NTFY_TOPIC}",
             "-H", f"Title: {title}",
             "-H", f"Priority: {priority}",
             "-H", "Tags: rotating_light,shot_mgc",
             "--data-binary", body],
            timeout=8, capture_output=True,
        )
    except Exception as e:
        print(f"[mgc_shot_close] ntfy push failed: {e}", file=sys.stderr)


def _shot_switch_post(key: str, on: bool, timeout: float = 10.0) -> dict:
    """POST shot_switch /api/mgc/{master,enabled}?on=... — returns JSON
    response. Raises on HTTP error."""
    url = f"{SHOT_SWITCH_URL}/api/mgc/{key}?on={'true' if on else 'false'}"
    req = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def disarm() -> tuple[bool, dict]:
    """POST shot_switch to set mgc.master=false AND mgc.enabled=false.
    Returns (ok, info_dict)."""
    info = {"steps": []}
    try:
        r1 = _shot_switch_post("enabled", on=False)
        info["steps"].append({"step": "enabled=false", "ok": True, "sync": r1.get("sync", {})})
    except Exception as e:
        info["steps"].append({"step": "enabled=false", "ok": False, "error": str(e)})
        return False, info
    try:
        r2 = _shot_switch_post("master", on=False)
        info["steps"].append({"step": "master=false", "ok": True, "sync": r2.get("sync", {})})
    except Exception as e:
        info["steps"].append({"step": "master=false", "ok": False, "error": str(e)})
        return False, info
    # Verify final state from canonical
    try:
        with urllib.request.urlopen(f"{SHOT_SWITCH_URL}/api/config", timeout=10) as resp:
            cfg = json.loads(resp.read().decode("utf-8"))
            info["final_mgc"] = cfg.get("mgc", {})
    except Exception as e:
        info["final_mgc_read_err"] = str(e)
    return True, info


def force_close_remote(mode: str, reason: str, timeout: float = 60.0) -> tuple[bool, dict]:
    """ssh hk-ecs 调 force_close helper — returns (ok, parsed_json_or_err)."""
    remote_cmd = (
        f"cd /home/admin/HK_Research && "
        f"set -a; . /home/admin/.futu_trade_creds 2>/dev/null; set +a; "
        f"{HK_ECS_PY} -m {HK_ECS_FORCE_CLOSE_MODULE} "
        f"--mode {mode} "
        f"--reason {json.dumps(reason)}"
    )
    try:
        proc = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
             "-o", "StrictHostKeyChecking=no", HK_ECS_SSH, remote_cmd],
            capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        return False, {"error": f"ssh hk-ecs timeout after {timeout}s",
                       "stdout": e.stdout, "stderr": e.stderr}
    except Exception as e:
        return False, {"error": f"ssh hk-ecs invocation failed: {e}"}
    out = proc.stdout.strip()
    err = proc.stderr.strip()
    # Try to parse the LAST line of stdout as JSON (force_close prints one JSON line)
    parsed = None
    if out:
        try:
            parsed = json.loads(out.splitlines()[-1])
        except Exception:
            pass
    info = {"rc": proc.returncode, "stdout": out, "stderr": err, "parsed": parsed}
    ok = (proc.returncode == 0)
    return ok, info


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Shot-MGC 一键关停 (disarm web + 可选 force close)",
    )
    ap.add_argument("--reason", required=True,
                    help="自由文本原因, 写 ntfy + audit log, NewsAnalyst 必须给")
    ap.add_argument("--no-close-position", action="store_true",
                    help="只 disarm, 不动 broker 仓位 (safer; default 关 = 同时强平)")
    ap.add_argument("--mode", choices=["live", "paper", "shadow"], default="live",
                    help="force_close mode (default live; debug 用 shadow 验链路)")
    ap.add_argument("--quiet", action="store_true",
                    help="不打印 JSON 到 stdout(audit log + ntfy 不受影响)")
    args = ap.parse_args()

    overall: dict = {
        "reason":   args.reason,
        "mode":     args.mode,
        "close_position": not args.no_close_position,
        "ts":       int(time.time()),
    }

    # Step 1: disarm via shot_switch web
    disarm_ok, disarm_info = disarm()
    overall["disarm"] = disarm_info
    if not disarm_ok:
        _push_ntfy(
            "[Shot-MGC] DISARM FAILED ⚠",
            f"reason: {args.reason}\n"
            f"shot_switch POST failed; daemon may still be armed!\n"
            f"steps: {json.dumps(disarm_info.get('steps'), ensure_ascii=False)}",
            priority="urgent",
        )
        _log({"action": "mgc_shot_close", "ok": False, "stage": "disarm",
              "result": overall})
        if not args.quiet:
            print(json.dumps(overall, ensure_ascii=False))
        return 2

    # Step 2: optional force-close on hk-ecs
    if args.no_close_position:
        overall["force_close"] = {"skipped": True, "reason": "--no-close-position"}
        _push_ntfy(
            "[Shot-MGC] DISARMED (no broker action)",
            f"reason: {args.reason}\n"
            f"web mgc.master=OFF mgc.enabled=OFF; existing position (if any) "
            f"left alone by trader request",
            priority="urgent",
        )
        _log({"action": "mgc_shot_close", "ok": True, "stage": "disarm_only",
              "result": overall})
        if not args.quiet:
            print(json.dumps(overall, ensure_ascii=False))
        return 0

    close_ok, close_info = force_close_remote(args.mode, args.reason)
    overall["force_close"] = close_info

    if close_ok:
        parsed = close_info.get("parsed") or {}
        lots = parsed.get("lots_closed", 0)
        fill_px = parsed.get("fill_px")
        action = parsed.get("action", "?")
        _push_ntfy(
            "[Shot-MGC] KILLED ✓" if lots > 0 else "[Shot-MGC] DISARMED (no pos)",
            f"reason: {args.reason}\n"
            f"action={action} lots_closed={lots} fill_px={fill_px}\n"
            f"mode={args.mode}",
            priority="urgent",
        )
        _log({"action": "mgc_shot_close", "ok": True, "stage": "full_close",
              "result": overall})
        if not args.quiet:
            print(json.dumps(overall, ensure_ascii=False))
        return 0
    else:
        _push_ntfy(
            "[Shot-MGC] HALF-KILLED ⚠⚠",
            f"reason: {args.reason}\n"
            f"web disarmed OK but force_close FAILED on hk-ecs\n"
            f"rc={close_info.get('rc')} err={close_info.get('error', '')[:200]}\n"
            f"shot_mgc position may still be open at broker!",
            priority="urgent",
        )
        _log({"action": "mgc_shot_close", "ok": False, "stage": "force_close_failed",
              "result": overall})
        if not args.quiet:
            print(json.dumps(overall, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    sys.exit(main())
