#!/usr/bin/env python3
"""一键关停 Shot-XAUUSD(给 NewsAnalyst 调用)。

调用场景:NewsAnalyst 检测到 ⭐⭐⭐ 反转 / 风险信号(如 Iran 突然停火 / Fed
unexpected hold / OPEC 政策反转)判断 Shot-XAUUSD 当前应立刻退出 → 调用此脚本。

操作链(原子):
  1. 通过 shot_switch web (127.0.0.1:18096) POST disarm
       /api/xauusd/master?on=false   →  3 prod EA 进入 SLOW(30s)-poll, 阻止新 grab
       /api/xauusd/enabled?on=false  →  即使 EA 错误 fast-poll 也不会 fire
     这一步本地原子写 3 个 prod 的 MQL5/Files/shot_config.json,
     通过 shot_switch 的 per-prod fanout。

  2. 推 ntfy 报告(钉钉/手机)— 给 trader 立刻知道发生了什么。

  3. 写 NewsAnalyst log + 退出码反映成败。

⚠️ XAUUSD 与 MGC 的关键差异:
  MGC 通过 Futu API 直接强平 broker 仓位(mgc_shot_close.py 的 ssh hk-ecs 步骤)。
  XAUUSD 没有等价通道 — MT5 在 docker 容器里, 只 Shot-XAUUSD EA 自己能下平
  仓单。**本脚本只 disarm 武装**, 现存持仓会继续由 EA 走 BE/prune/broker SL
  自然退出, 不会被立刻强平。
  
  若需立刻强平 XAUUSD 现存 Shot 持仓(SL 太远来不及): 
    a) VNC 进 mt5-prod / mt5-prod2 / mt5-prod3, 右键 Shot 仓位 → Close Position
    b) (TODO) EA 加 force_close 字段支持, 然后此脚本可加 --close-position 模式

Flags:
  --reason "<text>" : 写入 ntfy + audit log, NewsAnalyst **必须**给原因
  --quiet           : 不打印 JSON 到 stdout

Idempotent: 多次调用安全(已 OFF 再 OFF 是 no-op)。

退出码:
  0 = disarm 成功(3 prod 同步 ok)
  1 = disarm 部分失败(canonical 写了但某个 prod 文件 fanout 失败)
  2 = disarm 完全失败(shot_switch web POST 错)— EA 可能仍武装!

例子(NewsAnalyst LLM tool 调用):
  python3.11 /home/admin/OpusWorkspace/NewsAnalyst/XAUUSD_shot_close.py \\
    --reason "Iran ceasefire signed → XAU rally reversal expected"
"""
from __future__ import annotations
import argparse
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ── Paths / endpoints ──────────────────────────────────────────────────────

# shot_switch service on this host (new-va), uvicorn direct loopback (no nginx auth)
SHOT_SWITCH_URL = "http://127.0.0.1:18096"

# Audit log
HERE      = Path(__file__).resolve().parent
LOG_DIR   = HERE / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_LOG = LOG_DIR / "xauusd_shot_close.jsonl"

# ntfy topic (per XAUUSD account convention)
NTFY_TOPIC = "jasperli-zhh-xauusd"


def _log(rec: dict) -> None:
    """Append one JSONL line to audit log; failure non-fatal."""
    rec = {"ts": datetime.now(timezone.utc).isoformat(), **rec}
    try:
        with open(AUDIT_LOG, "a") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"[xauusd_shot_close] audit log write failed: {e}", file=sys.stderr)


def _push_ntfy(title: str, body: str, priority: str = "urgent") -> None:
    """Push notification via ntfy.sh — mirrors mgc_shot_close pattern."""
    try:
        subprocess.run(
            ["curl", "-fsS", "-X", "POST",
             f"https://ntfy.sh/{NTFY_TOPIC}",
             "-H", f"Title: {title}",
             "-H", f"Priority: {priority}",
             "-H", "Tags: rotating_light,shot_xauusd",
             "--data-binary", body],
            timeout=8, capture_output=True,
        )
    except Exception as e:
        print(f"[xauusd_shot_close] ntfy push failed: {e}", file=sys.stderr)


def _shot_switch_post(key: str, on: bool, timeout: float = 10.0) -> dict:
    """POST shot_switch /api/xauusd/{master,enabled}?on=... — returns JSON."""
    url = f"{SHOT_SWITCH_URL}/api/xauusd/{key}?on={'true' if on else 'false'}"
    req = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _step_ok(step: dict) -> bool:
    if not step.get("ok"):
        return False
    sync = step.get("sync", {})
    return all(sync.get(f"xauusd.{p}") == "ok" for p in ("prod", "prod2", "prod3"))


def disarm() -> tuple[bool, dict]:
    """POST shot_switch to set xauusd.enabled=false AND xauusd.master=false.

    Order: enabled=false FIRST (kills fire edge before master=false drops the
    fast-poll, so a 1s race can't sneak a fire through).

    Independence: each step tries even if the previous failed — failures can
    be transient (one POST timeout) and we want maximum disarm effort. The
    final assessment combines both step outcomes.

    Returns (full_ok, info_dict).
    full_ok = True iff BOTH steps' POSTs returned 200 AND both showed all 3
    xauusd per-prod fanout=ok in their sync response."""
    info: dict = {"steps": []}

    # Step a: enabled OFF
    try:
        r1 = _shot_switch_post("enabled", on=False)
        sync = r1.get("sync", {})
        info["steps"].append({"step": "enabled=false", "ok": True, "sync": sync})
    except Exception as e:
        info["steps"].append({"step": "enabled=false", "ok": False, "error": str(e)})

    # Step b: master OFF (ALWAYS try, even if step a failed — master=OFF alone
    # drops EA back to SLOW(30s) polling which itself prevents fast-edge fire)
    try:
        r2 = _shot_switch_post("master", on=False)
        sync = r2.get("sync", {})
        info["steps"].append({"step": "master=false", "ok": True, "sync": sync})
    except Exception as e:
        info["steps"].append({"step": "master=false", "ok": False, "error": str(e)})

    # Step c: best-effort read back canonical to confirm final state
    try:
        with urllib.request.urlopen(f"{SHOT_SWITCH_URL}/api/config", timeout=10) as resp:
            cfg = json.loads(resp.read().decode("utf-8"))
            info["final_xauusd"] = cfg.get("xauusd", {})
    except Exception as e:
        info["final_xauusd_read_err"] = str(e)

    full_ok = all(_step_ok(s) for s in info["steps"])
    return full_ok, info


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Shot-XAUUSD 一键关停 (disarm web; broker 现存仓位由 EA 自然退出)",
    )
    ap.add_argument("--reason", required=True,
                    help="自由文本原因, 写 ntfy + audit log, NewsAnalyst 必须给")
    ap.add_argument("--quiet", action="store_true",
                    help="不打印 JSON 到 stdout(audit log + ntfy 不受影响)")
    args = ap.parse_args()

    overall: dict = {
        "reason": args.reason,
        "ts":     int(time.time()),
    }

    full_ok, disarm_info = disarm()
    overall["disarm"] = disarm_info

    final = disarm_info.get("final_xauusd", {})
    final_master  = final.get("master")
    final_enabled = final.get("enabled")
    canonical_safe = (final_master is False and final_enabled is False)

    # Per-step breakdown — needed to report WHICH step failed (vs the older
    # last-step-only check that wrongly blamed all-prod-fanout when step 1
    # passed and step 2 errored before producing any sync info).
    steps = disarm_info.get("steps", [])
    step_enabled = next((s for s in steps if s.get("step") == "enabled=false"), {})
    step_master  = next((s for s in steps if s.get("step") == "master=false"),  {})
    enabled_ok = _step_ok(step_enabled)
    master_ok  = _step_ok(step_master)

    # Failed prods: union across both steps where sync wasn't "ok"
    failed_prods: list[str] = []
    for s in (step_enabled, step_master):
        sync = s.get("sync", {})
        for p in ("prod", "prod2", "prod3"):
            if sync.get(f"xauusd.{p}") != "ok" and p not in failed_prods:
                failed_prods.append(p)

    # Success criteria = our 2 POSTs returned cleanly with 3-prod fanout = ok.
    # The "final canonical" read is informational: if another writer (web UI
    # or racing POST) re-arms between our last write and our verify read,
    # that's NOT a script failure — we did our job. The ntfy flags the race.
    if full_ok:
        if canonical_safe:
            _push_ntfy(
                "[Shot-XAUUSD] DISARMED ✓",
                f"reason: {args.reason}\n"
                f"xauusd master=OFF enabled=OFF on 3 prods\n"
                f"已开仓位继续由 EA BE/prune/SL 自然退出 (本脚本不强平 XAUUSD)\n"
                f"要立刻强平: VNC 各 prod 手动 close",
                priority="urgent",
            )
        else:
            # Race: our POSTs landed, but canonical was re-armed before our verify.
            _push_ntfy(
                "[Shot-XAUUSD] DISARMED — but RE-ARMED externally ⚠",
                f"reason: {args.reason}\n"
                f"script POST master=OFF enabled=OFF succeeded on 3 prods, "
                f"but canonical re-read shows master={final_master} enabled={final_enabled} — "
                f"another writer (web UI / racing POST) re-armed within ms.\n"
                f"check shot.jasperli-zhh.asia + trader intent",
                priority="urgent",
            )
        _log({"action": "xauusd_shot_close",
              "ok": True,
              "stage": "disarm" if canonical_safe else "disarm_but_rearmed",
              "result": overall})
        if not args.quiet:
            print(json.dumps(overall, ensure_ascii=False))
        return 0

    # full_ok=False → at least one step had POST error or per-prod fanout fail
    # Distinguish: (a) both steps' POSTs landed but fanout partial vs
    #              (b) one or both POSTs errored out

    enabled_post_ok = step_enabled.get("ok") is True
    master_post_ok  = step_master.get("ok")  is True
    both_posts_landed = enabled_post_ok and master_post_ok

    if both_posts_landed:
        # POSTs returned 200 but at least one prod's fanout reported FAIL
        _push_ntfy(
            "[Shot-XAUUSD] DISARM PARTIAL ⚠",
            f"reason: {args.reason}\n"
            f"shot_switch POSTs 都 200 OK but per-prod fanout 失败: {failed_prods}\n"
            f"那些 prod 文件未更新, EA 仍读老 config — VNC check ASAP\n"
            f"steps: {json.dumps(steps, ensure_ascii=False)}",
            priority="urgent",
        )
        _log({"action": "xauusd_shot_close", "ok": False, "stage": "fanout_partial",
              "result": overall})
        if not args.quiet:
            print(json.dumps(overall, ensure_ascii=False))
        return 1

    # At least one POST errored. Report which step + whether the OTHER step
    # got us partial safety (master=OFF alone is enough to drop EA to SLOW
    # poll, which blocks fast-edge fire; enabled=OFF alone removes the trigger).
    partial_safety = []
    if enabled_post_ok:
        partial_safety.append("enabled=OFF landed (fire edge removed)")
    if master_post_ok:
        partial_safety.append("master=OFF landed (EA → SLOW 30s poll)")
    safety_note = (
        "PARTIAL SAFETY: " + " + ".join(partial_safety)
        if partial_safety else
        "NO disarm landed — EA may STILL be armed"
    )
    failed_steps = [s.get("step") for s in steps if not s.get("ok")]

    _push_ntfy(
        "[Shot-XAUUSD] DISARM FAILED ⚠⚠",
        f"reason: {args.reason}\n"
        f"shot_switch POST 失败: {failed_steps}\n"
        f"{safety_note}\n"
        f"manual fallback: ssh new-va, edit /data/shot/shot_config.json directly\n"
        f"steps: {json.dumps(steps, ensure_ascii=False)}",
        priority="urgent",
    )
    _log({"action": "xauusd_shot_close", "ok": False, "stage": "disarm_failed",
          "result": overall})
    if not args.quiet:
        print(json.dumps(overall, ensure_ascii=False))
    return 2


if __name__ == "__main__":
    sys.exit(main())
