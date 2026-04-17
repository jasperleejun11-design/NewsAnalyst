#!/usr/bin/env python3
"""
News Analyst Daemon — 完全独立于 AI Trader。

每小时一次 cycle：Cursor Agent CLI（`agent`）
  agent -p "prompt" --resume <chat-id> --force --trust --workspace <NEWS_HOME>
  - 首次会话：agent create-chat → 将 chat id 写入 SESSION_FILE
  - 一次性子进程，有超时，无死锁
  - 会话上下文通过 --resume 跨 cycle 保持
  - daemon 自己控制调度
"""
import os
import shutil
import sys
import subprocess
import time
import atexit
from datetime import datetime, timezone
from pathlib import Path

NEWS_HOME = Path(r"C:\Users\12965\NewsAnalyst")
LOG_DIR = NEWS_HOME / "logs"
PID_FILE = LOG_DIR / ".daemon.pid"
# Cursor Agent 会话（与旧版 Claude Code 的 .session_id 区分）
SESSION_FILE = LOG_DIR / ".cursor_session_id"
MACRO_FILE = NEWS_HOME / ".trader" / "macro.md"
NTFY_TOPIC = "jasperli-zhh-xauusd"

CYCLE_INTERVAL = 60 * 60  # 1 hour
CYCLE_TIMEOUT = 600  # 10 min max per cycle
CREATE_CHAT_TIMEOUT = 120  # agent create-chat

# 模型：GPT Fast（可用 CURSOR_AGENT_MODEL 覆盖，例如 gpt-5.4-xhigh-fast）
CURSOR_AGENT_MODEL = os.environ.get("CURSOR_AGENT_MODEL", "gpt-5.4-medium-fast")

LOG_DIR.mkdir(parents=True, exist_ok=True)

_cursor_agent_exe_cache = None


def get_cursor_agent_exe():
    """返回 Cursor Agent 可执行文件路径（agent / agent.cmd）。环境变量 CURSOR_AGENT 优先。"""
    global _cursor_agent_exe_cache
    if _cursor_agent_exe_cache:
        return _cursor_agent_exe_cache
    exe = os.environ.get("CURSOR_AGENT")
    if exe and Path(exe).exists():
        _cursor_agent_exe_cache = exe
        return exe
    w = shutil.which("agent")
    if w:
        _cursor_agent_exe_cache = w
        return w
    raise RuntimeError(
        "未找到 Cursor Agent CLI（`agent`）。请安装 Cursor Agent 并确保 PATH 中有 agent，"
        "或设置环境变量 CURSOR_AGENT 为 agent.cmd 的完整路径。"
    )


def _popen_no_window_kwargs():
    if sys.platform == "win32":
        return {"creationflags": subprocess.CREATE_NO_WINDOW}
    return {}

FIRST_PROMPT = (
    "你是XAUUSD的新闻分析师。你的搭档——交易员——在隔壁盯盘。\n"
    "你的工作：搜索黄金相关新闻，分析后写入 .trader/macro.md。\n\n"
    "先读一下 CLAUDE.md 了解你的完整职责和输出格式。\n"
    "如果 .trader/macro.md 已存在，先读一下上一份简报。\n\n"
    "搜索策略（重要！）：\n"
    "- 用英文关键词搜索英文源：gold price, XAUUSD, Fed, geopolitical risk, Iran, oil, DXY 等\n"
    "- 重点搜英文财经网站（Reuters, Bloomberg, CNBC, FX Empire, Kitco, ZeroHedge 等）\n"
    "- 也搜中文源作为补充：黄金、地缘政治、美联储、非农 等\n"
    "- 多次搜索覆盖不同角度：地缘、央行、经济数据、市场情绪\n"
    "- 最终简报必须全部用中文撰写，英文信息翻译成中文\n\n"
    "现在开始搜索，然后写一份完整的 .trader/macro.md。\n\n"
    "每次更新完 macro.md 后，必须发一条 ntfy 推送摘要：\n"
    "  python ntfy_push.py \"[新闻分析师] 标题\" \"摘要正文\"\n"
    "推送要简短有力（3-5个要点），全部中文，让老板手机上一眼看懂当前局势。\n\n"
    "记住：你只分析不交易。给方向判断，不给具体买卖点。\n"
)

CYCLE_PROMPT = (
    "一小时到了，新一轮新闻扫描。\n"
    "1. 先读 .trader/macro.md 回忆上一轮的分析\n"
    "2. 用英文关键词搜索英文财经源（Reuters, Bloomberg, Kitco 等），再搜中文源补充\n"
    "   搜索词示例：gold price today, XAUUSD, Fed rate, Iran sanctions, oil price, DXY 等\n"
    "3. 更新 .trader/macro.md（直接覆盖，全部中文）\n"
    "4. 发 ntfy 推送摘要：python ntfy_push.py \"[新闻分析师] 标题\" \"摘要\"\n"
    "   每次都要发，不管有没有重大变化。简短3-5个要点，让老板知道你在盯着。\n"
    "   如果没有重要变化，就说'本轮无重大变化，叙事不变，继续关注XXX'\n"
)


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [新闻分析师] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_DIR / "daemon.log", "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def send_ntfy(title, body):
    import json as _json
    from urllib.request import urlopen, Request
    try:
        payload = _json.dumps({
            "topic": NTFY_TOPIC,
            "title": title,
            "message": body,
        }, ensure_ascii=False).encode("utf-8")
        req = Request(
            "https://ntfy.sh",
            data=payload, method="POST",
            headers={"Content-Type": "application/json; charset=utf-8"})
        urlopen(req, timeout=15)
    except Exception:
        pass


def acquire_pid():
    my_pid = os.getpid()
    if PID_FILE.exists():
        try:
            old_pid = int(PID_FILE.read_text().strip())
            import ctypes
            handle = ctypes.windll.kernel32.OpenProcess(0x100000, False, old_pid)
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                log(f"已有实例在运行 (PID {old_pid})")
                return False
        except Exception:
            pass
    PID_FILE.write_text(str(my_pid))
    atexit.register(lambda: PID_FILE.unlink(missing_ok=True))
    return True


def is_market_open():
    now = datetime.now(timezone.utc)
    wd = now.weekday()
    if wd == 5:
        return False
    if wd == 6 and now.hour < 22:
        return False
    return True


def create_chat_session():
    """调用 `agent create-chat`，返回服务端 chat id。"""
    exe = get_cursor_agent_exe()
    r = subprocess.run(
        [exe, "create-chat"],
        cwd=str(NEWS_HOME),
        capture_output=True,
        text=True,
        timeout=CREATE_CHAT_TIMEOUT,
        **_popen_no_window_kwargs(),
    )
    sid = (r.stdout or "").strip()
    if r.returncode != 0 or len(sid) < 32:
        err = (r.stderr or "").strip()
        raise RuntimeError(
            f"agent create-chat 失败: exit={r.returncode} stderr={err!r} stdout={sid!r}"
        )
    return sid


def get_or_create_session_id():
    if SESSION_FILE.exists():
        sid = SESSION_FILE.read_text().strip()
        if sid:
            return sid, False
    sid = create_chat_session()
    SESSION_FILE.write_text(sid)
    return sid, True


def kill_process_tree(pid):
    try:
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(pid)],
            capture_output=True, timeout=10)
    except Exception:
        pass


def run_cursor_agent(prompt, log_path, session_id):
    """非交互执行 Cursor Agent：`agent -p` + `--resume` 保持会话。"""
    exe = get_cursor_agent_exe()
    cmd = [
        exe,
        "-p",
        prompt,
        "--output-format",
        "text",
        "--workspace",
        str(NEWS_HOME),
        "--model",
        CURSOR_AGENT_MODEL,
        "--force",
        "--trust",
        "--resume",
        session_id,
    ]

    proc = subprocess.Popen(
        cmd,
        cwd=str(NEWS_HOME),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        **_popen_no_window_kwargs(),
    )

    try:
        stdout_bytes, _ = proc.communicate(timeout=CYCLE_TIMEOUT)
        output = stdout_bytes.decode("utf-8", errors="replace") if stdout_bytes else ""

        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"=== 新闻分析师 {datetime.now()} ===\n")
            f.write(f"会话: {session_id}\n")
            f.write(f"退出码: {proc.returncode}\n\n")
            f.write("--- 输出 ---\n")
            f.write(output)
            f.write(f"\n=== 完成 ===\n")

        return proc.returncode == 0 and len(output.strip()) > 50, output

    except subprocess.TimeoutExpired:
        log(f"超时 ({CYCLE_TIMEOUT}s)，杀掉进程树")
        kill_process_tree(proc.pid)
        try:
            proc.wait(timeout=10)
        except Exception:
            pass
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"=== 超时 {datetime.now()} ===\n")
        return False, ""
    except Exception as e:
        log(f"子进程错误: {e}")
        kill_process_tree(proc.pid)
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"=== 错误: {e} ===\n")
        return False, ""


def main():
    os.chdir(str(NEWS_HOME))

    if not acquire_pid():
        return

    if not is_market_open():
        log("休市，不启动")
        return

    session_id, is_new = get_or_create_session_id()
    log(f"守护进程启动（单次执行模式）")
    log(f"会话: {session_id} ({'新建' if is_new else '恢复'})")
    send_ntfy("[新闻分析师] 启动",
              f"新闻分析师上线\n每小时更新宏观简报\n会话: {session_id[:8]}...")

    consecutive_failures = 0
    is_first = is_new

    while True:
        try:
            if not is_market_open():
                log("休市，休眠1小时")
                time.sleep(3600)
                continue

            if is_first:
                prompt = FIRST_PROMPT
            else:
                prompt = CYCLE_PROMPT

            log_path = LOG_DIR / f"cycle_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            label = "首次启动" if is_first else "定时扫描"
            log(f"开始周期 ({label})，日志={log_path.name}")

            cycle_start = time.time()
            ok, output = run_cursor_agent(prompt, log_path, session_id)

            if ok:
                consecutive_failures = 0
                is_first = False
                log(f"周期完成 ({len(output)} 字符)")
            else:
                consecutive_failures += 1
                log(f"周期失败 (第{consecutive_failures}次)")
                if consecutive_failures >= 5:
                    log("连续5次失败，新建会话")
                    send_ntfy("[新闻分析师] 会话重建",
                              f"连续{consecutive_failures}次失败，新建会话")
                    try:
                        session_id = create_chat_session()
                        SESSION_FILE.write_text(session_id)
                        is_first = True
                        consecutive_failures = 0
                    except Exception as e:
                        log(f"create-chat 失败: {e}")
                        consecutive_failures = 4  # 下次再试，避免死循环卡在 >=5
                elif consecutive_failures >= 3:
                    send_ntfy("[新闻分析师] 连续失败",
                              f"连续{consecutive_failures}次周期失败，检查日志")

            elapsed = time.time() - cycle_start
            sleep_time = max(60, CYCLE_INTERVAL - elapsed)
            log(f"下次周期在 {sleep_time:.0f}s 后")
            time.sleep(sleep_time)

        except KeyboardInterrupt:
            log("用户停止")
            break
        except Exception as e:
            log(f"循环错误: {e}")
            consecutive_failures += 1
            time.sleep(60)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--status":
            if PID_FILE.exists():
                pid = PID_FILE.read_text().strip()
                print(f"PID: {pid}")
                import ctypes
                try:
                    handle = ctypes.windll.kernel32.OpenProcess(0x100000, False, int(pid))
                    if handle:
                        ctypes.windll.kernel32.CloseHandle(handle)
                        print("状态: 运行中")
                    else:
                        print("状态: 已死 (PID 过期)")
                except Exception:
                    print("状态: 未知")
            else:
                print("未运行")
            if SESSION_FILE.exists():
                print(f"会话: {SESSION_FILE.read_text().strip()}")
            logs = sorted(LOG_DIR.glob("cycle_*.log"))
            if logs:
                print(f"\n最新日志: {logs[-1]}")
                print(logs[-1].read_text(encoding="utf-8", errors="replace")[-500:])
        elif sys.argv[1] == "--stop":
            if PID_FILE.exists():
                import signal
                pid = int(PID_FILE.read_text().strip())
                try:
                    os.kill(pid, signal.SIGTERM)
                    print(f"已停止 PID {pid}")
                except Exception:
                    print(f"PID {pid} 已经不在了")
                PID_FILE.unlink(missing_ok=True)
            else:
                print("未运行")
        else:
            print("用法: python daemon.py [--status|--stop]")
    else:
        main()
