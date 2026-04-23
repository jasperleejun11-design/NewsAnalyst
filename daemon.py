#!/usr/bin/env python3
"""
News Analyst Daemon — 完全独立于 AI Trader。

每小时一次 cycle：Claude Code CLI（`claude`）
  claude -p "prompt" --resume <session-id> --permission-mode bypassPermissions
  - 首次会话：生成 UUID，用 --session-id 创建；之后用 --resume 延续
  - 一次性子进程，有超时，无死锁
  - 会话上下文通过 --resume 跨 cycle 保持
  - daemon 自己控制调度
"""
import json
import os
import shutil
import signal
import sys
import subprocess
import time
import uuid
import atexit
from datetime import datetime, timedelta, timezone
from pathlib import Path

NEWS_HOME = Path(__file__).resolve().parent
LOG_DIR = NEWS_HOME / "logs"
PID_FILE = LOG_DIR / ".daemon.pid"
SESSION_FILE = LOG_DIR / ".claude_session_id"
MACRO_FILE = NEWS_HOME / ".trader" / "macro.md"
NTFY_TOPIC = "jasperli-zhh-xauusd"
NTFY_TOKEN_FILE = NEWS_HOME / ".ntfy_token"

CYCLE_INTERVAL = 6 * 60 * 60   # 开市期间每 6 小时一次常规扫描
CYCLE_TIMEOUT = 600            # 10 min max per cycle
HEARTBEAT_INTERVAL = 24 * 3600 # 每 24h 发一条"我还活着"推送
SESSION_REBUILD_INTERVAL = 24 * 3600  # 每 24h 主动重建会话（时间维度）
SESSION_MAX_CYCLES = 4         # 每 4 次常规周期也重建（约 24h，防万一时间漂移）
SLEEP_TICK = 60                # sleep 粒度（秒），用于中途检测触发文件
TRIGGER_FILE = LOG_DIR / ".urgent_trigger"  # alert_monitor 写入，daemon 拾取
WEEKEND_SUMMARY_MARKER = LOG_DIR / ".weekend_summary_date"  # 防重发标记
WEEKEND_SUMMARY_UTC_HOUR = 16  # 周日 16:00 UTC = 开盘前 6h

# 研究员推演：每 4h 轮动（UTC 00/04/08/12/16/20 各 :10 起 90min 窗口）— 独立于常规扫描
# 每 slot 只跑一次；marker 存 "YYYY-MM-DD-HH" 避免重放。
DAILY_FORECAST_MARKER = LOG_DIR / ".forecast_slot"
FORECAST_UTC_HOURS = (0, 4, 8, 12, 16, 20)
FORECAST_UTC_MINUTE = 10
FORECAST_WINDOW_MIN = 90

# 模型分级：按任务重要性选用不同成本的模型
# 可用 CLAUDE_MODEL 环境变量整体覆盖（用于调试）
_MODEL_OVERRIDE = os.environ.get("CLAUDE_MODEL", "")
CLAUDE_MODEL_ROUTINE  = _MODEL_OVERRIDE or "haiku"   # 常规6h扫描：便宜快速
CLAUDE_MODEL_BREAKING = _MODEL_OVERRIDE or "sonnet"  # 突发事件：需要判断力
CLAUDE_MODEL_WEEKEND  = _MODEL_OVERRIDE or "haiku"   # 周末汇总：信息整理为主
CLAUDE_MODEL_FORECAST = _MODEL_OVERRIDE or "opus"    # 每日研究员推演：深度论据 + 多框架推理，用 opus

IS_WINDOWS = sys.platform == "win32"

LOG_DIR.mkdir(parents=True, exist_ok=True)


def _unlink_quiet(p):
    """py3.6 兼容：无 missing_ok。"""
    try:
        p.unlink()
    except FileNotFoundError:
        pass
    except Exception:
        pass


def atomic_write_text(path: Path, text: str):
    """写 .tmp 再 rename，避免断电/进程被杀时留下半截文件。"""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(str(tmp), str(path))


_claude_exe_cache = None


def get_claude_exe():
    """返回 Claude Code CLI 可执行文件路径。环境变量 CLAUDE_EXE 优先。

    CLAUDE_EXE 接受：
      - 绝对/相对路径（会校验 exists）
      - 裸命令名（会走 PATH 查找）
    """
    global _claude_exe_cache
    if _claude_exe_cache:
        return _claude_exe_cache
    exe = os.environ.get("CLAUDE_EXE", "").strip()
    if exe:
        if Path(exe).exists():
            _claude_exe_cache = exe
            return exe
        w = shutil.which(exe)
        if w:
            _claude_exe_cache = w
            return w
    w = shutil.which("claude")
    if w:
        _claude_exe_cache = w
        return w
    raise RuntimeError(
        "未找到 Claude Code CLI（`claude`）。请安装 Claude Code 并确保 PATH 中有 claude，"
        "或设置环境变量 CLAUDE_EXE 为 claude 的完整路径或命令名。"
    )


def _popen_extra_kwargs():
    """Windows 下隐藏控制台窗口；Linux 下新建 session 便于整树 kill。"""
    if IS_WINDOWS:
        return {"creationflags": subprocess.CREATE_NO_WINDOW}
    return {"start_new_session": True}


NTFY_SCRIPT = NEWS_HOME / "ntfy_push.py"
MACRO_PATH_STR = str(MACRO_FILE)
CLAUDE_MD_PATH_STR = str(NEWS_HOME / "CLAUDE.md")

# MT5 实时价脚本：daemon 前置钩子，每轮开始前注入现价，防止 Claude 忘记读或捏造
GET_PRICE_SCRIPT = NEWS_HOME / "get_price.py"
# mt5linux 依赖 python3.11（system python 是 3.6）；可通过环境变量覆盖
PYTHON_FOR_MT5 = os.environ.get("NEWS_ANALYST_PY311", "/usr/bin/python3.11")
PRICE_FETCH_TIMEOUT = 15  # seconds

FIRST_PROMPT = (
    "你是XAUUSD的新闻分析师。你的搭档——交易员——在隔壁盯盘。\n"
    f"你的工作：搜索黄金相关新闻，分析后写入 {MACRO_PATH_STR}。\n\n"
    f"先读一下 {CLAUDE_MD_PATH_STR} 了解你的完整职责和输出格式。\n"
    f"如果 {MACRO_PATH_STR} 已存在，先读一下上一份简报。\n\n"
    "搜索策略（重要！）：\n"
    "- 用英文关键词搜索英文源：gold price, XAUUSD, Fed, geopolitical risk, Iran, oil, DXY 等\n"
    "- 重点搜英文财经网站（Reuters, Bloomberg, CNBC, FX Empire, Kitco, ZeroHedge 等）\n"
    "- 也搜中文源作为补充：黄金、地缘政治、美联储、非农 等\n"
    "- 多次搜索覆盖不同角度：地缘、央行、经济数据、市场情绪\n"
    "- 最终简报必须全部用中文撰写，英文信息翻译成中文\n\n"
    f"现在开始搜索，然后写一份完整的 {MACRO_PATH_STR}。\n\n"
    "每次更新完 macro.md 后，必须发一条 ntfy 推送摘要（用绝对路径调用，避免 cwd 干扰）：\n"
    f"  python {NTFY_SCRIPT} \"[新闻分析师] 标题\" \"摘要正文\"\n"
    "推送要简短有力（3-5个要点），全部中文，让老板手机上一眼看懂当前局势。\n\n"
    "记住：你只分析不交易。给方向判断，不给具体买卖点。\n"
)

CYCLE_PROMPT = (
    "6h 例行新闻扫描。\n\n"
    "【核心目标】告诉 AI Trader 过去 6 小时外面发生了什么。不写长篇研报，不给具体价位。\n\n"
    f"1. 读 {MACRO_PATH_STR} 回忆上轮叙事\n"
    f"2. **复盘上轮【对交易的启示】**：用 WebSearch 验证上轮的方向判断/预测是否已兑现/落空。\n"
    f"   若有清晰对错结论（无论对错），追加一条到 .trader/insights.md，格式：\n"
    "      ## [YYYY-MM-DD HH:MM UTC] 预测事件名\n"
    "      - 预测: XX（N 轮前的判断）\n"
    "      - 实际: YY（真实发生了什么）\n"
    "      - 误差原因: ZZ\n"
    "      - 下次修正: AA\n"
    "   若上轮无清晰可复盘的预测，跳过此步（但要在 log 里说明 '无可复盘'）。\n"
    "3. 英文源搜：gold price today, XAUUSD, Fed rate, Iran, oil price, DXY；中文源补\n"
    f"4. 更新 {MACRO_PATH_STR}（覆盖，全中文，≤80 行）\n"
    "5. 决定推送档位：\n"
    "   · 有**实质性新事件/数据/叙事变化** → 发📊常规推送（3-5 要点，人话）\n"
    "   · **无实质变化** → 发✅小推送（≤2 行，例如：✅ 本轮无实质变化｜现价 $X,XXX｜叙事延续：XX；关注下一事件：YY）\n"
    "     不要每次都写长篇推送（噪音污染 Trader 通道）\n"
    f"   推送命令：python {NTFY_SCRIPT} \"标题\" \"正文\"\n"
    "\n"
    "【硬禁令】禁止写具体支撑/阻力/进场价（'$4,680支撑''$4,860阻力''$4,750中期买点'都违规，daemon 会扫描 lint）。\n"
)

# 突发事件专用 prompt（由 alert_monitor 触发）
# {headline}, {source}, {keywords} 在运行时替换
BREAKING_PROMPT_TEMPLATE = (
    "🚨 突发事件警报！RSS监控器检测到高优先级事件，立即处理。\n\n"
    "触发事件：{headline}\n"
    "来源：{source}\n"
    "命中关键词：{keywords}\n"
    "触发时间：{trigger_time}\n\n"
    f"立即行动（专注于此事件，不做完整小时扫描）：\n"
    f"1. 读 {MACRO_PATH_STR} 了解当前叙事背景\n"
    "2. 用英文搜索此事件的最新详情（至少2-3次搜索确认）\n"
    "   评估：是否属实？官方确认还是传言？\n"
    "3. 分析对金价的即时影响：\n"
    "   - 方向（利多/利空/中性）\n"
    "   - 量级（小波动<$20 / 中波动$20-50 / 大波动>$50）\n"
    "   - 持续性（脉冲式 / 持续数小时 / 改变叙事）\n"
    "   - 来源可信度（Reuters/Bloomberg确认 vs 社交媒体传言）\n"
    f"4. 在 {MACRO_PATH_STR} 顶部插入【🚨 突发事件】区块，然后更新核心简报\n"
    f"5. 发紧急 ntfy 推送：python {NTFY_SCRIPT} \"🚨 [突发] 标题\" \"影响评估\"\n"
    "   推送格式：事件→影响→建议（如：暂避窗口 / 等确认再动）\n\n"
    "重要：如果搜索发现是误报或低影响，也要发推送说明（让老板放心）。\n"
)

# 周日 16:00 UTC 开盘前 6h 周末汇总 prompt
WEEKEND_SUMMARY_PROMPT = (
    "🌅 黄金市场今晚22:00 UTC开盘，距开盘还有6小时。\n"
    "你的任务：搜索整个周末（周五收盘至今）发生的事件，写一份【开盘前周末汇总】。\n\n"
    f"1. 先读 {MACRO_PATH_STR} 了解周五收盘时的叙事背景\n"
    "2. 搜索周末新闻（重点：周六、周日发生了什么）\n"
    "   关键词：gold weekend, geopolitical weekend, Fed officials, Iran, oil market weekend\n"
    "   也搜中文源：黄金 周末, 地缘 周末\n"
    "3. 评估：哪些事件会在今晚开盘时冲击金价？\n"
    "   - 周末地缘变化（停火/升级/谈判）\n"
    "   - 官员讲话（Fed官员周末表态）\n"
    "   - 亚洲/中东市场早盘情绪\n"
    "   - 原油、美元周末走势\n"
    f"4. 更新 {MACRO_PATH_STR}，在【一行结论】顶部加【开盘预警】标注\n"
    f"5. 发 ntfy 推送：python {NTFY_SCRIPT} \"🌅 [开盘预警] 周末汇总\" \"摘要\"\n"
    "   推送格式：\n"
    "   今晚22:00开盘 | 方向: 多/空/中性\n"
    "   ▲/▼ 周末最重要事件1\n"
    "   ▲/▼ 周末最重要事件2\n"
    "   ⚠ 开盘哑区: 开盘后5分钟禁追价\n"
)


# 研究员推演 prompt（每 4h 轮动触发：UTC 00/04/08/12/16/20 各 :10，独立于6h扫描/突发/周末汇总）
FORECAST_FILE = NEWS_HOME / ".trader" / "forecast.md"
FORECAST_PATH_STR = str(FORECAST_FILE)
METHODOLOGY_PATH_STR = str(NEWS_HOME / "docs" / "methodology.md")

DAILY_FORECAST_PROMPT = (
    "🔬 黄金市场研究员推演（每 4h 轮动：UTC 00/04/08/12/16/20 各 :10 触发）。\n\n"
    "【核心目标】告诉 AI Trader 盯盘期间可能遇到什么——只关心 24-72h 可操作视野。\n"
    "不是写基金经理看的年度展望。2027 目标价 Trader 盯盘用不上。\n\n"
    f"【必做步骤 · 精简版 · 严格遵循 {METHODOLOGY_PATH_STR} 的 10 步流程】\n"
    f"1. 读 {METHODOLOGY_PATH_STR} 对齐方法论\n"
    f"2. 读 {MACRO_PATH_STR} 回忆当前叙事；读 {FORECAST_PATH_STR} 看上次预测是否已落地\n"
    "3. 搜集 5 类研究员级信息（优先英文一手研报）：\n"
    "   ① 机构目标价最新（Goldman/JPM/UBS/HSBC/Citi/BofA）\n"
    "   ② 技术面（Fib/50日EMA/200日MA）— 仅作为客观参考锚，不写具体进场价\n"
    "   ③ 跨资产（DXY/10Y/30Y/TIPS/VIX）\n"
    "   ④ 物理需求（中国 SGE 溢价/印度节日/央行 WGC）\n"
    "   ⑤ 仓位层（COT managed money/GLD flows）\n"
    "4. 产出**两时间尺度**推演（砍掉 6 月+ 长期）：\n"
    "   · 短期 24-72h：最可操作视野\n"
    "   · 中期 1-4 周：中期驱动 + FOMC 催化\n"
    "   每情景必写：概率 + 触发条件 + 方向强度 + 数据论据\n"
    "5. 产出 **≥1 个独创框架**（不借用第三方）+ **反事实**（看多/看空各一段）\n"
    f"6. 覆盖写 {FORECAST_PATH_STR}（≤ 150 行；超出删旧留新——研究员产物也要 Trader 能读完）\n"
    f"7. 若推演结论有实质变化，同步更新 {MACRO_PATH_STR} 的【一行结论】和【关键倒计时】\n"
    f"8. 发推送（🔬前缀 · 老板手机版）：python {NTFY_SCRIPT} \"🔬[每日推演] 标题\" \"摘要\"\n"
    "\n"
    "【推送格式硬模板】\n"
    "```\n"
    "📍现价 $X,XXX (MT5实时)\n"
    "🎯 核心论点: XXX（一句话）\n"
    "📊 论据1-3: 具体数字+来源\n"
    "⚡ 24-72h: 方向+触发\n"
    "⚠ 哑区/风险\n"
    "```\n\n"
    "【硬禁令 · 四条】\n"
    "- ❌ 禁止捏造价位：所有 XAUUSD 现价用 daemon 前置钩子注入的 MT5 价；历史价带日期+来源；机构目标带机构名\n"
    "- ❌ 禁止写具体支撑/阻力/进场位（'$4,680支撑''$4,860阻力''$4,750-4,780中期买点'均违规）——那是 AI Trader 的技术分析领域\n"
    "- ❌ 禁止'无论据论点'：每条推送论点必须有数字+来源；没来源的判断不放进推送\n"
    "- ❌ 禁止把投行术语搬 push：'regime 切换/term premium/定价权' 留在 forecast.md；push 只说人话\n"
)


DAEMON_LOG_MAX_BYTES = 5 * 1024 * 1024  # 5MB


def _maybe_rotate_daemon_log():
    """daemon.log 超过 5MB 就轮转到 .1（覆盖上一份 .1）。"""
    p = LOG_DIR / "daemon.log"
    try:
        if p.exists() and p.stat().st_size > DAEMON_LOG_MAX_BYTES:
            os.replace(str(p), str(p.with_suffix(".log.1")))
    except Exception:
        pass


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [新闻分析师] {msg}"
    print(line, flush=True)
    try:
        _maybe_rotate_daemon_log()
        with open(LOG_DIR / "daemon.log", "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def _read_ntfy_token():
    tok = os.environ.get("NTFY_TOKEN", "").strip()
    if tok:
        return tok
    try:
        if NTFY_TOKEN_FILE.exists():
            return NTFY_TOKEN_FILE.read_text(encoding="utf-8").strip()
    except Exception:
        pass
    return ""


def fetch_price_preamble() -> str:
    """每轮开始前调用 get_price.py 获取 MT5 实时 XAUUSD，返回要拼到 prompt 顶部的一段。

    成功：返回带现价+时间戳+指引的块。
    失败：返回带错误原因+指引（让 Claude 在 macro.md 注明无现价锚，不得用新闻价替代）的块。
    """
    preamble_header = "【daemon 前置钩子 · 当前金价（MT5 实时）】"
    guidance_ok = (
        "上述 XAUUSD 现价由 daemon 调用 MT5 bridge 取得，**必须**直接引用到 macro.md 的"
        "'现价'字段，不得从新闻文字里摘价替代，不得自行捏造支撑/阻力位。"
    )
    guidance_fail = (
        "**价格源不可用**。按纪律：在 macro.md 顶部明确写'价格源不可用，本轮无现价锚'，"
        "不得以新闻文字中的价格作为替代。方向分析照常进行。"
    )
    try:
        result = subprocess.run(
            [PYTHON_FOR_MT5, str(GET_PRICE_SCRIPT)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=PRICE_FETCH_TIMEOUT,
        )
        stdout_txt = (result.stdout or b"").decode("utf-8", errors="replace").strip()
        stderr_txt = (result.stderr or b"").decode("utf-8", errors="replace").strip()
        if result.returncode == 0 and stdout_txt:
            return f"{preamble_header}\n{stdout_txt}\n\n{guidance_ok}\n"
        err_msg = (stderr_txt or stdout_txt or "unknown")[:300]
        return f"{preamble_header}\n⚠️ 调用失败（rc={result.returncode}）: {err_msg}\n\n{guidance_fail}\n"
    except subprocess.TimeoutExpired:
        return f"{preamble_header}\n⚠️ 调用超时（>{PRICE_FETCH_TIMEOUT}s）\n\n{guidance_fail}\n"
    except Exception as e:
        return f"{preamble_header}\n⚠️ 异常: {e}\n\n{guidance_fail}\n"


# ═══════════════════════════════════════════════════════════════════════════
# P0-3 · macro.md 价位违规 lint
# ═══════════════════════════════════════════════════════════════════════════
import re as _re

# 违规 pattern：分析师越界写具体支撑/阻力/进场价
# 允许：MT5 实时价行（含"现价""mid""bid""ask"）/历史事件价（含日期+来源括号）/机构目标（含机构名）
_PRICE_VIOLATION_PATTERNS = [
    # "$4,750 支撑/阻力/进场/入场/做多/做空"
    _re.compile(r"\$\s?[3-6],?\d{3}\s*(支撑|阻力|进场|入场|做多|做空|买点|卖点|止损|止盈)"),
    # "支撑在 $4,750"
    _re.compile(r"(支撑|阻力|入场|进场|买点|卖点|止损|止盈)\s*(位|在|区|看)?\s*\$\s?[3-6],?\d{3}"),
    # 范围："$4,680-$4,720 中期买点/区间多/空" 等（同行宽松匹配）
    _re.compile(r"\$\s?[3-6],?\d{3}\s*[-~至到–]+\s*\$?\s?[3-6],?\d{3}.{0,30}(中期买点|区间多|区间空|做多|做空|入场|买入|买点|卖点)"),
    # "回踩 $4,850 加多"
    _re.compile(r"(回踩|跌至|冲至)\s*\$\s?[3-6],?\d{3}\s*(加多|加空|加仓|入场|买入|进场)"),
]

# 允许出现 $X,XXX 的上下文（命中这些不算违规）—— 只在**整行**匹配时算白名单
_WHITELIST_LINE_MARKERS = [
    "现价", "mid", "bid", "ask", "spread", "MT5 实时",
    "Goldman", "JPM", "UBS", "HSBC", "Citi", "BofA", "Kitco", "UBP",  # 机构目标
    "高盛", "摩根", "富国", "瑞银",
    "Bloomberg", "Reuters", "CNN", "WGC", "CBO",  # 新闻/官方带源
    "历史", "3/3", "2025", "2024",  # 历史价参考
    "100% Fib", "200日MA", "50日EMA", "Fib",  # 客观技术锚点
]


def lint_macro_for_price_violations(text: str) -> list:
    """返回违规行列表（每条：(行号, 行内容, 匹配的 pattern 索引)）。空列表=通过。"""
    violations = []
    for i, line in enumerate(text.splitlines(), 1):
        # 白名单检查
        if any(marker in line for marker in _WHITELIST_LINE_MARKERS):
            continue
        # 扫 pattern
        for idx, pat in enumerate(_PRICE_VIOLATION_PATTERNS):
            if pat.search(line):
                violations.append((i, line.strip()[:120], idx))
                break
    return violations


def post_cycle_macro_lint():
    """cycle 完成后读 macro.md 检查；发现违规则写 log + 发小推送告警（不阻塞）。"""
    try:
        if not MACRO_FILE.exists():
            return
        text = MACRO_FILE.read_text(encoding="utf-8", errors="replace")
        violations = lint_macro_for_price_violations(text)
        if violations:
            log(f"⚠️ macro.md lint 发现 {len(violations)} 处价位违规：")
            for lineno, content, idx in violations[:5]:
                log(f"    L{lineno} pat#{idx}: {content}")
            # 悄悄告警（让用户知道 daemon 越界了），但不推送到 Trader（避免污染 Trader 通道）
            send_ntfy(
                "⚠️[lint] macro.md 发现价位违规",
                f"本轮 daemon 写入了 {len(violations)} 处具体支撑/阻力/入场价（分析师越界）。\n"
                f"示例：L{violations[0][0]}: {violations[0][1]}\n"
                f"请检查 CLAUDE.md 禁令是否生效（可能 session 未重建）。",
            )
    except Exception as e:
        log(f"lint 异常: {e}")


def post_cycle_news_signal():
    """P0: After each successful macro.md update, write machine-readable signal CSV."""
    try:
        from news_signal_writer import write_news_signal
        result = write_news_signal()
        if result:
            d, s, m, c = result
            log(f"[news_signal] dir={d:+d} str={s:.2f} mute={m} conf={c:.2f} → news_signal.csv")
        else:
            log("[news_signal] 跳过（macro.md 空或不存在）")
    except Exception as e:
        log(f"[news_signal] 写入失败（非阻塞）: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# P0-4 · 哑区事件日历 + T-30min 独立推送
# ═══════════════════════════════════════════════════════════════════════════
# 硬编码本周关键经济事件（UTC 时间）——需要每周手动维护，或未来接入 economic calendar API
# 格式: {"datetime_utc": "YYYY-MM-DD HH:MM", "name": "...", "lead_min": int, "type": "data|fomc|geopol"}
DUMB_ZONE_EVENTS = [
    {"dt": "2026-04-22 00:00", "name": "美伊停火到期", "lead_min": 30, "type": "geopol"},
    {"dt": "2026-04-22 13:45", "name": "美国 4 月 PMI（制造业+服务业）", "lead_min": 30, "type": "data"},
    {"dt": "2026-04-24 12:30", "name": "美国初请失业金", "lead_min": 30, "type": "data"},
    {"dt": "2026-04-25 14:00", "name": "密歇根大学通胀预期终值", "lead_min": 30, "type": "data"},
    {"dt": "2026-04-29 18:00", "name": "FOMC 会议决议 Day1", "lead_min": 120, "type": "fomc"},
    {"dt": "2026-04-30 18:00", "name": "FOMC 会议决议 Day2（鲍威尔最后一次）", "lead_min": 120, "type": "fomc"},
]
DUMB_ZONE_MARKER = LOG_DIR / ".dumb_zone_sent"  # 防同一事件多次推送


def _load_sent_dumb_zones() -> set:
    try:
        if DUMB_ZONE_MARKER.exists():
            return set(json.loads(DUMB_ZONE_MARKER.read_text(encoding="utf-8")))
    except Exception:
        pass
    return set()


def _save_sent_dumb_zones(sent: set):
    try:
        tmp = DUMB_ZONE_MARKER.with_suffix(".tmp")
        tmp.write_text(json.dumps(sorted(sent), ensure_ascii=False), encoding="utf-8")
        os.replace(str(tmp), str(DUMB_ZONE_MARKER))
    except Exception:
        pass


def check_dumb_zone_triggers():
    """检查本周事件表，若有事件进入 T-lead_min 窗口且今日未推送过，发 ⚠️ 哑区推送。
    由 main loop 每 SLEEP_TICK 调用一次（60s 粒度足够）。"""
    now = datetime.now(timezone.utc)
    sent = _load_sent_dumb_zones()

    for ev in DUMB_ZONE_EVENTS:
        try:
            ev_dt = datetime.strptime(ev["dt"], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        except Exception:
            continue

        mins_to = (ev_dt - now).total_seconds() / 60
        # 窗口：事件前 lead_min ± 5 分钟内触发（避免边界抖动错过）
        if not (ev["lead_min"] - 5 <= mins_to <= ev["lead_min"] + 5):
            continue

        ev_key = f"{ev['dt']}|{ev['name']}"
        if ev_key in sent:
            continue

        # 发哑区推送（独立于 daemon cycle，不跑 Claude）
        title = f"⚠️[哑区] {ev['name']} · T-{int(mins_to)}min"
        body = (
            f"⚠ 哑区预警\n"
            f"事件: {ev['name']}\n"
            f"UTC: {ev['dt']} (约 {int(mins_to)} 分钟后)\n"
            f"类型: {ev['type']}\n"
            f"纪律: "
            + {
                "fomc": "FOMC 前 2h 禁入场 · 数据后 5 分钟禁追价",
                "data": "数据前 30min 禁入场 · 发布后 5 分钟禁追价",
                "geopol": "重大地缘事件前后双向 spike 风险极高 · 禁新仓",
            }.get(ev["type"], "波动率放大，轻仓观望")
        )
        send_ntfy(title, body)
        log(f"🔔 哑区推送已发: {ev['name']} T-{int(mins_to)}min")

        sent.add(ev_key)
        _save_sent_dumb_zones(sent)


def send_ntfy(title, body):
    import json as _json
    from urllib.request import urlopen, Request
    try:
        payload = _json.dumps({
            "topic": NTFY_TOPIC,
            "title": title,
            "message": body,
        }, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json; charset=utf-8"}
        tok = _read_ntfy_token()
        if tok:
            headers["Authorization"] = f"Bearer {tok}"
        req = Request(
            "https://ntfy.sh",
            data=payload, method="POST",
            headers=headers)
        urlopen(req, timeout=15)
    except Exception:
        pass


def pid_alive(pid):
    """跨平台检测 pid 是否还活着。"""
    if pid <= 0:
        return False
    if IS_WINDOWS:
        try:
            import ctypes
            handle = ctypes.windll.kernel32.OpenProcess(0x100000, False, pid)
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
        except Exception:
            pass
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # 进程存在但我们没权限 signal——仍然算活着
        return True
    except Exception:
        return False


def acquire_pid():
    my_pid = os.getpid()
    if PID_FILE.exists():
        try:
            old_pid = int(PID_FILE.read_text().strip())
            if pid_alive(old_pid):
                log(f"已有实例在运行 (PID {old_pid})")
                return False
        except Exception:
            pass
    atomic_write_text(PID_FILE, str(my_pid))
    atexit.register(lambda: _unlink_quiet(PID_FILE))
    if not IS_WINDOWS:
        def _graceful_exit(signum, frame):
            log(f"收到信号 {signum}，退出")
            sys.exit(0)
        signal.signal(signal.SIGTERM, _graceful_exit)
        signal.signal(signal.SIGINT, _graceful_exit)
    return True


def cleanup_old_logs(keep=168):
    """保留最近 keep 份 cycle 日志（默认 168 ≈ 7 天 * 24 轮）。"""
    try:
        logs = sorted(LOG_DIR.glob("cycle_*.log"))
        for old in logs[:-keep]:
            _unlink_quiet(old)
    except Exception:
        pass


def is_market_open():
    """黄金现货交易窗口：周日 22:00 UTC → 周五 22:00 UTC。"""
    now = datetime.now(timezone.utc)
    wd = now.weekday()  # 0=Mon … 4=Fri, 5=Sat, 6=Sun
    if wd == 5:
        return False
    if wd == 4 and now.hour >= 22:
        return False
    if wd == 6 and now.hour < 22:
        return False
    return True


def seconds_until_open():
    """从现在到下一个开盘时刻（Sun 22:00 UTC）的秒数。已开盘返回 0。"""
    now = datetime.now(timezone.utc)
    wd = now.weekday()
    if is_market_open():
        return 0
    base = now.replace(hour=22, minute=0, second=0, microsecond=0)
    if wd == 4:  # Fri 22+ → Sun 22
        target = base + timedelta(days=2)
    elif wd == 5:  # Sat → Sun 22
        target = base + timedelta(days=1)
    else:  # Sun <22 → today 22
        target = base
    return max(0, int((target - now).total_seconds()))


def macro_mtime():
    """返回 .trader/macro.md 的修改时间；不存在返回 0。"""
    try:
        return MACRO_FILE.stat().st_mtime
    except FileNotFoundError:
        return 0.0
    except Exception:
        return 0.0


def new_session_id():
    """生成新 UUID 作为 Claude 会话 id 并原子写入 SESSION_FILE。"""
    sid = str(uuid.uuid4())
    atomic_write_text(SESSION_FILE, sid)
    return sid


def get_or_create_session_id():
    if SESSION_FILE.exists():
        sid = SESSION_FILE.read_text().strip()
        if sid:
            return sid, False
    return new_session_id(), True


def kill_process_tree(proc):
    """尽力 kill 整个进程树。"""
    try:
        if IS_WINDOWS:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                capture_output=True, timeout=10)
        else:
            try:
                pgid = os.getpgid(proc.pid)
                os.killpg(pgid, signal.SIGTERM)
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    except Exception:
        pass


def run_claude(prompt, log_path, session_id, is_first, model=None):
    """非交互执行 Claude Code：`claude -p` + `--session-id`(首次) 或 `--resume`(续会)。"""
    exe = get_claude_exe()
    cmd = [
        exe,
        "-p",
        prompt,
        "--output-format", "text",
        "--model", model or CLAUDE_MODEL_ROUTINE,
        "--permission-mode", "bypassPermissions",
    ]
    if is_first:
        cmd += ["--session-id", session_id]
    else:
        cmd += ["--resume", session_id]

    proc = subprocess.Popen(
        cmd,
        cwd=str(NEWS_HOME),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        **_popen_extra_kwargs(),
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
        kill_process_tree(proc)
        try:
            proc.wait(timeout=10)
        except Exception:
            pass
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"=== 超时 {datetime.now()} ===\n")
        return False, ""
    except Exception as e:
        log(f"子进程错误: {e}")
        kill_process_tree(proc)
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"=== 错误: {e} ===\n")
        return False, ""


def should_send_weekend_summary() -> bool:
    """周日 16:00-17:59 UTC，且今天还没发过。
    给 2h 窗口，避免 cycle 执行占用导致精确整点被错过。
    """
    now = datetime.now(timezone.utc)
    if now.weekday() != 6:
        return False
    if not (WEEKEND_SUMMARY_UTC_HOUR <= now.hour < WEEKEND_SUMMARY_UTC_HOUR + 2):
        return False
    try:
        if WEEKEND_SUMMARY_MARKER.exists():
            return WEEKEND_SUMMARY_MARKER.read_text().strip() != now.strftime("%Y-%m-%d")
    except Exception:
        pass
    return True


def mark_weekend_summary_sent():
    atomic_write_text(WEEKEND_SUMMARY_MARKER, datetime.now(timezone.utc).strftime("%Y-%m-%d"))


def current_forecast_slot() -> int | None:
    """当前时刻是否落在某个 forecast slot 的 :10-:10+WINDOW 窗口内？返回 slot hour 或 None。"""
    now = datetime.now(timezone.utc)
    for h in FORECAST_UTC_HOURS:
        slot_start = now.replace(hour=h, minute=FORECAST_UTC_MINUTE, second=0, microsecond=0)
        slot_end = slot_start + timedelta(minutes=FORECAST_WINDOW_MIN)
        if slot_start <= now < slot_end:
            return h
    return None


def _slot_label(slot_hour: int) -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d") + f"-{slot_hour:02d}"


def should_send_daily_forecast() -> bool:
    """当前 slot 是否该触发？slot 存在且 marker 里记录的不是当前 slot → 触发。

    触发条件不依赖 is_market_open()——研究员推演 7×24 都做。
    """
    slot = current_forecast_slot()
    if slot is None:
        return False
    label = _slot_label(slot)
    try:
        if DAILY_FORECAST_MARKER.exists():
            return DAILY_FORECAST_MARKER.read_text().strip() != label
    except Exception:
        pass
    return True


def mark_daily_forecast_sent():
    slot = current_forecast_slot()
    if slot is None:  # defensive: only called after should_send_daily_forecast()==True
        return
    atomic_write_text(DAILY_FORECAST_MARKER, _slot_label(slot))


def read_and_clear_trigger() -> dict:
    """读取并删除触发文件，返回触发信息 dict；不存在返回 {}。"""
    if not TRIGGER_FILE.exists():
        return {}
    try:
        data = json.loads(TRIGGER_FILE.read_text(encoding="utf-8"))
        _unlink_quiet(TRIGGER_FILE)
        return data
    except Exception:
        _unlink_quiet(TRIGGER_FILE)
        return {}


def build_breaking_prompt(trigger: dict) -> str:
    return BREAKING_PROMPT_TEMPLATE.format(
        headline=trigger.get("headline", "未知"),
        source=trigger.get("source", "未知"),
        keywords=", ".join(trigger.get("keywords", [])),
        trigger_time=trigger.get("time", "未知"),
    )


def interruptible_sleep(seconds: float) -> None:
    """分 SLEEP_TICK 粒度 sleep，期间检测触发文件。
    发现触发即提前返回（不消费文件），由主循环 read_and_clear_trigger 统一处理。"""
    deadline = time.time() + seconds
    while time.time() < deadline:
        tick = min(SLEEP_TICK, deadline - time.time())
        if tick <= 0:
            break
        time.sleep(tick)
        if TRIGGER_FILE.exists():
            log("⚡ sleep 期间检测到触发文件，提前返回主循环")
            return


def run_once():
    """单轮调试入口：跑一轮 cycle 就返回，不抢 PID、不进 loop。"""
    os.chdir(str(NEWS_HOME))
    session_id, is_new = get_or_create_session_id()
    log(f"[--once] 会话: {session_id} ({'新建' if is_new else '恢复'})")
    log_path = LOG_DIR / f"cycle_{datetime.now().strftime('%Y%m%d_%H%M%S')}_once.log"
    prompt = FIRST_PROMPT if is_new else CYCLE_PROMPT
    price_preamble = fetch_price_preamble()
    log(f"[--once] 前置钩子价格: {price_preamble.splitlines()[1] if len(price_preamble.splitlines()) > 1 else price_preamble[:120]}")
    wrapped_prompt = f"{price_preamble}\n---\n\n{prompt}"
    mtime_before = macro_mtime()
    ok, output = run_claude(wrapped_prompt, log_path, session_id, is_new)
    mtime_after = macro_mtime()
    touched = mtime_after != mtime_before
    log(f"[--once] 结束：ok={ok} 输出={len(output)}字符 macro更新={touched}")
    log(f"[--once] 日志: {log_path}")
    return 0 if (ok and touched) else 1


def _rebuild_session(session_id, reason=""):
    """新建会话并返回新 session_id。"""
    sid = new_session_id()
    log(f"会话重建：{reason or '未知原因'} → {sid[:8]}...")
    return sid


def _run_cycle(prompt, log_suffix, session_id, is_first_flag, cycle_count,
               consecutive_failures, last_success, last_session_rebuild,
               last_heartbeat, last_regular_cycle, is_breaking=False, model=None):
    """执行一轮 Claude 周期，返回更新后的状态变量 dict。"""
    label_map = {
        "breaking": f"🚨 突发响应",
        "first": "首次启动",
        "cycle": "定时扫描(6h)",
        "weekend": "🌅 周末汇总",
        "daily_forecast": "🔬 每日研究员推演",
    }
    label = label_map.get(log_suffix, log_suffix)
    log_path = LOG_DIR / f"cycle_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{log_suffix}.log"
    log(f"开始周期 ({label})，日志={log_path.name}")

    cycle_start = time.time()
    cleanup_old_logs()
    mtime_before = macro_mtime()

    # 前置钩子：注入 MT5 实时金价，Claude 不需要"记得"自己读
    price_preamble = fetch_price_preamble()
    log(f"前置钩子价格: {price_preamble.splitlines()[1] if len(price_preamble.splitlines()) > 1 else price_preamble[:120]}")
    wrapped_prompt = f"{price_preamble}\n---\n\n{prompt}"

    ok, output = run_claude(wrapped_prompt, log_path, session_id, is_first_flag, model=model)
    mtime_after = macro_mtime()

    if ok and mtime_after == mtime_before:
        log("警告：macro.md 未更新（mtime 未变），视为失败")
        ok = False

    cycle_count += 1
    now_ts = time.time()

    if ok:
        consecutive_failures = 0
        is_first_flag = False
        last_success = now_ts
        if not is_breaking:
            last_regular_cycle = now_ts
        log(f"周期完成 ({len(output)} 字符)")
        # P0-3 · 成功写完 macro 后扫价位违规（不阻塞，仅告警）
        post_cycle_macro_lint()
        # Phase 0 · 写机器可读信号供 MT5 EA 消费
        post_cycle_news_signal()
    else:
        consecutive_failures += 1
        log(f"周期失败 (第{consecutive_failures}次)")

    if now_ts - last_heartbeat >= HEARTBEAT_INTERVAL:
        age_h = (now_ts - last_success) / 3600
        send_ntfy(
            "[新闻分析师] 心跳",
            f"daemon存活\n已跑 {cycle_count} 轮\n距上次成功 {age_h:.1f}h",
        )
        last_heartbeat = now_ts

    elapsed = time.time() - cycle_start
    return dict(
        ok=ok,
        output=output,
        is_first=is_first_flag,
        cycle_count=cycle_count,
        consecutive_failures=consecutive_failures,
        last_success=last_success,
        last_session_rebuild=last_session_rebuild,
        last_heartbeat=last_heartbeat,
        last_regular_cycle=last_regular_cycle,
        elapsed=elapsed,
    )


def main():
    os.chdir(str(NEWS_HOME))

    if not acquire_pid():
        return

    session_id, is_new = get_or_create_session_id()
    log("守护进程启动")
    log(f"会话: {session_id} ({'新建' if is_new else '恢复'})")
    send_ntfy("[新闻分析师] 启动",
              f"新闻分析师上线\n每6h常规扫描 + 突发即时响应\n会话: {session_id[:8]}...")

    consecutive_failures = 0
    is_first = is_new
    last_heartbeat = time.time()
    last_success = time.time()
    last_session_rebuild = time.time()
    last_regular_cycle = 0.0   # 上次常规周期时间戳；0 = 从未跑过，启动时立即跑
    cycle_count = 0
    session_cycle_count = 0    # 当前会话内跑了多少次常规周期（达到上限则重建）

    while True:
        try:
            now_ts = time.time()

            # ── 优先级0：哑区事件日历检查（每次循环都查，60s 粒度）────────
            check_dumb_zone_triggers()

            # ── 优先级1：突发触发（任何时间，包括周末）─────────────────────
            trigger = read_and_clear_trigger()
            if trigger:
                log(f"⚡ 突发触发：{trigger.get('headline','')[:60]}")
                # 突发用 sonnet，但不计入 session_cycle_count（突发不影响重建节奏）
                state = _run_cycle(
                    build_breaking_prompt(trigger), "breaking",
                    session_id, is_first, cycle_count,
                    consecutive_failures, last_success, last_session_rebuild,
                    last_heartbeat, last_regular_cycle, is_breaking=True,
                    model=CLAUDE_MODEL_BREAKING,
                )
                # 更新状态
                is_first = state["is_first"]
                cycle_count = state["cycle_count"]
                consecutive_failures = state["consecutive_failures"]
                last_success = state["last_success"]
                last_heartbeat = state["last_heartbeat"]
                last_regular_cycle = state["last_regular_cycle"]

                if not state["ok"]:
                    consecutive_failures = state["consecutive_failures"]
                    if consecutive_failures >= 5:
                        session_id = _rebuild_session(session_id, f"连续{consecutive_failures}次失败")
                        send_ntfy("[新闻分析师] 会话重建", f"连续失败{consecutive_failures}次，新建会话")
                        is_first = True; consecutive_failures = 0; last_session_rebuild = time.time()
                    elif consecutive_failures >= 3:
                        send_ntfy("[新闻分析师] 连续失败", f"连续{consecutive_failures}次失败，检查日志")

                # 突发后等 10 分钟，继续轮询（期间可被新突发打断）
                post = max(60, 600 - state["elapsed"])
                log(f"突发处理完，{post:.0f}s 后恢复")
                interruptible_sleep(post)
                continue

            # ── 优先级2：研究员推演（UTC 00/04/08/12/16/20 :10 每 4h 轮动）─────
            if should_send_daily_forecast():
                _slot = current_forecast_slot()
                log(f"触发黄金研究员推演（slot {_slot:02d}:10 UTC / Opus）")
                state = _run_cycle(
                    DAILY_FORECAST_PROMPT, "daily_forecast",
                    session_id, is_first, cycle_count,
                    consecutive_failures, last_success, last_session_rebuild,
                    last_heartbeat, last_regular_cycle,
                    model=CLAUDE_MODEL_FORECAST,
                )
                mark_daily_forecast_sent()
                is_first = state["is_first"]
                cycle_count = state["cycle_count"]
                consecutive_failures = state["consecutive_failures"]
                last_success = state["last_success"]
                last_heartbeat = state["last_heartbeat"]
                last_regular_cycle = state["last_regular_cycle"]
                interruptible_sleep(SLEEP_TICK)
                continue

            # ── 优先级3：周日 16:00 UTC 开盘前周末汇总────────────────────
            if should_send_weekend_summary():
                log("触发周末汇总（周日16:00 UTC）")
                state = _run_cycle(
                    WEEKEND_SUMMARY_PROMPT, "weekend",
                    session_id, is_first, cycle_count,
                    consecutive_failures, last_success, last_session_rebuild,
                    last_heartbeat, last_regular_cycle,
                    model=CLAUDE_MODEL_WEEKEND,
                )
                mark_weekend_summary_sent()
                is_first = state["is_first"]
                cycle_count = state["cycle_count"]
                consecutive_failures = state["consecutive_failures"]
                last_success = state["last_success"]
                last_heartbeat = state["last_heartbeat"]
                last_regular_cycle = state["last_regular_cycle"]
                interruptible_sleep(SLEEP_TICK)
                continue

            # ── 优先级4：常规6h扫描（仅开市期间）────────────────────────
            if is_market_open() and (now_ts - last_regular_cycle) >= CYCLE_INTERVAL:
                # 双维度会话重建：时间（24h）或周期数（SESSION_MAX_CYCLES）触发
                time_expired   = not is_first and (now_ts - last_session_rebuild) >= SESSION_REBUILD_INTERVAL
                cycles_expired = not is_first and session_cycle_count >= SESSION_MAX_CYCLES
                if time_expired or cycles_expired:
                    parts = (["24h到期"] if time_expired else []) + ([f"达到{SESSION_MAX_CYCLES}轮上限"] if cycles_expired else [])
                    reason = "/".join(parts)
                    session_id = _rebuild_session(session_id, reason)
                    send_ntfy("[新闻分析师] 会话重建", f"上下文清零（{reason}），简报连续性由macro.md保持")
                    is_first = True
                    session_cycle_count = 0
                    last_session_rebuild = now_ts

                prompt = FIRST_PROMPT if is_first else CYCLE_PROMPT
                suffix = "first" if is_first else "cycle"
                state = _run_cycle(
                    prompt, suffix,
                    session_id, is_first, cycle_count,
                    consecutive_failures, last_success, last_session_rebuild,
                    last_heartbeat, last_regular_cycle,
                    model=CLAUDE_MODEL_ROUTINE,
                )
                is_first = state["is_first"]
                cycle_count = state["cycle_count"]
                consecutive_failures = state["consecutive_failures"]
                last_success = state["last_success"]
                last_heartbeat = state["last_heartbeat"]
                last_regular_cycle = state["last_regular_cycle"]
                if state["ok"]:
                    session_cycle_count += 1
                    log(f"当前会话已跑 {session_cycle_count}/{SESSION_MAX_CYCLES} 轮常规周期")

                if not state["ok"]:
                    if consecutive_failures >= 5:
                        session_id = _rebuild_session(session_id, f"连续{consecutive_failures}次失败")
                        send_ntfy("[新闻分析师] 会话重建", f"连续失败{consecutive_failures}次，新建会话")
                        is_first = True; consecutive_failures = 0
                        session_cycle_count = 0; last_session_rebuild = time.time()
                    elif consecutive_failures >= 3:
                        send_ntfy("[新闻分析师] 连续失败", f"连续{consecutive_failures}次失败，检查日志")

                interruptible_sleep(SLEEP_TICK)
                continue

            # ── 无任务：打一条休眠日志（仅在状态切换时）──────────────────
            if not is_market_open():
                secs_left = seconds_until_open()
                log(f"休市 / 周末，等待开盘（约{secs_left//3600}h{(secs_left%3600)//60}m）"
                    f" | 警报监控仍在运行")

            # 每 SLEEP_TICK 唤醒检查一次触发文件
            interruptible_sleep(SLEEP_TICK)

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
                pid_s = PID_FILE.read_text().strip()
                print(f"PID: {pid_s}")
                try:
                    alive = pid_alive(int(pid_s))
                    print(f"状态: {'运行中' if alive else '已死 (PID 过期)'}")
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
        elif sys.argv[1] == "--once":
            sys.exit(run_once())
        elif sys.argv[1] == "--price":
            # 只测试前置钩子，不跑 Claude 循环
            print(fetch_price_preamble())
        elif sys.argv[1] == "--forecast":
            # 立即手动触发一次每日研究员推演（跳过时间窗口判定，不写 marker）
            os.chdir(str(NEWS_HOME))
            session_id, is_new = get_or_create_session_id()
            print(f"[--forecast] 手动触发每日推演；会话: {session_id}")
            log_path = LOG_DIR / f"cycle_{datetime.now().strftime('%Y%m%d_%H%M%S')}_forecast_manual.log"
            price_preamble = fetch_price_preamble()
            wrapped_prompt = f"{price_preamble}\n---\n\n{DAILY_FORECAST_PROMPT}"
            ok, output = run_claude(wrapped_prompt, log_path, session_id, is_new, model=CLAUDE_MODEL_FORECAST)
            print(f"[--forecast] 结束：ok={ok} 输出={len(output)}字符 日志={log_path}")
            sys.exit(0 if ok else 1)
        elif sys.argv[1] == "--stop":
            if PID_FILE.exists():
                pid = int(PID_FILE.read_text().strip())
                try:
                    os.kill(pid, signal.SIGTERM)
                    print(f"已停止 PID {pid}")
                except ProcessLookupError:
                    print(f"PID {pid} 已经不在了")
                except Exception as e:
                    print(f"停止失败: {e}")
                _unlink_quiet(PID_FILE)
            else:
                print("未运行")
        else:
            print("用法: python daemon.py [--status|--stop|--once|--price|--forecast]")
    else:
        main()
