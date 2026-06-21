#!/usr/bin/env python3
"""
News Analyst Daemon — 完全独立于 AI Trader。

每小时一次 cycle：LLM CLI（默认 `codex`，兼容 `claude`）
  - codex：ephemeral 单轮执行，依赖 macro/forecast 文件维持跨轮上下文
  - claude：用 --session-id/--resume 延续会话
  - 一次性子进程，有超时，无死锁
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
SESSION_FILE = LOG_DIR / ".agent_session_id"
LEGACY_SESSION_FILE = LOG_DIR / ".claude_session_id"
AGENT_BACKEND_FILE = LOG_DIR / ".agent_backend"
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

# 研究员推演：每日 00:10 UTC（= BJ 08:10）跑 1 次，90min 窗口内完成；邮件 cron 01:00 UTC（= BJ 09:00）发送
# 每 slot 只跑一次；marker 存 "YYYY-MM-DD-HH" 避免重放。
DAILY_FORECAST_MARKER = LOG_DIR / ".forecast_slot"
FORECAST_UTC_HOURS = (0, 18)   # 0=工作日早晨; 18=周日 prep (= 周一 02:00 SGT 美盘开盘前 6h)
FORECAST_UTC_MINUTE = 10
FORECAST_WINDOW_MIN = 90
WEEKEND_FORECAST_HOUR = 18     # 仅周日 18:00 UTC slot 在 weekend 触发

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


_agent_exe_cache = {}


def get_agent_backend():
    """Return configured LLM backend: claude or codex.

    Priority:
      1. NEWS_ANALYST_AGENT environment variable
      2. logs/.agent_backend file
      3. codex (default)
    """
    backend = os.environ.get("NEWS_ANALYST_AGENT", "").strip().lower()
    if not backend and AGENT_BACKEND_FILE.exists():
        try:
            backend = AGENT_BACKEND_FILE.read_text(encoding="utf-8").strip().lower()
        except Exception:
            backend = ""
    backend = backend or "codex"
    if backend not in {"claude", "codex"}:
        raise RuntimeError(f"NEWS_ANALYST_AGENT must be 'claude' or 'codex', got {backend!r}")
    return backend


def get_agent_exe(backend):
    """Return CLI executable path for the selected backend.

    Environment overrides:
      - CLAUDE_EXE for claude
      - CODEX_EXE for codex
    """
    if backend in _agent_exe_cache:
        return _agent_exe_cache[backend]
    env_name = "CLAUDE_EXE" if backend == "claude" else "CODEX_EXE"
    default_cmd = "claude" if backend == "claude" else "codex"
    exe = os.environ.get(env_name, "").strip()
    if exe:
        if Path(exe).exists():
            _agent_exe_cache[backend] = exe
            return exe
        w = shutil.which(exe)
        if w:
            _agent_exe_cache[backend] = w
            return w
    w = shutil.which(default_cmd)
    if w:
        _agent_exe_cache[backend] = w
        return w
    raise RuntimeError(
        f"未找到 {backend} CLI (`{default_cmd}`)。请安装并确保 PATH 中有 {default_cmd}，"
        f"或设置环境变量 {env_name} 为完整路径或命令名。"
    )


def get_agent_model(backend, requested_model):
    """Translate legacy Claude tier names into backend-specific model names."""
    requested_model = requested_model or CLAUDE_MODEL_ROUTINE
    if backend == "claude":
        return requested_model

    codex_default = os.environ.get("CODEX_MODEL", "").strip()
    if codex_default:
        return codex_default
    tier_map = {
        CLAUDE_MODEL_ROUTINE: os.environ.get("CODEX_MODEL_ROUTINE", codex_default or "gpt-5.4-mini"),
        CLAUDE_MODEL_BREAKING: os.environ.get("CODEX_MODEL_BREAKING", codex_default or "gpt-5.4"),
        CLAUDE_MODEL_WEEKEND: os.environ.get("CODEX_MODEL_WEEKEND", codex_default or "gpt-5.4-mini"),
        CLAUDE_MODEL_FORECAST: os.environ.get("CODEX_MODEL_FORECAST", codex_default or "gpt-5.5"),
    }
    if requested_model in tier_map:
        return tier_map[requested_model]
    return codex_default or requested_model


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
    "5. 决定推送档位（硬性门槛 · 零影响=不推送）：\n"
    "   · 有**实质性新事件/数据/叙事变化** → 发📊常规推送（v3 分级版：⭐+因果链+量化+三档情景）\n"
    "   · **无实质变化但有下一节点价值** → 发✅小推送（≤2 行：现价 + 下一关键节点 + 倒计时）\n"
    "   · **完全零信息**（叙事无变 + 无下一近期节点 + 用户已知信息）→ **不发推送**，只在 log 写一句\"本轮无推送：理由 XX\"\n"
    "     用户明确指令'金价 0 影响的推送就不要发了'——降噪优先\n"
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
    "5. **判定推送门槛**（硬性，零影响=不推送）：\n"
    "   · 量级 ≥$15 (中/大波动) 或 改变叙事 → 发紧急推送\n"
    "   · 量级 <$15 (小波动) **但**有新触发条件/时间窗变化 → 发紧急推送\n"
    "   · 误报 / 地理误触发 / 付费墙复述旧观点 / 对金价零直接影响 → **不发推送**，只在 macro.md/log 里记录"
    "（理由：避免噪音污染 Trader 通道；用户明确指令'金价 0 影响的推送就不要发了'）\n"
    "\n"
    "6. **推送格式 · v6 BREAKING 紧凑中文版**（决定要推时严格按此输出，每段间一空行）：\n"
    "```\n"
    "🚨 [SPIKE/INTRADAY·★级·方向] 标题（数字+人名+机构）\n"
    "📍 $X,XXX (距前次±$XX · ATR(7)≈$XX)\n"
    "\n"
    "【事件】\n"
    "<2-3 句中文详情，含数字+人名+机构+来源★级>\n"
    "\n"
    "【信号】                                  ← 无数据/非数据事件时整段省略，不要留空标题\n"
    "actual X vs consensus Y = ±Nσ\n"
    "\n"
    "【跨市场·5min】                            ← 数据不可得时整段省略\n"
    "金 $X (±X%) · DXY XX (±X%) · Brent $X (±X%) · 共振 N/3\n"
    "\n"
    "【叙事】 A/B/C · [框架中文名] · <延续/新方向 含量化 ±$X · ATR 内/外>\n"
    "\n"
    "【交易】 SPIKE/INTRADAY/SWING · 多头止盈/空头加仓/观望 · 脉冲/数h/改变叙事\n"
    "\n"
    "【后续】 <下一事件 / 双向情景，1 行内>\n"
    "\n"
    "【哑区】 <暂避窗口 / 等确认 / 无哑区即可交易，1 行内>\n"
    "```\n"
    "**版式硬规则**（违反=push 不合格）：\n"
    "- 模板里所有 `← 注释` 和 `<占位>` 只是给你看的说明，**绝对不要复制**到 push body\n"
    "- 不输出 `⚠️ 现价用...` / `⚠️ 今日只用...` 这种模板自带的元规则（那是给你看的约束，不是 push 内容）\n"
    "- 【叙事】【交易】【后续】【哑区】各自 1 行，不要拆 3 个子标题分多行写\n"
    "- 【信号】和【跨市场】数据不可得时**整段省略**（连标题一起删），不要保留空标题或写 '无数据'\n"
    "- 段与段之间留一个空行；标题用全角【】，正文紧贴标题或下一行\n"
    "\n"
    "**HARD 强制中文**（push body 全中文，违反=重写）：\n"
    "- 国会/政府：House=众议院 · Senate=参议院 · Congress=美国国会 · White House=白宫 · Fed=美联储 · ECB=欧央行 · BoJ=日央行 · PBoC=人民银行\n"
    "- 决议/外交：war-powers resolution=战争权力决议 · veto=否决 · legal challenge=法律挑战 · ceasefire=停火协议 · framework agreement=框架协议 · MOU=谅解备忘录 · sanctions=制裁 · embargo=禁运 · détente/rapprochement=关系回暖\n"
    "- 美联储：hawkish=偏鹰 · dovish=偏鸽 · pivot=政策转向 · taper=缩表 · cut=降息 · hold=按兵不动\n"
    "- 通讯社：Reuters=路透 · AP=美联社 · Bloomberg=彭博 · WSJ=华尔街日报 · FT=金融时报 · NYT=纽约时报\n"
    "- 【叙事】**括号内框架名也必须中文**：例 `[geopolitical war-powers]` → `[地缘·战争权力]`；`[fed-pivot]` → `[联储转向]`；`[RYCC]` → `[油-通胀-美元三重压金]`\n"
    "- 数字 wire 速记必须翻译并保留原值：`49k`→`4.9 万 (49k)` · `150bp`→`150 个基点 (150bp)` · `517k`→`51.7 万 (517k)`；失业率/GDP/CPI 百分比和 $ 价格不翻\n"
    "- 法案编号原样保留：`H.Con.Res. 86` 不译；数据缩写 (NFP/CPI/PPI/ISM/UoM) 首次出现需 `非农 (NFP)` 形式带中文翻译\n"
    "- 句子里不许夹完整英文从句（'still needs Senate approval, and likely faces a veto' → '仍需参议院通过，且预计将遭白宫否决'）\n"
    "\n"
    "【BREAKING 影响分级标注 v2 · 严苛化】\n"
    "⭐ 必须严苛！多数 BREAKING 事件应在 ⭐⭐ 档（已 price-in/Trump 重复表态）。\n"
    "- ⭐⭐⭐⭐⭐ **决定性催化（>$50）**: 实质军事行动 / Fed 主席改向 / 重大数据大幅偏离 / 30Y 破 5.0%\n"
    "- ⭐⭐⭐⭐ 显著催化 ($25-50): 重大政策转向 / 央行紧急措辞\n"
    "- ⭐⭐⭐ 中等催化 ($10-25): 重要官员新表态 / 数据符合预期\n"
    "- ⭐⭐ **常规事件 (<$10·多数应在此档)**: 重复 Trump 表态 / 已 price-in 升级 / OPEC 象征性\n"
    "- ⭐ 边际/已兑现/脉冲: 第 N 次同类 / 重复新闻\n"
    "\n"
    "【BREAKING 推送门槛 · price-in 折扣】\n"
    "(1) 是 5min/30min 真新增量？否→减 1-2⭐\n"
    "(2) 类似事件市场反应过？是→减权\n"
    "(3) 价格 30min 内有反应？无→不应标 ⭐⭐⭐⭐+\n"
    "如果折扣后 ⭐数<3 且无新触发条件 → **不发推送**（降噪）\n"
    "\n"
    "【BREAKING 叙事级别二分法 · 取代旧'震荡基调'】\n"
    "**第一问**：这事件是新叙事还是旧叙事变量？\n"
    "- **A 新叙事**（创造新因果链/改变定价框架）→ ⭐⭐⭐⭐+ → 旗帜鲜明给方向\n"
    "  例: Fed 主席改向 / 实质军事行动首次 / 30Y 破 5.0% / Hormuz 实质封锁\n"
    "- **B 旧叙事变量微调**（已知框架内数据点·多数 BREAKING 属此）→ ⭐⭐ → 延续旧叙事方向（不是震荡）\n"
    "  例: Trump 又表态 / TACO 第 N 次 / 又一次空袭 / Iran 又一次方案\n"
    "  处理: 标注'在 [框架名] 内' + 给延续方向（如 RYCC 锁笼 → 偏空延续）\n"
    "- **C 噪音**（无信息增量）→ 不推送\n"
    "\n"
    "**关键**：B 类事件**给方向不写震荡**——例如 Iran 又强硬属 RYCC 框架内 → 旧空头方向延续 + ⭐⭐\n"
    "**禁止**：用'方向不清/双向夹压/震荡'掩饰对叙事级别的判断不足\n"
    "**真震荡只在**：多新叙事方向冲突 OR 系统真无信号\n"
    "\n"
    "【BREAKING 因果链必备链路】（必须命中至少一条 2-3 步传导）:\n"
    "- 油价 → 通胀预期 → 真实利率 → 持金成本\n"
    "- 谈判/地缘 → 避险买盘 → 金价直接\n"
    "- 美联储讲话/数据 → 利率预期 → DXY/真实利率 → 金价\n"
    "- 央行购金/ETF流 → 物理供需 → 金价\n"
    "\n"
    "【BREAKING 人话替换表】（同 DAILY，push 必翻译）\n"
    "- 'real yield压缩' → '实际利率下降'\n"
    "- 'COT positioning unwind' → '期货大户多头爆满见顶信号'\n"
    "- 'regime切换' → '大环境压制金价'\n"
    "- 'breakeven锁笼' → '油价撑高通胀预期'\n"
    "- 'RYCC锁笼' → '油价高+谈判破裂+美元强=三重压金'\n"
    "\n"
    "【BREAKING 推送前自检】（全 Yes 才推；任何 No 重写或不推）:\n"
    "(1) 量级 ≥$15 或改变叙事？（否=不推）\n"
    "(2) 第一行是 🚨[突发·⭐X 方向↑↓] 结论吗？\n"
    "(3) 事件行有 ⭐数 + 因果链 2 步 + 量化幅度 + 持续性吗？\n"
    "(4) 还有投行术语没翻译吗？\n"
    "(5) 有没有捏造支撑/阻力/进场价？（违规=重写）\n"
    "\n"
    f"   发推送命令：python {NTFY_SCRIPT} \"🚨 [突发] 简短标题\" \"v3 BREAKING 正文\"\n"
    "\n"
    "**关键**：分析完毕在 cycle log 里写一句结论\"是否推送\"+\"理由\"；不推送的不算失败，是降噪。\n"
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
    "   ④ 物理需求（亚洲 SGE 溢价/印度节日/央行 WGC）\n"
    "   ⑤ 仓位层（COT managed money/GLD flows）\n"
    "4. 产出**两时间尺度**推演（砍掉 6 月+ 长期）：\n"
    "   · 短期 24-72h：最可操作视野\n"
    "   · 中期 1-4 周：中期驱动 + FOMC 催化\n"
    "   每情景必写：概率 + 触发条件 + 方向强度 + 数据论据\n"
    "5. 产出 **≥1 个独创框架**（不借用第三方）+ **反事实**（看多/看空各一段）\n"
    f"6. 覆盖写 {FORECAST_PATH_STR}（≤ 150 行；超出删旧留新——研究员产物也要 Trader 能读完）\n"
    f"7. 若推演结论有实质变化，同步更新 {MACRO_PATH_STR} 的【一行结论】和【关键倒计时】\n"
    f"8. 发推送（🔬前缀 · 第一人称交易员叙事 · 4 段, 200-300 字）：python {NTFY_SCRIPT} \"🔬[每日推演] emoji+一句结论\" \"4段叙事正文\"\n"
    "\n"
    "【推送格式 v6 · 第一人称交易员叙事 (用户 2026-05-08 反馈: 喜欢这种)】\n"
    "不要分章节硬模板, 不要 [时间窗·叙事级别·方向] tag, 不要表格, 不要 ▲▼ bullet emoji.\n"
    "写得像顶级 trader 跟朋友讲电话, 4 段, 200-300 字:\n"
    "\n"
    "标题 (≤30 字): emoji + 一句结论. 不堆 tag.\n"
    "  例: ⚖️ 关税裁定已被定价，非农13小时后才是真炸弹\n"
    "      🎯 $100 涨幅是原油的功劳，谅解备忘录溢价还没进场\n"
    "\n"
    "第一段「我的判断」(2-3 句): 今天涨/跌的本质是什么, 用价格行为佐证.\n"
    "  例: '关税法院裁决和油价反弹今天已经被市场提前消化了, 1小时 +$9 把新闻吃完,\n"
    "       价格在 $4,698 附近今日被三次磁吸都没突破, 当前距高点只剩 $0.65.'\n"
    "\n"
    "第二段「我看不同」(2-3 句): 给一个 contrarian view + 关键论据.\n"
    "  例: '市场大家会说双向均衡, 我看不同: 伊斯兰革命卫队在谈判窗口内主动打美国海军,\n"
    "       这是强硬派刻意破局的信号, 谅解备忘录成功概率已经压到三成以下.'\n"
    "\n"
    "第三段「两/三个关键剧本」: 列举式, 每个带概率 + 触发条件 + 价位.\n"
    "  例: '两个关键剧本:\n"
    "       一, 非农弱(低于4.9万)+谅解备忘录破局(三成概率), 金价直上 $4,750-4,780;\n"
    "       二, 非农强(高于8万)+谅解备忘录签署(两成概率), 双重利空, 跌破 $4,660.'\n"
    "\n"
    "第四段「现在动作」: 无仓 / 有多 / 有空 三档具体动作 + 触发价.\n"
    "  例: '现在无仓就别动, 等非农后弱数据站稳 $4,720 再买, 强数据破 $4,678 再空.\n"
    "       有多仓先减半, 止损上移 $4,682.'\n"
    "\n"
    "末尾 ⚠️ 哑区警告 (如有): 一行.\n"
    "  例: '⚠️ 12:00 UTC 起绝对禁入场.'\n"
    "\n"
    "【7:41 风格硬规】\n"
    "1. 第一人称 '我的判断/我看不同', 不写 '建议/推测/可能'\n"
    "2. 价格写 $4,698 形式, 概率写 '三成/七成' 不写 30%/70%\n"
    "3. 不要 ⭐ 评级行 (评级在 macro.md 内部用, push 不复读)\n"
    "4. 不要术语 (actual vs consensus σ / ATR(7) / regime / breakeven 锁笼)\n"
    "   术语必须翻译: 'σ surprise' → '远超市场预期'; 'ATR(7) $40' → '比平均日波动 $40 大'\n"
    "5. 不要 [SPIKE/INTRADAY/SWING] tag (隐含在第四段的'触发价位'里)\n"
    "6. 不要表格 (单行/markdown |---|---|), 不要 ▲▼ bullet emoji\n"
    "7. 200-300 字, 上限 400 字; 信息密度 > 长度\n"
    "8. 第一段必须有'今日 ±$XX' (用 daemon 前置 today_change_dollar, 不从新闻文字摘)\n"
    "\n"
    "【内部分析方法 (用于推理, 不写进 push body)】\n"
    "下面 影响分级/已 price-in 折扣/多空打分/叙事级别二分法/因果链/人话替换表/推送前自检\n"
    "都是你思考的内部规则, 让你判定 '这事算不算', 但**最终 push 体不要复读这些工具.**\n"
    "Push body 只展示结论 + 论据 + 剧本 + 动作; 评分/⭐/折扣表/共振分数全部留在 macro.md.\n\n"
    "【v4 写作硬要求 · 7 条】\n"
    "1. **故事性**：要让\"不懂金融的朋友看完手机能理解为什么金价这样动\"\n"
    "2. **多空两栏对比**：左多头 / 右空头分开列，方便看力量平衡\n"
    "3. **每条事件必配因果解释**：事件→机制→影响 三步，不要两步跳\n"
    "4. **解释\"为什么这样\"**：金价为何冻结/为何爆拉/为何反应温和——必须给原因\n"
    "5. **完整预案**：关键事件给 3 档情景，每档带触发条件+价位\n"
    "6. **哑区完整**：时间 + 持续 + 风险类型（spike/滑点）\n"
    "7. **信息密度高 ≠ 长**：每段都要承载真信息，禁啰嗦\n\n"
    "【影响分级标注规则 v2 · 严苛化（避免狼来了效应）】\n"
    "⭐ 标签必须严苛！绝大多数日常事件应在 ⭐⭐ 档。⭐⭐⭐⭐⭐ 仅给真决定性催化。\n"
    "- ⭐⭐⭐⭐⭐ **决定性催化（>$50 移动）**: FOMC 决议改向 / NFP 大超预期 / 实质军事行动 / Fed 主席换人首发 / 30Y 破 5.0% / 央行紧急行动\n"
    "- ⭐⭐⭐⭐ 显著催化 ($25-50): PCE 大幅偏离共识 / 重大政策转向 / 主流央行措辞重大变化\n"
    "- ⭐⭐⭐ 中等催化 ($10-25): 重要数据符合预期 / 重要官员讲话\n"
    "- ⭐⭐ **常规事件 (<$10·多数 push 应在此档)**: Trump 重复表态 / 事件升级但已 price-in / OPEC 象征性增产 / 重复地缘事件\n"
    "- ⭐ 边际/已兑现/脉冲: 重复新闻 / 微小数据 / 第 N 次同类事件\n"
    "- ➡️ 中性: 无方向、信息更新\n"
    "\n"
    "【已 price-in 折扣表 · 每条事件必问】\n"
    "(1) 这是 5min/30min 内的真新增量吗？\n"
    "    是 → 全权重打分（基础⭐数）\n"
    "    否（24h/7d/30d 前已 price-in）→ **至少减 1-2 ⭐**\n"
    "(2) 市场之前是否已多次反应过类似事件？\n"
    "    是（叙事疲劳）→ 减权\n"
    "(3) 价格在事件出来后 30min 内实际反应？\n"
    "    无明显反应 → 市场已折扣 → **不应标 ⭐⭐⭐⭐+**\n"
    "\n"
    "【多空打分加边际权重·禁止简单加总】\n"
    "旧错误做法: 空 17 vs 多 0 = 净 -17（粗糙加总 stale 信号）\n"
    "新做法: **净边际 = Σ(每条 ⭐ × 新增量权重 0-1.0)**\n"
    "- Stale 信号 × 0.2 ≈ 几乎不计\n"
    "- 真新信号 × 1.0 = 全权重\n"
    "- 例: 5 条 stale ⭐⭐⭐ × 0.2 = 实际 ~1.0 净影响（vs 简单加 15 是错的）\n"
    "\n"
    "【核心方法论 · 叙事级别二分法（取代旧'默认震荡'规则）】\n"
    "每条事件**第一问**：是新叙事，还是旧叙事下的变量微调？\n"
    "\n"
    "**A. 新叙事**（创造新因果链/改变市场底层框架）\n"
    "  例: Fed 主席换人首发 / Powell 首提'通胀临时性' / 实质军事行动首次 / 30Y 破 5.0% / 衰退首次确认 / Hormuz 实质封锁开始\n"
    "  处理: ⭐⭐⭐⭐+ + **旗帜鲜明给方向 + 解释新逻辑链** + 三档情景预案\n"
    "\n"
    "**B. 旧叙事变量微调**（已知框架内的数据点·多数事件属此类）\n"
    "  例: Trump 又一次表态 / TACO 第 N 次 / OPEC 象征性增产 / 又一次空袭 / PCE 符合预期 / Iran 又一次方案 / Hormuz 紧张持续\n"
    "  处理: ⭐⭐ 评级 + **方向 = 旧叙事延续方向**（不是'震荡'！）+ 标注'在 [框架名] 内' + 量化幅度小（<$15）\n"
    "  例: Iran 又强硬 → RYCC 锁笼框架内 → 油价撑高通胀延续旧空头方向（明确给空头延续，不写'双向夹压'）\n"
    "\n"
    "**C. 噪音**（无信息增量）\n"
    "  例: 第 N 次同事件 RSS 转载 / 付费墙复述 / 地理误触发\n"
    "  处理: 不推送\n"
    "\n"
    "**关键差异**：\n"
    "- 旧规则'默认震荡' = 看不清 → 不给方向（偷懒）\n"
    "- 新规则'叙事级别二分' = 先判断事件性质 → 旧叙事变量也给方向（延续旧框架）\n"
    "- 真'震荡'只在两种情况：(1) 多个新叙事方向冲突；(2) 系统真无信号\n"
    "- 大多数日常事件属 B 类——给延续方向，不写震荡\n"
    "\n"
    "【单边方向硬要求】\n"
    "- 给方向必须配：(1) 叙事级别清楚（A 或 B）+ (2) 因果链明确 + (3) 量化幅度（即使小）\n"
    "- 不要为避免错方向而'用震荡掩饰判断不清'——这是另一种偷懒\n"
    "- 客观 ≠ 不给方向；客观 = 准确判断叙事级别后给该有的方向\n"
    "\n"
    "【发⭐⭐⭐⭐⭐前 5 问自检 · 全 Yes 才用】\n"
    "1. 这事件是真新增量（5min/30min 内发生）吗？\n"
    "2. 市场之前没反应过类似事件吗？\n"
    "3. 我能给出'为何这次和之前不同'吗？\n"
    "4. 价格行动在 30min 内确认了吗？\n"
    "5. 我是不是在用'强叙事'补偿信息不确定性？（如果是，标 ⭐⭐⭐ 就够）\n"
    "任一 No = 降档\n"
    "\n"
    "【因果链必备链路】每条事件至少命中一条 2-3 步传导:\n"
    "- 油价 → 通胀预期 → 真实利率 → 持金成本\n"
    "- 谈判/地缘 → 避险买盘 → 金价直接\n"
    "- 美联储讲话/数据 → 利率预期 → DXY/真实利率 → 金价\n"
    "- 央行购金/ETF流 → 物理供需 → 金价\n"
    "\n"
    "【人话替换表 · v5+全翻译版（push 必全部翻译，不留任何缩写代号）】\n"
    "**v5 多维结构 + 全人话**：禁止用任何代号/英文缩写/统计符号。多维不等于堆砌术语。\n"
    "\n"
    "**A. 时间窗标签**（push 标题禁用英文代号）:\n"
    "- 'SPIKE' → '短线分钟级' 或 '5min spike 机会'\n"
    "- 'INTRADAY' → '日内小时级' 或 '当日内'\n"
    "- 'SWING' → '中线 3-7 天'\n"
    "- 'POSITION' → '长线数周'\n"
    "\n"
    "**B. 叙事级别标签**（push 内禁用代号）:\n"
    "- 'A 新叙事' → '全新故事/新逻辑链'\n"
    "- 'B 旧叙事变量' → '老故事的变化（不改大方向）'\n"
    "- 'C 噪音' → '噪音不推送'\n"
    "\n"
    "**C. 内部框架代号→人话**:\n"
    "- 'RYCC 锁笼' / 'RYCC 2.0' → '三重压金（油价高+美伊谈不拢+美元强）'\n"
    "- 'TACO / TACO 第 N 次' → '嘴上强硬实际让步（第 N 次重复）'\n"
    "- 'real yield 压缩 / ceiling 掀开' → '实际利率下降 → 金价天花板打开'\n"
    "- 'COT contrarian / positioning unwind' → '期货大户多头爆满见顶信号'\n"
    "- 'regime 切换 / regime 决定性压制' → '大环境压制金价'\n"
    "- 'term premium 暴力回归' → '长期美债利率冲高'\n"
    "- 'breakeven 锁笼 / 通胀锁笼' → '油价撑高通胀预期'\n"
    "- '定价权转长端' → '美债长端飙升，无息金价被压'\n"
    "- '概念跃迁 / 硬资产' → 直接省掉或换'长线多头根基'\n"
    "- 'succession premium' → '沃什上任溢价'\n"
    "\n"
    "**D. 资产代号→中文全称**:\n"
    "- 'DXY' → '美元指数'\n"
    "- '30Y' → '30 年美债利率'\n"
    "- '10Y' → '10 年美债利率'\n"
    "- '2Y' → '2 年美债利率'\n"
    "- 'VIX' → '风险指数 VIX' 或 '市场恐慌指数'\n"
    "- 'Brent' → '国际原油（北海布伦特）'\n"
    "- 'WTI' → '美国原油（WTI）'\n"
    "- 'TIPS' → '通胀保值国债'\n"
    "- 'XAUUSD' → '黄金'\n"
    "- 'GLD/ETF' → '黄金 ETF'\n"
    "- 'COMEX' → '纽约商品交易所'\n"
    "- 'SGE' → '上海黄金交易所'\n"
    "- 'BoE/BoJ/ECB/PBOC' → '英国/日本/欧洲/中国央行'\n"
    "- 'FOMC' → '美联储利率会议'\n"
    "- 'NFP' → '美国非农就业数据'\n"
    "- 'CPI/PCE' → '消费者通胀（CPI）/个人消费通胀（PCE）'（首次解释，后续可简称）\n"
    "- 'GDP' → 'GDP（经济增速）'（首次解释）\n"
    "- 'PMI' → 'PMI（制造业/服务业景气指数）'\n"
    "- 'JOLTS' → '职位空缺数据'\n"
    "- 'ISM' → 'ISM 制造业/服务业指数'\n"
    "- 'ADP' → 'ADP 民间就业数据'\n"
    "\n"
    "**E. 统计符号→人话**:\n"
    "- 'actual vs consensus' → '数据 vs 市场预期'\n"
    "- 'σ surprise / sigma' → '偏差有多大' 或 '偏离预期 N 倍标准差'\n"
    "- 'ATR(7)' → '近 7 天平均日波动'\n"
    "- 'basis point / bp' → '基点（0.01%）'\n"
    "- 'price-in / priced in' → '已被市场消化/预期'\n"
    "- 'hawkish / dovish' → '鹰派（紧缩）/ 鸽派（宽松）'\n"
    "- 'easing bias' → '降息倾向'\n"
    "- 'pivot' → '政策转向'\n"
    "\n"
    "**F. 事件代号→中文人话**:\n"
    "- 'Project Freedom' → \"'自由号'护航行动\"\n"
    "- 'Maritime Freedom Construct / MFC' → '海上自由联盟'\n"
    "- 'Operation Sentinel' → \"'哨兵行动'\"\n"
    "- 'War Powers Act' → '战争权力法案'\n"
    "- 'Hormuz' → '霍尔木兹海峡'\n"
    "- 'IRGC' → '伊朗革命卫队'\n"
    "- 'Khamenei' → '哈梅内伊'\n"
    "- 'Powell' → '鲍威尔（美联储主席）'（首次解释）\n"
    "- 'Warsh' → '沃什（候任美联储主席）'\n"
    "\n"
    "**最后自检**：发送前通读全文，凡是一个非金融专业的朋友看了会问'什么意思?'的词 → 替换为人话。\n"
    "\n"
    "【推送前自检 · 硬性】发送前逐条自问，全 Yes 才发:\n"
    "(1) 每条事件有 ⭐数 + 因果链 2-3 步 + 量化幅度吗？\n"
    "(2) 24-72h 看点有三档情景（鹰/中/鸽 或 利空/震荡/利多）+ 每档价位区间吗？\n"
    "(3) 多空打分行有吗？（空X星 vs 多Y星 = 净±Z星）\n"
    "(4) 还有投行术语没翻译吗？\n"
    "(5) 第一行是 🎯 方向结论吗？\n"
    "任何一条 No = 重写。\n"
    "\n"
    "【硬禁令 · 七条】\n"
    "- ❌ 禁止捏造价位：所有 XAUUSD 现价用 daemon 前置钩子注入的 MT5 价；历史价带日期+来源；机构目标带机构名\n"
    "- ❌ 禁止写具体支撑/阻力/进场位（'$4,680支撑''$4,860阻力''$4,750-4,780中期买点'均违规）——那是 AI Trader 的技术分析领域\n"
    "- ❌ 禁止'无论据论点'：每条推送论点必须有数字+来源；没来源的判断不放进推送\n"
    "- ❌ 禁止把投行术语搬 push：所有术语必须按【人话替换表】翻译；macro.md/forecast.md 可保留术语\n"
    "- ❌ 禁止'结论后置'：第一行必须是 🎯 方向结论；交易员先看方向，再读因果链\n"
    "- ❌ 禁止'事实碎片'：每条 ▼/▲ 必须带【影响等级⭐数】+【因果链 2-3 步】+【量化幅度】；只列事实不解读=废话\n"
    "- ❌ 禁止'看点无情景'：⚡ 24-72h 看点必须给三档情景（鹰派/中性/鸽派 或 利空/震荡/利多），每档带星级和价位区间\n"
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


def _market_session_note() -> str:
    """检测 XAUUSD 是否在 5x24 交易时段。返回 awareness note (空字符串=开盘中)。

    XAUUSD 交易时段（UTC）：
    - 周日 22:00 → 周五 22:00（连续 5 天）
    - 周末关闭：周五 22:00 UTC → 周日 22:00 UTC（约 48h）
    """
    now = datetime.now(timezone.utc)
    wd = now.weekday()  # Mon=0...Sun=6
    h = now.hour
    closed = False
    detail = ""
    if wd == 5:  # Saturday
        closed = True
        detail = f"周六全天关闭，距下次开盘约 {46 - h}h"
    elif wd == 6 and h < 22:  # Sunday before 22 UTC
        closed = True
        detail = f"周日开盘前，距亚洲开盘约 {22 - h}h{60 - now.minute}min"
    elif wd == 4 and h >= 22:  # Friday after 22 UTC
        closed = True
        detail = "周五已收盘 22:00 UTC，下次开盘周日 22:00 UTC"
    if not closed:
        return ""
    return (
        f"\n⚠️ **市场休市中**（{detail}）\n"
        "**交易时段 awareness · 硬性规则**：\n"
        "- 当前为 XAUUSD 周末/节假日关闭时段（5x24 之外）\n"
        "- **禁止把\"价格不动\"解读为\"多空打平/冻结/僵持\"** — 是市场关闭，不是真信号\n"
        "- 上次有效价格来自 MT5 最后一次 tick（即周五收盘价）\n"
        "- 分析应聚焦：①周末事件累积 ②开盘 gap 风险 ③下次开盘时间\n"
        "- 价格相关叙事禁止使用：\"X 小时冻结\"\"持平\"\"双向夹压\"\"力量打平\" — 改写为\"周末休市，距开盘 X 时\"\n"
    )


def fetch_price_preamble() -> str:
    """每轮开始前调用 get_price.py 获取 MT5 实时 XAUUSD，返回要拼到 prompt 顶部的一段。

    成功：返回带现价+时间戳+指引的块。
    失败：返回带错误原因+指引（让 Claude 在 macro.md 注明无现价锚，不得用新闻价替代）的块。
    """
    preamble_header = "【daemon 前置钩子 · 当前金价 + 今日锚 (MT5 broker)】"
    session_note = _market_session_note()
    guidance_ok = (
        "⚠️ 硬规则: 上述 XAUUSD 现价 + 今日开盘锚 由 daemon 调 MT5 bridge 取得 (broker D1 bar 开盘价).\n"
        "  - macro.md / 推送里写 '今日±$XX' 时, 必须用上面的 today 数据 (vs broker D1 开)\n"
        "  - 不得从新闻文字 / WebSearch 里摘 '今日 +$XX' 替代 (yfinance 期金 / 24h 移动 不是今日)\n"
        "  - 上方 1h/4h/24h 移动是相对参考, 不要把 24h 移动写成 '今日'\n"
        "  - 不得自行捏造支撑/阻力位"
    )
    guidance_fail = (
        "**价格源不可用**. 按纪律: 在 macro.md 顶部明确写'价格源不可用, 本轮无现价锚',\n"
        "不得以新闻文字中的价格作为替代; 推送里禁写 '今日±$XX'. 方向分析照常进行."
    )
    try:
        result = subprocess.run(
            [PYTHON_FOR_MT5, str(GET_PRICE_SCRIPT), "--full"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=PRICE_FETCH_TIMEOUT,
            env={**os.environ, "MT5_BACKEND": "http", "MT5LINUX_PORT": "8101"},
        )
        stdout_txt = (result.stdout or b"").decode("utf-8", errors="replace").strip()
        stderr_txt = (result.stderr or b"").decode("utf-8", errors="replace").strip()
        if result.returncode == 0 and stdout_txt:
            return f"{preamble_header}\n{stdout_txt}\n{session_note}\n{guidance_ok}\n"
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
    """cycle 完成后读 macro.md 检查；发现违规仅写 log，不推送。"""
    try:
        if not MACRO_FILE.exists():
            return
        text = MACRO_FILE.read_text(encoding="utf-8", errors="replace")
        violations = lint_macro_for_price_violations(text)
        if violations:
            log(f"⚠️ macro.md lint 发现 {len(violations)} 处价位违规：")
            for lineno, content, idx in violations[:5]:
                log(f"    L{lineno} pat#{idx}: {content}")
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
    # Marker-file kill switch (shared with ntfy_push.py). Effective on next
    # daemon restart for this function; ntfy_push.py picks it up immediately.
    if (NEWS_HOME / ".push_disabled").exists():
        return
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
    # One-time migration from the legacy Claude-only session file.
    if LEGACY_SESSION_FILE.exists():
        try:
            sid = LEGACY_SESSION_FILE.read_text().strip()
            if sid:
                atomic_write_text(SESSION_FILE, sid)
                return sid, False
        except Exception:
            pass
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


def run_agent(prompt, log_path, session_id, is_first, model=None):
    """Run the configured non-interactive agent backend.

    Claude keeps the historical resumable session behavior. Codex currently runs
    ephemeral cycles and relies on macro.md/forecast.md for continuity; this
    avoids daemon stalls from backend session locks while keeping backend choice
    parameterized.
    """
    backend = get_agent_backend()
    exe = get_agent_exe(backend)
    selected_model = get_agent_model(backend, model)
    if backend == "claude":
        cmd = [
            exe,
            "-p",
            prompt,
            "--output-format", "text",
            "--model", selected_model,
            "--permission-mode", "bypassPermissions",
        ]
        if is_first:
            cmd += ["--session-id", session_id]
        else:
            cmd += ["--resume", session_id]
    else:
        cmd = [
            exe, "--search",
            "--ask-for-approval", "never",
            "exec",
            "--ephemeral",
            "--cd", str(NEWS_HOME),
            "--sandbox", "danger-full-access",
            "--model", selected_model,
            prompt,
        ]

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
            f.write(f"后端: {backend}\n")
            f.write(f"会话: {session_id}\n")
            f.write(f"退出码: {proc.returncode}\n\n")
            f.write("--- 输出 ---\n")
            f.write(output)
            f.write(f"\n=== 完成 ===\n")

        # 合法 no-op 信号（v3 降噪规则）：分析师判定零影响主动跳过推送
        out_stripped = output.strip()
        is_legitimate_no_op = any(
            marker in out_stripped
            for marker in ("不推送", "降噪", "无需更新")
        )
        ok_run = proc.returncode == 0 and (len(out_stripped) > 30 or is_legitimate_no_op)
        return ok_run, output

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


run_claude = run_agent  # backwards-compatible name used by existing call sites


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

    工作日: 走 UTC 0 slot (开盘前 prep, 含周日 22:00 UTC 之后开盘的部分).
    周末 (用户 2026-05-09 加): 仅周日 18:00 UTC 一次 prep forecast
                              (= 周一 02:00 SGT, 美盘开盘前 6h).
    高级别突发 (alert_monitor 触发) 独立路径, 不受此限.
    """
    slot = current_forecast_slot()
    if slot is None:
        return False
    market_open = is_market_open()
    now = datetime.now(timezone.utc)
    wd = now.weekday()
    if market_open:
        # 工作日: 只走 UTC 0 slot (避免 18 slot 在工作日多跑一次)
        if slot != 0:
            return False
    else:
        # 周末: 仅周日 18:00 UTC slot 跑 (跨周末 prep)
        if wd != 6 or slot != WEEKEND_FORECAST_HOUR:
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
    """执行一轮分析师周期，返回更新后的状态变量 dict。"""
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

    # 前置钩子：注入 MT5 实时金价，避免模型遗漏实时价格读取
    price_preamble = fetch_price_preamble()
    log(f"前置钩子价格: {price_preamble.splitlines()[1] if len(price_preamble.splitlines()) > 1 else price_preamble[:120]}")
    wrapped_prompt = f"{price_preamble}\n---\n\n{prompt}"

    ok, output = run_claude(wrapped_prompt, log_path, session_id, is_first_flag, model=model)
    mtime_after = macro_mtime()

    if ok and mtime_after == mtime_before:
        # 合法 no-op：v3 降噪规则下 Claude 主动判定零影响、不推送、不更新 macro.md
        out_lower = (output or "").strip()
        is_legitimate_no_op = any(
            marker in out_lower
            for marker in ("不推送", "降噪", "无需更新", "本轮无推送")
        )
        if is_breaking or is_legitimate_no_op:
            log("周期：分析师判定零影响/合理 no-op（不更新 macro.md，不计失败）")
        else:
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
    backend = get_agent_backend()
    log("守护进程启动")
    log(f"后端: {backend}")
    log(f"会话: {session_id} ({'新建' if is_new else '恢复'})")
    send_ntfy("[新闻分析师] 启动",
              f"新闻分析师上线\n后端: {backend}\n每6h常规扫描 + 突发即时响应\n会话: {session_id[:8]}...")

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
