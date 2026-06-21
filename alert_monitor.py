#!/usr/bin/env python3
"""
Breaking news alert monitor for XAUUSD.

轮询 RSS 源（每2分钟），检测高影响事件，写触发文件供 daemon.py 拾取。
无需 Claude，纯关键词评分，极轻量。
"""
import hashlib
import json
import logging
import os
import signal
import sys
import time
import atexit
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

NEWS_HOME = Path(__file__).resolve().parent
LOG_DIR = NEWS_HOME / "logs"
TRIGGER_FILE = LOG_DIR / ".urgent_trigger"
SEEN_FILE = LOG_DIR / ".alert_seen"
RECENT_TRIGGERS_FILE = LOG_DIR / ".alert_recent_triggers"
PID_FILE = LOG_DIR / ".alert.pid"
DAEMON_PID_FILE = LOG_DIR / ".daemon.pid"  # daemon.py PID,用于 SIGUSR1 主动唤醒

# Sidecar: AITraderV5 直接读取的地缘警报文件（绕过 daemon 延迟）
GEO_ALERT_FILE = Path("/home/admin/OpusWorkspace/AITraderV5/data/cockpit/geo_alerts.json")
GEO_ALERT_SCORE_MIN = 20   # 写入 sidecar 的最低分（低于此不写）
GEO_ALERT_MAX_AGE_H = 4    # sidecar 保留最近 N 小时的条目

POLL_INTERVAL = 120   # 每2分钟轮询一次
TRIGGER_THRESHOLD = 10  # 评分达到此值才写触发文件
MAX_SEEN = 1000        # 最多记住多少条已见标题

# 故事级冷却：同一组关键词的重复故事在窗口内静音
STORY_COOLDOWN_MIN = 90       # 冷却窗口（分钟）
STORY_JACCARD_THRESHOLD = 0.5 # Jaccard 重叠 ≥ 此值视为同故事

RSS_FEEDS = [
    # 英文一线财经（Reuters RSS 已关闭，改走 Google News 代理）
    ("Reuters via Google", "https://news.google.com/rss/search?q=when:1d+source:reuters&hl=en-US"),
    ("CNBC World",         "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("MarketWatch",        "https://feeds.marketwatch.com/marketwatch/topstories/"),
    ("Investing.com",      "https://www.investing.com/rss/news.rss"),
    ("Seeking Alpha",      "https://seekingalpha.com/market_currents.xml"),
    # 英文国际新闻
    ("CNN Edition",        "http://rss.cnn.com/rss/edition.rss"),
    ("BBC World",          "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("Al Jazeera",         "https://www.aljazeera.com/xml/rss/all.xml"),
    # 英文财经深度
    ("FT Home",            "https://www.ft.com/rss/home"),
    ("FT Markets",         "https://www.ft.com/markets?format=rss"),
    # 中文财经
    ("新浪财经",            "http://rss.sina.com.cn/news/allnews/finance.xml"),
    ("华尔街见闻",          "https://wallstreetcn.com/news.xml"),
    # 官方源（低频高权重）
    ("Fed 新闻",            "https://www.federalreserve.gov/feeds/press_all.xml"),
    ("Fed 讲话",            "https://www.federalreserve.gov/feeds/speeches.xml"),
    ("ECB 新闻",            "https://www.ecb.europa.eu/rss/press.html"),
    # 高频快讯（Fed 讲话/经济数据第一时间）
    ("ForexLive",          "https://www.forexlive.com/feed/news"),
    ("Reuters Business via Google", "https://news.google.com/rss/search?q=when:6h+source:reuters+(fed+OR+powell+OR+waller+OR+bowman+OR+cpi+OR+nfp)&hl=en-US"),
    ("Bloomberg via Google",        "https://news.google.com/rss/search?q=when:6h+source:bloomberg+(fed+OR+powell+OR+waller+OR+bowman+OR+rate+cut)&hl=en-US"),
]

# 关键词评分表。分值越高 = 越紧急。
# Tier-1 (10): 立即移动金价的事件
# Tier-2 (6):  重要，需要分析
# Tier-3 (3):  值得关注，单独不够触发
KEYWORDS: dict[str, int] = {
    # ─── Tier-1 (10) · 立即移动金价 ─────────────────────────────
    # 英文 · 地缘/军事
    "nuclear deal":          10,
    "nuclear agreement":     10,
    "nuclear talks":         10,
    "missile strike":        10,
    "military strike":       10,
    "airstrike":             10,
    "drone strike":          10,
    "bombing":               10,
    "war declared":          10,
    "war breaks out":        10,
    "strait of hormuz":      10,
    "oil embargo":           10,
    "ceasefire":             10,
    "ceasefire collapsed":   10,
    "ceasefire expires":     10,
    "fighting resumes":      10,
    # 美伊协议签字专项 (2026-06-14 加，签字即触发)
    "iran deal signed":      10,
    "mou signed":            10,
    "iran signed":           10,
    "peace deal signed":     10,
    "hormuz open":           10,
    "hormuz opens":          10,
    "vance iran":            10,
    "islamabad declaration": 10,
    "iran accord":           10,
    "iran agreement signed": 10,
    "hormuz reopens":        10,
    "iran war ends":         10,
    "flash crash":           10,
    # 英文 · 货币/金融危机
    "emergency rate":        10,
    "emergency cut":         10,
    "fed rate decision":     10,
    "rate cut":              10,
    "rate hike":             10,
    "fomc decision":         10,
    "bank collapse":         10,
    "bank run":              10,
    "market crash":          10,
    "circuit breaker":       10,
    "trading halt":          10,
    "sovereign default":     10,
    # 中文 · 地缘/军事
    "核协议":                 10,
    "核谈判":                 10,
    "停火":                   10,
    "停战":                   10,
    "战争":                   10,
    "开战":                   10,
    "空袭":                   10,
    "轰炸":                   10,
    "导弹袭击":               10,
    "霍尔木兹":               10,
    # 中文 · 货币/金融
    "加息":                   10,
    "降息":                   10,
    "美联储决议":             10,
    "金融危机":               10,
    "熔断":                   10,
    "暴跌":                   10,
    "暴涨":                   10,
    # ─── Tier-1 · 俄伊会谈专项 (4/27 active 监控) ──────────────────
    "araghchi":              10,
    "uranium custody":       10,
    "uranium transfer":      10,
    "russia iran alliance":  10,
    "russia iran treaty":    10,
    "iran russia treaty":    10,
    "moscow tehran joint":   10,
    "kremlin tehran joint":  10,
    "safety guarantees":     10,
    "strategic partnership upgrade": 10,
    "阿拉格希":               10,
    "俄伊联盟":               10,
    "俄伊条约":               10,
    "俄伊战略":               10,
    "保管浓缩铀":             10,
    "战略伙伴升级":           10,
    "安全保护伞":             10,
    # ─── Tier-1 · Powell 记者会专项 (4/29 active 监控，5/15 Warsh 接班前) ──
    "transitory":            10,
    "powell dovish":         10,
    "powell hawkish":        10,
    "powell pivot":          10,
    "powell rate cut":       10,
    "rate cut path":         10,
    "rate cut signal":       10,
    "easing bias removed":   10,
    "easing bias retained":  10,
    "qt-for-cuts":           10,
    "warsh dovish":          10,
    "warsh hawkish":         10,
    "powell final":          10,
    "fed pivot":             10,
    "通胀临时性":             10,
    "鲍威尔转向":             10,
    "鲍威尔鸽派":             10,
    "鲍威尔鹰派":             10,
    "降息路径":               10,
    "鹰派分歧":               10,
    "鸽派分歧":               10,
    # ─── Tier-1 · PCE/GDP 4/30 专项 (~17h后发布) ──────────────────
    "core pce":              10,
    "pce hot":               10,
    "pce cool":              10,
    "pce jumps":             10,
    "pce surges":            10,
    "pce drops":             10,
    "pce eases":             10,
    "pce above":             10,
    "pce below":             10,
    "pce beat":              10,
    "pce miss":              10,
    "pce surprise":          10,
    "pce inflation":         10,
    "core inflation":        10,
    "fed preferred gauge":   10,
    "gdp contracts":         10,
    "gdp shrinks":           10,
    "gdp beats":             10,
    "gdp exceeds":           10,
    "gdp surprise":          10,
    "advance gdp":           10,
    "stagflation surge":     10,
    "核心pce":                10,
    "pce数据":                10,
    "pce通胀":                10,
    "通胀降温":               10,
    "通胀超预期":             10,
    "通胀低于预期":           10,
    "通胀加速":               10,
    "经济萎缩":               10,
    "gdp 萎缩":               10,
    "滞胀确认":               10,
    # ─── Tier-1 · Project Freedom 专项 (5/4 启动·active监控) ─────────
    "project freedom":       10,
    "maritime freedom":      10,
    "tanker attack":         10,
    "tanker mined":          10,
    "tanker hit":            10,
    "ship mined":            10,
    "tanker boarded":        10,
    "ship attacked hormuz":  10,
    "iran attacks tanker":   10,
    "iran attacks ship":     10,
    "iran attacks escort":   10,
    "iran attacks navy":     10,
    "iran fires escort":     10,
    "iran sinks":            10,
    "navy strikes iran":     10,
    "us strikes iran":       10,
    "hormuz reopens":        10,
    "hormuz reopened":       10,
    "hormuz cleared":        10,
    "naval coalition":       10,
    "freedom of navigation": 10,
    "护航行动":               10,
    "美军护航":               10,
    "护航船被攻击":           10,
    "护航船触雷":             10,
    "霍尔木兹重开":           10,
    "海军联盟":               10,
    "海峡冲突":               10,
    "击沉护航":               10,
    "击中护航":               10,
    # ─── Tier-1 · 美债利率专项 (5/4 30Y破5%·active监控) ─────────────
    "30-year yield 5":       10,
    "30y yield 5":           10,
    "30-year tops 5":        10,
    "yield breaks 5":        10,
    "yield tops 5.10":       10,
    "yield tops 5.20":       10,
    "yield tops 5.30":       10,
    "30y above 5":           10,
    "treasury rout":         10,
    "bond rout":             10,
    "bond massacre":         10,
    "long bond crisis":      10,
    "bond vigilantes":       10,
    "treasury selloff":      10,
    "auction failed":        10,
    "auction tail":          10,
    "weak treasury demand":  10,
    "fed emergency qe":      10,
    "operation twist":       10,
    "yield curve control":   10,
    "ycc":                   10,
    "fed buys treasuries":   10,
    "fed buying long bonds": 10,
    "30年美债破5":            10,
    "美债破5":                10,
    "长债危机":               10,
    "国债拍卖失败":           10,
    "美债收益率飙升":         10,
    "美联储紧急购债":         10,
    "美联储扭转操作":         10,
    "收益率曲线控制":         10,

    # ─── Tier-2 (6) · 重要，需要分析 ─────────────────────────────
    # 英文 · 地缘
    "iran":                  6,
    "israel":                6,
    "sanctions":             6,
    "north korea":           6,
    "taiwan strait":         6,
    "hormuz":                6,
    "retaliation":           6,
    "retaliate":             6,
    "attack":                6,
    "bombs":                 6,
    "escalation":            6,
    "escalate":              6,
    # 英文 · 政要（推文级市场动量）
    "trump":                 6,
    "biden":                 6,
    "netanyahu":             6,
    "putin":                 6,
    "xi jinping":            6,
    "khamenei":              6,
    "zelenskyy":             6,
    "zelensky":              6,
    "modi":                  6,
    # 英文 · Fed 官员（讲话级别）
    "powell":                6,
    "waller":                6,
    "williams":              6,
    "jefferson":             6,
    "cook":                  6,
    "barr":                  6,
    "bostic":                6,
    "daly":                  6,
    "kashkari":              6,
    "bowman":                6,
    # 英文 · 货币/经济数据
    "fomc":                  6,
    "federal reserve":       6,
    "inflation":             6,
    "cpi":                   6,
    "pce":                   6,
    "nfp":                   6,
    "nonfarm payroll":       6,
    "jobs report":           6,
    "gdp":                   6,
    "recession":             6,
    "stagflation":           6,
    "pmi":                   6,
    # 英文 · 黄金/美元
    "gold surges":           6,
    "gold plunges":          6,
    "gold rally":            6,
    "gold slumps":           6,
    "gold spike":            6,
    "xauusd":                6,
    "dxy":                   6,
    "treasury yield":        6,
    "dollar falls":          6,
    "dollar rises":          6,
    "central bank gold":     6,
    # 中文 · 政要+国家
    "伊朗":                   6,
    "以色列":                 6,
    "乌克兰":                 6,
    "俄罗斯":                 6,
    "朝鲜":                   6,
    "台海":                   6,
    "特朗普":                 6,
    "拜登":                   6,
    "普京":                   6,
    "内塔尼亚胡":             6,
    "鲍威尔":                 6,
    "习近平":                 6,
    "泽连斯基":               6,
    # 中文 · 经济数据
    "非农":                   6,
    "通胀":                   6,
    "美联储":                 6,
    "美债":                   6,
    "衰退":                   6,
    "滞胀":                   6,
    # ─── Tier-2 · 俄伊会谈专项辅助词 (与 putin/iran/普京/伊朗组合可累积过线) ──
    "lavrov":                6,
    "kremlin":               6,
    "tehran moscow":         6,
    "moscow tehran":         6,
    "拉夫罗夫":               6,
    "克里姆林":               6,
    "德黑兰":                 6,
    "圣彼得堡":               6,
    # ─── Tier-2 · Powell 记者会辅助词 (与 powell/fed 组合可累积过线) ──
    "policy pivot":          6,
    "easing bias":           6,
    "data dependent":        6,
    "fed easing":            6,
    "fed tightening":        6,
    "miran":                 6,
    "hammack":               6,
    "logan":                 6,
    "dissent":               6,
    "dovish":                6,
    "hawkish":               6,
    "鸽派":                   6,
    "鹰派":                   6,
    "分歧":                   6,
    "缩表":                   6,
    # ─── Tier-1.5 · 月份 + 降息/加息 组合(FOMC dissent / repricing 早期信号) ──
    "july rate cut":        10,
    "july cut":             10,
    "september rate cut":   10,
    "september cut":        10,
    "december rate cut":    10,
    "december cut":         10,
    "july hike":            10,
    "september hike":       10,
    "open to cut":           8,
    "cut on the table":      8,
    "on the table":          4,
    "support rate cut":     10,
    "backs rate cut":       10,
    "calls for rate cut":   10,
    "calls for cut":        10,
    "7月降息":               10,
    "9月降息":               10,
    "12月降息":              10,
    "支持降息":               8,
    "呼吁降息":               8,
    # ─── Tier-2 · PCE/GDP 辅助词 ──────────────────────────────────
    "personal consumption":  6,
    "consumption expenditures": 6,
    "real gdp":              6,
    "preliminary gdp":       6,
    "month over month":      6,
    "year over year":        6,
    "y/y":                   6,
    "m/m":                   6,
    "annualized":            6,
    "consensus":             6,
    "环比":                   6,
    "同比":                   6,
    "预期":                   6,
    "共识":                   6,
    # ─── Tier-2 · Project Freedom 辅助词 ──────────────────────────
    "escort":                6,
    "convoy":                6,
    "stranded ships":        6,
    "shipping lane":         6,
    "centcom":               6,
    "destroyer":             6,
    "frigate":               6,
    "carrier strike":        6,
    "护航":                   6,
    "护送":                   6,
    "中立船":                 6,
    "中立商船":               6,
    "海军":                   6,
    "驱逐舰":                 6,
    "航母":                   6,
    # ─── Tier-2 · 美债利率辅助词 ────────────────────────────────
    "long bond":             6,
    "long-dated":            6,
    "duration risk":         6,
    "bond fund":             6,
    "primary dealer":        6,
    "treasury auction":      6,
    "tlt":                   6,
    "yield spike":           6,
    "yield surge":           6,
    "5.0%":                  6,
    "5.1%":                  6,
    "5.2%":                  6,
    "5.3%":                  6,
    "美债拍卖":               6,
    "长期国债":               6,
    "收益率":                 6,
    "基点":                   6,
    "扭转操作":               6,

    # ─── Tier-3 (3) · 一般关注 ───────────────────────────────────
    # 英文
    "gold":                  3,
    "oil price":             3,
    "crude":                 3,
    "brent":                 3,
    "wti":                   3,
    "barrel":                3,
    "geopolitical":          3,
    "tariff":                3,
    "trade war":             3,
    "debt ceiling":          3,
    "quantitative":          3,
    "imf":                   3,
    "world bank":            3,
    "safe haven":            3,
    "flight to quality":     3,
    "risk off":              3,
    "surge":                 3,
    "plunge":                3,
    # 中文
    "黄金":                   3,
    "金价":                   3,
    "油价":                   3,
    "美元":                   3,
    "地缘":                   3,
    "关税":                   3,
    "制裁":                   3,
    "避险":                   3,
    "多头":                   3,
    "空头":                   3,
}


# ─── Fast-path geo gate ───────────────────────────────────────────────────────
# Bypass daemon LLM cycle (~3min) for unambiguous Tier-1 keyword events.
# alert_monitor writes news_signal.csv directly → EA cache reload in <10s.
# daemon's later news_signal_writer.write_news_signal() refines (preserve or reset
# clock per direction match) via its existing _read_existing logic.

# Subset of Tier-1 keywords with UNAMBIGUOUS XAUUSD direction.
# +1 = bullish XAUUSD (safe-haven flight / dovish surprise / bond stress)
# -1 = bearish XAUUSD (de-escalation / hawkish surprise / risk-on)
# Ambiguous (iran, israel, powell alone, ceasefire alone) are intentionally excluded.
KEYWORD_DIRECTION_MAP: dict[str, int] = {
    # ── +1 · safe-haven flight: military escalation ──
    "missile strike":        +1, "military strike":      +1, "airstrike":            +1,
    "drone strike":          +1, "bombing":              +1, "war declared":         +1,
    "war breaks out":        +1, "ceasefire collapsed":  +1, "ceasefire expires":    +1,
    "fighting resumes":      +1, "tanker attack":        +1, "tanker mined":         +1,
    "tanker hit":            +1, "ship mined":           +1, "tanker boarded":       +1,
    "ship attacked hormuz":  +1, "iran attacks tanker":  +1, "iran attacks ship":    +1,
    "iran attacks escort":   +1, "iran attacks navy":    +1, "iran fires escort":    +1,
    "iran sinks":            +1, "navy strikes iran":    +1, "us strikes iran":      +1,
    "oil embargo":           +1,
    "战争":                  +1, "开战":                 +1, "空袭":                 +1,
    "轰炸":                  +1, "导弹袭击":             +1, "护航船被攻击":         +1,
    "护航船触雷":            +1, "击沉护航":             +1, "击中护航":             +1,
    # ── +1 · financial crisis / flight to safety ──
    "bank collapse":         +1, "bank run":             +1, "market crash":         +1,
    "circuit breaker":       +1, "trading halt":         +1, "sovereign default":    +1,
    "flash crash":           +1, "treasury rout":        +1, "bond rout":            +1,
    "bond massacre":         +1, "long bond crisis":     +1, "auction failed":       +1,
    "weak treasury demand":  +1, "fed emergency qe":     +1, "operation twist":      +1,
    "yield curve control":   +1, "ycc":                  +1, "fed buys treasuries":  +1,
    "fed buying long bonds": +1,
    "金融危机":              +1, "熔断":                 +1, "暴跌":                 +1,
    "长债危机":              +1, "国债拍卖失败":         +1, "美联储紧急购债":       +1,
    "美联储扭转操作":        +1, "收益率曲线控制":       +1,
    # ── +1 · dovish Fed (lower real yields → gold up) ──
    "emergency rate":        +1, "emergency cut":        +1, "rate cut":             +1,
    "powell dovish":         +1, "powell pivot":         +1, "powell rate cut":      +1,
    "rate cut path":         +1, "rate cut signal":      +1, "warsh dovish":         +1,
    "fed pivot":             +1, "easing bias retained": +1, "july rate cut":        +1,
    "july cut":              +1, "september rate cut":   +1, "september cut":        +1,
    "december rate cut":     +1, "december cut":         +1, "support rate cut":     +1,
    "backs rate cut":        +1, "calls for rate cut":   +1, "calls for cut":        +1,
    "降息":                  +1, "鲍威尔鸽派":           +1, "鲍威尔转向":           +1,
    "降息路径":              +1, "7月降息":              +1, "9月降息":              +1,
    "12月降息":              +1, "支持降息":             +1, "呼吁降息":             +1,
    # ── -1 · de-escalation / risk-on ──
    "iran deal signed":      -1, "iran signed":          -1, "iran agreement signed": -1,
    "iran accord":           -1, "peace deal signed":    -1, "mou signed":            -1,
    "iran war ends":         -1, "hormuz open":          -1, "hormuz opens":          -1,
    "hormuz reopens":        -1, "hormuz reopened":      -1, "hormuz cleared":        -1,
    "霍尔木兹重开":          -1,
    # ── -1 · hawkish Fed (higher real yields → gold down) ──
    "rate hike":             -1, "powell hawkish":       -1, "warsh hawkish":         -1,
    "easing bias removed":   -1, "july hike":            -1, "september hike":        -1,
    "加息":                  -1, "鲍威尔鹰派":           -1,
}

# Sidecar paths for fast-path geo signal (mirrors news_signal_writer.py SIGNAL_FILE_PROD{,2})
GEO_SIGNAL_PROD = Path(
    "/data/mt5/data/.wine/drive_c/users/abc/AppData/Roaming/"
    "MetaQuotes/Terminal/Common/Files/news_signal.csv"
)
GEO_SIGNAL_PROD2 = Path(
    "/data/mt5/data-prod2/.wine/drive_c/users/abc/AppData/Roaming/"
    "MetaQuotes/Terminal/Common/Files/news_signal.csv"
)
GEO_PRELIMINARY_TTL_SECS  = 14400  # 4h — matches news_signal_writer.GEO_TTL_SECS (EA staleness=2h is effective cap)
GEO_PRELIMINARY_STRENGTH   = 0.50  # mid-tier (LLM may refine to 0.80 later via same-direction preserve)
GEO_PRELIMINARY_CONFIDENCE = 0.50  # just at gate threshold (GEO_CONF_MIN=0.50)


def infer_direction(matched_kws: list) -> int | None:
    """Return +1/-1 from matched keywords if at least one direction-mapped keyword hit
    AND all hits agree. Returns None if no direction-map hit OR conflicting directions."""
    directions = set()
    for kw in matched_kws:
        if kw in KEYWORD_DIRECTION_MAP:
            directions.add(KEYWORD_DIRECTION_MAP[kw])
    if len(directions) == 1:
        return next(iter(directions))
    return None


# Negation pre-filter — counterfactual / denial / "ruled out" headlines.
# Pure keyword matching has 0% negation awareness; without this filter,
# "Trump denies missile strike" would write fast-path +1 (wrong direction).
# Proximity-based: negation marker within ±NEGATION_WINDOW_CHARS of any
# direction-mapped keyword in the headline → skip fast-path, let LLM handle.
# Conservative: false-positive skip costs 3min latency, false-negative
# wrong direction costs 3min wrong-side hedge. We prefer skip.
import re

NEGATION_WINDOW_CHARS = 30

_NEGATION_PATTERN_EN = re.compile(
    r"\b(?:no|not|never|none|n't|"
    r"deni(?:es|ed|al|es')|deny(?:ing)?|"
    r"reject(?:s|ed|ing)?|refuse(?:s|d|ing)?|"
    r"rule[ds]? out|ruling out|"
    r"despite|without|"
    r"fails? to|failed to|unable to|"
    r"downplays?|downplayed|"
    r"dismiss(?:es|ed|ing)?|"
    r"false|fake|debunked|hoax|untrue|baseless|"
    r"no longer|no more)\b",
    re.IGNORECASE,
)

_NEGATION_PATTERN_CN = re.compile(
    "否认|否决|拒绝|否定|排除|"
    "并未|并无|并没|未会|不会|不再|不曾|"
    "辟谣|澄清|不实|无意|未能|不成|"
    "失败|失利|破裂|搁置|搁浅|流产"
)


def has_negation_near_keyword(text: str, keyword: str) -> bool:
    """Return True if any negation marker is within ±NEGATION_WINDOW_CHARS
    of the keyword's position in text. Case-insensitive for English; Chinese
    uses multi-char markers only to avoid bare-"不" false positives."""
    text_lower = text.lower()
    kw_lower = keyword.lower()
    pos = text_lower.find(kw_lower)
    while pos >= 0:
        ws = max(0, pos - NEGATION_WINDOW_CHARS)
        we = min(len(text), pos + len(kw_lower) + NEGATION_WINDOW_CHARS)
        window = text[ws:we]
        if _NEGATION_PATTERN_EN.search(window) or _NEGATION_PATTERN_CN.search(window):
            return True
        pos = text_lower.find(kw_lower, pos + 1)
    return False


def wake_daemon() -> bool:
    """SIGUSR1 → daemon.py,绕过 60s 轮询让它立即 pickup trigger / refine fast-path CSV。
    Best-effort:daemon 不在或 PID stale 时静默 no-op,不影响 alert_monitor 主链。"""
    try:
        if not DAEMON_PID_FILE.exists():
            return False
        pid = int(DAEMON_PID_FILE.read_text().strip())
        os.kill(pid, signal.SIGUSR1)
        return True
    except (ProcessLookupError, ValueError, OSError, PermissionError):
        return False


def has_directional_negation(text: str, matched_kws: list) -> bool:
    """Check if any direction-mapped matched keyword has a negation marker
    within its proximity window. Used to suppress fast-path on counterfactual
    headlines (LLM will handle the nuance later)."""
    return any(
        has_negation_near_keyword(text, kw)
        for kw in matched_kws
        if kw in KEYWORD_DIRECTION_MAP
    )


def write_preliminary_news_signal(direction: int, headline: str, score: int,
                                   matched_kws: list) -> bool:
    """Fast-path write to news_signal.csv. Bypasses daemon LLM cycle.

    daemon's later news_signal_writer.write_news_signal() will:
      - preserve our written_at + mute_until if LLM concludes same direction
      - reset clock to LLM time if LLM disagrees direction
    Either path is correct: preliminary gives <30s EA hedge; LLM refines within 3min.
    """
    if direction not in (-1, +1):
        return False
    now_ts = int(time.time())
    timestamp = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    mute_until = now_ts + GEO_PRELIMINARY_TTL_SECS
    content = (
        "timestamp,direction,strength,mute_until,confidence\n"
        f"{timestamp},{direction:+d},{GEO_PRELIMINARY_STRENGTH:.3f},"
        f"{mute_until},{GEO_PRELIMINARY_CONFIDENCE:.3f}\n"
    )
    written = []
    for sig_file in (GEO_SIGNAL_PROD, GEO_SIGNAL_PROD2):
        try:
            sig_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = sig_file.with_suffix(".csv.tmp")
            tmp.write_text(content, encoding="utf-8")
            os.replace(str(tmp), str(sig_file))
            written.append(sig_file.parent.parent.name)  # 'data' or 'data-prod2' marker
        except Exception as e:
            logger.warning(f"[fast-path] 写入 {sig_file} 失败: {e}")
    if written:
        logger.warning(
            f"⚡ FAST-PATH geo signal: dir={direction:+d} str={GEO_PRELIMINARY_STRENGTH} "
            f"conf={GEO_PRELIMINARY_CONFIDENCE} mute=+4h kws={matched_kws} "
            f"score={score} | {headline[:70]}"
        )
        return True
    return False


# ─── 工具函数 ─────────────────────────────────────────────────────────────────

def _setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("alert_monitor")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("[%(asctime)s] [警报监控] %(message)s", "%Y-%m-%d %H:%M:%S")
    # 控制台
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    # 文件
    fh = logging.FileHandler(LOG_DIR / "alert_monitor.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


logger = _setup_logging()


def _unlink_quiet(p: Path):
    try:
        p.unlink()
    except Exception:
        pass


def _item_uid(text: str) -> str:
    return hashlib.md5(text.encode("utf-8", errors="replace")).hexdigest()


def load_seen() -> set:
    try:
        if SEEN_FILE.exists():
            return set(json.loads(SEEN_FILE.read_text(encoding="utf-8")))
    except Exception:
        pass
    return set()


def save_seen(seen: set):
    try:
        # 用排序列表保证裁剪的确定性（按字典序，与先后无关但可重现）
        items = sorted(seen)[-MAX_SEEN:]
        tmp = SEEN_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(items), encoding="utf-8")
        os.replace(str(tmp), str(SEEN_FILE))
    except Exception:
        pass


def score_headline(title: str, description: str = "") -> tuple:
    """返回 (total_score, matched_keywords)。
    每个关键词只计一次分，无论在文中出现多少次（防止重复词刷高分）。
    """
    text = (title + " " + description).lower()
    total = 0
    matched = []
    for kw, score in KEYWORDS.items():
        if kw in text and kw not in matched:  # 已匹配的 kw 跳过，防重复计分
            total += score
            matched.append(kw)
    return total, matched


# ─── RSS 解析 ─────────────────────────────────────────────────────────────────

def fetch_feed(name: str, url: str) -> list:
    """抓取并解析 RSS/Atom，返回 list of dict。"""
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0 XAUAlertBot/1.0"})
        with urlopen(req, timeout=12) as resp:
            content = resp.read()
        root = ET.fromstring(content)

        items = []
        # RSS 2.0
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link")  or "").strip()
            desc  = (item.findtext("description") or "").strip()
            guid  = (item.findtext("guid")  or link or title)
            if title:
                items.append({"title": title, "link": link, "desc": desc,
                              "guid": guid, "source": name})
        # Atom
        if not items:
            atom = "http://www.w3.org/2005/Atom"
            for entry in root.iter(f"{{{atom}}}entry"):
                t  = entry.find(f"{{{atom}}}title")
                l  = entry.find(f"{{{atom}}}link")
                s  = entry.find(f"{{{atom}}}summary")
                i  = entry.find(f"{{{atom}}}id")
                title = (t.text if t is not None else "").strip()
                link  = (l.get("href", "") if l is not None else "").strip()
                desc  = (s.text if s is not None else "").strip()
                guid  = (i.text if i is not None else link or title).strip()
                if title:
                    items.append({"title": title, "link": link, "desc": desc,
                                  "guid": guid, "source": name})
        return items
    except URLError as e:
        logger.warning(f"网络错误 [{name}]: {e}")
        return []
    except ET.ParseError as e:
        logger.warning(f"XML解析错误 [{name}]: {e}")
        return []
    except Exception as e:
        logger.warning(f"未知错误 [{name}]: {e}")
        return []


# ─── 触发逻辑 ──────────────────────────────────────────────────────────────────

def load_recent_triggers() -> list:
    """读取最近触发记录，返回 [(epoch_ts, frozenset(kws)), ...]，并按窗口剪枝。"""
    if not RECENT_TRIGGERS_FILE.exists():
        return []
    try:
        items = json.loads(RECENT_TRIGGERS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []
    cutoff = time.time() - STORY_COOLDOWN_MIN * 60
    return [(ts, frozenset(kws)) for ts, kws in items if ts >= cutoff]


def save_recent_triggers(triggers: list):
    try:
        items = [[ts, sorted(kws)] for ts, kws in triggers]
        tmp = RECENT_TRIGGERS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(items), encoding="utf-8")
        os.replace(str(tmp), str(RECENT_TRIGGERS_FILE))
    except Exception:
        pass


def is_duplicate_story(matched_kws: list, recent: list) -> tuple:
    """检查是否与最近触发故事重叠。返回 (is_dup, jaccard, matched_recent_kws)。"""
    new = frozenset(matched_kws)
    if not new:
        return False, 0.0, frozenset()
    best_j = 0.0
    best_kws = frozenset()
    for _, old in recent:
        if not old:
            continue
        inter = len(new & old)
        union = len(new | old)
        j = inter / union if union else 0.0
        if j > best_j:
            best_j = j
            best_kws = old
    return best_j >= STORY_JACCARD_THRESHOLD, best_j, best_kws


def write_trigger(headline: str, source: str, score: int, matched_kws: list):
    trigger = {
        "time":     datetime.now(timezone.utc).isoformat(),
        "headline": headline,
        "source":   source,
        "score":    score,
        "keywords": matched_kws,
    }
    tmp = TRIGGER_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(trigger, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(str(tmp), str(TRIGGER_FILE))
    logger.warning(f"🚨 突发触发！score={score} [{source}] {headline[:80]}")


def append_geo_alert(headline: str, source: str, score: int, matched_kws: list):
    """将高分事件追加到 AITraderV5 geo_alerts sidecar，绕过 daemon 延迟直达 Opus prompt。"""
    if score < GEO_ALERT_SCORE_MIN:
        return
    try:
        now = datetime.now(timezone.utc)
        cutoff = now.timestamp() - GEO_ALERT_MAX_AGE_H * 3600
        # 读取现有条目
        existing = []
        if GEO_ALERT_FILE.exists():
            try:
                data = json.loads(GEO_ALERT_FILE.read_text(encoding="utf-8"))
                existing = data.get("alerts", [])
            except Exception:
                existing = []
        # 剔除过期条目
        existing = [
            a for a in existing
            if datetime.fromisoformat(a["time"]).timestamp() >= cutoff
        ]
        # 追加新条目（相同 headline 5min 内不重复）
        recent_headlines = {
            a["headline"] for a in existing
            if now.timestamp() - datetime.fromisoformat(a["time"]).timestamp() < 300
        }
        if headline not in recent_headlines:
            existing.append({
                "time":     now.isoformat(),
                "headline": headline,
                "source":   source,
                "score":    score,
                "keywords": matched_kws,
            })
        payload = {"last_updated": now.isoformat(), "alerts": existing}
        GEO_ALERT_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = GEO_ALERT_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(str(tmp), str(GEO_ALERT_FILE))
    except Exception as e:
        logger.warning(f"[geo_alert] 写入失败: {e}")


def poll_once(seen: set, recent_triggers: list) -> tuple:
    """轮询所有 RSS，返回 (new_seen, triggered, new_recent_triggers)。"""
    new_seen = set(seen)
    new_recent = list(recent_triggers)
    triggered = False

    for name, url in RSS_FEEDS:
        items = fetch_feed(name, url)
        for item in items:
            uid = _item_uid(item["guid"] or item["title"])
            if uid in new_seen:
                continue
            new_seen.add(uid)

            score, matched = score_headline(item["title"], item["desc"])
            if score >= TRIGGER_THRESHOLD:
                # 故事级冷却：同关键词集合在 90min 窗口内 Jaccard ≥ 0.5 视为同故事
                is_dup, j, dup_kws = is_duplicate_story(matched, new_recent)
                if is_dup:
                    logger.info(
                        f"故事冷却中，跳过: score={score} jaccard={j:.2f} "
                        f"kws={matched} ~ {sorted(dup_kws)} | {item['title'][:60]}"
                    )
                    continue
                # 无论是否触发，高分事件都写入 sidecar 供 Opus 实时读取
                append_geo_alert(item["title"], item["source"], score, matched)
                # Fast-path geo gate: 若 keyword 方向无歧义，立即写 news_signal.csv 给 EA
                # 绕过 daemon LLM cycle (~3min)，让 EA <30s 感知。daemon 后续 refine。
                wrote_anything = False
                fast_dir = infer_direction(matched)
                if fast_dir is not None:
                    text_for_negation = item["title"] + " " + item.get("desc", "")
                    if has_directional_negation(text_for_negation, matched):
                        logger.info(
                            f"[fast-path] negation 跳过: dir={fast_dir:+d} "
                            f"kws={matched} | {item['title'][:60]}"
                        )
                    else:
                        write_preliminary_news_signal(fast_dir, item["title"], score, matched)
                        wrote_anything = True
                # 如果触发文件已存在（daemon 尚未处理上一个），不覆盖，只记录
                if TRIGGER_FILE.exists():
                    logger.info(f"触发器待处理中，跳过: score={score} {item['title'][:60]}")
                else:
                    write_trigger(item["title"], item["source"], score, matched)
                    new_recent.append((time.time(), frozenset(matched)))
                    triggered = True
                    wrote_anything = True
                # 主动唤醒 daemon — fast-path 写了 CSV 或 trigger 文件都通知它
                # (SIGUSR1 → daemon 跳出 sleep,LLM cycle 立即跑,refine CSV)
                if wrote_anything and wake_daemon():
                    logger.info(f"[wake-daemon] SIGUSR1 → daemon (refine ASAP)")

    # 剪枝过期记录
    cutoff = time.time() - STORY_COOLDOWN_MIN * 60
    new_recent = [(ts, kws) for ts, kws in new_recent if ts >= cutoff]
    return new_seen, triggered, new_recent


# ─── 主循环 ───────────────────────────────────────────────────────────────────

def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # PID 单例保护
    my_pid = os.getpid()
    if PID_FILE.exists():
        try:
            old_pid = int(PID_FILE.read_text().strip())
            os.kill(old_pid, 0)
            logger.error(f"警报监控已在运行 (PID {old_pid})，退出")
            return
        except (ProcessLookupError, ValueError):
            pass
        except Exception:
            pass
    PID_FILE.write_text(str(my_pid))
    atexit.register(lambda: _unlink_quiet(PID_FILE))

    def _on_signal(sig, frame):
        logger.info(f"收到信号 {sig}，退出")
        sys.exit(0)
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    logger.info(f"启动 (PID={my_pid})，轮询间隔={POLL_INTERVAL}s，触发阈值={TRIGGER_THRESHOLD}")

    # 首次扫描：只建立基线，不触发（避免历史新闻误报）
    logger.info("首次扫描：建立基线...")
    seen = load_seen()
    for name, url in RSS_FEEDS:
        items = fetch_feed(name, url)
        for item in items:
            seen.add(_item_uid(item["guid"] or item["title"]))
    save_seen(seen)
    recent_triggers = load_recent_triggers()
    logger.info(
        f"基线完成，已记录 {len(seen)} 条标题，"
        f"故事冷却记录 {len(recent_triggers)} 条，开始实时监控"
    )

    while True:
        time.sleep(POLL_INTERVAL)
        try:
            seen, triggered, recent_triggers = poll_once(seen, recent_triggers)
            save_seen(seen)
            save_recent_triggers(recent_triggers)
            if not triggered:
                logger.info(f"扫描完成，无突发事件（追踪 {len(seen)} 条）")
        except Exception as e:
            logger.error(f"轮询错误: {e}")


if __name__ == "__main__":
    main()
