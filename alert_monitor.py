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
                # 如果触发文件已存在（daemon 尚未处理上一个），不覆盖，只记录
                if TRIGGER_FILE.exists():
                    logger.info(f"触发器待处理中，跳过: score={score} {item['title'][:60]}")
                else:
                    write_trigger(item["title"], item["source"], score, matched)
                    new_recent.append((time.time(), frozenset(matched)))
                    triggered = True

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
