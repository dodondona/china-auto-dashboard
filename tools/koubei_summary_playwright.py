#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Autohome 口コミ取得（Playwright）→ CSV & 簡易サマリー
完全オフライン版（OpenAI 不使用）／堅牢化版

使い方:
  python tools/koubei_summary_playwright.py 448 5
"""

import re
import time
import argparse
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Tuple
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# URL
LIST_URL_FIRST = "https://k.autohome.com.cn/{vid}#pvareaid=3454440"
LIST_URL_PAGED = "https://k.autohome.com.cn/{vid}/index_{page}.html?#listcontainer"

# 複数パターンのカードセレクタ（PC/モバイル想定）
CARD_PATTERNS = [
    {
        "cards": [
            "div#listcontainer div.list-box div.li",  # モバイル新
            "div.mouthcon",                           # 汎用
        ],
        "title": [".title a", ".tit a", ".tt a", ".title", "h3 a"],
        "text":  [".text-con", ".con .text", ".mouth-main .text-con", ".contxt", ".tx", ".text"],
        "date":  [".date", ".time", ".user .time", ".mouth-main .time"],
        "score": [".score", ".rating .score", ".mouth-main .score"],
    },
    {
        "cards": [
            "div.koubei-list > div.item",             # PC/旧来
        ],
        "title": [".title a", "h3 a", ".mouth-title a", ".tit a"],
        "text":  [".text-con", ".text", ".mouth-main .text-con"],
        "date":  [".date", ".time", ".user .time"],
        "score": [".score", ".rating .score"],
    },
]

WAIT_SELS = ["#listcontainer", ".koubei-list", ".list-box", ".mouthcon"]  # いずれか出現を待つ
SCROLL_STEPS = 10
SCROLL_PAUSE = 0.8
CLICK_MORE_SELECTORS = [
    "a.more", "button.more", "a:has-text('更多')", "a:has-text('展开')"
]

@dataclass
class ReviewRow:
    review_id: str
    date: str
    rating: str
    title: str
    text: str
    pros_zh: str
    cons_zh: str
    sentiment: str

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def pick_first_text(el, selectors) -> str:
    if el is None:
        return ""
    for sel in selectors:
        node = el.select_one(sel)
        if node and node.get_text(strip=True):
            return clean_text(node.get_text(" ", strip=True))
    return ""

def quick_sentiment(text: str) -> str:
    t = text.lower()
    pos_kw = ["满意", "喜欢", "安静", "省", "划算", "优秀", "舒服", "值得", "推荐", "好"]
    neg_kw = ["不满", "一般", "噪音", "差", "短", "硬", "糟糕", "抱怨", "问题", "坏"]
    pos = sum(1 for k in pos_kw if k in t)
    neg = sum(1 for k in neg_kw if k in t)
    if pos > neg:
        return "positive"
    if neg > pos:
        return "negative"
    return "mixed"

def split_pros_cons_zh(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""
    pros_markers = ["优点", "優點", "优处", "满意", "喜欢", "优点：", "優點："]
    cons_markers = ["缺点", "缺陷", "不足", "不满", "问题", "槽点", "缺点：", "不足："]
    pros, cons = [], []
    sentences = re.split(r"[。！？!?\n]+", text)
    for s in sentences:
        s = s.strip()
        if not s:
            continue
        if any(m in s for m in cons_markers):
            cons.append(s)
        elif any(m in s for m in pros_markers):
            pros.append(s)
        else:
            if re.search(r"[静省舒值好优爽稳顺]", s):
                pros.append(s)
            elif re.search(r"[噪硬短差慢抖颠烦糟]", s):
                cons.append(s)
    return " / ".join(pros[:6]), " / ".join(cons[:6])

def parse_with_patterns(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict[str, Any]] = []
    for pat in CARD_PATTERNS:
        cards = []
        for csel in pat["cards"]:
            hit = soup.select(csel)
            if hit:
                cards = hit
                break
        if not cards:
            continue
        for card in cards:
            title = pick_first_text(card, pat["title"])
            text  = pick_first_text(card, pat["text"])
            date  = pick_first_text(card, pat["date"])
            score = pick_first_text(card, pat["score"])
            rid = ""
            m = re.search(r"data\-koubeiid=['\"]?(\d+)", str(card))
            if not m:
                a = card.select_one("a[href*='koubei']")
                if a and a.has_attr("href"):
                    mm = re.search(r"(\d+)", a["href"])
                    rid = mm.group(1) if mm else ""
            else:
                rid = m.group(1)
            text = clean_text(text)
            pros_zh, cons_zh = split_pros_cons_zh(text)
            rows.append(asdict(ReviewRow(
                review_id=rid, date=date, rating=score, title=title,
                text=text, pros_zh=pros_zh, cons_zh=cons_zh, sentiment=quick_sentiment(text)
            )))
        if rows:
            break
    return rows

def save_debug(vid: str, pno: int, page):
    html = page.content()
    with open(f"debug_{vid}_p{pno}.html", "w", encoding="utf-8") as f:
        f.write(html)
    page.screenshot(path=f"screenshot_{vid}_p{pno}.png", full_page=True)

def wait_any(page) -> None:
    # どれか出るまで最大15秒待つ
    for sel in WAIT_SELS:
        try:
            page.wait_for_selector(sel, timeout=5000)
            return
        except PWTimeout:
            continue
    # 何も出なかった場合も次へ（スクロールで出るケースあり）

def scroll_and_expand(page):
    # “もっと見る”があれば押す
    for sel in CLICK_MORE_SELECTORS:
        try:
            loc = page.locator(sel)
            if loc and loc.count() > 0:
                for i in range(min(loc.count(), 3)):
                    try:
                        loc.nth(i).click(timeout=1000)
                    except Exception:
                        pass
        except Exception:
            pass
    # 段階スクロール
    last_h = 0
    for _ in range(SCROLL_STEPS):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(SCROLL_PAUSE)
        try:
            h = page.evaluate("document.body.scrollHeight")
        except Exception:
            h = 0
        if h == last_h:
            break
        last_h = h

def open_context(pw, ua_mode: str):
    if ua_mode == "pc":
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        viewport = {"width": 1366, "height": 900}
    else:
        ua = "Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Mobile Safari/537.36"
        viewport = {"width": 412, "height": 915}
    ctx = pw.chromium.launch(headless=True).new_context(
        viewport=viewport,
        user_agent=ua,
        locale="zh-CN",
        extra_http_headers={"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"},
    )
    return ctx

def fetch_reviews(vid: str, pages: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with sync_playwright() as pw:
        # UA: PC → モバイル の順にフォールバック
        for mode in ["pc", "mobile"]:
            ctx = open_context(pw, mode)
            page = ctx.new_page()
            try:
                # 1ページ目
                url1 = LIST_URL_FIRST.format(vid=vid)
                page.goto(url1, wait_until="networkidle", timeout=60000)
                wait_any(page)
                scroll_and_expand(page)
                html = page.content()
                rows = parse_with_patterns(html)
                print(f"[{vid}][{mode}] p1: {len(rows)} reviews")
                if len(rows) == 0:
                    save_debug(vid, 1, page)
                tmp = rows[:]
                # 2以降
                for p in range(2, pages + 1):
                    url = LIST_URL_PAGED.format(vid=vid, page=p)
                    page.goto(url, wait_until="networkidle", timeout=60000)
                    wait_any(page)
                    scroll_and_expand(page)
                    html = page.content()
                    rows = parse_with_patterns(html)
                    print(f"[{vid}][{mode}] p{p}: {len(rows)} reviews")
                    if len(rows) == 0:
                        save_debug(vid, p, page)
                    tmp.extend(rows)
                if tmp:
                    out = tmp
                    print(f"[{vid}] success with UA mode: {mode}, total={len(out)}")
                    break
            finally:
                try:
                    page.context.browser.close()
                except Exception:
                    pass
    return out

def write_outputs(vid: str, rows: List[Dict[str, Any]]):
    df = pd.DataFrame(rows)
    for c in ["pros_zh", "cons_zh", "sentiment"]:
        if c not in df.columns:
            df[c] = "" if c != "sentiment" else "mixed"
    csv_path = f"autohome_reviews_{vid}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    total = len(df)
    pos = int((df["sentiment"].astype(str).str.lower()=="positive").sum())
    neg = int((df["sentiment"].astype(str).str.lower()=="negative").sum())
    mix = int((df["sentiment"].astype(str).str.lower()=="mixed").sum())
    def ratio(n: int) -> float:
        return 0.0 if total==0 else round(n/total*100, 1)

    def split_terms(series: pd.Series):
        return series.dropna().astype(str).str.split(" / ").explode().str.strip()

    pros_top = split_terms(df["pros_zh"]).value_counts().head(10)
    cons_top = split_terms(df["cons_zh"]).value_counts().head(10)

    lines = []
    lines.append(f"【車両ID: {vid}】口コミ取得サマリー（OpenAI不使用）")
    lines.append(f"件数: {total}")
    lines.append(f"Sentiment: Positive {pos} ({ratio(pos)}%), Mixed {mix} ({ratio(mix)}%), Negative {neg} ({ratio(neg)}%)")
    lines.append("")
    lines.append("＜ポジティブ頻出（zh 抜粋）＞")
    for t, c in pros_top.items():
        lines.append(f"- {t} ({c})")
    lines.append("")
    lines.append("＜ネガティブ頻出（zh 抜粋）＞")
    for t, c in cons_top.items():
        lines.append(f"- {t} ({c})")
    lines.append("")
    lines.append("※ 0件ページがある場合は debug_*.html / screenshot_*.png を保存しています。")

    with open(f"autohome_reviews_{vid}_summary.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("vehicle_id", help="Autohome vehicle id (例: 448)")
    ap.add_argument("pages", type=int, help="取得ページ数（例: 5）")
    args = ap.parse_args()
    vid = args.vehicle_id.strip()
    pages = max(1, int(args.pages))

    rows = fetch_reviews(vid, pages)
    write_outputs(vid, rows)
    print(f"✅ autohome_reviews_{vid}.csv / _summary.txt 生成完了（OpenAI 不使用）")

if __name__ == "__main__":
    main()
