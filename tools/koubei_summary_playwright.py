#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Autohome 口コミ取得（Playwright）→ CSV & 簡易サマリー
完全オフライン版（OpenAI 不使用）

使い方:
  python tools/koubei_summary_playwright.py 7806 5
"""

import sys
import re
import time
import argparse
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Tuple
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ----------------------------
# 設定
# ----------------------------
LIST_URL_FIRST = "https://k.autohome.com.cn/{vid}#pvareaid=3454440"
LIST_URL_PAGED = "https://k.autohome.com.cn/{vid}/index_{page}.html?#listcontainer"

# モバイル/PC 両方を網羅するセレクタ群
CARD_SELECTORS = [
    "div#listcontainer div.list-box div.li",     # k.autohome のモバイル新
    "div.koubei-list > div.item",                # 旧来
    "div#listcontainer .mouthcon",               # 別表記
    "div.mouthcon",                              # 予備
]
TITLE_SELECTORS = [".title a", ".tit a", "h3 a", ".mouth-title a", ".tt a", ".title"]
TEXT_SELECTORS  = [".text-con", ".text", ".con .text", ".mouth-main .text-con", ".contxt", ".tx"]
DATE_SELECTORS  = [".date", ".time", ".user .time", ".mouth-main .time"]
RATING_SELECTORS= [".score", ".rating .score", ".mouth-main .score"]

SCROLL_STEPS = 8          # 遅延ロードのためのスクロール回数
SCROLL_PAUSE = 0.8        # 各スクロール後の待機秒
WAIT_SELECTOR = "#listcontainer"  # 初期描画待ち

# ----------------------------
# データ構造
# ----------------------------
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

# ----------------------------
# ユーティリティ
# ----------------------------
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
    """完全オフラインの簡易判定（ざっくり傾向把握用）"""
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
    """
    本文から“優点/缺点”っぽい文を抽出（ヒューリスティック）
    """
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
            # キーワードなしは雰囲気で振り分け
            if re.search(r"[静省舒值好优爽稳顺]", s):
                pros.append(s)
            elif re.search(r"[噪硬短差慢抖颠烦糟]", s):
                cons.append(s)
    return " / ".join(pros[:6]), " / ".join(cons[:6])

def parse_list_html(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    # カード収集
    cards = []
    for sel in CARD_SELECTORS:
        cards = soup.select(sel)
        if cards:
            break
    items: List[Dict[str, Any]] = []
    for card in cards:
        title = pick_first_text(card, TITLE_SELECTORS)
        text  = pick_first_text(card, TEXT_SELECTORS)
        date  = pick_first_text(card, DATE_SELECTORS)
        rating= pick_first_text(card, RATING_SELECTORS)

        rid = ""
        m = re.search(r"data\-koubeiid=['\"]?(\d+)", str(card))
        if m:
            rid = m.group(1)
        else:
            # hrefなどから救済
            a = card.select_one("a[href*='koubei']")
            if a and a.has_attr("href"):
                mm = re.search(r"(\d+)", a["href"])
                if mm:
                    rid = mm.group(1)

        text = clean_text(text)
        pros_zh, cons_zh = split_pros_cons_zh(text)
        senti = quick_sentiment(text)
        items.append(asdict(ReviewRow(
            review_id=rid, date=date, rating=rating, title=title,
            text=text, pros_zh=pros_zh, cons_zh=cons_zh, sentiment=senti
        )))
    return items

def save_debug_html(vid: str, page_no: int, html: str):
    path = f"debug_{vid}_p{page_no}.html"
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass

def load_page_with_scroll(page, url: str) -> str:
    page.goto(url, wait_until="load", timeout=60000)
    try:
        page.wait_for_selector(WAIT_SELECTOR, timeout=15000)
    except PWTimeout:
        # 無くてもスクロールでレンダされることがあるので続行
        pass

    # 遅延ロード対策：段階スクロール
    last_height = 0
    for _ in range(SCROLL_STEPS):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(SCROLL_PAUSE)
        try:
            height = page.evaluate("document.body.scrollHeight")
        except Exception:
            height = 0
        if height == last_height:
            break
        last_height = height
    return page.content()

def fetch_reviews(vid: str, pages: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 412, "height": 915},  # モバイル相当
            user_agent="Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Mobile Safari/537.36",
            locale="zh-CN",
            extra_http_headers={
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            },
        )
        # 画像やフォントはブロック（軽量化）
        ctx.route("**/*", lambda route: route.abort()
                  if route.request.resource_type in {"image", "font"}
                  else route.continue_())
        page = ctx.new_page()

        # 1ページ目
        url1 = LIST_URL_FIRST.format(vid=vid)
        html = load_page_with_scroll(page, url1)
        items = parse_list_html(html)
        print(f"[{vid}] p1: {len(items)} reviews")
        if len(items) == 0:
            save_debug_html(vid, 1, html)
        out.extend(items)

        # 2ページ目以降
        for p in range(2, pages + 1):
            url = LIST_URL_PAGED.format(vid=vid, page=p)
            html = load_page_with_scroll(page, url)
            items = parse_list_html(html)
            print(f"[{vid}] p{p}: {len(items)} reviews")
            if len(items) == 0:
                save_debug_html(vid, p, html)
            out.extend(items)

        browser.close()
    return out

def write_outputs(vid: str, rows: List[Dict[str, Any]]):
    df = pd.DataFrame(rows)
    # storywriter が使う列を確保
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
    lines.append("※ 0件ページがある場合は debug_*.html を保存しています。")

    with open(f"autohome_reviews_{vid}_summary.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("vehicle_id", help="Autohome vehicle id (例: 7806)")
    ap.add_argument("pages", type=int, help="取得ページ数（例: 5）")
    args = ap.parse_args()

    vid = args.vehicle_id.strip()
    pages = max(1, int(args.pages))

    rows = fetch_reviews(vid, pages)
    write_outputs(vid, rows)
    print(f"✅ autohome_reviews_{vid}.csv / _summary.txt 生成完了（OpenAI 不使用）")

if __name__ == "__main__":
    main()
