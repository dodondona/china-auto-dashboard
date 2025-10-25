#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Autohome 口コミ取得（Playwright）→ CSV ＆ 簡易サマリー
完全オフライン版：OpenAI 依存なし

使い方:
  python tools/koubei_summary_playwright.py 7806 5
"""

import sys
import re
import time
import argparse
from dataclasses import dataclass, asdict
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ----------------------------
# 設定
# ----------------------------
LIST_URL_FIRST = "https://k.autohome.com.cn/{vid}#pvareaid=3454440"
LIST_URL_PAGED = "https://k.autohome.com.cn/{vid}/index_{page}.html?#listcontainer"

# Autohome 側の構造は変化し得るため、複数候補セレクタでフォールバック
CARD_SELECTORS = [
    "div.koubei-list>div.item",              # 旧来パターン
    "div#listcontainer div.list-box div.li", # 代替パターン
    "div#listcontainer .mouthcon",           # さらに代替
]

TITLE_SELECTORS = [
    ".title a", ".tit a", "h3 a", ".mouth-title a"
]

TEXT_SELECTORS = [
    ".text-con", ".text", ".mouth-main .text-con", ".con .text"
]

DATE_SELECTORS = [
    ".date", ".time", ".mouth-main .time", ".user .time"
]

RATING_SELECTORS = [
    ".rating .score", ".mouth-main .score", ".score"
]

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
    """完全オフラインの超簡易判定（雰囲気レベル）。"""
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

def split_pros_cons_zh(text: str) -> (str, str):
    """
    口コミ本文から「優点/缺点」っぽい句をざっくり抽出（中国語のまま）。
    サイトの構造変化に強いよう、キーワードで粗くスプリット。
    """
    if not text:
        return "", ""
    # 代表的キーワード
    pros_markers = ["优点", "優點", "优", "优处", "满意", "喜欢", "优点：", "優點："]
    cons_markers = ["缺点", "缺陷", "不足", "不满", "问题", "槽点", "缺点：", "不足："]
    pros, cons = [], []

    # 文単位に分割
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
            # キーワードなし：ヒューリスティックに寄せる
            if "静" in s or "省" in s or "舒" in s or "值" in s or "好" in s:
                pros.append(s)
            elif "噪" in s or "硬" in s or "短" in s or "差" in s or "慢" in s:
                cons.append(s)

    # " / " 区切り（既存 storywriter が前提にしている形式）
    pros_s = " / ".join(pros[:6])
    cons_s = " / ".join(cons[:6])
    return pros_s, cons_s

def parse_list_html(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    items = []
    cards = []
    for sel in CARD_SELECTORS:
        cards = soup.select(sel)
        if cards:
            break
    for card in cards:
        title = pick_first_text(card, TITLE_SELECTORS)
        text = pick_first_text(card, TEXT_SELECTORS)
        date = pick_first_text(card, DATE_SELECTORS)
        rating = pick_first_text(card, RATING_SELECTORS)
        rid_match = re.search(r"data\-koubeiid=['\"]?(\d+)", str(card))
        review_id = rid_match.group(1) if rid_match else ""
        text = clean_text(text)
        pros_zh, cons_zh = split_pros_cons_zh(text)
        senti = quick_sentiment(text)
        row = ReviewRow(
            review_id=review_id,
            date=date,
            rating=rating,
            title=title,
            text=text,
            pros_zh=pros_zh,
            cons_zh=cons_zh,
            sentiment=senti
        )
        items.append(asdict(row))
    return items

def fetch_reviews(vid: str, pages: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        def get(url: str) -> str:
            page.goto(url, wait_until="load", timeout=60000)
            # Lazy load 防止のため軽く待機
            time.sleep(1.5)
            return page.content()

        # 1ページ目
        html = get(LIST_URL_FIRST.format(vid=vid))
        out.extend(parse_list_html(html))

        # 2ページ目以降
        for p in range(2, pages + 1):
            url = LIST_URL_PAGED.format(vid=vid, page=p)
            html = get(url)
            out.extend(parse_list_html(html))

        browser.close()
    return out

def write_outputs(vid: str, rows: List[Dict[str, Any]]):
    df = pd.DataFrame(rows)
    # 最低限の列構造（storywriter が使う列を確保）
    if "pros_zh" not in df.columns:
        df["pros_zh"] = ""
    if "cons_zh" not in df.columns:
        df["cons_zh"] = ""
    if "sentiment" not in df.columns:
        df["sentiment"] = "mixed"

    csv_path = f"autohome_reviews_{vid}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    # 簡易統計
    total = len(df)
    pos = int((df["sentiment"].astype(str).str.lower()=="positive").sum())
    neg = int((df["sentiment"].astype(str).str.lower()=="negative").sum())
    mix = int((df["sentiment"].astype(str).str.lower()=="mixed").sum())

    def ratio(n: int) -> float:
        return 0.0 if total==0 else round(n/total*100, 1)

    # 頻出語（ざっくり：スラッシュ区切りを集計）
    def split_terms(series: pd.Series):
        return series.dropna().astype(str).str.split(" / ").explode().str.strip()

    pros_top = split_terms(df["pros_zh"]).value_counts().head(10)
    cons_top = split_terms(df["cons_zh"]).value_counts().head(10)

    lines = []
    lines.append(f"【車両ID: {vid}】口コミ取得サマリー")
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
    lines.append("※ 完全オフライン抽出（OpenAI 不使用）")

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
    print(f"✅ autohome_reviews_{vid}.csv / _summary.txt を生成しました（OpenAI 不使用）")

if __name__ == "__main__":
    main()
