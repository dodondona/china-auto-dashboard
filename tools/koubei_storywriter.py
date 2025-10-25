#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Autohome 口碑スクレイピング（Playwright, API不要）
出力:
  autohome_reviews_<ID>.csv
  autohome_reviews_<ID>_summary.txt

使い方:
  python tools/koubei_summary_playwright.py 7806 5
"""
import sys, os, re, time, csv
import argparse
from typing import List, Dict, Any
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd

BASE1 = "https://k.autohome.com.cn/{vid}#pvareaid=3454440"              # 1ページ目
BASEX = "https://k.autohome.com.cn/{vid}/index_{p}.html?#listcontainer" # 2ページ目以降

def build_urls(vid: str, pages: int) -> List[str]:
    urls = []
    for p in range(1, pages+1):
        if p == 1:
            urls.append(BASE1.format(vid=vid))
        else:
            urls.append(BASEX.format(vid=vid, p=p))
    return urls

def fetch_html(urls: List[str], timeout_ms=30000) -> List[str]:
    htmls = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context()
        for u in urls:
            page = ctx.new_page()
            page.goto(u, timeout=timeout_ms, wait_until="load")
            # スクロールで lazy 部分を読み込み
            for _ in range(3):
                page.mouse.wheel(0, 2000)
                time.sleep(0.6)
            htmls.append(page.content())
            page.close()
        ctx.close()
        browser.close()
    return htmls

def parse_reviews(html: str) -> List[Dict[str, Any]]:
    """
    Autohomeの口碑は構造が頻繁に変わるため、堅めの抽出:
    - レビューアイテムを包含する大きめのカードdivを広めに探索
    - タイトル/本文/優点/缺点/评分 等のキーワードで抽出
    """
    soup = BeautifulSoup(html, "html.parser")
    cards = []
    # よくあるリスト領域: idやclassに listcontainer / koubei / mouth などが含まれる
    candidates = soup.select('div[id*="list"], div[class*="koubei"], div[class*="mouth"], ul[class*="list"]')
    if not candidates:
        candidates = [soup]  # 最終保険: 全体から拾う
    for root in candidates:
        # 1レビューっぽい塊（タイトル+本文）を推定
        items = root.find_all(["div","li","article","section"], recursive=True)
        for it in items:
            txt = it.get_text(" ", strip=True)
            if not txt or len(txt) < 60:  # 短すぎる要素は除外
                continue
            # 口コミの特徴キーワードでフィルタ
            if not re.search(r"(优点|優点|缺点|缺陷|不足|评价|口碑|点评|试驾|试乘|试用|体验|续航|静音|噪音|加速|刹车|空间|内饰|外观|配置)", txt):
                continue
            # タイトル候補
            title = ""
            h = it.find(["h1","h2","h3","h4"])
            if h and h.get_text(strip=True):
                title = h.get_text(strip=True)
            # 優点/缺点 部分
            pros_zh, cons_zh = "", ""
            pros_m = re.search(r"(优点|優点)[:：]\s*(.+?)($|缺点|不足|—|-)", txt)
            if pros_m:
                pros_zh = pros_m.group(2).strip()
            cons_m = re.search(r"(缺点|不足)[:：]\s*(.+?)($|优点|優点|—|-)", txt)
            if cons_m:
                cons_zh = cons_m.group(2).strip()
            # 本文（雑に）
            body = txt
            # スコア（あれば）
            score = None
            s_m = re.search(r"(\d\.\d|\d)分", txt)
            if s_m:
                try:
                    score = float(s_m.group(1))
                except:
                    score = None
            # 件の信頼性: pros/consどちらか＋本文の長さ
            if (pros_zh or cons_zh or (score is not None)) and len(body) >= 80:
                cards.append({
                    "title_zh": title,
                    "pros_zh": pros_zh,
                    "cons_zh": cons_zh,
                    "body_zh": body,
                    "score": score
                })
    return cards

def summarize_counts(rows: List[Dict[str, Any]]) -> str:
    df = pd.DataFrame(rows)
    total = len(df)
    if total == 0:
        return "レビューが取得できませんでした。"
    # 簡易“頻出語”集計（pros/consを term 分解）
    def split_terms(series):
        s = series.fillna("").astype(str).str.split(r"[，,。；;、/｜|]+").explode().str.strip()
        s = s[s != ""]
        return s
    pros_top = split_terms(df["pros_zh"]).value_counts().head(10) if "pros_zh" in df else pd.Series(dtype=int)
    cons_top = split_terms(df["cons_zh"]).value_counts().head(10) if "cons_zh" in df else pd.Series(dtype=int)
    lines = [f"件数: {total}"]
    if not pros_top.empty:
        lines.append("优点TOP: " + " / ".join([f"{k}({v})" for k,v in pros_top.items()]))
    if not cons_top.empty:
        lines.append("缺点TOP: " + " / ".join([f"{k}({v})" for k,v in cons_top.items()]))
    return "\n".join(lines)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("vehicle_id")
    ap.add_argument("pages", nargs="?", default="5")
    args = ap.parse_args()

    vid = str(args.vehicle_id).strip()
    pages = int(args.pages)

    urls = build_urls(vid, pages)
    htmls = fetch_html(urls)
    all_rows: List[Dict[str, Any]] = []
    for h in htmls:
        rows = parse_reviews(h)
        all_rows.extend(rows)

    # CSV
    out_csv = f"autohome_reviews_{vid}.csv"
    pd.DataFrame(all_rows).to_csv(out_csv, index=False, encoding="utf-8-sig")

    # Summary TXT
    out_txt = f"autohome_reviews_{vid}_summary.txt"
    with open(out_txt, "w", encoding="utf-8") as f:
        f.write(summarize_counts(all_rows))

    print(f"✅ generated: {out_csv}")
    print(f"✅ generated: {out_txt}")

if __name__ == "__main__":
    main()
