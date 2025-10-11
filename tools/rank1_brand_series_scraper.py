#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
rank1_brand_series_scraper.py
────────────────────────────────────────────
Autohomeの rank/1 ページから:
 - HTML全体をPlaywrightで取得
 - JSON内部の "autohome://car/seriesmain?seriesid=…" を抽出して series_url を生成
 - [data-rank-num] の順番で .tw-text-lg/.tw-font-medium 内テキストから brand/model を推定
 - 出力は brand + model + series_url まで（series.csvは作らない）

★ これが「brand.csv はできたが series.csv はできてない」状態の完全復元版。
"""

from __future__ import annotations
import re, os, sys, argparse
import pandas as pd
from bs4 import BeautifulSoup

def _lazy_sync_playwright():
    from playwright.sync_api import sync_playwright
    return sync_playwright


def fetch_html_with_playwright(url: str, wait_ms: int = 2000, max_scrolls: int = 15) -> str:
    """rank/1ページ全体をPlaywrightで読み込み"""
    sp = _lazy_sync_playwright()
    with sp() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"),
            viewport={"width": 1440, "height": 2200},
        )
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(wait_ms)

        last_h = 0
        for _ in range(max_scrolls):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(wait_ms)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h

        html = page.content()
        context.close()
        browser.close()
        return html


def extract_series_urls_from_html(html: str) -> list[str]:
    """
    HTML文字列全体から autohome://car/seriesmain?seriesid=xxxx を抽出し、
    https://www.autohome.com.cn/<id>/ に変換。
    """
    ids = re.findall(r'autohome://car/seriesmain\?seriesid=(\d+)', html)
    urls = [f"https://www.autohome.com.cn/{sid}/" for sid in ids]
    # 重複除去（順序保持）
    seen, uniq = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def extract_rows_from_html(html: str):
    """
    [data-rank-num] の順番に .tw-text-lg/.tw-font-medium を抽出し、
    title_raw から brand/model を分割。series_url は seriesid対応リストから補う。
    """
    soup = BeautifulSoup(html, "lxml")
    divs = soup.select("[data-rank-num]")
    urls = extract_series_urls_from_html(html)

    rows = []
    for idx, div in enumerate(divs, 1):
        rank = div.get("data-rank-num", "").strip()
        name_el = div.select_one(".tw-text-lg, .tw-font-medium")
        title_raw = (name_el.get_text(strip=True) if name_el else "").strip()
        if not title_raw:
            continue

        brand, model = split_brand_model(title_raw)

        # series_urlが足りなければ空文字（HTML上で全件対応しているわけではない）
        series_url = urls[idx - 1] if idx - 1 < len(urls) else ""

        rows.append({
            "rank_seq": idx,
            "rank": rank,
            "brand": brand,
            "model": model,
            "count": "",
            "series_url": series_url,
            "brand_conf": "0.70",
            "series_conf": "0.70",
            "title_raw": title_raw,
            "brand_ja": "",
            "model_ja": "",
        })
    return rows


def split_brand_model(title: str):
    """
    例: 宝马3系 → brand=宝马, model=3系
        比亚迪 海豹 → brand=比亚迪, model=海豹
    """
    s = title.strip()
    if not s:
        return "", ""

    # スペース or 英数境界 or 数字境界で分ける
    # 优先: 半角スペース・全角スペース
    for sep in [" ", "　"]:
        if sep in s:
            parts = s.split(sep, 1)
            return parts[0], parts[1]

    # 中国語＋数字の混成（例: 宝马3系, 比亚迪e2）
    m = re.match(r"^([\u4e00-\u9fffA-Za-z]+)(.+)$", s)
    if m:
        return m.group(1), m.group(2)
    return s, ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="https://www.autohome.com.cn/rank/1 等")
    ap.add_argument("--out", default="data/autohome_raw_rank1_with_brand.csv")
    ap.add_argument("--wait-ms", type=int, default=2000)
    ap.add_argument("--max-scrolls", type=int, default=15)
    args = ap.parse_args()

    html = fetch_html_with_playwright(args.url, wait_ms=args.wait_ms, max_scrolls=args.max_scrolls)
    rows = extract_rows_from_html(html)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    pd.DataFrame(rows).to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"[ok] rows={len(rows)} -> {args.out}")


if __name__ == "__main__":
    main()
