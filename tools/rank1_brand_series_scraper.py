#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
rank1_brand_series_scraper.py

Autohome ランキング基礎ページ（https://www.autohome.com.cn/rank/1）を対象に、
- brand.csv は生成（静的DOMに出るブランド側のアンカーから抽出）
- series.csv は「タブ切替後にXHRで挿入される動的領域」を必要とするため、
  この“戻した段階”では未対応（DOMに無ければファイルを作らない）

※ つまり「brand.csvはできた一方、series.csvはできてません」の状態を再現します。

依存:
  pip install playwright beautifulsoup4 lxml pandas
  python -m playwright install chromium
"""

from __future__ import annotations
import re, os, sys, argparse
from typing import List, Dict, Tuple, Optional
import pandas as pd
from bs4 import BeautifulSoup

def _lazy_import_playwright():
    from playwright.sync_api import sync_playwright
    return sync_playwright

# 車系/ブランド詳細ページの典型的なリンク（末尾が数値ID）
ANCHOR_PATTERNS = [
    r"https?://www\.autohome\.com\.cn/\d+/?$",
    r"//www\.autohome\.com\.cn/\d+/?$",
    r"^/\d+/?$",
]

INT_RE = re.compile(r"\d+")
def _normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return u
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return "https://www.autohome.com.cn" + u
    return u

def fetch_html(url: str, wait_ms: int = 1800, max_scrolls: int = 12) -> str:
    sync_playwright = _lazy_import_playwright()
    with sync_playwright() as p:
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

def parse_brand_from_rank1(html: str) -> List[Dict]:
    """
    rank/1 のページに静的に出ている“ブランド側”のリンク群を拾う素朴抽出。
    （環境によりDOM構造差があるため、厳密なrank値等は期待しない）
    """
    soup = BeautifulSoup(html, "lxml")
    rows, seen = [], set()

    # ざっくり a[href] から、ブランド名っぽい title/text を拾う
    anchors = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if any(re.search(p, href) for p in ANCHOR_PATTERNS):
            anchors.append(a)

    for idx, a in enumerate(anchors, 1):
        href = _normalize_url(a["href"])
        if not href or href in seen:
            continue
        seen.add(href)

        title_attr = (a.get("title") or "").strip()
        text = (a.get_text(strip=True) or "").strip()
        title_raw = title_attr or text

        # 「ブランド名 っぽい」ものを brand に入れる（ルール最小限）
        brand = title_raw
        if not brand:
            continue

        rows.append({
            "rank_seq": idx,
            "brand": brand,
            "brand_url": href,
            "title_raw": title_raw,
        })
    return rows

def parse_series_from_rank1(html: str) -> List[Dict]:
    """
    rank/1 の“シリーズ（车系）”はタブ切替後にXHRで注入されることが多く、
    この“戻した段階”では DOM に存在しないため抽出しない。
    → DOMに系列ブロックが見つからなければ空を返す。
    """
    soup = BeautifulSoup(html, "lxml")

    # シリーズ領域（例：タブ "车系" 押下後に出るリスト）が無ければ空
    # ここでは軽くキーワードで探すだけ（存在しない想定）
    series_container = soup.find(lambda tag: tag.name in ("div", "section")
                                 and ("车系" in (tag.get_text(strip=True) or "")
                                      or "系列" in (tag.get_text(strip=True) or "")))
    if not series_container:
        return []  # ← ここが今回の「series.csv ができていない」理由

    # （将来的に対応するなら、ここで series_container 内のリンク/タイトルを解析）
    return []

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", help="https://www.autohome.com.cn/rank/1 等", required=False)
    ap.add_argument("--from-html", help="保存済みHTMLを解析（Playwright不要）", required=False)
    ap.add_argument("--out-brand", default="brand.csv")
    ap.add_argument("--out-series", default="series.csv")
    ap.add_argument("--wait-ms", type=int, default=1800)
    ap.add_argument("--max-scrolls", type=int, default=12)
    args = ap.parse_args()

    if not args.url and not args.from_html:
        print("ERROR: --url または --from-html のいずれかを指定してください。", file=sys.stderr)
        sys.exit(1)

    if args.from_html:
        with open(args.from_html, "r", encoding="utf-8", errors="ignore") as f:
            html = f.read()
    else:
        html = fetch_html(args.url, wait_ms=args.wait_ms, max_scrolls=args.max_scrolls)

    # brand 側は出る（＝今回「brand.csv はできた」）
    brand_rows = parse_brand_from_rank1(html)
    if brand_rows:
        pd.DataFrame(brand_rows).to_csv(args.out_brand, index=False, encoding="utf-8-sig")
        print(f"[ok] brand rows={len(brand_rows)} -> {args.out_brand}")
    else:
        print("[warn] brand rows=0（ページ構造変化／要素未出現の可能性）")

    # series 側は、タブ切替＋XHR注入後でないとDOMに無い → 見つからなければファイルを作らない
    series_rows = parse_series_from_rank1(html)
    if series_rows:
        pd.DataFrame(series_rows).to_csv(args.out_series, index=False, encoding="utf-8-sig")
        print(f"[ok] series rows={len(series_rows)} -> {args.out_series}")
    else:
        print("[info] series rows=0 -> series.csv は作成しません（タブ未クリック/XHR未取得のため）")

if __name__ == "__main__":
    main()
