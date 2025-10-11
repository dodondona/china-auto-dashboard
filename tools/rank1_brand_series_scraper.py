#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
rank1_brand_series_scraper.py (brand-only, rows限定版)

目的:
- https://www.autohome.com.cn/rank/1 を開く
- 「品牌月销榜」タブをクリック（存在すれば）
- ランキング行 [data-rank-num] の中だけを走査し、ブランド名を抽出
- brand.csv を出力（series.csv はこの段階では作らない＝以前の状態に戻す）

依存:
  pip install playwright beautifulsoup4 lxml pandas
  python -m playwright install chromium
"""

from __future__ import annotations
import re, os, sys, argparse
import pandas as pd
from bs4 import BeautifulSoup

def _lazy_sync_playwright():
    from playwright.sync_api import sync_playwright
    return sync_playwright

def fetch_dom_after_click_brand(url: str, wait_ms: int = 2200, max_scrolls: int = 16) -> str:
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

        # ページ末尾までスクロール（遅延ロード対策）
        last_h = 0
        for _ in range(max_scrolls):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(wait_ms)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h

        # 「品牌月销榜」をクリック（あれば）
        try:
            loc = page.get_by_text("品牌月销榜", exact=True)
            if loc.count() == 0:
                loc = page.locator("text=品牌月销榜")
            if loc and loc.is_visible():
                loc.first.click()
                page.wait_for_timeout(wait_ms)
        except Exception:
            pass

        # クリック後にも軽くスクロール
        for _ in range(max_scrolls // 2):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(wait_ms)

        html = page.content()
        context.close()
        browser.close()
        return html

def extract_brand_rows(html: str):
    """
    ランキング行 [data-rank-num] の中だけを見る。
    行内の名称は .tw-text-lg / .tw-font-medium に入っている（添付HTMLの解析結果）｡
    """
    soup = BeautifulSoup(html, "lxml")

    # ランキング行を限定取得
    rows = soup.select("[data-rank-num]")
    out = []
    seen = set()

    for row in rows:
        # rank
        rank_attr = row.get("data-rank-num") or ""
        rank_str = rank_attr.strip() if isinstance(rank_attr, str) else ""
        # 名称（ブランド名）
        name_el = row.select_one(".tw-text-lg, .tw-font-medium")
        brand = (name_el.get_text(strip=True) if name_el else "").strip()

        if not rank_str or not brand:
            continue

        # URLはブランド専用のhrefがDOMに無いケースが多いので、当面は空か疑似URL
        # 以前のフェーズでは brand.csv の主目的は「名前リスト化」だったためURL必須ではない。
        brand_url = ""

        key = (rank_str, brand)
        if key in seen:
            continue
        seen.add(key)

        out.append({
            "rank_seq": int(rank_str) if rank_str.isdigit() else rank_str,
            "brand": brand,
            "brand_url": brand_url,
            "title_raw": brand,
        })

    # rank_seqで安定化
    out.sort(key=lambda r: (r["rank_seq"] if isinstance(r["rank_seq"], int) else 999999, r["brand"]))
    # rank_seqを1..Nに振り直す（表示順維持）
    for i, r in enumerate(out, 1):
        r["rank_seq"] = i

    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="https://www.autohome.com.cn/rank/1 等")
    ap.add_argument("--out-brand", default="data/brand.csv")
    ap.add_argument("--wait-ms", type=int, default=2200)
    ap.add_argument("--max-scrolls", type=int, default=16)
    args = ap.parse_args()

    html = fetch_dom_after_click_brand(args.url, wait_ms=args.wait_ms, max_scrolls=args.max_scrolls)
    brand_rows = extract_brand_rows(html)

    if brand_rows:
        os.makedirs(os.path.dirname(args.out_brand), exist_ok=True)
        pd.DataFrame(brand_rows).to_csv(args.out_brand, index=False, encoding="utf-8-sig")
        print(f"[ok] brand rows={len(brand_rows)} -> {args.out_brand}")
    else:
        print("[warn] brand rows=0（ランキング行を取得できませんでした。タブ未反映/構造変化の可能性）")

if __name__ == "__main__":
    main()
