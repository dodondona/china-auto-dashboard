#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
rank1_brand_series_scraper.py  (reverted-minimal, brand only)

目的:
- https://www.autohome.com.cn/rank/1 を対象に
  ① ページを開く → ② 「品牌月销榜」タブをクリック → ③ 表示されたブランド一覧から brand.csv を作成
- series.csv はこの段階では作らない（= 以前の「brandはできた / seriesは未作成」を再現）

依存:
  pip install playwright beautifulsoup4 lxml pandas
  python -m playwright install chromium
"""

from __future__ import annotations
import re, os, sys, argparse, time
from typing import List, Dict
import pandas as pd
from bs4 import BeautifulSoup

def _lazy_sync_playwright():
    from playwright.sync_api import sync_playwright
    return sync_playwright

# 以前の過度に厳しい「末尾が数値ID」制限は撤廃。autohome配下の a[href] を候補にする。
AUTOMATCH_DOMAIN = re.compile(r"^https?://[^/]*autohome\.com\.cn/|^//[^/]*autohome\.com\.cn/|^/")

def _abs_url(href: str) -> str:
    href = (href or "").strip()
    if not href:
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://www.autohome.com.cn" + href
    return href

def fetch_dom_after_click_brand(url: str, wait_ms: int = 2200, max_scrolls: int = 16) -> str:
    sync_playwright = _lazy_sync_playwright()
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

        # ページ末尾までゆっくりスクロール（遅延ロード対策）
        last_h = 0
        for _ in range(max_scrolls):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(wait_ms)
            h = page.evaluate("document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h

        # ★ ブランド月次タブをクリック（存在すれば）
        #    文言: 「品牌月销榜」(Brand Monthly) / 「品牌周销榜」もあるが、月次のみ対象
        try:
            # まずは exact で探す
            loc = page.get_by_text("品牌月销榜", exact=True)
            if loc.count() == 0:
                # 緩め探索（余白や別要素の兼ね合いで一致しないケース）
                loc = page.locator("text=品牌月销榜")
            if loc and loc.is_visible():
                loc.first.click()
                page.wait_for_timeout(wait_ms)
                # クリック後にもスクロールして遅延部分を露出
                last_h = 0
                for _ in range(max_scrolls // 2):
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(wait_ms)
                    h = page.evaluate("document.body.scrollHeight")
                    if h == last_h:
                        break
                    last_h = h
        except Exception:
            # 見つからない場合はそのまま（= 既にブランドタブ表示か、構造が同一DOMに出ている）
            pass

        html = page.content()
        # デバッグ用に保存（必要ならコメント解除）
        # with open("data/debug_rank1_after_brand.html", "w", encoding="utf-8") as f:
        #     f.write(html)

        context.close()
        browser.close()
        return html

def extract_brands_from_html(html: str) -> List[Dict]:
    """
    ブランドタブ表示後の DOM からブランド名/リンクを抽出する。
    - a[href] が autohome ドメイン配下で、テキストが「ブランド名らしい」短めの中国語/英語を採用
    - 重複を除外
    """
    soup = BeautifulSoup(html, "lxml")
    rows, seen = [], set()

    # rank番号と名前がセットで並ぶカードが多いので、該当ブロックを広めに走査
    # ここでは a[href] を走査して、ブランド名っぽいテキストだけを拾う。
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        txt = (a.get("title") or a.get_text(strip=True) or "").strip()

        if not href or not txt:
            continue
        if not AUTOMATCH_DOMAIN.search(href):
            continue

        # 「品牌」「车系」「销量」「查成交价」など明らかにブランド名でないものは除外
        bad_kw = ("车系", "销量", "成交价", "排行榜", "首页", "文章", "视频", "直播", "论坛", "口碑",
                  "经销商", "二手车", "降价", "工具", "反馈", "问题举报", "关于我们", "联系我们", "招贤", "营业执照")
        if any(k in txt for k in bad_kw):
            continue

        # ブランド名らしさ: 3～10文字程度 / 先頭英字または中日韓文字を含む
        if not (2 <= len(txt) <= 12):
            continue
        if not re.search(r"[A-Za-z\u4e00-\u9fff]", txt):
            continue

        absurl = _abs_url(href)
        key = (txt, absurl)
        if key in seen:
            continue
        seen.add(key)

        rows.append({
            "brand": txt,
            "brand_url": absurl,
            "title_raw": txt,
        })

    # ヒットが多すぎる場合は簡易フィルタ（同一 brand の複数URLは最初の一件だけ）
    uniq, picked = set(), []
    for r in rows:
        b = r["brand"]
        if b in uniq:
            continue
        uniq.add(b)
        picked.append(r)

    # rank_seq を付与
    out = []
    for i, r in enumerate(picked, 1):
        out.append({"rank_seq": i, **r})
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="https://www.autohome.com.cn/rank/1 等")
    ap.add_argument("--out-brand", default="data/brand.csv")
    ap.add_argument("--wait-ms", type=int, default=2200)
    ap.add_argument("--max-scrolls", type=int, default=16)
    args = ap.parse_args()

    html = fetch_dom_after_click_brand(args.url, wait_ms=args.wait_ms, max_scrolls=args.max_scrolls)
    brand_rows = extract_brands_from_html(html)

    if brand_rows:
        os.makedirs(os.path.dirname(args.out_brand), exist_ok=True)
        pd.DataFrame(brand_rows).to_csv(args.out_brand, index=False, encoding="utf-8-sig")
        print(f"[ok] brand rows={len(brand_rows)} -> {args.out_brand}")
    else:
        print("[warn] brand rows=0（タブ未反映 or 構造変更の可能性。debug HTML を確認してください）")

    # series.csv はこの段階では作らない = 何もしない

if __name__ == "__main__":
    main()
