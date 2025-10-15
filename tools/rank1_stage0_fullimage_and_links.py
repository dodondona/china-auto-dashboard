#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rank1_stage0_fullimage_and_links.py
Autohomeランキングページから順位順に series_url を抽出し、
フルページ画像を保存する。
"""

import re
import os
import time
import argparse
from playwright.sync_api import sync_playwright

# === 改善済み: Autohomeのクエリパラメータにも対応 ===
SERIES_HREF_RE = re.compile(
    r"(?:/series/(\d+)\.html)(?:[?#].*)?$|(?:/(\d+))(?:/)?(?:[?#].*)?$",
    re.I
)


def _abs_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://www.autohome.com.cn" + href
    return "https://www.autohome.com.cn/" + href


def _series_id_from_href(href: str) -> str:
    if not href:
        return ""
    m = SERIES_HREF_RE.search(href)
    if not m:
        return ""
    sid = m.group(1) or m.group(2) or ""
    return sid if sid.isdigit() else ""


def _extract_rank_link_pairs(page):
    """ランキング順に series リンクを抽出"""
    anchors = page.query_selector_all("a[href]")
    seen = set()
    results = []

    for a in anchors:
        href = a.get_attribute("href")
        if not href:
            continue
        if "/series/" in href or re.match(r"^/\d+/?", href):
            full_url = _abs_url(href)
            sid = _series_id_from_href(href)
            if sid and sid not in seen:
                seen.add(sid)
                results.append((len(results) + 1, full_url))

    print(f"[debug] Collected {len(results)} links")
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", type=str, default="https://www.autohome.com.cn/rank/1")
    parser.add_argument("--outdir", type=str, default="data/html_rank1")
    parser.add_argument("--max", type=int, default=50)
    parser.add_argument("--wait-ms", type=int, default=250)
    parser.add_argument("--max-scrolls", type=int, default=200)
    parser.add_argument("--full-image", action="store_true")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    out_img = os.path.join(args.outdir, "rank1_full.png")
    out_csv = os.path.join(args.outdir, "index.csv")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 800})
        print(f"[info] Navigating to {args.url}")
        page.goto(args.url, wait_until="networkidle")

        # スクロール実施
        for i in range(args.max_scrolls):
            page.mouse.wheel(0, 2000)
            time.sleep(args.wait_ms / 1000)
            if i % 20 == 0:
                print(f"  scroll {i}/{args.max_scrolls}")
        time.sleep(2.0)

        # === フル画像キャプチャ ===
        print(f"[info] Saving full screenshot: {out_img}")
        page.screenshot(path=out_img, full_page=True)

        # === リンク抽出 ===
        pairs = _extract_rank_link_pairs(page)
        if not pairs:
            print("No series links collected.")
            with open(os.path.join(args.outdir, "rank_page.html"), "w", encoding="utf-8") as f:
                f.write(page.content())
            raise SystemExit(1)

        # === CSV出力 ===
        with open(out_csv, "w", encoding="utf-8") as f:
            f.write("rank,series_url\n")
            for rank, link in pairs[:args.max]:
                f.write(f"{rank},{link}\n")

        print(f"[info] Saved {len(pairs[:args.max])} links to {out_csv}")
        browser.close()


if __name__ == "__main__":
    main()
