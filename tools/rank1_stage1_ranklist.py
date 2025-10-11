#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rank1_stage1_ranklist.py
Autohomeランキングページ(例: https://www.autohome.com.cn/rank/1)
から、車系名・リンク・販売台数・タイトルをCSV出力する。
"""

import re, csv, asyncio, argparse
from playwright.async_api import async_playwright

async def scrape_ranklist(url: str, out: str, wait_ms: int = 300, max_scrolls: int = 200):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print(f"[info] navigating to {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # ページをゆっくりスクロール（遅延読み込み対策）
        for i in range(max_scrolls):
            await page.evaluate("(pos => window.scrollTo(0, pos))", i * 500)
            await page.wait_for_timeout(wait_ms)

        html = await page.content()
        await browser.close()

    # HTMLから抽出
    # 各車両カードには data-seriesid が付与されている
    pattern = re.compile(
        r'data-seriesid="(\d+)"[\s\S]*?<img[^>]+src="([^"]+)"[\s\S]*?alt="([^"]+)"[\s\S]*?href="(https://www\.autohome\.com\.cn/\d+/)"[\s\S]*?class="font-bold[^>]*>(\d+)</',
        re.S,
    )
    items = pattern.findall(html)
    rows = []
    for idx, m in enumerate(items, start=1):
        sid, img_url, title_raw, series_url, count = m
        seriesname = re.sub(r"【|】", "", title_raw).strip()
        rows.append({
            "rank_seq": idx,
            "seriesname": seriesname,
            "series_url": series_url,
            "title_raw": title_raw,
            "count": count,
            "img_url": img_url
        })

    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys() if rows else
                                ["rank_seq", "seriesname", "series_url", "title_raw", "count", "img_url"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"[saved] {out} ({len(rows)} rows)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--wait-ms", type=int, default=250)
    ap.add_argument("--max-scrolls", type=int, default=180)
    args = ap.parse_args()
    asyncio.run(scrape_ranklist(args.url, args.out, args.wait_ms, args.max_scrolls))

if __name__ == "__main__":
    main()
