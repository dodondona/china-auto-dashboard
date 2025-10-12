#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
rank1_stage1_ranklist.py
Autohome ランキングページから車系名・リンク・販売台数などを抽出して CSV 保存。

- Playwright を利用して動的にロードされるリストを取得
- 各行の rank, name, count, link, 画像URL を収集
"""

import csv, re, asyncio, argparse
from playwright.async_api import async_playwright

async def scrape_ranklist(url, wait_ms=200, max_scrolls=200):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print(f"[info] navigating to {url}")
        await page.goto(url, wait_until="domcontentloaded")

        # スクロールして動的読み込みを完了させる
        for i in range(max_scrolls):
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            await page.wait_for_timeout(wait_ms)
        print("[info] scrolling done")

        # ランキング項目抽出
        items = await page.query_selector_all("li.rank-list-item, div.list-wrap li, .rank-list li")
        rows = []
        for i, item in enumerate(items, start=1):
            title = await item.inner_text()
            html = await item.inner_html()
            m_url = re.search(r'href="(https://www\.autohome\.com\.cn/\d+/)"', html)
            m_img = re.search(r'<img[^>]+src="([^"]+)"', html)
            m_count = re.search(r'(\d{4,})[^\d]*辆', title)
            m_rank_change = re.search(r'(↑|↓)\s*(\d+)', title)
            rank_change = 0
            if m_rank_change:
                sign = 1 if m_rank_change.group(1) == "↑" else -1
                rank_change = sign * int(m_rank_change.group(2))
            rows.append({
                "rank_seq": i,
                "seriesname": title.strip().split("\n")[0] if title else "",
                "series_url": m_url.group(1) if m_url else "",
                "count": m_count.group(1) if m_count else "",
                "img_url": m_img.group(1) if m_img else "",
                "rank_change": rank_change,
            })

        await browser.close()
        return rows


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="ランキングURL (例: https://www.autohome.com.cn/rank/1)")
    parser.add_argument("--out", required=True, help="出力CSVパス")
    parser.add_argument("--wait-ms", type=int, default=200, help="スクロール間の待機ミリ秒")
    parser.add_argument("--max-scrolls", type=int, default=200, help="最大スクロール回数")
    args = parser.parse_args()

    rows = await scrape_ranklist(args.url, args.wait_ms, args.max_scrolls)
    print(f"[info] {len(rows)} rows scraped")

    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys() if rows else
                                ["rank_seq", "seriesname", "series_url", "count", "img_url", "rank_change"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"[saved] {args.out} ({len(rows)} rows)")


if __name__ == "__main__":
    asyncio.run(main())
