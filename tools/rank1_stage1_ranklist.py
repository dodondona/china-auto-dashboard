#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stage1: Autohomeランキングページから車種・順位・URL・画像URL・ランク推移を取得
"""

import asyncio, csv, re, os
from playwright.async_api import async_playwright

async def scrape_rank_page(url, out_csv):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)

        cars = await page.query_selector_all("div.rank-list > div")
        rows = []
        rank_seq = 1
        for car in cars:
            title = await car.inner_text()
            link_elem = await car.query_selector("a")
            link = await link_elem.get_attribute("href") if link_elem else ""

            img_elem = await car.query_selector("img")
            img_url = await img_elem.get_attribute("src") if img_elem else ""

            # 車種名
            name_elem = await car.query_selector("a")
            name = await name_elem.inner_text() if name_elem else ""

            # 販売台数
            count_elem = await car.query_selector(".rank-list__sales em")
            count = await count_elem.inner_text() if count_elem else ""

            # ランク変動
            change_elem = await car.query_selector(".rank-list__change")
            rank_change = 0
            if change_elem:
                text = await change_elem.inner_text()
                if "↑" in text:
                    m = re.search(r"↑(\d+)", text)
                    if m: rank_change = int(m.group(1))
                elif "↓" in text:
                    m = re.search(r"↓(\d+)", text)
                    if m: rank_change = -int(m.group(1))

            rows.append({
                "rank_seq": rank_seq,
                "seriesname": name.strip(),
                "series_url": link.strip() if link else "",
                "count": count.strip(),
                "rank_change": rank_change,
                "image_url": img_url.strip() if img_url else ""
            })
            rank_seq += 1

        os.makedirs(os.path.dirname(out_csv), exist_ok=True)
        with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        await browser.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    asyncio.run(scrape_rank_page(args.url, args.out))
