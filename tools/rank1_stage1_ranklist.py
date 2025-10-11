#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rank1_stage1_ranklist.py
Autohomeランキング (https://www.autohome.com.cn/rank/1) から
車名・URL・販売台数・画像URL・タイトルをCSV化。
"""

import csv, asyncio, argparse, re
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

async def fetch_html(url: str, wait_ms: int = 300, max_scrolls: int = 200) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print(f"[info] navigating {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # スクロールでlazy load要素を読み込む
        for i in range(max_scrolls):
            await page.evaluate("(y => window.scrollTo(0, y))", i * 800)
            await page.wait_for_timeout(wait_ms)

        html = await page.content()
        await browser.close()
        return html

def parse_rank_html(html: str):
    soup = BeautifulSoup(html, "html.parser")
    car_items = soup.select("li.rank-list-item")
    rows = []
    for idx, li in enumerate(car_items, start=1):
        # 画像
        img = li.select_one("img")
        img_url = img["src"] if img and img.has_attr("src") else ""

        # タイトル（例：【秦L】比亚迪_秦L报价_秦L图片_汽车之家）
        title_tag = li.select_one("a[href^='https://www.autohome.com.cn/']")
        title_raw = title_tag["title"] if title_tag and title_tag.has_attr("title") else (title_tag.text.strip() if title_tag else "")
        series_url = title_tag["href"] if title_tag and title_tag.has_attr("href") else ""

        # 車名
        name_tag = li.select_one("h3.car-name")
        seriesname = name_tag.text.strip() if name_tag else re.sub(r"【|】", "", title_raw)

        # 台数
        cnt_tag = li.select_one(".font-bold")
        count = cnt_tag.text.strip().replace("辆", "") if cnt_tag else ""

        # 価格（例: 7.99-17.49万）
        price_tag = li.select_one(".rank-price")
        price = price_tag.text.strip() if price_tag else ""

        # ランク変動（例: ↑2, ↓3, -）
        change_tag = li.select_one(".rank-change")
        rank_change = change_tag.text.strip() if change_tag else "0"

        rows.append({
            "rank_seq": idx,
            "seriesname": seriesname,
            "series_url": series_url,
            "title_raw": title_raw,
            "count": count,
            "img_url": img_url,
            "price": price,
            "rank_change": rank_change
        })
    return rows

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--wait-ms", type=int, default=300)
    ap.add_argument("--max-scrolls", type=int, default=150)
    args = ap.parse_args()

    html = await fetch_html(args.url, args.wait_ms, args.max_scrolls)
    rows = parse_rank_html(html)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys() if rows else
            ["rank_seq","seriesname","series_url","title_raw","count","img_url","price","rank_change"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"[saved] {args.out} ({len(rows)} rows)")

if __name__ == "__main__":
    asyncio.run(main())
