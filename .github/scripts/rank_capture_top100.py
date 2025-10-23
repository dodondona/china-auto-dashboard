# -*- coding: utf-8 -*-
# .github/scripts/rank_capture_top100.py
#
# 目的: Autohomeランキング上位100台分を確実に取得。
# 出力: CSV (rank, name, units, link, price) を ./csv/rank_top100.csv に保存。
# 画像は取得せず、HTMLパースのみ。余計な機能なし。

import asyncio
import os
import re
import csv
from pathlib import Path
from urllib.parse import urlparse
from playwright.async_api import async_playwright

BASE_URL = "https://www.autohome.com.cn/rank/1"
OUT_DIR = Path("csv")
OUT_FILE = OUT_DIR / "rank_top100.csv"
BASE = "https://www.autohome.com.cn"

async def scroll_until_100(page):
    """加载更多を押しながら100位まで読み込む"""
    loaded = 0
    for _ in range(50):  # 安全上限
        items = await page.locator("div[data-rank-num]").count()
        if items >= 100:
            print(f"✅ reached {items} items, stopping scroll")
            break
        try:
            btn = page.locator("text=/加载更多|更多/").first
            if await btn.is_visible():
                await btn.click()
                await page.wait_for_timeout(1200)
            else:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(800)
        except Exception:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(800)
        loaded = items
        print(f"scrolling... currently {loaded} cars loaded")
    print("scroll finished")

async def extract_car_data(card):
    """各カードのデータ抽出"""
    rank = await card.get_attribute("data-rank-num")

    name_el = card.locator(".tw-text-nowrap.tw-text-lg").first
    name = (await name_el.inner_text()).strip() if await name_el.count() else None

    # 販売台数（车系销量）
    units = None
    txt = (await card.inner_text()).replace("\n", " ")
    m = re.search(r'(\d{4,6})', txt)
    if m:
        units = m.group(1)

    # 価格（xx.xx-xx.xx万）
    price = None
    m2 = re.search(r"\d+(?:\.\d+)?-\d+(?:\.\d+)?万", txt)
    if m2:
        price = m2.group(0)

    # 各車両ページへのリンク
    link = None
    btn = card.locator("button[data-series-id]").first
    if await btn.count():
        sid = await btn.get_attribute("data-series-id")
        if sid:
            link = f"{BASE}/{sid}"
    else:
        a = card.locator("a[href]").first
        if await a.count():
            href = (await a.get_attribute("href")) or ""
            if re.fullmatch(r"/\d{3,6}/?", href):
                link = BASE + href
            elif href.startswith("http"):
                link = href

    return {
        "rank": rank,
        "name": name,
        "units": units,
        "price": price,
        "link": link
    }

async def main():
    OUT_DIR.mkdir(exist_ok=True, parents=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()

        print(f"loading {BASE_URL}")
        await page.goto(BASE_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(1000)

        # スクロールして100位まで読み込む
        await scroll_until_100(page)

        # カード取得
        cards = page.locator("div[data-rank-num]")
        count = await cards.count()
        print(f"total cards loaded: {count}")

        rows = []
        for i in range(count):
            card = cards.nth(i)
            data = await extract_car_data(card)
            rows.append(data)
            if data.get("rank") and int(data["rank"]) >= 100:
                break

        # rank順に整列
        rows.sort(key=lambda x: int(x["rank"]) if x["rank"] else 9999)
        with open(OUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=["rank", "name", "units", "price", "link"])
            writer.writeheader()
            writer.writerows(rows)

        print(f"✅ saved top 100 → {OUT_FILE}")
        await ctx.close()
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
