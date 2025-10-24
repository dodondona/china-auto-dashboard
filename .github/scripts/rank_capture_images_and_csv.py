# -*- coding: utf-8 -*-
# .github/scripts/rank_capture_images_and_csv.py

import asyncio
import os
import re
import csv
from pathlib import Path
from urllib.parse import urlparse
from playwright.async_api import async_playwright

RANK_URLS = ["https://www.autohome.com.cn/rank/1"]
PUBLIC_DIR = Path("public")
IMG_DIR = PUBLIC_DIR / "autohome_images"
CSV_PATH = PUBLIC_DIR / "autohome_ranking_with_image_urls.csv"
BASE = "https://www.autohome.com.cn"
PUBLIC_PREFIX = os.environ.get("PUBLIC_PREFIX", "").rstrip("/")


def sanitize_filename(s: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", s.strip())
    return s[:80].strip("_") or "car"


async def scroll_to_100(page):
    """100位までスクロール"""
    print("🔄 Scrolling until 100th rank loaded...")
    loaded = 0
    for i in range(80):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1000)
        cards = await page.locator("div[data-rank-num]").count()
        if cards > loaded:
            loaded = cards
            print(f"  currently loaded: {loaded} items")
        if loaded >= 100:
            break
        try:
            btn = page.locator("text=/加载更多|下一页|更多/")
            if await btn.first.is_visible():
                await btn.first.click()
                await page.wait_for_timeout(1200)
        except Exception:
            pass


async def extract_card_record(card):
    """カード要素から主要データを抽出"""
    rank = await card.get_attribute("data-rank-num")
    try:
        rank_num = int(rank) if rank else None
    except:
        rank_num = None

    name = None
    name_el = card.locator(".tw-text-nowrap.tw-text-lg").first
    if await name_el.count():
        name = (await name_el.inner_text()).strip()

    price = None
    text = (await card.inner_text()).replace("\n", " ")
    m = re.search(r"\d+(?:\.\d+)?-\d+(?:\.\d+)?万", text)
    if m:
        price = m.group(0)

    # link
    link = None
    btn = card.locator("button[data-series-id]").first
    if await btn.count():
        sid = await btn.get_attribute("data-series-id")
        if sid:
            link = f"{BASE}/{sid}"
    if not link:
        a = card.locator("a[href]").first
        if await a.count():
            href = (await a.get_attribute("href")) or ""
            href = href.strip()
            if re.fullmatch(r"/\d{3,6}/?", href):
                link = BASE + href

    # units
    units = None
    m2 = re.findall(r'(\d{1,3}(?:,\d{3})+|\d{4,6})', text)
    if m2:
        try:
            units = int(m2[-1].replace(",", ""))
        except:
            units = None

    # delta（先月比：安定版）
    delta = None
    try:
        icon_up = await card.locator('svg use[href*="icon-up"], svg path[fill*="#FF6600"]').count()
        icon_down = await card.locator('svg use[href*="icon-down"], svg path[fill*="#1CCD99"]').count()
        num_el = card.locator(".tw-text-[#FF6600], .tw-text-[#1CCD99]").last
        delta_text = ""
        if await num_el.count():
            delta_text = (await num_el.inner_text()).strip()
        if delta_text:
            sign = "+" if icon_up else "-" if icon_down else ""
            delta = f"{sign}{delta_text}"
    except Exception:
        delta = None

    return {
        "rank": rank_num,
        "name": name,
        "price": price,
        "link": link,
        "units": units,
        "delta_vs_last_month": delta,
    }


async def screenshot_card_image(card, rank, name):
    """画像を要素スクリーンショット"""
    img = card.locator("img").first
    handle = None
    if await img.count():
        handle = await img.element_handle()
    else:
        handle = await card.element_handle()
    fname = f"{rank:03d}_{sanitize_filename(name or 'car')}.png"
    path = IMG_DIR / fname
    await handle.screenshot(path=path, type="png")
    return fname


async def main():
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    PUBLIC_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()

        all_rows = []
        for url in RANK_URLS:
            print(f"🌐 Visiting: {url}")
            await page.goto(url, wait_until="domcontentloaded")
            await page.wait_for_timeout(1200)
            await scroll_to_100(page)

            cards = page.locator("div[data-rank-num]")
            count = await cards.count()
            print(f"✅ Total cards loaded: {count}")

            for i in range(min(count, 100)):
                card = cards.nth(i)
                rec = await extract_card_record(card)
                if rec["rank"] is None:
                    continue
                fname = await screenshot_card_image(card, rec["rank"], rec["name"])
                rec["image_url"] = (
                    f"{PUBLIC_PREFIX}/autohome_images/{fname}"
                    if PUBLIC_PREFIX
                    else f"/autohome_images/{fname}"
                )
                all_rows.append(rec)

        all_rows.sort(key=lambda r: (r["rank"] if r["rank"] else 9999))
        headers = ["rank", "name", "units", "delta_vs_last_month", "link", "price", "image_url"]
        with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for r in all_rows:
                writer.writerow({k: r.get(k) for k in headers})

        await ctx.close()
        await browser.close()

    print(f"\n✅ Done. Saved {len(all_rows)} entries to {CSV_PATH}")
    print(f"🖼  Images saved under {IMG_DIR.resolve()}")


if __name__ == "__main__":
    asyncio.run(main())
