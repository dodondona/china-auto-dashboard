#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# 完全オリジナル構造準拠版
# 唯一の変更点: URLをheziランキングに差し替え
#   https://www.autohome.com.cn/rank/1-1-0-0_9000-hezi-x-x/
#
# 目的:
#   - Autohome heziランキングをPlaywrightで取得
#   - 上位（最大100件）を抽出し、画像＋series_idをCSV化
#   - 既存 autohome_pipeline の出力構造と完全互換
#
# 出力:
#   public/autohome_images/*.png
#   public/autohome_ranking_with_image_urls.csv
#
# 依存:
#   playwright, pandas, beautifulsoup4, lxml, requests
# -----------------------------------------------------------

import asyncio, os, re, csv, time
from pathlib import Path
from playwright.async_api import async_playwright

# ★ 唯一の差し替え
RANK_URLS = ["https://www.autohome.com.cn/rank/1-1-0-0_9000-hezi-x-x/"]

PUBLIC_DIR = Path("public")
IMG_DIR = PUBLIC_DIR / "autohome_images"
CSV_PATH = PUBLIC_DIR / "autohome_ranking_with_image_urls.csv"
BASE = "https://www.autohome.com.cn"
PUBLIC_PREFIX = os.environ.get("PUBLIC_PREFIX", "").rstrip("/")

def sanitize_filename(s: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", (s or "car").strip())
    return s[:80].strip("_") or "car"

async def scroll_and_load(page, target=100):
    """ページを下までスクロールし、必要に応じて“加载更多”をクリック"""
    seen = 0
    last_update = time.time()
    for _ in range(60):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1.0)
        count = await page.locator("div[data-rank-num]").count()
        if count > seen:
            seen = count
            last_update = time.time()
        try:
            btn = page.locator("text=/加载更多|下一页|更多/").first
            if await btn.is_visible():
                await btn.click()
                await asyncio.sleep(1.5)
        except Exception:
            pass
        if seen >= target:
            break
        if time.time() - last_update > 5:
            break

async def main():
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1280, "height": 2000})
        all_rows = []

        for url in RANK_URLS:
            await page.goto(url, timeout=60000)
            await scroll_and_load(page)
            cards = page.locator("div[data-rank-num]")
            count = await cards.count()
            for i in range(count):
                try:
                    card = cards.nth(i)
                    rank = await card.get_attribute("data-rank-num") or str(i + 1)
                    sid = await card.get_attribute("data-series-id")
                    if not sid:
                        html = await card.inner_html()
                        m = re.search(r"/(\d+)/", html)
                        sid = m.group(1) if m else None
                    name = await card.locator("h4,h3,h2,.tw-text-lg,.rank-list-info").nth(0).inner_text(timeout=1000)
                    name = re.sub(r"\s+", " ", name).strip()
                    fname = sanitize_filename(f"{rank}_{sid or i}_{name}.png")
                    fpath = IMG_DIR / fname
                    await card.screenshot(path=fpath)
                    rel_url = f"{PUBLIC_PREFIX}/autohome_images/{fname}" if PUBLIC_PREFIX else str(fpath)
                    all_rows.append({
                        "rank": rank,
                        "series_id": sid or "",
                        "name": name,
                        "image_url": rel_url,
                        "series_url": f"{BASE}/series/{sid}.html" if sid else "",
                    })
                except Exception as e:
                    print(f"[warn] card {i}: {e}")
            await asyncio.sleep(1)
        await browser.close()

    # CSV出力
    with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["rank", "series_id", "name", "image_url", "series_url"])
        writer.writeheader()
        writer.writerows(all_rows)

if __name__ == "__main__":
    asyncio.run(main())
