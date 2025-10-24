# -*- coding: utf-8 -*-
# 改良版: Lazy-load完了まで確実に全カードを読み込む
# 構造・出力互換: public/autohome_images/*.png + public/autohome_ranking_with_image_urls.csv

import asyncio, os, re, csv, time
from pathlib import Path
from urllib.parse import urlparse
from playwright.async_api import async_playwright

RANK_URLS = [
    "https://www.autohome.com.cn/rank/1",
]

PUBLIC_DIR = Path("public")
IMG_DIR = PUBLIC_DIR / "autohome_images"
CSV_PATH = PUBLIC_DIR / "autohome_ranking_with_image_urls.csv"
BASE = "https://www.autohome.com.cn"
PUBLIC_PREFIX = os.environ.get("PUBLIC_PREFIX", "").rstrip("/")

def sanitize_filename(s: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", s.strip())
    return s[:80].strip("_") or "car"

async def scroll_and_load(page, target=100):
    """下までスクロール＋加载更多＋Lazy-load完了待ち"""
    seen = 0
    last_update = time.time()
    for _ in range(60):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1000)

        # 件数をチェック
        count = await page.locator("div[data-rank-num]").count()
        if count > seen:
            seen = count
            last_update = time.time()

        # “加载更多”ボタン対応
        try:
            btn = page.locator("text=/加载更多|下一页|更多/")
            if await btn.first.is_visible():
                await btn.first.click()
                await page.wait_for_timeout(1500)
        except Exception:
            pass

        # 100件以上 or 進展なし5秒で抜ける
        if seen >= target:
            break
        if time.time() - last_update > 5:
            break

    # 最後にLazy-load画像を確実に読み込ませる
    await page.wait_for_timeout(2000)
    await page.evaluate("window.scrollTo(0, 0)")
    await page.wait_for_timeout(1000)
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await page.wait_for_timeout(2000)

async def extract_card_record(card):
    rank = await card.get_attribute("data-rank-num")
    try:
        rank_num = int(rank) if rank else None
    except:
        rank_num = None

    # name
    name = None
    name_el = card.locator(".tw-text-nowrap.tw-text-lg").first
    if await name_el.count():
        name = (await name_el.inner_text()).strip()
    else:
        for tag in ["h1","h2","h3","h4"]:
            t = card.locator(tag)
            if await t.count():
                name = (await t.first.inner_text()).strip()
                break

    # price
    price = None
    text = (await card.inner_text()).replace("\n"," ")
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
            elif re.match(r"^https?://www\.autohome\.com\.cn/\d{3,6}/?$", href):
                link = href

    # units
    units = None
    m2 = re.findall(r'(\d{1,3}(?:,\d{3})+|\d{4,6})', text)
    if m2:
        try:
            units = int(m2[-1].replace(",", ""))
        except:
            units = None

    # delta
    delta = None
    svg = card.locator("svg").first
    if await svg.count():
        neighbor_text = await (await svg.element_handle()).evaluate("(el)=>el.parentElement && el.parentElement.innerText || ''")
        m3 = re.search(r"\d+", neighbor_text or "")
        if m3:
            num = m3.group(0)
            svg_html = await svg.inner_html()
            colors = set(re.findall(r'fill="(#?[0-9a-fA-F]{3,6})"', svg_html))
            sign = ""
            if any(c.lower() in {"#f60","#ff6600"} for c in colors):
                sign = "+"
            elif any(c.lower() in {"#1ccd99","#00cc99","#1ccd9a"} for c in colors):
                sign = "-"
            delta = f"{sign}{num}" if num else None

    return {"rank": rank_num, "name": name, "price": price, "link": link, "units": units, "delta_vs_last_month": delta}

async def screenshot_card_image(card, rank, name):
    img = card.locator("img").first
    handle = None
    if await img.count():
        handle = await img.element_handle()
    else:
        candidate = card.locator("div:has(img)").first
        if await candidate.count():
            handle = await candidate.element_handle()
        else:
            handle = await card.element_handle()

    fname = f"{rank:03d}_{sanitize_filename(name or 'car')}.png"
    path = IMG_DIR / fname
    # 画像描画を少し待ってから撮る
    await card.wait_for_timeout(1000)
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
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            print("🔄 Scrolling and loading...")
            await scroll_and_load(page)

            cards = page.locator("div[data-rank-num]")
            count = await cards.count()
            print(f"  loaded cards: {count}")
            rows = []
            for i in range(count):
                card = cards.nth(i)
                rec = await extract_card_record(card)
                if rec["rank"] is None:
                    continue
                fname = await screenshot_card_image(card, rec["rank"], rec["name"])
                rec["image_url"] = f"{PUBLIC_PREFIX}/autohome_images/{fname}" if PUBLIC_PREFIX else f"/autohome_images/{fname}"
                rows.append(rec)

            rows.sort(key=lambda r: (r["rank"] if r["rank"] is not None else 10**9))
            all_rows.extend(rows)

        headers = ["rank","name","units","delta_vs_last_month","link","price","image_url"]
        with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for r in all_rows:
                w.writerow({k: r.get(k) for k in headers})

        await ctx.close()
        await browser.close()

    print(f"✅ CSV: {CSV_PATH}")
    print(f"✅ Images: {len(list(IMG_DIR.glob('*.png')))} files under {IMG_DIR}")

if __name__ == "__main__":
    asyncio.run(main())
