# -*- coding: utf-8 -*-
# 改良版: Lazy-load待機と確実なスクリーンショット、100件に限定
# 出力互換: public/autohome_images/*.png + public/autohome_ranking_with_image_urls.csv

import asyncio, os, re, csv, time
from pathlib import Path
from playwright.async_api import async_playwright

RANK_URLS = ["https://www.autohome.com.cn/rank/1"]
PUBLIC_DIR = Path("public")
IMG_DIR = PUBLIC_DIR / "autohome_images"
CSV_PATH = PUBLIC_DIR / "autohome_ranking_with_image_urls.csv"
BASE = "https://www.autohome.com.cn"
PUBLIC_PREFIX = os.environ.get("PUBLIC_PREFIX", "").rstrip("/")

def sanitize_filename(s: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", (s or "car").strip())
    return s[:80].strip("_") or "car"

async def scroll_and_load(page, target=100):
    """下までスクロール＋加载更多＋Lazy-load待ち"""
    seen = 0
    last_update = time.time()
    for _ in range(60):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1.0)

        count = await page.locator("div[data-rank-num]").count()
        if count > seen:
            seen = count
            last_update = time.time()

        # “加载更多/更多/下一页” 対応（あれば）
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

    # 最後に Lazy-load 画像を確実に読み込ませる
    await asyncio.sleep(2.0)
    await page.evaluate("window.scrollTo(0, 0)")
    await asyncio.sleep(1.0)
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await asyncio.sleep(2.0)

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
        # 親要素のテキストから数字を拾う
        try:
            handle = await svg.element_handle()
            neighbor_text = await handle.evaluate("(el)=>el.parentElement && el.parentElement.innerText || ''")
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
        except:
            pass

    return {"rank": rank_num, "name": name, "price": price, "link": link, "units": units, "delta_vs_last_month": delta}

async def screenshot_card_image(card, rank, name):
    # 画像が含まれる領域を優先
    loc = card.locator("img").first
    if not await loc.count():
        loc = card.locator("div:has(img)").first
    if not await loc.count():
        loc = card  # 最悪カード全体
    # 描画を少し待ってからスクショ（Locator は wait_for_timeout を持たない）
    await asyncio.sleep(1.0)
    fname = f"{(rank or 0):03d}_{sanitize_filename(name)}.png"
    path = IMG_DIR / fname
    await loc.screenshot(path=str(path), type="png")
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
            # 先頭100件だけ（多く出ても100位まで）
            limit = min(100, count)
            rows = []
            for i in range(limit):
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
