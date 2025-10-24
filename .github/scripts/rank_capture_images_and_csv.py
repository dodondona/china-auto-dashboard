# -*- coding: utf-8 -*-
# .github/scripts/rank_capture_images_and_csv.py
#
# Autohomeランキングを開き、100位までの
#  - rank / name / units / delta_vs_last_month / link / price / image_url
# を収集。画像はカード内の見た目をそのまま要素スクリーンショットで保存。
# delta（先月比）は、HTML内の <svg> viewBox / path 形状から ↑/↓ を判定し数値に符号付け。

import asyncio
import os
import re
import csv
from pathlib import Path
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
    """100位までスクロールしてロード完了を待つ"""
    print("🔄 Scrolling until 100th rank loaded...")
    loaded = 0
    for _ in range(80):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1000)
        cards = await page.locator("div[data-rank-num]").count()
        if cards > loaded:
            loaded = cards
            print(f"  currently loaded: {loaded} items")
        if loaded >= 100:
            print("✅ 100 items loaded.")
            break
        try:
            btn = page.locator("text=/加载更多|下一页|更多/")
            if await btn.first.is_visible():
                await btn.first.click()
                await page.wait_for_timeout(1200)
        except Exception:
            pass


async def extract_card_record(card):
    """カード要素から主要フィールドを抽出"""
    # rank
    rank = await card.get_attribute("data-rank-num")
    try:
        rank_num = int(rank) if rank else None
    except Exception:
        rank_num = None

    # name
    name = None
    name_el = card.locator(".tw-text-nowrap.tw-text-lg").first
    if await name_el.count():
        name = (await name_el.inner_text()).strip()

    # price（例: 9.98-15.98万）
    price = None
    text = (await card.inner_text()).replace("\n", " ")
    m_price = re.search(r"\d+(?:\.\d+)?-\d+(?:\.\d+)?万", text)
    if m_price:
        price = m_price.group(0)

    # link（series id優先）
    link = None
    btn = card.locator("button[data-series-id]").first
    if await btn.count():
        sid = await btn.get_attribute("data-series-id")
        if sid:
            link = f"{BASE}/{sid}"
    if not link:
        a = card.locator("a[href]").first
        if await a.count():
            href = (await a.get_attribute("href") or "").strip()
            if re.fullmatch(r"/\d{3,6}/?", href):
                link = BASE + href
            elif re.match(r"^https?://www\.autohome\.com\.cn/\d{3,6}/?$", href):
                link = href

    # units（テキスト中の4～6桁数字を末尾寄りで拾う簡易法）
    units = None
    m_units = re.findall(r'(\d{1,3}(?:,\d{3})+|\d{4,6})', text)
    if m_units:
        try:
            units = int(m_units[-1].replace(",", ""))
        except Exception:
            units = None

    # delta（先月比）— SVGの形状(viewBox/path)から↑/↓を判定＋数字抽出
    delta = None
    try:
        delta = await card.evaluate(r"""
        (root)=>{
          let sign = '';
          const svgs = [...root.querySelectorAll('svg[viewBox]')];
          for (const svg of svgs) {
            const vb = (svg.getAttribute('viewBox') || '').trim();
            // 上昇：縦長（8.58 x 14.3）／下降：横長（14.3 x 8.58）
            if (/8\.58\s+14\.3/.test(vb)) sign = 'up';
            if (/14\.3\s+8\.58/.test(vb)) sign = 'down';
            const path = svg.querySelector('path');
            if (path) {
              const d = (path.getAttribute('d') || '').toLowerCase();
              // ↑パス（上向き矢印）はM0系統の上向きベクトル
              if (/m0.*l4.*l8/i.test(d) || /0\s*0\s*8\.58\s*14\.3/.test(d)) sign = 'up';
              // ↓パス（下向き矢印）はM8系統の下向きベクトル
              if (/m8.*l4.*l0/i.test(d) || /0\s*0\s*14\.3\s*8\.58/.test(d)) sign = 'down';
            }
          }

          // 数字部分をテキストから拾う（上限2桁）
          const txt = root.innerText.replace(/\s+/g,'');
          const m = txt.match(/(\d{1,2})(?:位)?$/);
          const num = m ? m[1] : (txt.match(/(\d{1,2})/)||[])[1];
          if (!num) return null;
          if (sign==='up') return '+' + num;
          if (sign==='down') return '-' + num;
          return num;
        }
        """)
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
    """画像（見た目そのまま）を要素スクリーンショットで保存"""
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
                    if PUBLIC_PREFIX else f"/autohome_images/{fname}"
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
