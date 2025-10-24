# -*- coding: utf-8 -*-
# .github/scripts/rank_capture_images_and_csv.py
#
# Autohomeランキングページを100位までスクロールし、
# 順位、車名、販売台数、変動、リンク、価格、画像をCSV化する

import asyncio, os, re, csv
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
    print("🔄 Scrolling until 100th rank loaded...")
    loaded = 0
    for _ in range(80):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1000)
        cards = await page.locator('div[data-rank-num]').count()
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
    m_price = re.search(r"\d+(?:\.\d+)?-\d+(?:\.\d+)?万", text)
    if m_price: price = m_price.group(0)

    link = None
    btn = card.locator("button[data-series-id]").first
    if await btn.count():
        sid = await btn.get_attribute("data-series-id")
        if sid:
            sid = sid.split("|")[1] if "|" in sid else sid
            link = f"{BASE}/{sid}"
    if not link:
        a = card.locator("a[href]").first
        if await a.count():
            href = (await a.get_attribute("href") or "").strip()
            if re.fullmatch(r"/\d{3,6}/?", href):
                link = BASE + href
            elif re.match(r"^https?://www\.autohome\.com\.cn/\d{3,6}/?$", href):
                link = href

    units = None
    m_units = re.findall(r'(\d{1,3}(?:,\d{3})+|\d{4,6})', text)
    if m_units:
        try: units = int(m_units[-1].replace(",", ""))
        except: units = None

    delta = None
    try:
        delta = await card.evaluate(r"""
        (root)=>{
          function signFromSvg(svg){
            const path = svg.querySelector('path');
            const fillAttr = (path && path.getAttribute('fill') || svg.getAttribute('fill') || '').toLowerCase();
            if (fillAttr.includes('#ff6600') || fillAttr.includes('#f60')) return '+';
            if (fillAttr.includes('#1ccd99')) return '-';
            return '';
          }
          function signFromColorAround(el){
            const color = (getComputedStyle(el).color || '').replace(/\s+/g,'').toLowerCase();
            if (color.includes('255,102,0') || color.includes('#ff6600')) return '+';
            if (color.includes('28,205,153') || color.includes('#1ccd99')) return '-';
            return '';
          }
          function nearestSmallIntFrom(node){
            function readSib(start, forward=true, steps=4){
              let s='', n=start, c=0;
              while(n && c<steps){
                n = forward ? n.nextSibling : n.previousSibling;
                if(!n) break;
                if(n.nodeType===3){ s += (n.textContent||'').trim()+' '; }
                else if(n.nodeType===1){ s += (n.textContent||'').trim()+' '; }
                c++;
              }
              return s;
            }
            let t = readSib(node,true,4) + ' ' + readSib(node,false,2);
            let m = t.match(/\b(\d{1,2})\b/);
            if(m) return m[1];
            const p = node.parentElement;
            if(p){
              const tt = (p.textContent||'').trim().slice(0,40);
              m = tt.match(/\b(\d{1,2})\b/);
              if(m) return m[1];
            }
            return null;
          }
          const svgs = [...root.querySelectorAll('svg')];
          for(const svg of svgs){
            const s = signFromSvg(svg);
            if(!s) continue;
            const num = nearestSmallIntFrom(svg);
            if(num) return s + num;
          }
          const all = root.querySelectorAll('*');
          for(const el of all){
            const txt = (el.textContent||'').trim();
            if(!/^\d{1,2}$/.test(txt)) continue;
            const s = signFromColorAround(el);
            if(s) return s + txt;
          }
          return null;
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
    img = card.locator("img").first
    handle = await (img.element_handle() if await img.count() else card.element_handle())
    fname = f"{rank:03d}_{sanitize_filename(name or 'car')}.png"
    await handle.screenshot(path=str(IMG_DIR/fname), type="png")
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
            cards = page.locator('div[data-rank-num]')
            count = await cards.count()
            for i in range(min(count, 100)):
                card = cards.nth(i)
                rec = await extract_card_record(card)
                if rec["rank"] is None: continue
                fname = await screenshot_card_image(card, rec["rank"], rec["name"])
                rec["image_url"] = (f"{PUBLIC_PREFIX}/autohome_images/{fname}" if PUBLIC_PREFIX else f"/autohome_images/{fname}")
                all_rows.append(rec)
        all_rows.sort(key=lambda r: (r["rank"] if r["rank"] else 9999))
        headers = ["rank","name","units","delta_vs_last_month","link","price","image_url"]
        with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for r in all_rows: writer.writerow({k:r.get(k) for k in headers})
        await ctx.close(); await browser.close()
    print(f"\n✅ Done. Saved {len(all_rows)} entries to {CSV_PATH}")

if __name__ == "__main__":
    asyncio.run(main())
