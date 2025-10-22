import asyncio, os, time, re, math
from playwright.async_api import async_playwright
from urllib.parse import urlparse

URLS_FILE = "urls.txt"
OUT_DIR = "captures"

JS_CLEAN = """
(() => {
  const kill = [
    '.app-down','.float-tool','.to-top','.ui-overlay','.DownloadClient',
    '.ad-fixed','.fixedtools','.athm-side-tool','.athm-mini-im','.kefu',
    '.btn-askPrice','.price-float','.go-top','.right-fixed'
  ];
  kill.forEach(s => document.querySelectorAll(s).forEach(el => el.remove()));
  [...document.querySelectorAll('*')].forEach(e=>{
    const cs=getComputedStyle(e);
    if(cs.position==='fixed') e.style.position='static';
  });
})();
"""

def safe_name(url):
    u = urlparse(url)
    sid = re.search(r'/series/(\\d+)', u.path or '')
    name = sid.group(1) if sid else re.sub(r'\\W+','_',u.path or 'page')
    return name

async def capture():
    os.makedirs(OUT_DIR, exist_ok=True)
    urls = [x.strip() for x in open(URLS_FILE, encoding="utf-8") if x.strip()]
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            "--disable-gpu","--disable-dev-shm-usage","--no-sandbox"
        ])
        ctx = await browser.new_context(
            viewport={"width":2560,"height":2000},
            device_scale_factor=2
        )
        page = await ctx.new_page()

        for url in urls:
            name = safe_name(url)
            print(f"[+] {name}: {url}")
            await page.goto(url, wait_until="networkidle")
            await page.add_script_tag(content=JS_CLEAN)
            # lazy要素全展開
            prev = 0
            for _ in range(20):
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
                await page.wait_for_timeout(600)
                h = await page.evaluate("document.body.scrollHeight")
                if h == prev: break
                prev = h
            await page.evaluate("window.scrollTo(0,0)")
            await page.wait_for_timeout(800)

            # フルページ保存
            out_path = os.path.join(OUT_DIR, f"{name}.png")
            await page.screenshot(path=out_path, full_page=True)
            print(f"  saved {out_path}")
        await ctx.close(); await browser.close()

if __name__ == "__main__":
    asyncio.run(capture())
