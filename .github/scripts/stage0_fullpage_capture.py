# .github/scripts/stage0_fullpage_capture.py （抜粋差し替え）

import asyncio, os, time, re
from urllib.parse import urlparse
from playwright.async_api import async_playwright

URLS_FILE = "urls.txt"
OUT_DIR = "./captures"   # 相対パスに固定

DEFAULT_URLS = [
    "https://www.autohome.com.cn/config/series/7806.html#pvareaid=3454437",
]

# ファイル名を安全化（英数と._-のみ許可）
_SANITIZE = re.compile(r'[^A-Za-z0-9._-]+')

def safe_name(url: str) -> str:
    u = urlparse(url)
    # なるべく series ID を使う（なければホスト+パス要約）
    m = re.search(r'/series/(\d+)', u.path or '')
    base = m.group(1) if m else (u.netloc + (u.path or ''))
    frag = u.fragment or ""
    # 連結して安全化
    raw = (base + ("_" + frag if frag else "")).strip()
    name = _SANITIZE.sub('_', raw).strip('._-')
    # 念のため先頭の / や . を除去（絶対・親参照を防止）
    return name.lstrip('/.')

async def capture():
    os.makedirs(OUT_DIR, exist_ok=True)

    try:
        with open(URLS_FILE, encoding="utf-8") as f:
            urls = [x.strip() for x in f if x.strip()]
        print(f"✅ loaded {len(urls)} URLs from {URLS_FILE}")
    except FileNotFoundError:
        urls = DEFAULT_URLS
        print(f"⚠️  {URLS_FILE} not found → fallback to DEFAULT_URLS ({len(urls)}件)")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            "--disable-gpu","--disable-dev-shm-usage","--no-sandbox"
        ])
        ctx = await browser.new_context(
            viewport={"width":2560,"height":2000},
            device_scale_factor=2
        )
        page = await ctx.new_page()

        JS_CLEAN = """
        (() => {
          const kill = ['.app-down','.float-tool','.to-top','.ui-overlay','.DownloadClient',
            '.ad-fixed','.fixedtools','.athm-side-tool','.athm-mini-im','.kefu',
            '.btn-askPrice','.price-float','.go-top','.right-fixed'];
          kill.forEach(s => document.querySelectorAll(s).forEach(el => el.remove()));
          [...document.querySelectorAll('*')].forEach(e=>{
            const cs=getComputedStyle(e);
            if(cs.position==='fixed') e.style.position='static';
          });
        })()
        """

        for url in urls:
            name = safe_name(url)
            print(f"[+] {name}: {url}")

            await page.goto(url, wait_until="networkidle")
            await page.add_script_tag(content=JS_CLEAN)

            # lazyロードを全展開
            prev = 0
            for _ in range(20):
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
                await page.wait_for_timeout(600)
                h = await page.evaluate("document.body.scrollHeight")
                if h == prev: break
                prev = h
            await page.evaluate("window.scrollTo(0,0)")
            await page.wait_for_timeout(800)

            out_path = os.path.join(OUT_DIR, f"{name}.png")  # 必ず相対パス
            # 念のため親ディレクトリ作成（相対配下のみ）
            os.makedirs(os.path.dirname(out_path), exist_ok=True)

            await page.screenshot(path=out_path, full_page=True)
            print(f"  saved {out_path}")

        await ctx.close()
        await browser.close()

if __name__ == "__main__":
    asyncio.run(capture())
