# -*- coding: utf-8 -*-
# .github/scripts/stage0_fullpage_capture.py
#
# Playwrightで対象URLを開き、無限スクロール/「加载更多」クリックを繰り返した後、
# lazy-load画像のURLを可能な限り "http(s)" に昇格させ、レンダリング後HTML(.htm)を ./captures/ に保存します。

import asyncio
import os
import re
from urllib.parse import urlparse
from playwright.async_api import async_playwright

URLS_FILE = "urls.txt"
OUT_DIR = "./captures"

DEFAULT_URLS = [
    "https://www.autohome.com.cn/rank/1",
]

_SANITIZE = re.compile(r'[^A-Za-z0-9._-]+')

def sanitize_filename(s: str) -> str:
    s = _SANITIZE.sub("_", s).strip("_")
    return (s or "page")[:100]

async def scroll_and_load(page):
    for _ in range(30):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(800)
        try:
            btn = page.locator("text=/加载更多|下一页|更多/")
            if await btn.first.is_visible():
                await btn.first.click()
                await page.wait_for_timeout(1200)
        except Exception:
            pass

async def promote_lazy_images(page):
    # data-src, data-original, data-lazy-src, data-url, srcset, style(background-image)
    js = r"""
(() => {
  const imgs = Array.from(document.querySelectorAll('img'));
  for (const img of imgs) {
    const cand = img.getAttribute('data-src')
      || img.getAttribute('data-original')
      || img.getAttribute('data-lazy-src')
      || img.getAttribute('data-url');

    const ss = img.getAttribute('srcset');
    const style = img.getAttribute('style') || (img.parentElement && img.parentElement.getAttribute('style')) || '';

    function firstUrlFromSrcset(s) {
      if (!s) return null;
      const first = s.split(',')[0].trim().split(' ')[0];
      return first && (first.startsWith('http://') || first.startsWith('https://')) ? first : null;
    }
    function urlFromStyle(s) {
      const m = s && s.match(/url\((['"]?)(.*?)\1\)/);
      if (m && (m[2].startsWith('http://') || m[2].startsWith('https://'))) return m[2];
      return null;
    }

    const urlFromSS = firstUrlFromSrcset(ss);
    const urlFromStyleAttr = urlFromStyle(style);

    if (cand && (cand.startsWith('http://') || cand.startsWith('https://')) && !(img.src || '').startsWith('http')) {
      img.src = cand;
    } else if (urlFromSS && !(img.src || '').startsWith('http')) {
      img.src = urlFromSS;
    } else if (urlFromStyleAttr && !(img.src || '').startsWith('http')) {
      img.src = urlFromStyleAttr;
    }
    img.loading = 'eager';
  }
})();
"""
    await page.evaluate(js)
    await page.wait_for_timeout(1500)  # ネットワーク反映待ち

async def capture():
    urls = []
    if os.path.exists(URLS_FILE):
        with open(URLS_FILE, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                u = line.strip()
                if u and not u.startswith("#"):
                    urls.append(u)
    if not urls:
        urls = DEFAULT_URLS[:]

    os.makedirs(OUT_DIR, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-web-security"])
        ctx = await browser.new_context()
        page = await ctx.new_page()

        for url in urls:
            print(f"==> {url}")
            await page.goto(url, wait_until="domcontentloaded")
            await page.wait_for_timeout(1200)

            await scroll_and_load(page)
            await promote_lazy_images(page)  # ★ 画像URLを可能な限りHTTPに

            parsed = urlparse(url)
            base = (parsed.netloc + parsed.path).strip("/").replace("/", "_")
            name = sanitize_filename(base)

            html = await page.content()
            html_path = os.path.join(OUT_DIR, f"{name}.htm")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"  saved {html_path}")

        await ctx.close()
        await browser.close()

def main():
    asyncio.run(capture())

if __name__ == "__main__":
    main()
