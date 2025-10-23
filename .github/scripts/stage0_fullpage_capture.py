# -*- coding: utf-8 -*-
# .github/scripts/stage0_fullpage_capture.py
#
# 無限スクロール→lazy画像URLを昇格→レンダリング後HTML(.htm)保存
# 併せて img の src 種別の統計を captures/_meta.json に出力します（デバッグ用）

import asyncio, os, re, json
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

PROMOTE_JS = r"""
(() => {
  const imgs = Array.from(document.querySelectorAll('img'));
  function firstUrlFromSrcset(s) {
    if (!s) return null;
    const first = s.split(',')[0].trim().split(' ')[0];
    return first && (first.startsWith('http://') || first.startsWith('https://')) ? first : null;
  }
  function urlFromStyle(s) {
    if (!s) return null;
    const m = s.match(/url\((['"]?)(.*?)\1\)/);
    if (m && (m[2].startsWith('http://') || m[2].startsWith('https://'))) return m[2];
    return null;
  }

  let counts = {http:0, data:0, blank:0, other:0};
  for (const img of imgs) {
    // 昇格前の分類
    const cur = img.getAttribute('src') || '';
    if (cur.startsWith('http')) counts.http++;
    else if (cur.startsWith('data:image')) counts.data++;
    else if (cur === '') counts.blank++;
    else counts.other++;

    // lazy候補
    const cand = img.getAttribute('data-src')
      || img.getAttribute('data-original')
      || img.getAttribute('data-lazy-src')
      || img.getAttribute('data-url');
    const ss = img.getAttribute('srcset');
    const style = img.getAttribute('style') || (img.parentElement && img.parentElement.getAttribute('style')) || '';

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
  return counts;
})();
"""

COUNT_JS = r"""
(() => {
  const imgs = Array.from(document.querySelectorAll('img'));
  let counts = {http:0, data:0, blank:0, other:0};
  for (const img of imgs) {
    const cur = img.getAttribute('src') || '';
    if (cur.startsWith('http')) counts.http++;
    else if (cur.startsWith('data:image')) counts.data++;
    else if (cur === '') counts.blank++;
    else counts.other++;
  }
  return counts;
})();
"""

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
    meta = {"pages":[]}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-web-security"])
        ctx = await browser.new_context()
        page = await ctx.new_page()

        for url in urls:
            await page.goto(url, wait_until="domcontentloaded")
            await page.wait_for_timeout(1200)
            await scroll_and_load(page)

            before = await page.evaluate(COUNT_JS)
            await page.wait_for_timeout(300)
            promoted = await page.evaluate(PROMOTE_JS)
            await page.wait_for_timeout(1500)
            after = await page.evaluate(COUNT_JS)

            parsed = urlparse(url)
            base = (parsed.netloc + parsed.path).strip("/").replace("/", "_")
            name = sanitize_filename(base)
            html = await page.content()
            html_path = os.path.join(OUT_DIR, f"{name}.htm")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html)

            meta["pages"].append({
                "url": url,
                "file": html_path,
                "img_counts_before": before,
                "img_counts_after": after,
                "promote_seen": promoted,
            })
            print(f"[saved] {html_path}  img(before)=>{before}  img(after)=>{after}")

        await ctx.close()
        await browser.close()

    with open(os.path.join(OUT_DIR, "_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

def main():
    asyncio.run(capture())

if __name__ == "__main__":
    main()
