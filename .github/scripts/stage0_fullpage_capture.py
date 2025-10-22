# -*- coding: utf-8 -*-
# .github/scripts/stage0_fullpage_capture.py
#
# Playwrightで対象URLを開き、無限スクロール/「加载更多」クリックを繰り返した後、
# レンダリング後のHTML(.htm)とスクリーンショット(.png)を ./captures/ に保存します。
#
# 使い方（GitHub Actions内で実行想定）:
#   python .github/scripts/stage0_fullpage_capture.py
#
# 併用ファイル:
#   - urls.txt があれば、各行のURLを巡回。無ければ DEFAULT_URLS を使用。

import asyncio
import os
import re
import sys
from urllib.parse import urlparse
from playwright.async_api import async_playwright

URLS_FILE = "urls.txt"
OUT_DIR = "./captures"   # 相対パス固定

DEFAULT_URLS = [
    "https://www.autohome.com.cn/rank/1",  # 例：総合月販ランキング
]

_SANITIZE = re.compile(r'[^A-Za-z0-9._-]+')


def sanitize_filename(s: str) -> str:
    s = _SANITIZE.sub("_", s).strip("_")
    if not s:
        s = "page"
    return s[:100]


async def scroll_and_load(page):
    # 遅延ロード/仮想リストに備えて複数回スクロール＋「加载更多」対応
    # 必要に応じて回数や待機時間は調整可
    for _ in range(30):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(800)
        # 「加载更多」「下一页」「更多」などがあればクリック
        try:
            btn = page.locator("text=/加载更多|下一页|更多/")
            if await btn.first.is_visible():
                await btn.first.click()
                await page.wait_for_timeout(1200)
        except Exception:
            pass


async def capture():
    # URLリスト
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

            # スクロール & 可能なら「加载更多」
            await scroll_and_load(page)

            # 保存名はホスト＋パス（安全化）
            parsed = urlparse(url)
            base = (parsed.netloc + parsed.path).strip("/").replace("/", "_")
            name = sanitize_filename(base) or "page"

            # レンダリング後HTML保存
            html = await page.content()
            html_path = os.path.join(OUT_DIR, f"{name}.htm")
            os.makedirs(os.path.dirname(html_path), exist_ok=True)
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"  saved {html_path}")

            # フルページスクショ保存
            img_path = os.path.join(OUT_DIR, f"{name}.png")
            os.makedirs(os.path.dirname(img_path), exist_ok=True)
            await page.screenshot(path=img_path, full_page=True)
            print(f"  saved {img_path}")

        await ctx.close()
        await browser.close()


def main():
    asyncio.run(capture())


if __name__ == "__main__":
    main()
