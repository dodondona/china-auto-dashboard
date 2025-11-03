# -*- coding: utf-8 -*-
import re, asyncio, html, requests, sys
import pandas as pd
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
HDRS = {
    "User-Agent": UA,
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://www.autohome.com.cn/",
}

def extract_maker_from_title(title: str):
    if not title: return None
    for pat in [
        re.compile(r"】([^_【】]+)_"),
        re.compile(r"】([^|｜\-–—【】\s]+)[\|｜\-–—]"),
        re.compile(r"】\s*([^_【】]+)\s"),
    ]:
        m = pat.search(title)
        if m:
            t = m.group(1).strip()
            return re.sub(r"[【】\[\]（）()|｜\-–—]+", "", t).strip()
    return None

async def fetch_title_playwright(page, url):
    strategies = [
        {"wait_until": "domcontentloaded", "timeout": 90000},
        {"wait_until": "load", "timeout": 90000},
        {"wait_until": "networkidle", "timeout": 90000},
    ]
    for st in strategies:
        try:
            await page.goto(url, **st)
            return (await page.title()).strip()
        except PWTimeout:
            continue
        except Exception:
            continue
    return None

def fetch_title_requests(url):
    try:
        r = requests.get(url, headers=HDRS, timeout=30)
        r.raise_for_status()
        m = re.search(r"<title[^>]*>(.*?)</title>", r.text, re.I | re.S)
        if not m: return None
        return html.unescape(m.group(1)).strip()
    except Exception:
        return None

async def main():
    infile = Path(sys.argv[1])
    df = pd.read_csv(infile, encoding="utf-8-sig")
    df["manufacturer"] = None
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=UA, locale="zh-CN")
        page = await ctx.new_page()
        for i, url in enumerate(df["link"].dropna().unique(), 1):
            print(f"[{i}/{len(df)}] visiting {url}")
            title = await fetch_title_playwright(page, url)
            if not title:
                title = fetch_title_requests(url)
            maker = extract_maker_from_title(title)
            print(f" → title: {title or '(none)'}")
            print(f" → extracted manufacturer: {maker or '-'}")
            df.loc[df["link"] == url, "manufacturer"] = maker
        await ctx.close(); await browser.close()
    out = infile.with_name(infile.stem + "_with_maker.csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\n✅ Done. Saved → {out}")

if __name__ == "__main__":
    asyncio.run(main())
