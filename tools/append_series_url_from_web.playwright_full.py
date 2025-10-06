#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_from_web.playwright_full.py
--------------------------------------------
Autohomeランキングページ (/rank/1) をPlaywrightで全スクロールし、
上位50件分の「車系ページURL」を抽出してCSVに追加する。

出力: _with_series.csv
依存: playwright, pandas
"""

import asyncio, re, time, argparse, pandas as pd
from playwright.async_api import async_playwright
from pathlib import Path

async def fetch_series_urls(url: str, max_rounds=30, idle_ms=650, min_delta=3):
    print(f"🌐 開始: {url}")
    urls, prev_count = [], 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle")

        # 無限スクロールで全行をロード
        for i in range(max_rounds):
            await page.mouse.wheel(0, 10000)
            await asyncio.sleep(idle_ms / 1000)
            html = await page.content()
            matches = re.findall(r'href="(?:https:)?//www\.autohome\.com\.cn/(\d{3,7})/', html)
            uniq = list(dict.fromkeys(matches))
            delta = len(uniq) - prev_count
            print(f"  ⤷ round {i+1}: {len(uniq)}件 (+{delta})")
            if delta < min_delta:
                break
            prev_count = len(uniq)
        await browser.close()
        return uniq

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", default="https://www.autohome.com.cn/rank/1")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--name-col", default="model")
    ap.add_argument("--max-rounds", type=int, default=30)
    ap.add_argument("--idle-ms", type=int, default=650)
    ap.add_argument("--min-delta", type=int, default=3)
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    series_ids = asyncio.run(fetch_series_urls(args.rank_url,
                                               args.max_rounds,
                                               args.idle_ms,
                                               args.min_delta))
    print(f"✅ 抽出完了: {len(series_ids)}件")
    urls = [f"https://www.autohome.com.cn/{sid}/" for sid in series_ids]

    # 行数が一致しない場合は上位のみ使用
    n = min(len(df), len(urls))
    df = df.head(n).copy()
    df["series_url"] = urls[:n]
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"💾 保存完了: {args.output}")

if __name__ == "__main__":
    main()
