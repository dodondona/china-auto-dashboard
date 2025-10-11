#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Stage 2: 各車種ページを巡回して type_hint と title_raw を補完
"""

import csv
import asyncio
from playwright.async_api import async_playwright

INPUT = "data/autohome_rank_stage1.csv"
OUTPUT = "data/autohome_rank_stage2.csv"

async def enrich():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        rows_out = []
        with open(INPUT, newline="", encoding="utf-8-sig") as f:
            reader = list(csv.DictReader(f))

        for i, r in enumerate(reader, start=1):
            url = r["series_url"]
            type_hint = "Unknown"
            title_raw = ""
            print(f"[{i}/{len(reader)}] visiting {url}")

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(2000)

                title_raw = await page.title()

                # ページ上部にある車種分類（纯电动 / 插电混动 / 燃油）
                try:
                    text = await page.text_content("body")
                    if "纯电" in text:
                        type_hint = "EV"
                    elif "插电" in text or "混动" in text:
                        type_hint = "PHEV"
                    elif "燃油" in text:
                        type_hint = "Gasoline"
                except:
                    pass

            except Exception as e:
                print(f"[err] {url}: {e}")

            r["type_hint"] = type_hint
            r["title_raw"] = title_raw
            rows_out.append(r)

        await browser.close()

    with open(OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=rows_out[0].keys())
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"[saved] {OUTPUT} ({len(rows_out)} rows)")

if __name__ == "__main__":
    asyncio.run(enrich())
