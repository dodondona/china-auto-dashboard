#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stage2: 各車種ページからEV/PHEV区分を抽出しCSVに追記
"""

import asyncio, csv, os
from playwright.async_api import async_playwright

async def enrich(in_csv, out_csv):
    rows = []
    with open(in_csv, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for row in rows:
            type_hint = "Unknown"
            url = row.get("series_url")
            if not url:
                row["type_hint"] = type_hint
                continue
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                await page.wait_for_timeout(1000)
                text = await page.content()
                if any(k in text for k in ["纯电动", "电动车"]):
                    type_hint = "EV"
                elif "插电混动" in text:
                    type_hint = "PHEV"
                elif "增程" in text:
                    type_hint = "EREV"
            except Exception:
                type_hint = "Unknown"
            row["type_hint"] = type_hint

        await browser.close()

    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--inp", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    asyncio.run(enrich(args.inp, args.out))
