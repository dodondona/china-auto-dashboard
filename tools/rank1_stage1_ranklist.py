#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Stage 1: Autohome 車系月間ランキング抽出
https://www.autohome.com.cn/rank/1 から上位50車両を取得

出力: data/autohome_rank_stage1.csv
項目:
 rank_seq,rank,seriesname,series_url,count,price,rank_change,ev_count,phev_count,img_url
"""

import csv
import re
import asyncio
from playwright.async_api import async_playwright

OUTPUT = "data/autohome_rank_stage1.csv"
TARGET_URL = "https://www.autohome.com.cn/rank/1"

async def scrape():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)

        # 自動スクロール（下位車種が lazy-load の場合）
        for _ in range(5):
            await page.mouse.wheel(0, 2000)
            await page.wait_for_timeout(1000)

        html = await page.content()

        # ===== 各車両要素を抽出 =====
        items = await page.query_selector_all("li.rank-list-item")
        rows = []

        for idx, li in enumerate(items[:50], start=1):
            try:
                rank = await li.query_selector_eval(".rank-num", "el => el.innerText.trim()")
                name = await li.query_selector_eval(".rank-list-title a", "el => el.innerText.trim()")
                href = await li.query_selector_eval(".rank-list-title a", "el => el.href")
                count = await li.query_selector_eval(".rank-list-num", "el => el.innerText.replace('辆','').trim()")

                # 価格範囲
                try:
                    price = await li.query_selector_eval(".rank-list-price", "el => el.innerText.trim()")
                except:
                    price = ""

                # ランク変動（上昇・下降・維持）
                try:
                    change = await li.query_selector_eval(".rank-list-change span", "el => el.innerText.trim()")
                    change = change if change else "0"
                except:
                    change = "0"

                # EV/PHEV 内訳（あれば）
                try:
                    evphev = await li.query_selector_eval(".rank-list-subinfo", "el => el.innerText.trim()")
                    ev_count, phev_count = "", ""
                    m = re.search(r"纯电动[:：](\d+)辆.*?插电混合[:：](\d+)辆", evphev)
                    if m:
                        ev_count, phev_count = m.groups()
                except:
                    ev_count, phev_count = "", ""

                # 画像URL
                try:
                    img_url = await li.query_selector_eval("img", "el => el.src")
                except:
                    img_url = ""

                rows.append([
                    idx, rank, name, href, count, price, change,
                    ev_count, phev_count, img_url
                ])
            except Exception as e:
                print(f"[warn] {idx}: {e}")
                continue

        await browser.close()

    # ===== CSV 出力 =====
    with open(OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["rank_seq","rank","seriesname","series_url","count","price","rank_change","ev_count","phev_count","img_url"])
        writer.writerows(rows)

    print(f"[saved] {OUTPUT} ({len(rows)} rows)")

if __name__ == "__main__":
    asyncio.run(scrape())
