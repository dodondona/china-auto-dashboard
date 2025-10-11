#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Autohomeの /rank/ ページを Playwright で開き、車種ページ(series_url)を抽出して
入力CSVに left-join し、series_url カラムを付与する。

前提:
- 入力CSVには少なくとも rank(整数) があること
- 出力CSVに series_url を追記
- キャッシュは使わない(毎回取得)

使い方:
python tools/append_series_url_from_web.playwright_full.py \
  --rank-url https://www.autohome.com.cn/rank/1-3-1071-x/ \
  --input data/autohome_raw_2025-08.csv \
  --output data/autohome_raw_2025-08_with_series.csv \
  --name-col model --max-rounds 1 --idle-ms 200 --min-delta 0
"""

import argparse
import asyncio
import csv
import re
from typing import Dict, List, Tuple

import pandas as pd
from playwright.async_api import async_playwright

RANK_ROW_RE = re.compile(r'/(\d+)/')  # e.g. href="https://www.autohome.com.cn/7806/"

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--rank-url", required=True)
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--name-col", default="model")  # CSV側のモデル名列(照合の参考に使うだけ)
    p.add_argument("--max-rounds", type=int, default=1)
    p.add_argument("--idle-ms", type=int, default=200)
    p.add_argument("--min-delta", type=int, default=0)
    return p.parse_args()

async def fetch_rank_table(rank_url: str) -> List[Tuple[int, str]]:
    """
    /rank/ページを開いて (rank, series_url) のリストを返す
    rank は 1 始まり。series_url は https://www.autohome.com.cn/<id>/ の完全URLに揃える
    """
    results: List[Tuple[int, str]] = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(rank_url, wait_until="networkidle", timeout=60000)

        # Autohomeのランキング表行を全部拾う。行ごとに rank と a[href] を取得
        rows = await page.query_selector_all("table, .rank-list, .rank-list table, .rank-table tr, tr")
        # 上記は保険。実体は…リンクが /<series_id>/ を含む a を列挙し、順番=rank とみなす
        anchors = await page.query_selector_all("a[href*='autohome.com.cn/']")
        hrefs = []
        for a in anchors:
            href = await a.get_attribute("href")
            if not href:
                continue
            m = RANK_ROW_RE.search(href)
            if m:
                # 正規化(プロトコル/末尾スラ付与)
                sid = m.group(1)
                hrefs.append(f"https://www.autohome.com.cn/{sid}/")

        # 出現順のユニーク化（同じseriesへの複数リンクがあるため）
        seen = set()
        uniq = []
        for h in hrefs:
            if h not in seen:
                uniq.append(h)
                seen.add(h)

        for idx, h in enumerate(uniq, start=1):
            results.append((idx, h))

        await browser.close()
    return results

def left_join_series_url(df: pd.DataFrame, rank_map: Dict[int, str]) -> pd.DataFrame:
    df2 = df.copy()
    if "rank" not in df2.columns:
        # rank_seq しか無い場合の保険
        if "rank_seq" in df2.columns:
            df2["rank"] = df2["rank_seq"]
        else:
            raise KeyError("input CSV must have 'rank' or 'rank_seq' column.")

    df2["rank"] = pd.to_numeric(df2["rank"], errors="coerce").astype("Int64")
    df2["series_url"] = df2["rank"].map(rank_map)
    return df2

def main():
    args = _parse_args()
    print(f"🧾 input: {args.input}")
    print(f"🌐 scraping: {args.rank_url}")

    rank_pairs = asyncio.run(fetch_rank_table(args.rank_url))
    rank_map = {r: u for r, u in rank_pairs}
    if not rank_map:
        print("▲ No series urls found in HTML")
    else:
        print(f"✓ scraped {len(rank_map)} series urls")

    df = pd.read_csv(args.input)
    out = left_join_series_url(df, rank_map)
    out.to_csv(args.output, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"► {args.input} -> {args.output}")

if __name__ == "__main__":
    main()
