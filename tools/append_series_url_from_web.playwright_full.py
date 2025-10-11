#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Autohomeの series_url と title を付与するだけの最小スクリプト
- 入力: CSV (rank_seq, rank, brand, model, ... など。列名の厳密さは要求しない)
- 出力: _with_series.csv (series_url, title_raw を追加/更新)
- キャッシュ一切なし
- Autohome内で series_url が空の行だけ検索して補完（既に埋まっている行は尊重）

検索方針:
  1) すでに series_url がある → そのURLに直接アクセスして <title> を採取
  2) series_url が無い → Autohomeの検索で brand+model を叩いて、最有力の車系ページを1件拾う
     - 具体: https://sou.autohome.com.cn/zonghe?type=1&q=<brand+model>
     - 検索結果内の「/xxxx/」の車系トップ(数字IDで終わる)リンクを優先
"""

import argparse
import asyncio
import csv
import re
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

SEARCH_URL_TMPL = "https://sou.autohome.com.cn/zonghe?type=1&q={q}"
AUTONAME_RE = re.compile(r"https?://www\.autohome\.com\.cn/(\d{3,6})/?")

def guess_best_series_link(html: str) -> str | None:
    # 車系トップへのリンク（例: https://www.autohome.com.cn/7806/）
    # 似たリンクが複数あるので、最初のものを返す
    cands = re.findall(r'href="(https?://www\.autohome\.com\.cn/\d{3,6}/)"', html)
    return cands[0] if cands else None

def extract_title_from_html(html: str) -> str | None:
    # <title> ... </title>
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip()
    return None

async def fetch_page_html(page, url: str) -> str:
    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
    return await page.content()

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=False, help="省略時は *_with_series.csv に自動変換")
    ap.add_argument("--brand-col", default="brand")
    ap.add_argument("--model-col", default="model")
    ap.add_argument("--series-url-col", default="series_url")
    ap.add_argument("--title-col", default="title_raw")
    return ap.parse_args()

async def main_async(args):
    src = Path(args.input)
    if not src.exists():
        raise SystemExit(f"Input not found: {src}")

    df = pd.read_csv(src)
    if args.output:
        out_path = Path(args.output)
    else:
        out_path = src.with_name(src.stem + "_with_series.csv")

    # 列が無ければ作る
    for col in (args.series_url_col, args.title_col):
        if col not in df.columns:
            df[col] = ""

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-gpu","--no-sandbox"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (compatible; AutohomeScraper/1.0; +https://example.com/bot-ua)",
            locale="zh-CN"
        )
        page = await context.new_page()

        for idx, row in df.iterrows():
            brand = str(row.get(args.brand_col, "")).strip()
            model = str(row.get(args.model_col, "")).strip()
            series_url = str(row.get(args.series_url_col, "")).strip()
            title = str(row.get(args.title_col, "")).strip()

            # 1) series_url 既存 → title だけ拾い直す
            if series_url:
                try:
                    html = await fetch_page_html(page, series_url)
                    title_found = extract_title_from_html(html) or title
                    df.at[idx, args.title_col] = title_found
                    continue
                except Exception:
                    # 失敗したら検索にフォールバック
                    pass

            # 2) 検索
            if not (brand or model):
                continue
            q = (brand + " " + model).strip().replace(" ", "+")
            search_url = SEARCH_URL_TMPL.format(q=q)

            try:
                html = await fetch_page_html(page, search_url)
                best = guess_best_series_link(html)
                if best:
                    # 取得できたら、titleも取りに行く
                    html2 = await fetch_page_html(page, best)
                    title_found = extract_title_from_html(html2) or ""
                    df.at[idx, args.series_url_col] = best
                    df.at[idx, args.title_col] = title_found
            except Exception:
                # どうしてもダメなら空のまま進む
                pass

        await browser.close()

    df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Wrote: {out_path}")

def main():
    args = parse_args()
    asyncio.run(main_async(args))

if __name__ == "__main__":
    main()
