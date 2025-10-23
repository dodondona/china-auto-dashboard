# -*- coding: utf-8 -*-
# .github/scripts/stage_add_manufacturer_from_title.py
#
# 目的:
#  - 既存の CSV（rank/name/…/link を含む）を読み、
#  - 各行の link (= https://www.autohome.com.cn/<seriesId>) を開いて <title> を取得、
#  - タイトル形式「【車名】メーカー_…」から メーカー名 を抽出し、manufacturer 列として追記。
# 使い方（例）:
#   python .github/scripts/stage_add_manufacturer_from_title.py csv/autohome_rank.csv
#   # または複数:
#   python .github/scripts/stage_add_manufacturer_from_title.py csv/*.csv
#
# 出力:
#  - 入力ファイルの隣に  *_with_maker.csv  を生成。既存列は一切変更しません。

import sys
import re
from pathlib import Path
import asyncio
import pandas as pd
from playwright.async_api import async_playwright

TITLE_PATTERNS = [
    re.compile(r"】([^_【】]+)_"),        # 例: 【秦PLUS】比亚迪_秦PLUS报价_...
    re.compile(r"】([^|｜\-–—【】\s]+)[\|｜\-–—]"),  # 例: 仕切りが | や – の場合
    re.compile(r"】\s*([^_【】]+)\s"),   # 例: スペース区切り
]

def extract_maker_from_title(title: str) -> str | None:
    if not title:
        return None
    t = title.strip()
    for pat in TITLE_PATTERNS:
        m = pat.search(t)
        if m:
            maker = m.group(1).strip()
            # 安全整形（不要な記号を削る）
            maker = re.sub(r"[【】\[\]（）()|｜\-–—]+", "", maker).strip()
            if maker:
                return maker
    return None

async def fetch_title(page, url: str) -> str | None:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(800)
        return await page.title()
    except Exception:
        return None

async def annotate_file(csv_path: Path):
    df = pd.read_csv(csv_path)
    if "link" not in df.columns:
        print(f"[skip] {csv_path} (no 'link' column)")
        return

    # 既にある場合は上書きせず、欠損のみ補完
    if "manufacturer" not in df.columns:
        df["manufacturer"] = ""

    # 重複URLはまとめて取得
    targets = {}
    for i, row in df.iterrows():
        if isinstance(row.get("manufacturer", ""), str) and row["manufacturer"].strip():
            continue
        link = str(row.get("link") or "").strip()
        if link.startswith("http"):
            targets.setdefault(link, None)

    if targets:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context()
            page = await ctx.new_page()

            for url in targets.keys():
                title = await fetch_title(page, url)
                maker = extract_maker_from_title(title or "")
                targets[url] = maker or ""

            await ctx.close()
            await browser.close()

        # 反映
        for i, row in df.iterrows():
            if not (isinstance(row.get("manufacturer", ""), str) and row["manufacturer"].strip()):
                link = str(row.get("link") or "").strip()
                if link in targets and targets[link]:
                    df.at[i, "manufacturer"] = targets[link]

    out_path = csv_path.with_name(csv_path.stem + "_with_maker.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"✅ {out_path}  rows={len(df)}  (added/filled 'manufacturer')")

async def main():
    if len(sys.argv) < 2:
        print("Usage: python .github/scripts/stage_add_manufacturer_from_title.py <csv1> [<csv2> ...]")
        sys.exit(1)

    for arg in sys.argv[1:]:
        path = Path(arg)
        if path.exists() and path.suffix.lower() == ".csv":
            await annotate_file(path)
        else:
            # グロブ対応
            for p in Path().glob(arg):
                if p.suffix.lower() == ".csv":
                    await annotate_file(p)

if __name__ == "__main__":
    asyncio.run(main())
