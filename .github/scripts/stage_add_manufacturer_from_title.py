# -*- coding: utf-8 -*-
# .github/scripts/stage_add_manufacturer_from_title.py
#
# 目的:
#   - 既存の CSV（rank/name/.../link を含む）を読み、
#   - 各行の link (= https://www.autohome.com.cn/<seriesId>) を開いて <title> を取得、
#   - タイトル形式「【車名】メーカー_…」から メーカー名 を抽出し、manufacturer 列として追記。
#   - 結果は *_with_maker.csv として保存。
#
# 特徴:
#   ✅ 処理途中の進行状況をリアルタイム出力
#   ✅ 既存列・フォーマットは一切変更なし
#   ✅ 途中で中断しても既存ファイルに影響なし

import sys
import re
from pathlib import Path
import asyncio
import pandas as pd
from playwright.async_api import async_playwright

# リアルタイム出力（GitHub Actions用）
sys.stdout.reconfigure(line_buffering=True)

TITLE_PATTERNS = [
    re.compile(r"】([^_【】]+)_"),         # 例: 【秦PLUS】比亚迪_秦PLUS报价_...
    re.compile(r"】([^|｜\-–—【】\s]+)[\|｜\-–—]"),  # 例: 仕切りが | や – の場合
    re.compile(r"】\s*([^_【】]+)\s"),     # 例: スペース区切り
]

def extract_maker_from_title(title: str) -> str | None:
    """タイトル文字列からメーカー名を抽出"""
    if not title:
        return None
    t = title.strip()
    for pat in TITLE_PATTERNS:
        m = pat.search(t)
        if m:
            maker = m.group(1).strip()
            maker = re.sub(r"[【】\[\]（）()|｜\-–—]+", "", maker).strip()
            if maker:
                return maker
    return None


async def fetch_title(page, url: str) -> str | None:
    """ページを開いて<title>を取得"""
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(800)
        return await page.title()
    except Exception as e:
        print(f"⚠️ Error fetching {url}: {e}")
        return None


async def annotate_file(csv_path: Path):
    """CSV 1件分の処理"""
    print(f"\n=== Processing {csv_path} ===")
    df = pd.read_csv(csv_path)
    if "link" not in df.columns:
        print(f"[skip] {csv_path} (no 'link' column)")
        return

    # manufacturer列がなければ追加
    if "manufacturer" not in df.columns:
        df["manufacturer"] = ""

    # 重複除外してURL一覧を作成
    targets = {}
    for _, row in df.iterrows():
        if isinstance(row.get("manufacturer", ""), str) and row["manufacturer"].strip():
            continue
        link = str(row.get("link") or "").strip()
        if link.startswith("http"):
            targets.setdefault(link, None)

    if not targets:
        print("No new links to process.")
        out_path = csv_path.with_name(csv_path.stem + "_with_maker.csv")
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"✅ {out_path}  rows={len(df)} (no new data)")
        return

    print(f"Total unique links to fetch: {len(targets)}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()

        for idx, url in enumerate(targets.keys(), 1):
            print(f"[{idx}/{len(targets)}] visiting {url}")
            title = await fetch_title(page, url)
            maker = extract_maker_from_title(title or "")
            print(f" → title: {title[:80] if title else '(none)'}")
            print(f" → extracted manufacturer: {maker or '-'}\n")
            targets[url] = maker or ""

        await ctx.close()
        await browser.close()

    # 取得結果を反映
    for i, row in df.iterrows():
        if not (isinstance(row.get("manufacturer", ""), str) and row["manufacturer"].strip()):
            link = str(row.get("link") or "").strip()
            if link in targets and targets[link]:
                df.at[i, "manufacturer"] = targets[link]

    out_path = csv_path.with_name(csv_path.stem + "_with_maker.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"✅ {out_path}  rows={len(df)} (filled manufacturer column)\n")


async def main():
    if len(sys.argv) < 2:
        print("Usage: python .github/scripts/stage_add_manufacturer_from_title.py <csv1> [<csv2> ...]")
        sys.exit(1)

    # glob対応
    args = []
    for arg in sys.argv[1:]:
        p = Path(arg)
        if p.exists() and p.suffix.lower() == ".csv":
            args.append(p)
        else:
            for f in Path().glob(arg):
                if f.suffix.lower() == ".csv":
                    args.append(f)

    if not args:
        print("No CSV files matched.")
        sys.exit(0)

    for path in args:
        await annotate_file(path)


if __name__ == "__main__":
    asyncio.run(main())
