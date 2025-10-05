#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
tools/enrich_brand_scrape.py

入力CSV（rank_seq,rank,name,count）に brand 列を付与する。
方法：ランキングページの各「車系詳細ページ」へ遷移し、<title>（なければ見出し）から「ブランド-車系」を取得。
辞書不要・LLM不要。

使い方:
  python tools/enrich_brand_scrape.py \
    --rank-url "https://www.autohome.com.cn/rank/1/2025-08.html" \
    --in  data/autohome_raw_2025-08.csv \
    --out data/autohome_rank_2025-08.csv
"""

import re
import csv
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import asyncio
from playwright.async_api import async_playwright

# ---------- 正規化 ----------
def norm_model(s: str) -> str:
    if not s:
        return ""
    t = s.strip()
    # 全角/半角スペース除去・連続空白を1つに
    t = re.sub(r"[\u3000\s]+", " ", t)
    # 末尾の装飾っぽい語をゆるく削る（例: 新能源 / MAX 等は残す）
    # 過度に削ると別車系と衝突するので最小限
    return t

def split_brand_model(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    "上汽大众-朗逸 参数配置_汽车之家" → ("上汽大众", "朗逸")
    フォールバック: "div.subnav-title-name" の "品牌-车系"
    """
    if not text:
        return (None, None)
    # ハイフンは各種（-, –, —, －）
    m = re.split(r"\s*[-–—－]\s*", text, maxsplit=1)
    if len(m) < 2:
        return (None, None)

    brand = m[0].strip()
    right = m[1].strip()
    # モデル名は右側の先頭の語（ただし "Model Y" のような英字空白は保持）
    # 記号以降は捨てる（例: "参数配置_汽车之家" など）
    mm = re.match(r"([A-Za-z0-9\u4e00-\u9fff\+\- ]+)", right)
    model = mm.group(1).strip() if mm else right
    return (brand or None, model or None)

# ---------- ランキングページ → 車系詳細リンク抽出 ----------
async def collect_car_links(rank_url: str) -> List[str]:
    links: List[str] = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(rank_url, timeout=60000)
        # aタグのhrefをすべて取って、車系詳細リンクに絞る
        hrefs = await page.eval_on_selector_all(
            "a[href]",
            "els => els.map(e => e.href)"
        )
        # 形式: https://www.autohome.com.cn/614/ のような数値直下
        for h in hrefs:
            if re.match(r"^https://www\.autohome\.com\.cn/\d+/?", h):
                links.append(h.split("#", 1)[0])  # #以降は除去
        await browser.close()
    # 重複排除（順序維持）
    seen = set()
    uniq = []
    for h in links:
        if h not in seen:
            seen.add(h)
            uniq.append(h)
    return uniq

# ---------- 車系詳細 → ブランド／モデル取得 ----------
async def build_brand_map(rank_url: str, delay_ms: int = 300) -> Dict[str, str]:
    """
    車系詳細ページを巡回して {model_normalized: brand} を作る。
    <title> を最優先、ダメなら div.subnav-title-name をフォールバック。
    """
    brand_map: Dict[str, str] = {}
    links = await collect_car_links(rank_url)
    if not links:
        return brand_map

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        for link in links:
            try:
                page = await browser.new_page()
                await page.goto(link, timeout=45000)
                title = await page.title()

                brand, model = split_brand_model(title)
                if not (brand and model):
                    # fallback: ページ見出し
                    try:
                        txt = await page.inner_text("div.subnav-title-name")
                        brand, model = split_brand_model(txt)
                    except Exception:
                        brand, model = (None, None)

                if brand and model:
                    brand_map[norm_model(model)] = brand.strip()

                await page.close()
                if delay_ms > 0:
                    await asyncio.sleep(delay_ms / 1000)
            except Exception:
                # 失敗は無視して継続
                pass
        await browser.close()
    return brand_map

# ---------- CSV 読み書き ----------
def read_rows(csv_path: Path) -> List[dict]:
    rows: List[dict] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def write_rows(csv_path: Path, rows: List[dict]) -> None:
    # rank_seq,rank,name,brand,count の順で出力
    fields = ["rank_seq", "rank", "name", "brand", "count"]
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            out = {
                "rank_seq": row.get("rank_seq"),
                "rank": row.get("rank"),
                "name": row.get("name"),
                "brand": row.get("brand", "未知"),
                "count": row.get("count"),
            }
            w.writerow(out)

# ---------- メイン ----------
async def main_async(args):
    in_path = Path(args.input)
    out_path = Path(args.output)

    rows = read_rows(in_path)
    # 先に正規化したキーを作っておく
    for r in rows:
        r["_name_norm"] = norm_model(r.get("name", ""))

    brand_map = await build_brand_map(args.rank_url, delay_ms=args.delay_ms)

    # 照合（厳密一致 → 前方/後方のゆるい一致）
    for r in rows:
        nm = r["_name_norm"]
        brand = brand_map.get(nm)
        if not brand:
            # startswith/endswith の緩い照合
            for k, v in brand_map.items():
                if nm and (nm.startswith(k) or k.startswith(nm)):
                    brand = v
                    break
        r["brand"] = brand if brand else "未知"

    # 出力
    write_rows(out_path, rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", required=True, help="ランキングページURL（例: https://www.autohome.com.cn/rank/1/2025-08.html）")
    ap.add_argument("--in", dest="input", required=True, help="入力CSV（vlm_rank_reader.pyの出力）")
    ap.add_argument("--out", dest="output", required=True, help="出力CSV（brand列を付与）")
    ap.add_argument("--delay-ms", type=int, default=300, help="詳細ページ巡回の間隔ms（ブロック/負荷対策）")
    args = ap.parse_args()
    asyncio.run(main_async(args))

if __name__ == "__main__":
    main()
