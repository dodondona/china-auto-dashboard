#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
ランキングページをキャプチャ＋VLMで順位/モデル/台数を抽出し、
詳細ページをスクレイピングしてブランド名を補完する。

改良:
- ブランド取得は <title> を優先、無ければ <div.subnav-title-name> を利用。
- ブランド名が取れない場合は "未知" を入れる。
"""

import os, re, csv, json, base64, argparse, asyncio
from pathlib import Path
from typing import List, Dict
from playwright.async_api import async_playwright
from openai import OpenAI

# ------------------- VLM PROMPT -------------------
SYSTEM_PROMPT = """あなたは表の読み取りに特化したアシスタントです。
画像は中国の自動車販売ランキングです。
UIや広告は無視し、順位/車名/販売台数 を抽出してください。
"""
USER_PROMPT = "画像から rank, model, sales を CSV形式で返してください。"

# ------------------- VLM 呼び出し -------------------
def call_vlm(image_path: Path, api_key: str, model="gpt-4o-mini"):
    client = OpenAI(api_key=api_key)
    b64 = base64.b64encode(image_path.read_bytes()).decode("ascii")

    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": [
                {"type": "text", "text": USER_PROMPT},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
            ]},
        ],
    )
    text = resp.choices[0].message.content.strip()
    rows = []
    for line in text.splitlines():
        parts = [c.strip() for c in line.split(",")]
        if len(parts) >= 3 and parts[0].isdigit():
            try:
                rows.append({
                    "rank": int(parts[0]),
                    "model": parts[1],
                    "sales": int(parts[2].replace(",", "")),
                })
            except Exception:
                continue
    return rows

# ------------------- Playwright: スクショ -------------------
async def capture_fullpage(url: str, out_path: Path, viewport=(1380, 2400)):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": viewport[0], "height": viewport[1]})
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(3000)
        await page.screenshot(path=str(out_path), full_page=True)
        await browser.close()

# ------------------- ブランド取得 -------------------
async def fetch_brand_map(rank_url: str) -> Dict[str, str]:
    """
    ランキングページから各モデルの詳細リンクを辿り、ブランド名を取得。
    - <title> を優先
    - 無ければ <div.subnav-title-name> を利用
    """
    brand_map = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(rank_url, timeout=60000)

        # 車系リンクを取得
        links = await page.eval_on_selector_all(
            "a[href*='/']",
            "els => els.map(e => e.href)"
        )
        car_links = [l for l in links if re.match(r"https://www.autohome.com.cn/\\d+/", l)]

        for link in car_links:
            try:
                sub = await browser.new_page()
                await sub.goto(link, timeout=30000)

                # 1) title から取得
                title = await sub.title()
                brand, model = None, None
                if "-" in title:
                    brand, model = title.split("-", 1)
                    brand, model = brand.strip(), model.strip()
                else:
                    # 2) fallback: div.subnav-title-name
                    try:
                        txt = await sub.inner_text("div.subnav-title-name")
                        if "-" in txt:
                            brand, model = txt.split("-", 1)
                            brand, model = brand.strip(), model.strip()
                    except:
                        pass

                if brand and model:
                    brand_map[model] = brand

                await sub.close()
            except Exception as e:
                print(f"[WARN] brand fetch failed: {link} {e}")
        await browser.close()
    return brand_map

# ------------------- MAIN -------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-url", required=True)
    ap.add_argument("--out", default="result.csv")
    ap.add_argument("--openai-api-key", default=os.getenv("OPENAI_API_KEY"))
    args = ap.parse_args()

    tiles_dir = Path("tiles")
    tiles_dir.mkdir(exist_ok=True)
    full_path = tiles_dir / "full.jpg"

    # 1) ランキングページをスクショ
    asyncio.run(capture_fullpage(args.from_url, full_path))

    # 2) VLMで順位/車名/台数を抽出
    rows = call_vlm(full_path, args.openai_api_key)

    # 3) ブランド補完
    brand_map = asyncio.run(fetch_brand_map(args.from_url))
    for r in rows:
        # モデル名で一致検索（部分一致も検討）
        brand = None
        for k, v in brand_map.items():
            if r["model"].startswith(k) or k.startswith(r["model"]):
                brand = v
                break
        r["brand"] = brand if brand else "未知"

    # 4) CSV出力
    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["rank","model","brand","sales"])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[DONE] {len(rows)} rows -> {args.out}")

if __name__ == "__main__":
    main()
