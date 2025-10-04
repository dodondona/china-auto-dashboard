#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL からのフルページスクショ取得（Playwright）→ VLM（AI目視）で「順位/車名/台数」を抽出 → CSV に出力。

- OCRは使いません。人の“目視”に近い Vision-Language Model (VLM) を使って表から値を読み取ります。
- OpenAI (gpt-4o-mini 推奨) を標準、Gemini も選択可。
- --from-url でURLを渡すと、全自動でスクショ→抽出→CSVまで行います。
- 既存の tiles/*.png を直接読ませることも可能 (--input)。

インストール例:
  pip install --upgrade openai pillow playwright
  playwright install chromium
  # （Gemini利用時のみ）
  pip install --upgrade google-generativeai

実行例:
  # URLから一発
  python vlm_rank_reader.py ^
    --from-url "https://www.autohome.com.cn/rank/1-3-1071-x/2025-08.html" ^
    --out data/autohome_raw_2025-08.csv ^
    --model gpt-4o-mini
"""

import os, io, re, glob, csv, time, json, argparse, asyncio
from PIL import Image
from playwright.async_api import async_playwright

# ===============================
# VLM呼び出し（OpenAI or Gemini）
# ===============================

import openai
import google.generativeai as genai

def call_vlm_openai(img_bytes, model="gpt-4o-mini", prompt=""):
    client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role":"system","content":"You are a vision-language assistant."},
            {"role":"user","content":[
                {"type":"text","text":prompt},
                {"type":"image_url","image_url":{"url":"data:image/png;base64,"+img_bytes.decode()}}
            ]}
        ],
        temperature=0,
    )
    return resp.choices[0].message.content.strip()

# ===============================
# Playwrightでフルページスクショ
# ===============================

async def screenshot_fullpage(url, outdir="tiles", tile_height=1600, tile_overlap=200):
    os.makedirs(outdir, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto(url)
        full_height = await page.evaluate("document.body.scrollHeight")
        width = await page.evaluate("document.body.scrollWidth")
        n = 0
        for y in range(0, full_height, tile_height - tile_overlap):
            clip = {"x":0,"y":y,"width":width,"height":min(tile_height, full_height-y)}
            path = os.path.join(outdir, f"tile_{n:03d}.png")
            await page.screenshot(path=path, clip=clip)
            n += 1
        await browser.close()

# ===============================
# CSV保存
# ===============================

def save_csv(rows, outpath):
    fieldnames = ["rank_seq","rank","brand","model","count"]
    with open(outpath,"w",newline="",encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "rank_seq": row.get("rank_seq",""),
                "rank": row.get("rank",""),
                # ←ここを修正: brand が空なら "未知"
                "brand": row.get("brand", "").strip() if row.get("brand") else "未知",
                "model": row.get("model",""),
                "count": row.get("count",""),
            })

# ===============================
# メイン処理
# ===============================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-url")
    parser.add_argument("--input", help="tiles/*.png")
    parser.add_argument("--out", default="result.csv")
    parser.add_argument("--tile-height", type=int, default=1600)
    parser.add_argument("--tile-overlap", type=int, default=200)
    parser.add_argument("--model", default="gpt-4o-mini")
    args = parser.parse_args()

    if args.from_url:
        asyncio.run(screenshot_fullpage(args.from_url, outdir="tiles", tile_height=args.tile_height, tile_overlap=args.tile_overlap))
        args.input = "tiles/tile_*.png"

    # ここでVLM呼び出しなどを実行する処理が入る（省略、あなたの既存処理そのまま）
    # 例:
    rows = [
        {"rank_seq":"1","rank":"1","brand":"比亚迪","model":"秦PLUS","count":"12345"},
        {"rank_seq":"2","rank":"2","brand":"","model":"Model Y","count":"12000"}, # brand空テスト
    ]

    save_csv(rows, args.out)
    print("CSV saved:", args.out)

if __name__ == "__main__":
    main()
