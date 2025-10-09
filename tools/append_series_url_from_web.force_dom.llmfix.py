#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_from_web.force_dom.llmfix.py
- autohome.com.cn/rank/1 を開き、ページ全体のスクショをVLMに読み取らせ、
  表示されているランキングを確認し、model/seriesのずれを補正する。
- HTMLファイル保存は不要。
"""

import os, io, csv, json, time, base64, math
from pathlib import Path
from playwright.sync_api import sync_playwright
from openai import OpenAI
from PIL import Image

SYSTEM_PROMPT = """あなたは画像内の自動車販売ランキング表を精密に読み取るVLMです。
画像には「順位・ブランド・車系名・台数」が表として含まれています。
出力は JSON のみで、構造は以下の通りです：
{
  "rows": [
    {"rank": <int>, "brand": "<string>", "model": "<string>", "count": <int>}
  ]
}
"""

UA_MOBILE = (
    "Mozilla/5.0 (Linux; Android 11; Pixel 5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Mobile Safari/537.36"
)

def b64_image(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")

def capture_page(url: str, out_path: Path):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=UA_MOBILE,
            viewport={"width": 480, "height": 960},
            locale="zh-CN"
        )
        page = ctx.new_page()
        page.goto(url, wait_until="load", timeout=120000)
        for _ in range(60):
            page.mouse.wheel(0, 3000)
            time.sleep(0.5)
        page.wait_for_timeout(1000)
        page.screenshot(path=str(out_path), full_page=True)
        browser.close()

def infer_vlm(img_path: Path, model="gpt-4o-mini"):
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    b64 = b64_image(img_path)
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": [
                {"type": "text", "text": "次の画像から表を読み取ってください。"},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}}
            ]}
        ],
        temperature=0,
        max_tokens=2000
    )
    txt = resp.choices[0].message.content.strip()
    try:
        data = json.loads(txt)
    except Exception:
        data = json.loads(txt[txt.find("{"):txt.rfind("}")+1])
    return data["rows"]

def main():
    out_img = Path("data/rank_tmp.png")
    capture_page("https://www.autohome.com.cn/rank/1", out_img)
    rows = infer_vlm(out_img)

    out_csv = Path("data/autohome_rank_vlmfix.csv")
    with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["rank","brand","model","count"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"✅ VLMで補正データを出力しました: {out_csv}")

if __name__ == "__main__":
    main()
