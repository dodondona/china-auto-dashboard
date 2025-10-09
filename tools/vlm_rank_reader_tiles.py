#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader_tiles.py
- /rank/1 を開く（www と m を自動リトライ）
- 最下部までスクロールしてフルページをスクショ
- 画像を縦タイルに分割して VLM (gpt-4o / gpt-4o-mini) に一括投入
- 画像に表示された順番のまま、rank/brand/model/count を JSON で返させる
- CSV (rank, brand, model, count) を出力

使い方:
  python tools/vlm_rank_reader_tiles.py \
    --url https://www.autohome.com.cn/rank/1 \
    --out data/autohome_rank_YYYY-MM_vlmfix.csv \
    --model gpt-4o-mini
"""

import os, io, re, csv, math, time, base64, json, argparse
from pathlib import Path
from typing import List
from openai import OpenAI
from playwright.sync_api import sync_playwright, TimeoutError
from PIL import Image

UA_MOBILE = (
    "Mozilla/5.0 (Linux; Android 11; Pixel 5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Mobile Safari/537.36"
)

SYSTEM_PROMPT = """あなたは画像から自動車販売ランキング表を読むVLMです。
入力はランキングページのスクリーンショットを「上から下の順」に並べた複数画像です。
各画像に写る表をすべて読み取り、画面に表示された順に、行ごとに
  - rank: 行頭の整数（必ず読み取る。読めなければ null）
  - brand: ブランド名（中国語）
  - model: 車系名（中国語。角括弧【】があればその内側を優先）
  - count: 月販台数の整数（無ければ null）
を JSON 配列で返してください。

重要:
- 配列の順序は「画面の上から下」の順（入力画像の順）に揃えること。並べ替え禁止。
- JSON 以外のテキストは出力しないこと。
"""

def b64_image_bytes(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")

def goto_with_retries(page, url: str, timeout_ms: int = 120000):
    candidates = [
        (url, "load"),
        (url, "domcontentloaded"),
    ]
    # m版も試す（軽い）
    if "autohome.com.cn/rank/1" in url:
        candidates += [
            ("https://m.autohome.com.cn/rank/1", "load"),
            ("https://m.autohome.com.cn/rank/1", "domcontentloaded"),
        ]
    last = None
    for u, wait in candidates:
        try:
            page.goto(u, wait_until=wait, timeout=timeout_ms)
            return u
        except TimeoutError as e:
            last = e
            page.wait_for_timeout(1200)
    raise last or TimeoutError("goto retries exhausted")

def scroll_to_bottom(page, idle_ms=700, max_rounds=60):
    prev_h = 0
    stable = 0
    for _ in range(max_rounds):
        page.mouse.wheel(0, 20000)
        page.wait_for_timeout(idle_ms)
        h = page.evaluate("() => document.body.scrollHeight || document.documentElement.scrollHeight || 0")
        if h == prev_h: stable += 1
        else: stable = 0
        prev_h = h
        if stable >= 3:
            break
    return prev_h

def capture_fullpage_screenshot(url: str, out_png: Path):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(
            user_agent=UA_MOBILE,
            viewport={"width": 480, "height": 960},
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
        )
        page = ctx.new_page()
        reached = goto_with_retries(page, url, timeout_ms=120000)
        page.wait_for_load_state("networkidle")
        scroll_to_bottom(page, idle_ms=700, max_rounds=60)
        # 先頭でヘッダ被りを避ける
        page.evaluate("() => window.scrollTo(0, 0)")
        page.wait_for_timeout(200)
        page.screenshot(path=str(out_png), full_page=True)
        browser.close()

def slice_vertical(img_path: Path, max_slice_h: int = 2200) -> List[bytes]:
    im = Image.open(img_path)
    H = im.height
    n = math.ceil(H / max_slice_h)
    out = []
    for i in range(n):
        top = i * max_slice_h
        bottom = min(H, (i+1) * max_slice_h)
        crop = im.crop((0, top, im.width, bottom))
        buf = io.BytesIO()
        crop.save(buf, format="PNG")
        out.append(buf.getvalue())
    return out

def call_vlm_on_tiles(tiles: List[bytes], model: str) -> list:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    messages = [
        {"role":"system","content": SYSTEM_PROMPT},
        {"role":"user","content":[{"type":"text","text":"次の画像群を順に解析してください。"}]}
    ]
    for b in tiles:
        messages[1]["content"].append({"type":"image_url","image_url":{"url": f"data:image/png;base64,{b64_image_bytes(b)}"}})

    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        max_tokens=3500,
        messages=messages,
    )
    txt = resp.choices[0].message.content.strip()
    # JSON抽出に強くする
    try:
        data = json.loads(txt)
    except Exception:
        start = txt.find("[")
        end = txt.rfind("]")+1
        data = json.loads(txt[start:end])
    rows = data if isinstance(data, list) else data.get("rows", [])
    out = []
    for r in rows:
        # 正規化
        rank = r.get("rank")
        try:
            rank = int(str(rank).strip())
        except Exception:
            rank = None
        brand = (r.get("brand") or "").strip()
        model_name = (r.get("model") or "").strip()
        cnt = r.get("count")
        try:
            cnt = int(str(cnt).replace(",","")) if cnt not in (None,"") else None
        except Exception:
            cnt = None
        out.append({"rank": rank, "brand": brand, "model": model_name, "count": cnt})
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="https://www.autohome.com.cn/rank/1")
    ap.add_argument("--out", required=True)
    ap.add_argument("--model", default="gpt-4o-mini")
    ap.add_argument("--screenshot", default="data/_rank_fullpage.png")
    args = ap.parse_args()

    out_png = Path(args.screenshot)
    out_png.parent.mkdir(parents=True, exist_ok=True)

    print(f"📥 navigate & capture: {args.url}")
    capture_fullpage_screenshot(args.url, out_png)
    tiles = slice_vertical(out_png, max_slice_h=2200)
    print(f"🖼️ tiles: {len(tiles)}")

    rows = call_vlm_on_tiles(tiles, args.model)

    # rank が欠損の行は “視覚上の順” が保たれている前提で補完
    # まずNoneを末尾にして安定ソートし、次に順番で1..nを再付与（但し既存rankを尊重）
    tmp = []
    auto = 1
    for r in rows:
        tmp.append(r)
    # rankが全部埋まっていればそのまま使う／欠損があれば自動採番
    any_missing = any(x["rank"] is None for x in tmp)
    if any_missing:
        normalized = []
        for i, r in enumerate(tmp, start=1):
            rk = r["rank"] if r["rank"] is not None else i
            normalized.append({**r, "rank": rk})
        rows = normalized
    # 最終ソート
    rows = sorted(rows, key=lambda x: int(x["rank"]))

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["rank","brand","model","count"])
        w.writeheader()
        w.writerows(rows)
    print(f"✅ saved: {args.out} (rows={len(rows)})")

if __name__ == "__main__":
    main()
