#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL → フルページスクショ → タイル（オーバーラップ付き）→ VLMで行抽出 → CSV
"""

import os, io, re, sys, csv, json, time, base64, argparse
from pathlib import Path
from typing import List, Dict
from PIL import Image
from playwright.sync_api import sync_playwright
from openai import OpenAI
from bs4 import BeautifulSoup
import requests

# ----------------------------- プロンプト -----------------------------
SYSTEM_PROMPT = """あなたは表の読み取りに特化した視覚アシスタントです。
画像は中国の自動車販売ランキングのリストです。
UIの飾りやボタン（例: 查成交价、下载App）や広告は無視してください。
出力は JSON のみ。構造:
{
  "rows": [
    {"rank": <int|null>, "name": "<string>", "count": <int|null>}
  ]
}
ルール:
- 1行につき {"rank","name","count"} を出力。見えている行だけでよい。
- count は数字（销量等）。カンマや空白は取り除いた整数。
- ブランド行も省略せず残す。
- JSON 以外は出力しない。
"""

USER_PROMPT = "この画像に見えている全ての行（rank/name/count）をJSONだけで返してください。"

# ----------------------------- 画像分割（overlap付き） -----------------------------
def split_full_image(full_path: Path, out_dir: Path, tile_height: int, overlap: int) -> List[Path]:
    im = Image.open(full_path).convert("RGB")
    W, H = im.size
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []
    y, i = 0, 0
    step = max(1, tile_height - overlap)
    while y < H:
        y2 = min(y + tile_height, H)
        tile = im.crop((0, y, W, y2))
        p = out_dir / f"tile_{i:02d}.jpg"
        tile.save(p, "JPEG", quality=85, optimize=True)
        paths.append(p)
        i += 1
        if y2 >= H:
            break
        y += step
    print(f"[INFO] {len(paths)} tiles saved -> {out_dir}")
    return paths

def grab_fullpage_to(url: str, out_dir: Path, viewport=(1380, 2400)) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    full_path = out_dir / "full.jpg"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            viewport={"width": viewport[0], "height": viewport[1]},
            device_scale_factor=2,
        )
        page.goto(url, wait_until="networkidle", timeout=120000)
        page.screenshot(path=full_path, full_page=True, type="jpeg", quality=85)
        browser.close()
    return full_path

# ----------------------------- OpenAI VLM -----------------------------
class OpenAIVLM:
    def __init__(self, model: str, api_key: str | None):
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY が未設定です。")
        self.client = OpenAI(api_key=api_key)
        self.model = model

    def infer_json(self, image_path: Path) -> dict:
        b64 = base64.b64encode(image_path.read_bytes()).decode("ascii")
        resp = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            max_tokens=1200,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": [
                    {"type":"text","text":USER_PROMPT},
                    {"type":"image_url","image_url":{"url":f"data:image/jpeg;base64,{b64}"}}
                ]},
            ],
            response_format={"type": "json_object"},
        )
        txt = resp.choices[0].message.content
        try:
            return json.loads(txt)
        except Exception:
            return {"rows": []}

# ----------------------------- マージ＆並べ替え -----------------------------
def normalize_rows(rows_in: List[dict]) -> List[dict]:
    out = []
    for r in rows_in:
        name = (r.get("name") or "").strip()
        if not name:
            continue

        rank = r.get("rank")
        if isinstance(rank, float):
            rank = int(rank)
        if not isinstance(rank, int):
            rank = None

        cnt = r.get("count")
        if isinstance(cnt, str):
            t = cnt.replace(",", "").replace(" ", "")
            cnt = int(t) if t.isdigit() else None
        if isinstance(cnt, float):
            cnt = int(cnt)

        out.append({"rank": rank, "name": name, "count": cnt})
    return out

def merge_dedupe_sort(list_of_rows: List[List[dict]]) -> List[dict]:
    merged: List[dict] = []
    seen = set()
    for rows in list_of_rows:
        for r in rows:
            key = (r.get("name") or "").replace(" ", "").replace("\u3000", "")
            if key and key not in seen:
                seen.add(key)
                merged.append(r)

    merged.sort(key=lambda r: (-(r.get("count") or 0), r.get("name")))
    for i, r in enumerate(merged, 1):
        r["rank_seq"] = i
    return merged

# ----------------------------- MAIN -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-url", required=True)
    ap.add_argument("--tile-height", type=int, default=1200)
    ap.add_argument("--tile-overlap", type=int, default=220)
    ap.add_argument("--out", default="result.csv")
    ap.add_argument("--provider", default="openai")
    ap.add_argument("--model", default="gpt-4o")
    ap.add_argument("--openai-api-key", default=os.getenv("OPENAI_API_KEY"))
    ap.add_argument("--fullpage-split", action="store_true")
    args = ap.parse_args()

    full_path = grab_fullpage_to(args.from_url, Path("tiles"))

    if args.fullpage_split:
        tile_paths = split_full_image(full_path, Path("tiles"), args.tile_height, args.tile_overlap)
    else:
        tile_paths = [full_path]

    vlm = OpenAIVLM(model=args.model, api_key=args.openai_api_key)
    all_rows: List[List[dict]] = []
    for p in tile_paths:
        data = vlm.infer_json(p)
        rows = normalize_rows(data.get("rows", []))
        print(f"[INFO] {p.name}: {len(rows)} rows")
        all_rows.append(rows)

    merged = merge_dedupe_sort(all_rows)
    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["rank_seq","rank","name","count"])
        w.writeheader()
        for r in merged:
            w.writerow(r)

    print(f"[DONE] rows={len(merged)} -> {args.out}")

if __name__ == "__main__":
    main()
