#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL → フルページスクショ → タイル分割 → VLMで行抽出 → CSV
- Autohome ページの <title> や パンくずから brand を優先抽出
- 失敗時のみ LLM フォールバック
"""

import os, csv, json, base64, argparse, time, re
from pathlib import Path
from typing import List, Dict
from PIL import Image
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
from openai import OpenAI
import requests
from bs4 import BeautifulSoup

# ----------------------------- VLM（表読み取り） -----------------------------
SYSTEM_PROMPT = """あなたは表の読み取りに特化した視覚アシスタントです。
画像は中国の自動車販売ランキングです。広告やUI部品は無視してください。
出力は JSON のみ。構造:
{"rows":[{"rank":<int|null>,"name":"<string>","count":<int|null>}]}
"""
USER_PROMPT = "販売台数ランキング表をすべて読み取り、順位(rank)/車名(name)/販売台数(count) をJSONで返してください。"

# ----------------------------- ブランド分離（LLMフォールバック） -----------------------------
BRAND_PROMPT = """你是中国车系名称解析助手。给定一个“车系/车型名称”，请输出对应的【品牌/厂商】与【车型名】。
输出 JSON: {"brand":"<string>","model":"<string>"}
"""

# ----------------------------- タイル分割 -----------------------------
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
        tile.save(p, "JPEG", quality=90, optimize=True)
        paths.append(p)
        i += 1
        if y2 >= H:
            break
        y += step
    return paths

# ----------------------------- スクショ -----------------------------
def grab_fullpage_to(url: str, out_dir: Path, viewport=(1380, 2400)) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    full_path = out_dir / "full.jpg"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": viewport[0], "height": viewport[1]},
            device_scale_factor=3,  # ★豆腐対策で解像度上げ
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124 Safari/537.36",
            locale="zh-CN",
            extra_http_headers={"Accept-Language": "zh-CN,zh;q=0.9"},
        )
        page = ctx.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=180000)
            page.wait_for_selector("body", state="visible", timeout=10000)
            time.sleep(2.5)
            page.screenshot(path=str(full_path), full_page=True)
            browser.close()
            return full_path
        except Exception:
            browser.close()
            raise

# ----------------------------- OpenAI 呼び出し -----------------------------
def vlm_extract_rows(image_path: Path, model="gpt-4o") -> List[Dict]:
    client = OpenAI()
    b64 = base64.b64encode(image_path.read_bytes()).decode("utf-8")
    msgs = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": [
            {"type": "input_text", "text": USER_PROMPT},
            {"type": "input_image", "image_url": f"data:image/jpeg;base64,{b64}"}
        ]}
    ]
    resp = client.responses.create(model=model, input=msgs, temperature=0)
    try:
        return json.loads(resp.output_text)["rows"]
    except Exception:
        return []

# ----------------------------- ブランド解決 -----------------------------
def resolve_brand_via_autohome(name: str) -> Dict[str, str]:
    """Autohome で検索し、title/pan-breadcrumbからブランドを抜く"""
    try:
        search_url = f"https://www.autohome.com.cn/fastsearch?type=3&q={name}"
        r = requests.get(search_url, timeout=10)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "lxml")
        a = soup.find("a", href=re.compile(r"/\d+/"))
        if not a:
            return None
        car_url = "https:" + a["href"] if a["href"].startswith("//") else a["href"]
        r2 = requests.get(car_url, timeout=10)
        if r2.status_code != 200:
            return None
        soup2 = BeautifulSoup(r2.text, "lxml")
        title = soup2.find("title")
        if title:
            m = re.match(r"(.*?)-(.*?)_.*", title.text.strip())
            if m:
                return {"brand": m.group(1), "model": name}
        return None
    except Exception:
        return None

def vlm_split_brand(name: str, model="gpt-4o") -> Dict[str, str]:
    # まず Autohome から取得
    r = resolve_brand_via_autohome(name)
    if r:
        return r
    # フォールバックで LLM
    client = OpenAI()
    msgs = [
        {"role": "system", "content": BRAND_PROMPT},
        {"role": "user", "content": name}
    ]
    resp = client.chat.completions.create(model=model, messages=msgs, temperature=0, max_tokens=64)
    try:
        return json.loads(resp.choices[0].message.content.strip())
    except Exception:
        return {"brand": "未知", "model": name}

# ----------------------------- CSV -----------------------------
def write_csv(path: Path, rows: List[Dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["rank_seq","rank","name","brand","count"])
        w.writeheader()
        w.writerows(rows)

# ----------------------------- メイン -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--tile-height", type=int, default=900)
    ap.add_argument("--overlap", type=int, default=120)
    ap.add_argument("--model", default="gpt-4o")
    args = ap.parse_args()

    tmp_dir = Path("tiles")
    full_img = grab_fullpage_to(args.url, tmp_dir)
    tiles = split_full_image(full_img, tmp_dir, args.tile_height, args.overlap)

    all_rows: List[Dict] = []
    for tile in tiles:
        rows = vlm_extract_rows(tile, model=args.model)
        for r in rows:
            if not r.get("name"):
                continue
            bm = vlm_split_brand(r["name"], model=args.model)
            all_rows.append({
                "rank_seq": str(len(all_rows)+1),
                "rank": r.get("rank"),
                "name": r.get("name"),
                "brand": bm.get("brand","未知"),
                "count": r.get("count"),
            })
        time.sleep(1.2)

    write_csv(Path(args.out), all_rows)
    print(f"[OK] {len(all_rows)} rows -> {args.out}")

if __name__ == "__main__":
    main()
