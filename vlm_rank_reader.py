#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL → フルページスクショ（オーバーラップ付き）→ VLMで rank/brand/model/count 抽出
さらに HTML 解析で model ごとのリンクを取得し、CSVに統合

改良点:
- brand は辞書マッピングで日本語表記に統一
- model は LLM で日本語翻訳列を追加
- 各モデルの Autohome 詳細ページ URL を追加
"""

import os, io, re, sys, csv, json, time, base64, argparse
from pathlib import Path
from typing import List, Dict
from PIL import Image
from playwright.sync_api import sync_playwright
from openai import OpenAI
from bs4 import BeautifulSoup

# ----------------------------- ブランド辞書 -----------------------------
BRAND_MAP = {
    "比亚迪": "BYD",
    "吉利": "吉利",
    "奇瑞": "奇瑞",
    "长安": "長安",
    "上汽通用五菱": "五菱",
    "上汽通用别克": "別克",
    "大众": "フォルクスワーゲン",
    "丰田": "トヨタ",
    "日产": "日産",
    "本田": "ホンダ",
    "特斯拉": "Tesla",
    "小米": "小米",
    "赛力斯": "賽力斯",
    "理想": "理想",
    "蔚来": "蔚来",
    # 必要に応じて追加
}

# ----------------------------- プロンプト -----------------------------
SYSTEM_PROMPT = """あなたは表の読み取りに特化した視覚アシスタントです。
画像は中国の自動車販売ランキングのリストです。
出力は JSON のみ。構造:
{
  "rows": [
    {"rank": <int|null>, "brand": "<string|null>", "model": "<string>", "count": <int|null>}
  ]
}
ルール:
- 1行につき {"rank","brand","model","count"} を出力。
- brand が分からない場合は null。
- count は数字。
- JSON 以外は出力しない。
"""

USER_PROMPT = "この画像に見えている全ての行（rank/brand/model/count）をJSONだけで返してください。"

# ----------------------------- スクショ＆HTML -----------------------------
def grab_fullpage_and_html(url: str, out_dir: Path, viewport=(1380, 2400)) -> tuple[Path, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    full_path = out_dir / "full.jpg"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            viewport={"width": viewport[0], "height": viewport[1]},
            device_scale_factor=2,
        )
        page.goto(url, wait_until="networkidle", timeout=90000)
        html = page.content()
        page.screenshot(path=full_path, full_page=True, type="jpeg", quality=85)
        browser.close()
    return full_path, html

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
            max_tokens=1500,
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

    def translate_model_jp(self, name: str) -> str:
        if not name:
            return ""
        prompt = f"次の中国語車種名を自然な日本語に翻訳してください。\n{name}\n出力は日本語名のみ。"
        resp = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            max_tokens=100,
            messages=[
                {"role": "system", "content": "あなたは中国語の自動車モデル名を日本語に翻訳するアシスタントです。"},
                {"role": "user", "content": prompt},
            ],
        )
        return resp.choices[0].message.content.strip()

# ----------------------------- 正規化・マージ -----------------------------
def normalize_rows(rows_in: List[dict], vlm: OpenAIVLM, url_map: dict) -> List[dict]:
    out = []
    for r in rows_in:
        model = (r.get("model") or "").strip()
        brand = (r.get("brand") or "").strip()
        count = r.get("count")

        # ブランド日本語化
        brand_jp = BRAND_MAP.get(brand, brand)

        # モデル翻訳
        model_jp = vlm.translate_model_jp(model) if model else ""

        # URLリンク付与（model名で突合）
        link = url_map.get(model, "")

        # 数字整形
        if isinstance(count, str):
            t = count.replace(",", "").replace(" ", "")
            count = int(t) if t.isdigit() else None
        if isinstance(count, float):
            count = int(count)

        out.append({
            "rank": r.get("rank"),
            "brand": brand_jp,
            "model": model,
            "model_jp": model_jp,
            "count": count,
            "url": link
        })
    return out

def merge_dedupe_sort(list_of_rows: List[List[dict]]) -> List[dict]:
    merged: List[dict] = []
    seen = set()
    for rows in list_of_rows:
        for r in rows:
            key = (r.get("brand"), r.get("model"))
            if key not in seen:
                seen.add(key)
                merged.append(r)

    merged.sort(key=lambda r: (-(r.get("count") or 0), r.get("brand"), r.get("model")))
    for i, r in enumerate(merged, 1):
        r["rank_seq"] = i
    return merged

# ----------------------------- HTML解析でURL取得 -----------------------------
def extract_model_links(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    url_map = {}
    for a in soup.select("a"):
        href = a.get("href", "")
        text = (a.get_text() or "").strip()
        if href and re.search(r"/\d+/", href) and text:
            if not href.startswith("http"):
                href = "https://www.autohome.com.cn" + href
            url_map[text] = href
    return url_map

# ----------------------------- MAIN -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-url", required=True)
    ap.add_argument("--tile-height", type=int, default=1200)
    ap.add_argument("--tile-overlap", type=int, default=220)
    ap.add_argument("--out", default="result.csv")
    ap.add_argument("--model", default="gpt-4o")
    ap.add_argument("--openai-api-key", default=os.getenv("OPENAI_API_KEY"))
    ap.add_argument("--fullpage-split", action="store_true")
    args = ap.parse_args()

    # スクショとHTML
    full_path, html = grab_fullpage_and_html(args.from_url, Path("tiles"))
    url_map = extract_model_links(html)

    # 分割
    if args.fullpage_split:
        tile_paths = split_full_image(full_path, Path("tiles"), args.tile_height, args.tile_overlap)
    else:
        tile_paths = [full_path]

    # VLM
    vlm = OpenAIVLM(model=args.model, api_key=args.openai_api_key)
    all_rows: List[List[dict]] = []
    for p in tile_paths:
        data = vlm.infer_json(p)
        rows = normalize_rows(data.get("rows", []), vlm, url_map)
        print(f"[INFO] {p.name}: {len(rows)} rows")
        all_rows.append(rows)

    # マージ & CSV
    merged = merge_dedupe_sort(all_rows)
    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["rank_seq","rank","brand","model","model_jp","count","url"])
        w.writeheader()
        for r in merged:
            w.writerow(r)

    print(f"[DONE] rows={len(merged)} -> {args.out}")

if __name__ == "__main__":
    main()
