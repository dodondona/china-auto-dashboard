#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL → フルページスクショ → タイル分割（overlap付き）→ VLMで行抽出 → CSV

修正点:
- Playwright側でCJKフォントを強制適用 → 豆腐(□)防止
- LLMで brand と model を推定 (brand列追加)
- CSV出力前に name を除去（fieldnames 不一致エラー対応）
"""

import os, csv, json, base64, argparse, time
from pathlib import Path
from typing import List
from PIL import Image
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
from openai import OpenAI

# ----------------------------- VLM プロンプト -----------------------------
SYSTEM_PROMPT = """あなたは表の読み取りに特化した視覚アシスタントです。
画像は中国の自動車販売ランキングです。
UI部品や広告は無視してください。
出力は JSON のみ。構造:
{
  "rows": [
    {"rank": <int|null>, "name": "<string>", "count": <int|null>}
  ]
}
"""
USER_PROMPT = "この画像に見えている全ての行を JSON で返してください。"

BRAND_PROMPT = """次の車名からブランド名とモデル名を分離してください。
出力は JSON のみ。構造:
{"brand":"<string>","model":"<string>"}
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

# ----------------------------- スクショ（HTMLは読まない） -----------------------------
def grab_fullpage_to(url: str, out_dir: Path, viewport=(1380, 2400)) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    full_path = out_dir / "full.jpg"

    MAX_RETRY = 3
    for attempt in range(1, MAX_RETRY + 1):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                viewport={"width": viewport[0], "height": viewport[1]},
                device_scale_factor=2,
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="zh-CN",
                extra_http_headers={"Accept-Language": "zh-CN,zh;q=0.9"},
            )
            page = ctx.new_page()

            # 豆腐防止：CJKフォント強制（WorkflowでCJKフォント導入済み前提）
            page.add_init_script("""
              try {
                const style = document.createElement('style');
                style.setAttribute('data-screenshot-font-patch','1');
                style.textContent = `
                  * { font-family:
                      "Noto Sans CJK SC","WenQuanYi Zen Hei","Noto Sans CJK JP",
                      "Noto Sans","Microsoft YaHei","PingFang SC",sans-serif !important; }
                `;
                document.documentElement.appendChild(style);
              } catch(e){}
            """)

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=180000)
                # 本文らしき要素を緩く待機
                for sel in ["table", ".rank-list", ".content", "body"]:
                    try:
                        page.wait_for_selector(sel, timeout=60000)
                        break
                    except PwTimeout:
                        continue
                # Webフォント読み込みをできるだけ待つ
                try:
                    page.evaluate("return document.fonts && document.fonts.ready.then(()=>true)")
                except Exception:
                    pass
                page.wait_for_timeout(800)
                # ベストエフォートで安定
                try:
                    page.wait_for_load_state("networkidle", timeout=15000)
                except PwTimeout:
                    pass
                page.wait_for_timeout(500)

                page.screenshot(path=full_path, full_page=True, type="jpeg", quality=90)
                browser.close()
                return full_path

            except PwTimeout:
                browser.close()
                if attempt == MAX_RETRY:
                    raise
                time.sleep(1.5 * attempt)
                continue

# ----------------------------- OpenAI VLM -----------------------------
class OpenAIVLM:
    def __init__(self, model: str, api_key: str | None):
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY 未設定")
        self.client = OpenAI(api_key=api_key)
        self.model = model

    def infer_json(self, image_path: Path) -> dict:
        b64 = base64.b64encode(image_path.read_bytes()).decode("ascii")
        resp = self.client.chat.completions.create(
            model=self.model,  # 例: gpt-4o
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
        try:
            return json.loads(resp.choices[0].message.content)
        except Exception:
            return {"rows":[]}

    def split_brand_model(self, name: str) -> dict:
        prompt = BRAND_PROMPT + f"\n車名: {name}"
        resp = self.client.chat.completions.create(
            model=self.model,  # 同じモデルでOK（テキストだけ）
            temperature=0,
            max_tokens=200,
            messages=[
                {"role":"system","content":"ブランド名とモデルを識別するアシスタントです。"},
                {"role":"user","content":prompt},
            ],
            response_format={"type":"json_object"},
        )
        try:
            return json.loads(resp.choices[0].message.content)
        except Exception:
            return {"brand":"", "model":name}

# ----------------------------- 正規化 & 重複排除 -----------------------------
def normalize_rows(rows_in: List[dict]) -> List[dict]:
    out = []
    for r in rows_in:
        name = (r.get("name") or "").strip()
        if not name:
            continue
        rank = r.get("rank")
        cnt = r.get("count")
        if isinstance(cnt, str):
            t = cnt.replace(",", "").replace(" ", "")
            cnt = int(t) if t.isdigit() else None
        out.append({"rank": rank, "name": name, "count": cnt})
    return out

def merge_dedupe_sort(list_of_rows: List[List[dict]]) -> List[dict]:
    merged: List[dict] = []
    seen = set()
    for rows in list_of_rows:
        for r in rows:
            key = (r.get("name") or "").replace(" ", "").replace("\u3000","")
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
    ap.add_argument("--model", default="gpt-4o")
    ap.add_argument("--openai-api-key", default=os.getenv("OPENAI_API_KEY"))
    ap.add_argument("--fullpage-split", action="store_true")
    args = ap.parse_args()

    # 1) フルページキャプチャ
    full_path = grab_fullpage_to(args.from_url, Path("tiles"))

    # 2) 分割
    if args.fullpage_split:
        tile_paths = split_full_image(full_path, Path("tiles"), args.tile_height, args.tile_overlap)
    else:
        tile_paths = [full_path]

    # 3) VLM読み取り
    vlm = OpenAIVLM(model=args.model, api_key=args.openai_api_key)
    all_rows: List[List[dict]] = []
    for p in tile_paths:
        data = vlm.infer_json(p)
        rows = normalize_rows(data.get("rows", []))
        print(f"[INFO] {p.name}: {len(rows)} rows")
        all_rows.append(rows)

    # 4) マージ & ブランド分解
    merged = merge_dedupe_sort(all_rows)
    for r in merged:
        bm = vlm.split_brand_model(r["name"])
        r["brand"] = bm.get("brand","")
        r["model"] = bm.get("model", r["name"])
        r.pop("name", None)   # ★ ここを追加：name を削除してCSV項目に一致させる

    # 5) CSV出力
    with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["rank_seq","rank","brand","model","count"])
        w.writeheader()
        for r in merged:
            w.writerow(r)

    print(f"[DONE] rows={len(merged)} -> {args.out}")

if __name__ == "__main__":
    main()
