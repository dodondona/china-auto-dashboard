#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL からのフルページスクショ取得（Playwright）→ VLM（AI目視）で「順位/車名/台数」を抽出 → CSV に出力。

- OCRは使いません。人の“目視”に近い Vision-Language Model (VLM) を使って表から値を読み取ります。
- OpenAI (gpt-4o / gpt-4o-mini 推奨) を標準、Gemini も選択可。
- --from-url でURLを渡すと、全自動でスクショ→抽出→CSVまで行います。
- 既存の tiles/*.png を直接読ませることも可能 (--input)。
"""

import os, io, re, glob, csv, time, json, base64, argparse
from typing import List, Dict, Tuple, Optional
from PIL import Image, ImageDraw

# ---- 重要: 巨大画像でも Pillow が落ちないようにする -------------------------
# DecompressionBombError を無効化（巨大ページのフルスクショ対策）
Image.MAX_IMAGE_PIXELS = None

# ----------------------------- 画像取得（Playwright） -----------------------------
def grab_fullpage_to(out_dir: str, url: str, viewport_w: int, viewport_h: int,
                     device_scale_factor: float, split: bool, tile_height: int) -> List[str]:
    """
    URL を開いてフルページでスクショ。split=True なら縦方向にタイル分割して保存。
    戻り値は処理対象とする画像ファイルパスのリスト。
    """
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        raise RuntimeError("Playwright が見つかりません。`pip install playwright && playwright install chromium` を実行してください。") from e

    os.makedirs(out_dir, exist_ok=True)
    full_path = os.path.join(out_dir, "_page_full.jpg")  # ← JPEG に変更

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": viewport_w, "height": viewport_h},
            device_scale_factor=device_scale_factor,
        )
        page = context.new_page()
        # 読み込みが重い時に備え、ナビゲーションのタイムアウトを延長
        page.set_default_navigation_timeout(90_000)
        page.goto(url, wait_until="networkidle")
        # JPEGで保存（容量軽減）。quality は 0-100
        page.screenshot(path=full_path, full_page=True, type="jpeg", quality=82)
        browser.close()

    if split:
        return split_full_image(full_path, out_dir, tile_height)
    else:
        return [full_path]

def split_full_image(full_path: str, out_dir: str, tile_height: int) -> List[str]:
    im = Image.open(full_path).convert("RGB")
    W, H = im.size
    paths = []
    idx = 0
    for y0 in range(0, H, tile_height):
        y1 = min(y0 + tile_height, H)
        tile = im.crop((0, y0, W, y1))
        p = os.path.join(out_dir, f"tile_{idx:02d}.jpg")
        tile.save(p, format="JPEG", quality=90, optimize=True)
        paths.append(p)
        idx += 1
    print(f"[INFO] {len(paths)} tiles saved -> {out_dir}")
    return paths

# ----------------------------- 画像 → VLM 入力整形 -----------------------------
def load_and_downscale_for_vlm(path: str, max_side: int = 2200, jpeg_quality: int = 85) -> Tuple[str, Tuple[int,int]]:
    """
    VLMのトークン/コスト節約のため、長辺 max_side に収めて JPEG 化。Data URL (base64) を返す。
    """
    im = Image.open(path).convert("RGB")
    w, h = im.size
    if max(w, h) > max_side:
        scale = max_side / float(max(w, h))
        im = im.resize((int(w*scale), int(h*scale)), Image.LANCZOS)
    buf = io.BytesIO()
    im.save(buf, format="JPEG", quality=jpeg_quality, optimize=True)
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/jpeg;base64,{b64}", im.size

def strict_json_from_text(txt: str) -> dict:
    m = re.search(r"```json\s*(\{.*?\})\s*```", txt, flags=re.S)
    if m:
        return json.loads(m.group(1))
    m = re.search(r"(\{[\s\S]*\})", txt)
    if m:
        return json.loads(m.group(1))
    m = re.search(r"(\[[\s\S]*\])", txt)
    if m:
        return {"rows": json.loads(m.group(1))}
    return json.loads(txt)

def rows_from_payload(payload) -> List[dict]:
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = payload.get("rows") or payload.get("data") or payload.get("items") or []
    else:
        rows = []
    norm = []
    for r in rows:
        rank = r.get("rank")
        if isinstance(rank, float): rank = int(rank)
        if not isinstance(rank, int): rank = None
        name = (r.get("name") or r.get("brand") or r.get("model") or "").strip()
        cnt  = r.get("count") or r.get("sales") or r.get("units")
        if isinstance(cnt, str):
            t = cnt.replace(",", "").replace(" ", "")
            cnt = int(t) if t.isdigit() else None
        if isinstance(cnt, float):
            cnt = int(cnt)
        norm.append({"rank": rank, "name": name, "count": cnt})
    return norm

def merge_and_reindex(all_rows: List[dict]) -> List[dict]:
    with_rank = [r for r in all_rows if isinstance(r.get("rank"), int)]
    no_rank   = [r for r in all_rows if not isinstance(r.get("rank"), int)]
    out = sorted(with_rank, key=lambda x: x["rank"]) + no_rank
    dedup, seen = [], set()
    for r in out:
        key = (r.get("rank"), r.get("name"), r.get("count"))
        if r.get("name") and r.get("count") and key not in seen:
            seen.add(key)
            dedup.append(r)
    for i, r in enumerate(dedup, start=1):
        r["rank_seq"] = i
    return dedup

# ----------------------------- プロンプト -----------------------------
SYSTEM_PROMPT = """あなたは表の読み取りに特化した視覚アシスタントです。
画像は中国の自動車販売ランキング（車系の月販台数）です。
UIの飾り・ボタン・注釈（例: 查成交价、下载App 等）は無視してください。
出力は JSON のみ。構造:
{
  "rows": [
    {"rank": <int|null>, "name": "<string>", "count": <int|null>}
  ]
}
ルール:
- name は車系名（例: 轩逸、Model Y、朗逸）。メーカー名のみの行は除外。
- count は月間台数の整数。カンマや空白は取り除く。
- 画像にない行は作らない。自信がない行は省略してよい。
- JSON以外は返さない。説明文や余計な文字は不要。
"""

def make_user_prompt(band_hints: Optional[Dict[str,str]] = None) -> str:
    if not band_hints:
        return "画像内のランキング表から、見えている行だけを正確に抽出して、JSONだけを返してください。"
    return f"""画像内のランキング表から行を抽出し、JSONだけを返してください。
（X帯ヒント: rank={band_hints['rank']} / name={band_hints['name']} / count={band_hints['count']}。UIノイズは無視）"""

# ----------------------------- クライアント実装 -----------------------------
class OpenAIClient:
    def __init__(self, model: str, api_key: Optional[str] = None, base_url: Optional[str] = None):
        from openai import OpenAI
        if api_key:
            os.environ["OPENAI_API_KEY"] = api_key
        if base_url:
            os.environ["OPENAI_BASE_URL"] = base_url
        self.client = OpenAI()
        self.model = model

    def infer(self, data_urls: List[str], system_prompt: str, user_prompt: str, max_retries: int = 5, throttle_ms: int = 800) -> str:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": [
                {"type": "text", "text": user_prompt},
                *[{"type":"image_url","image_url":{"url":u}} for u in data_urls]
            ]}
        ]
        err = None
        for k in range(max_retries):
            try:
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=0,
                    response_format={"type": "json_object"},
                )
                return resp.choices[0].message.content
            except Exception as e:
                err = e
                time.sleep( (throttle_ms/1000.0) * (k+1) )
        raise err

class GeminiClient:
    def __init__(self, model: str, api_key: str):
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model)

    def infer(self, data_urls: List[str], system_prompt: str, user_prompt: str, max_retries: int = 3) -> str:
        import google.generativeai as genai
        parts = [system_prompt, "\n", user_prompt]
        for u in data_urls:
            header, b64 = u.split(",", 1)
            mime = header.split(";")[0].split(":")[1]
            parts.append(genai.upload_file(io.BytesIO(base64.b64decode(b64)), mime_type=mime))
        err = None
        for k in range(max_retries):
            try:
                resp = self.model.generate_content(parts)
                return resp.text
            except Exception as e:
                err = e
                time.sleep(1.2*(k+1))
        raise err

# ----------------------------- メイン -----------------------------
def main():
    ap = argparse.ArgumentParser(description="VLM(目視)抽出: URL→スクショ→抽出→CSV")
    ap.add_argument("--input", default=None, help='既存画像を使う場合のグロブ。例 "tiles/tile_*.png"')
    ap.add_argument("--from-url", default=None, help="このURLを開いてフルページスクショ")
    ap.add_argument("--out-dir", default="tiles")
    ap.add_argument("--fullpage-split", action="store_true", help="フルページを縦に分割して保存")
    ap.add_argument("--tile-height", type=int, default=1200)
    ap.add_argument("--viewport-w", type=int, default=1680)
    ap.add_argument("--viewport-h", type=int, default=2600)
    ap.add_argument("--device-scale-factor", type=float, default=3.0)

    ap.add_argument("--provider", choices=["openai","gemini"], default="openai")
    ap.add_argument("--model", default="gpt-4o")  # ここを既定で gpt-4o に
    ap.add_argument("--openai-api-key", default=os.getenv("OPENAI_API_KEY"))
    ap.add_argument("--openai-base-url", default=os.getenv("OPENAI_BASE_URL"))
    ap.add_argument("--gemini-model", default="gemini-1.5-flash")
    ap.add_argument("--gemini-api-key", default=os.getenv("GEMINI_API_KEY"))

    ap.add_argument("--max-side", type=int, default=2200, help="VLMへ送る前に長辺をこのpxに縮小")
    ap.add_argument("--jpeg-quality", type=int, default=85)
    ap.add_argument("--throttle-ms", type=int, default=800)

    ap.add_argument("--rank-x", default=None)
    ap.add_argument("--name-x", default=None)
    ap.add_argument("--count-x", default=None)

    ap.add_argument("--csv", default="result.csv")
    args = ap.parse_args()

    image_paths: List[str] = []
    if args.from_url:
        image_paths = grab_fullpage_to(
            out_dir=args.out_dir,
            url=args.from_url,
            viewport_w=args.viewport_w,
            viewport_h=args.viewport_h,
            device_scale_factor=args.device_scale_factor,
            split=args.fullpage_split,
            tile_height=args.tile_height,
        )
    if args.input:
        image_paths.extend(sorted(glob.glob(args.input)))

    image_paths = sorted(dict.fromkeys(image_paths))
    if not image_paths:
        print("[WARN] 画像が見つかりません。--from-url または --input を指定してください。")
        return

    if args.provider == "openai":
        if not (args.openai_api_key or os.getenv("OPENAI_API_KEY")):
            raise RuntimeError("OPENAI_API_KEY が未設定です。--openai-api-key または環境変数で指定してください。")
        client = OpenAIClient(model=args.model, api_key=args.openai_api_key, base_url=args.openai_base_url)
    else:
        if not (args.gemini_api_key or os.getenv("GEMINI_API_KEY")):
            raise RuntimeError("GEMINI_API_KEY が未設定です。--gemini-api-key または環境変数で指定してください。")
        client = GeminiClient(model=args.gemini_model, api_key=args.gemini_api_key)

    band_hints = None
    if args.rank_x and args.name_x and args.count_x:
        band_hints = {"rank": args.rank_x, "name": args.name_x, "count": args.count_x}
    user_prompt = make_user_prompt(band_hints)

    all_rows: List[dict] = []
    for p in image_paths:
        data_url, _ = load_and_downscale_for_vlm(p, max_side=args.max_side, jpeg_quality=args.jpeg_quality)
        txt = client.infer([data_url], SYSTEM_PROMPT, user_prompt, throttle_ms=args.throttle_ms)
        try:
            payload = strict_json_from_text(txt)
        except Exception:
            try:
                payload = json.loads(txt)
            except Exception:
                payload = {"rows": []}
        rows = rows_from_payload(payload)
        for r in rows:
            r["_image"] = os.path.basename(p)
        print(f"[INFO] {os.path.basename(p)} -> {len(rows)} rows")
        all_rows.extend(rows)

    merged = merge_and_reindex(all_rows)

    with open(args.csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["rank_seq","rank","name","count","_image"])
        w.writeheader()
        for r in merged:
            w.writerow({
                "rank_seq": r.get("rank_seq"),
                "rank": r.get("rank"),
                "name": r.get("name"),
                "count": r.get("count"),
                "_image": r.get("_image"),
            })
    print(f"[DONE] Wrote {len(merged)} rows -> {args.csv}")

if __name__ == "__main__":
    main()
