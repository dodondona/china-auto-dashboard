#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL → フルページスクショ（Playwright）→ 画像を分割（オーバーラップあり）→ VLMで
ランキング表「順位 / 名前（ブランドでも車系でもOK） / 台数」を読み取り → CSV 出力。

ポイント
- メーカー名だけの行も「絶対に省略しない」。ブランド/車系の別は区別せず、見えている行はそのまま出す。
- 途切れ対策: タイル分割時に上下オーバーラップ（デフォルト 240px）。同一行の重複はマージ時に解消。
- PIL の DecompressionBombError を無効化（GitHub Actions の巨大PNG対応）。
- OpenAI(gpt-4o 既定) か Gemini を選択可。
"""

import os, io, re, glob, csv, time, json, base64, argparse
from typing import List, Dict, Tuple, Optional
from PIL import Image

# 巨大画像でも止まらないように（GitHub Actions のフルページPNG対策）
Image.MAX_IMAGE_PIXELS = None

# ----------------------------- 画像取得（Playwright） -----------------------------
def grab_fullpage_to(out_dir: str, url: str, viewport_w: int, viewport_h: int,
                     device_scale_factor: float, split: bool,
                     tile_height: int, tile_overlap: int) -> List[str]:
    """
    URL を開いてフルページスクショ。split=True なら縦方向にオーバーラップ付きで分割。
    戻り値は処理対象の画像ファイルパスのリスト。
    """
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        raise RuntimeError(
            "Playwright が見つかりません。`pip install playwright && python -m playwright install --with-deps chromium` を実行してください。"
        ) from e

    os.makedirs(out_dir, exist_ok=True)
    full_path = os.path.join(out_dir, "_page_full.png")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": viewport_w, "height": viewport_h},
            device_scale_factor=device_scale_factor,
        )
        page = context.new_page()
        # ネットワークが落ち着くまで待機
        page.goto(url, wait_until="networkidle", timeout=60_000)
        page.screenshot(path=full_path, full_page=True)
        browser.close()

    if split:
        return split_full_image(full_path, out_dir, tile_height, tile_overlap)
    else:
        return [full_path]


def split_full_image(full_path: str, out_dir: str, tile_height: int, tile_overlap: int) -> List[str]:
    """
    フル画像を縦に分割。上下に tile_overlap ピクセルだけ重ねて切り出す（途切れ対策）。
    """
    im = Image.open(full_path).convert("RGB")
    W, H = im.size
    paths = []
    idx = 0

    # 次の開始位置 = 前タイルの終端 - overlap
    y0 = 0
    while y0 < H:
        y1 = min(y0 + tile_height, H)
        tile = im.crop((0, y0, W, y1))
        p = os.path.join(out_dir, f"tile_{idx:02d}.jpg")
        tile.save(p, "JPEG", quality=92, optimize=True)
        paths.append(p)
        idx += 1
        if y1 >= H:
            break
        y0 = max(0, y1 - tile_overlap)

    print(f"[INFO] {len(paths)} tiles saved -> {out_dir} (overlap={tile_overlap})")
    return paths


# ----------------------------- 画像 → VLM 入力整形 -----------------------------
def load_and_downscale_for_vlm(path: str, max_side: int = 1800, jpeg_quality: int = 82) -> Tuple[str, Tuple[int, int]]:
    """
    VLM コスト節約のため長辺 max_side に縮小 → JPEG 化。Data URL (base64) を返す。
    """
    im = Image.open(path).convert("RGB")
    w, h = im.size
    if max(w, h) > max_side:
        scale = max_side / float(max(w, h))
        im = im.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
    buf = io.BytesIO()
    im.save(buf, format="JPEG", quality=jpeg_quality, optimize=True)
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/jpeg;base64,{b64}", im.size


def strict_json_from_text(txt: str) -> dict:
    """
    モデル応答から JSON オブジェクトを確実に抽出。
    ```json ...``` 優先、だめなら最初の { ... } / [ ... ] を拾う。
    """
    m = re.search(r"```json\s*(\{.*?\})\s*```", txt, flags=re.S)
    if m:
        return json.loads(m.group(1))
    m = re.search(r"(\{[\s\S]*\})", txt)
    if m:
        return json.loads(m.group(1))
    m = re.search(r"(\[[\s\S]*\])", txt)
    if m:
        return {"rows": json.loads(m.group(1))}
    # 最後の手段
    return json.loads(txt)


def rows_from_payload(payload) -> List[dict]:
    """
    payload -> rows 正規化。 {"rows":[{rank,name,count},...]} を想定し、方言を許容。
    ※ ここでは絶対にフィルタしない（ブランド名だけでも残す）
    """
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = payload.get("rows") or payload.get("data") or payload.get("items") or []
    else:
        rows = []
    norm = []
    for r in rows:
        name = (r.get("name") or r.get("brand") or r.get("model") or "").strip()
        if not name:
            continue

        rank = r.get("rank")
        if isinstance(rank, float):
            rank = int(rank)
        if not isinstance(rank, int):
            rank = None

        cnt = r.get("count") or r.get("sales") or r.get("units")
        if isinstance(cnt, str):
            t = re.sub(r"[,\s台辆辆臺輛]", "", cnt)
            cnt = int(t) if t.isdigit() else None
        if isinstance(cnt, float):
            cnt = int(cnt)

        norm.append({"rank": rank, "name": name, "count": cnt})
    return norm


def merge_and_reindex(all_rows: List[dict]) -> List[dict]:
    """
    複数タイル結果の結合。
    - rank があるものは rank 昇順
    - rank 無しはそのまま追加
    - 完全重複 (rank, name, count) は除外
    """
    with_rank = [r for r in all_rows if isinstance(r.get("rank"), int)]
    no_rank = [r for r in all_rows if not isinstance(r.get("rank"), int)]

    out = sorted(with_rank, key=lambda x: x["rank"]) + no_rank

    dedup, seen = [], set()
    for r in out:
        key = (r.get("rank"), r.get("name"), r.get("count"))
        if r.get("name") and key not in seen:
            seen.add(key)
            dedup.append(r)

    # 表示用の rank_seq（連番）
    for i, r in enumerate(dedup, start=1):
        r["rank_seq"] = i
    return dedup


# ----------------------------- プロンプト -----------------------------
SYSTEM_PROMPT = """あなたは表の読み取りに特化した視覚アシスタントです。
画像は「自動車のランキング表（ブランド or 車系 の月販台数）」です。

重要: “メーカー名だけの行（ブランド行）も含めて、見えている行は1行ずつ漏れなく抽出”してください。
UIの装飾・ボタン・広告など（例: 查成交价、下载App 等）は無視。

出力は JSON のみ:
{
  "rows": [
    {"rank": <int|null>, "name": "<string>", "count": <int|null>},
    ...
  ]
}

ルール:
- name は、ブランド（例: 大众、丰田、比亚迪 等）でも、車系（例: 轩逸、Model Y 等）でも良い。
- count は月間台数の整数。カンマ/空白/「台」などは除去して数値に。
- 表にない行は作らない。確信が持てない行は省略してよい。
- 説明文は不要。JSON だけを返す。
"""

def make_user_prompt() -> str:
    return "ランキング表の行をそのまま抽出し、JSONだけを返してください。"


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

    def infer(self, data_urls: List[str], system_prompt: str, user_prompt: str, max_retries: int = 4, tpm_pause_ms: int = 800) -> str:
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
                # 軽いTPM/RPM制御（429対策）
                time.sleep(tpm_pause_ms/1000.0 * (k+1))
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
    # 画像入力
    ap.add_argument("--input", default=None, help='既存画像グロブ（例: "tiles/tile_*.jpg"）')
    # URLからの自動取得
    ap.add_argument("--from-url", default=None, help="このURLを開いてフルページスクショ")
    ap.add_argument("--out-dir", default="tiles")
    ap.add_argument("--fullpage-split", action="store_true", help="フルページを縦に分割して保存")
    ap.add_argument("--tile-height", type=int, default=1200)
    ap.add_argument("--tile-overlap", type=int, default=240, help="上下の重なり(px) 途切れ対策")
    ap.add_argument("--viewport-w", type=int, default=1680)
    ap.add_argument("--viewport-h", type=int, default=2600)
    ap.add_argument("--device-scale-factor", type=float, default=3.0)

    # VLM設定
    ap.add_argument("--provider", choices=["openai","gemini"], default="openai")
    ap.add_argument("--model", default="gpt-4o")  # 既定: gpt-4o（miniではない）
    ap.add_argument("--openai-api-key", default=os.getenv("OPENAI_API_KEY"))
    ap.add_argument("--openai-base-url", default=os.getenv("OPENAI_BASE_URL"))
    ap.add_argument("--gemini-model", default="gemini-1.5-flash")
    ap.add_argument("--gemini-api-key", default=os.getenv("GEMINI_API_KEY"))

    # 送信前の縮小・画質
    ap.add_argument("--max-side", type=int, default=1800)
    ap.add_argument("--jpeg-quality", type=int, default=82)

    # 出力
    ap.add_argument("--csv", default="result.csv")

    args = ap.parse_args()

    # 画像の準備
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
            tile_overlap=args.tile_overlap,
        )
    if args.input:
        image_paths.extend(sorted(glob.glob(args.input)))

    image_paths = sorted(dict.fromkeys(image_paths))
    if not image_paths:
        print("[WARN] 画像が見つかりません。--from-url または --input を指定してください。")
        return

    # VLM クライアント
    user_prompt = make_user_prompt()
    if args.provider == "openai":
        if not (args.openai_api_key or os.getenv("OPENAI_API_KEY")):
            raise RuntimeError("OPENAI_API_KEY が未設定です。--openai-api-key または Secrets/環境変数で指定してください。")
        client = OpenAIClient(model=args.model, api_key=args.openai_api_key, base_url=args.openai_base_url)
    else:
        if not (args.gemini_api_key or os.getenv("GEMINI_API_KEY")):
            raise RuntimeError("GEMINI_API_KEY が未設定です。--gemini-api-key または環境変数で指定してください。")
        client = GeminiClient(model=args.gemini_model, api_key=args.gemini_api_key)

    # 読み取り
    all_rows: List[dict] = []
    for p in image_paths:
        data_url, _ = load_and_downscale_for_vlm(p, max_side=args.max_side, jpeg_quality=args.jpeg_quality)
        txt = client.infer([data_url], SYSTEM_PROMPT, user_prompt)
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

    # CSV 出力
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
