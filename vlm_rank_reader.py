#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL → フルページスクショ（Playwright）→ 画像を分割（オーバーラップあり）→ VLMで
ランキング表「順位 / 名前（ブランド or 車系） / 台数」を読み取り → CSV 出力。

変更点（timeout対策）:
- domcontentloaded → networkidle の二段待機 + 最大3回リトライ
- 自動スクロールで遅延読み込みを完了
- ナビゲーション/待機のタイムアウト拡大
- UA/locale/timezoneを実ブラウザっぽく
"""

import os, io, re, glob, csv, time, json, base64, argparse
from typing import List, Dict, Tuple, Optional
from PIL import Image
Image.MAX_IMAGE_PIXELS = None  # 大きなPNGでもエラー停止しない

# ----------------------------- Playwright 抓取 -----------------------------
def _scroll_to_bottom(page, pause_ms: int = 350, max_steps: int = 80):
    """遅延読み込みを確実に終わらせるため末尾までスクロール"""
    last_height = 0
    for _ in range(max_steps):
        page.evaluate("window.scrollBy(0, document.documentElement.clientHeight * 0.9)")
        time.sleep(pause_ms / 1000.0)
        h = page.evaluate("document.documentElement.scrollHeight")
        if h == last_height:
            break
        last_height = h

def grab_fullpage_to(out_dir: str, url: str, viewport_w: int, viewport_h: int,
                     device_scale_factor: float, split: bool,
                     tile_height: int, tile_overlap: int) -> List[str]:
    """
    URL を開いてフルページでスクショ。split=True なら縦オーバーラップ付きで分割。
    リトライ/二段待機/自動スクロールで networkidle タイムアウトを緩和。
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except Exception as e:
        raise RuntimeError(
            "Playwright が見つかりません。`pip install playwright && python -m playwright install --with-deps chromium`"
        ) from e

    os.makedirs(out_dir, exist_ok=True)
    full_path = os.path.join(out_dir, "_page_full.png")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": viewport_w, "height": viewport_h},
            device_scale_factor=device_scale_factor,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
        )
        page = context.new_page()
        page.set_default_navigation_timeout(120_000)
        page.set_default_timeout(120_000)

        last_err = None
        for attempt in range(1, 4):  # 最大3回
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                # 広告等で networkidle が来ないことがあるので、まずは読み込みを促進
                _scroll_to_bottom(page, pause_ms=350, max_steps=80)
                # それでも残っているリクエストを待つ
                page.wait_for_load_state("networkidle", timeout=60_000)
                # 念押しで最上部へ戻す
                page.evaluate("window.scrollTo(0,0)")
                page.wait_for_timeout(600)
                page.screenshot(path=full_path, full_page=True)
                break
            except PWTimeout as e:
                last_err = e
                if attempt < 3:
                    time.sleep(1.5 * attempt)  # 少し待ってリトライ
                    continue
                raise
        browser.close()

    return split_full_image(full_path, out_dir, tile_height, tile_overlap) if split else [full_path]

def split_full_image(full_path: str, out_dir: str, tile_height: int, tile_overlap: int) -> List[str]:
    from PIL import Image
    im = Image.open(full_path).convert("RGB")
    W, H = im.size
    paths = []
    idx = 0
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
def load_and_downscale_for_vlm(path: str, max_side: int = 1800, jpeg_quality: int = 82) -> Tuple[str, Tuple[int,int]]:
    from PIL import Image
    im = Image.open(path).convert("RGB")
    w, h = im.size
    if max(w, h) > max_side:
        scale = max_side / float(max(w, h))
        im = im.resize((int(w*scale), int(h*scale)), Image.LANCZOS)
    import io, base64
    buf = io.BytesIO()
    im.save(buf, format="JPEG", quality=jpeg_quality, optimize=True)
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/jpeg;base64,{b64}", im.size

def strict_json_from_text(txt: str) -> dict:
    m = re.search(r"```json\s*(\{.*?\})\s*```", txt, flags=re.S)
    if m: return json.loads(m.group(1))
    m = re.search(r"(\{[\s\S]*\})", txt)
    if m: return json.loads(m.group(1))
    m = re.search(r"(\[[\s\S]*\])", txt)
    if m: return {"rows": json.loads(m.group(1))}
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
        name = (r.get("name") or r.get("brand") or r.get("model") or "").strip()
        if not name: continue
        rank = r.get("rank")
        if isinstance(rank, float): rank = int(rank)
        if not isinstance(rank, int): rank = None
        cnt  = r.get("count") or r.get("sales") or r.get("units")
        if isinstance(cnt, str):
            t = re.sub(r"[,\s台辆臺輛]", "", cnt)
            cnt = int(t) if t.isdigit() else None
        if isinstance(cnt, float): cnt = int(cnt)
        norm.append({"rank": rank, "name": name, "count": cnt})
    return norm

def merge_and_reindex(all_rows: List[dict]) -> List[dict]:
    with_rank = [r for r in all_rows if isinstance(r.get("rank"), int)]
    no_rank   = [r for r in all_rows if not isinstance(r.get("rank"), int)]
    out = sorted(with_rank, key=lambda x: x["rank"]) + no_rank
    dedup, seen = [], set()
    for r in out:
        key = (r.get("rank"), r.get("name"), r.get("count"))
        if r.get("name") and key not in seen:
            seen.add(key); dedup.append(r)
    for i, r in enumerate(dedup, start=1):
        r["rank_seq"] = i
    return dedup

SYSTEM_PROMPT = """あなたは表の読み取りに特化した視覚アシスタントです。
画像は「自動車のランキング表（ブランド or 車系 の月販台数）」です。
重要: メーカー名だけの行（ブランド行）も含め、見えている行は漏らさず抽出してください。
UIの装飾・ボタン・広告は無視。
出力は JSON のみ:
{
  "rows": [
    {"rank": <int|null>, "name": "<string>", "count": <int|null>}
  ]
}
- name はブランド/車系どちらでも可
- count は整数（カンマ/空白/「台」等は除去）
- JSON 以外は出さない
"""
def make_user_prompt() -> str:
    return "ランキング表の行をそのまま抽出し、JSONだけを返してください。"

class OpenAIClient:
    def __init__(self, model: str, api_key: Optional[str] = None, base_url: Optional[str] = None):
        from openai import OpenAI
        if api_key: os.environ["OPENAI_API_KEY"] = api_key
        if base_url: os.environ["OPENAI_BASE_URL"] = base_url
        self.client = OpenAI(); self.model = model
    def infer(self, data_urls: List[str], system_prompt: str, user_prompt: str, max_retries: int = 4, tpm_pause_ms: int = 800) -> str:
        messages = [
            {"role":"system","content":system_prompt},
            {"role":"user","content":[{"type":"text","text":user_prompt}, *[{"type":"image_url","image_url":{"url":u}} for u in data_urls]]}
        ]
        err=None
        for k in range(max_retries):
            try:
                resp=self.client.chat.completions.create(
                    model=self.model, messages=messages, temperature=0,
                    response_format={"type":"json_object"},
                )
                return resp.choices[0].message.content
            except Exception as e:
                err=e; time.sleep(tpm_pause_ms/1000.0*(k+1))
        raise err

class GeminiClient:
    def __init__(self, model: str, api_key: str):
        import google.generativeai as genai
        genai.configure(api_key=api_key); self.model = genai.GenerativeModel(model)
    def infer(self, data_urls: List[str], system_prompt: str, user_prompt: str, max_retries: int = 3) -> str:
        import google.generativeai as genai, io, base64
        parts=[system_prompt,"\n",user_prompt]
        for u in data_urls:
            header,b64=u.split(",",1); mime=header.split(";")[0].split(":")[1]
            parts.append(genai.upload_file(io.BytesIO(base64.b64decode(b64)), mime_type=mime))
        err=None
        for k in range(max_retries):
            try:
                resp=self.model.generate_content(parts); return resp.text
            except Exception as e:
                err=e; time.sleep(1.2*(k+1))
        raise err

def main():
    ap = argparse.ArgumentParser(description="VLM(目視)抽出: URL→スクショ→抽出→CSV")
    ap.add_argument("--input", default=None)
    ap.add_argument("--from-url", default=None)
    ap.add_argument("--out-dir", default="tiles")
    ap.add_argument("--fullpage-split", action="store_true")
    ap.add_argument("--tile-height", type=int, default=1200)
    ap.add_argument("--tile-overlap", type=int, default=240)
    ap.add_argument("--viewport-w", type=int, default=1680)
    ap.add_argument("--viewport-h", type=int, default=2600)
    ap.add_argument("--device-scale-factor", type=float, default=3.0)
    ap.add_argument("--provider", choices=["openai","gemini"], default="openai")
    ap.add_argument("--model", default="gpt-4o")
    ap.add_argument("--openai-api-key", default=os.getenv("OPENAI_API_KEY"))
    ap.add_argument("--openai-base-url", default=os.getenv("OPENAI_BASE_URL"))
    ap.add_argument("--gemini-model", default="gemini-1.5-flash")
    ap.add_argument("--gemini-api-key", default=os.getenv("GEMINI_API_KEY"))
    ap.add_argument("--max-side", type=int, default=1800)
    ap.add_argument("--jpeg-quality", type=int, default=82)
    ap.add_argument("--csv", default="result.csv")
    args = ap.parse_args()

    image_paths=[]
    if args.from_url:
        image_paths = grab_fullpage_to(
            out_dir=args.out_dir, url=args.from_url,
            viewport_w=args.viewport_w, viewport_h=args.viewport_h,
            device_scale_factor=args.device_scale_factor,
            split=args.fullpage-split, tile_height=args.tile_height,
            tile_overlap=args.tile_overlap,
        )
    if args.input:
        image_paths.extend(sorted(glob.glob(args.input)))
    image_paths = sorted(dict.fromkeys(image_paths))
    if not image_paths:
        print("[WARN] 画像が見つかりません。--from-url または --input を指定してください。"); return

    user_prompt = make_user_prompt()
    if args.provider=="openai":
        if not (args.openai_api_key or os.getenv("OPENAI_API_KEY")):
            raise RuntimeError("OPENAI_API_KEY が未設定です。")
        client = OpenAIClient(model=args.model, api_key=args.openai_api_key, base_url=args.openai_base_url)
    else:
        if not (args.gemini_api_key or os.getenv("GEMINI_API_KEY")):
            raise RuntimeError("GEMINI_API_KEY が未設定です。")
        client = GeminiClient(model=args.gemini_model, api_key=args.gemini_api_key)

    all_rows=[]
    for p in image_paths:
        data_url,_ = load_and_downscale_for_vlm(p, max_side=args.max_side, jpeg_quality=args.jpeg_quality)
        txt = client.infer([data_url], SYSTEM_PROMPT, user_prompt)
        try:
            payload = strict_json_from_text(txt)
        except Exception:
            try: payload=json.loads(txt)
            except Exception: payload={"rows":[]}
        rows = rows_from_payload(payload)
        for r in rows: r["_image"]=os.path.basename(p)
        print(f"[INFO] {os.path.basename(p)} -> {len(rows)} rows")
        all_rows.extend(rows)

    merged = merge_and_reindex(all_rows)
    with open(args.csv, "w", newline="", encoding="utf-8-sig") as f:
        w=csv.DictWriter(f, fieldnames=["rank_seq","rank","name","count","_image"])
        w.writeheader()
        for r in merged:
            w.writerow({k:r.get(k) for k in ["rank_seq","rank","name","count","_image"]})
    print(f"[DONE] Wrote {len(merged)} rows -> {args.csv}")

if __name__ == "__main__":
    main()
