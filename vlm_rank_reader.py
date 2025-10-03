#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL → Playwrightでフルページ撮影（分割可）→ GPT-4o で表の「順位/車名/台数」を目視抽出 → CSV出力

※ モデルは常に GPT-4o 固定（引数で変更不可）
"""

import os, io, re, glob, csv, time, json, base64, argparse
from typing import List, Dict, Tuple, Optional
from PIL import Image

# ===================== Playwright: スクショ取得（堅牢版） =====================
def grab_fullpage_to(out_dir: str, url: str, viewport_w: int, viewport_h: int,
                     device_scale_factor: float, split: bool, tile_height: int) -> List[str]:
    """
    URL を開いてフルページスクショ。split=True なら縦方向にタイル分割。
    - wait_until="domcontentloaded" でナビ待機を緩める
    - 追加でnetworkidleを軽く待つ（失敗は無視）
    - ゆっくりスクロールして遅延読み込みを発火
    - 最大3回リトライ
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    os.makedirs(out_dir, exist_ok=True)
    full_path = os.path.join(out_dir, "_page_full.png")

    def smooth_scroll(page):
        page.evaluate("""
        () => new Promise(resolve => {
          let y = 0;
          const step = 800;
          const timer = setInterval(() => {
            window.scrollBy(0, step);
            y += step;
            if (y + window.innerHeight >= document.body.scrollHeight) {
              clearInterval(timer);
              setTimeout(resolve, 800);
            }
          }, 200);
        })
        """)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
        )
        context = browser.new_context(
            viewport={"width": viewport_w, "height": viewport_h},
            device_scale_factor=device_scale_factor,
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"),
            locale="zh-CN",
            extra_http_headers={
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        page = context.new_page()
        page.set_default_navigation_timeout(90_000)
        page.set_default_timeout(90_000)

        last_err = None
        for attempt in range(3):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                try:
                    page.wait_for_load_state("networkidle", timeout=10_000)
                except PWTimeout:
                    pass  # 無視して進む
                smooth_scroll(page)
                page.screenshot(path=full_path, full_page=True)
                break
            except Exception as e:
                last_err = e
                if attempt == 2:
                    browser.close()
                    raise
                time.sleep(2.0 + attempt)  # 少し待ってリトライ
        browser.close()

    return split_full_image(full_path, out_dir, tile_height) if split else [full_path]

def split_full_image(full_path: str, out_dir: str, tile_height: int) -> List[str]:
    im = Image.open(full_path).convert("RGB")
    W, H = im.size
    paths = []
    idx = 0
    for y0 in range(0, H, tile_height):
        y1 = min(y0 + tile_height, H)
        tile = im.crop((0, y0, W, y1))
        p = os.path.join(out_dir, f"tile_{idx:02d}.png")
        tile.save(p)
        paths.append(p)
        idx += 1
    print(f"[INFO] {len(paths)} tiles saved -> {out_dir}")
    return paths

# ===================== 画像 → VLM 入力整形 =====================
def load_and_downscale_for_vlm(path: str, max_side: int = 1600, jpeg_quality: int = 80) -> Tuple[str, Tuple[int,int]]:
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
    return json.loads(txt)

def rows_from_payload(payload) -> List[dict]:
    """payload -> rows 正規化。 {"rows":[{rank,name,count},...]} 想定で方言許容。"""
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
        if name:
            norm.append({"rank": rank, "name": name, "count": cnt})
    return norm

def merge_and_reindex(all_rows: List[dict]) -> List[dict]:
    """複数画像結果の結合と rank_seq の付与。rank付き優先で昇順ソート。"""
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

# ===================== プロンプト =====================
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

def make_user_prompt() -> str:
    return "画像内のランキング表から、見えている行だけを正確に抽出して、JSONだけを返してください。"

# ===================== OpenAI クライアント（gpt-4o固定） =====================
class OpenAIClient:
    """
    モデルは常に gpt-4o を使用。引数からの上書きは不可。
    """
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None):
        from openai import OpenAI
        if api_key:
            os.environ["OPENAI_API_KEY"] = api_key
        if base_url:
            os.environ["OPENAI_BASE_URL"] = base_url
        self.client = OpenAI()
        self.model = "gpt-4o"  # ここで固定

    def infer(self, data_urls: List[str], system_prompt: str, user_prompt: str, max_retries: int = 8) -> str:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": [
                {"type": "text", "text": user_prompt},
                *[{"type":"image_url","image_url":{"url":u}} for u in data_urls]
            ]}
        ]
        last_err = None
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
                # 429対策：メッセージから待機msやRetry-Afterを読む
                wait = 1.2 * (k + 1)
                txt = str(e)
                m = re.search(r"try again in (\d+)\s*ms", txt)
                if m:
                    wait = max(wait, int(m.group(1)) / 1000.0)
                try:
                    ra = getattr(e, "response", None)
                    if ra and hasattr(ra, "headers"):
                        retry_after = ra.headers.get("retry-after")
                        if retry_after:
                            wait = max(wait, float(retry_after))
                except Exception:
                    pass
                last_err = e
                time.sleep(min(wait, 10.0))
        raise last_err

# ===================== メイン =====================
def main():
    ap = argparse.ArgumentParser(description="VLM(目視)抽出: URL→スクショ→抽出→CSV 一気通貫（gpt-4o固定）")
    # 入力
    ap.add_argument("--from-url", default=None, help="このURLを開いてフルページスクショ")
    ap.add_argument("--input", default=None, help='既存画像のグロブ。例 "tiles/tile_*.png"')

    # スクショ設定
    ap.add_argument("--out-dir", default="tiles")
    ap.add_argument("--fullpage-split", action="store_true", help="フルページを縦に分割して保存")
    ap.add_argument("--tile-height", type=int, default=1200)
    ap.add_argument("--viewport-w", type=int, default=1680)
    ap.add_argument("--viewport-h", type=int, default=2600)
    ap.add_argument("--device-scale-factor", type=float, default=3.0)

    # OpenAI
    ap.add_argument("--openai-api-key", default=os.getenv("OPENAI_API_KEY"))
    ap.add_argument("--openai-base-url", default=os.getenv("OPENAI_BASE_URL"))

    # 軽量化
    ap.add_argument("--max-side", type=int, default=1600)
    ap.add_argument("--jpeg-quality", type=int, default=80)

    # スロットル/制御
    ap.add_argument("--throttle-ms", type=int, default=1000, help="各タイル送信の間隔[ms]")
    ap.add_argument("--limit-tiles", type=int, default=0, help="先頭Nタイルだけ処理(0=無制限)")

    # 出力
    ap.add_argument("--csv", default="result.csv")
    args = ap.parse_args()

    # 画像準備
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
    if args.limit_tiles and image_paths:
        image_paths = image_paths[: args.limit_tiles]
    if not image_paths:
        print("[WARN] 画像が見つかりません。--from-url または --input を指定してください。")
        return

    # VLM クライアント（gpt-4o固定）
    if not (args.openai_api_key or os.getenv("OPENAI_API_KEY")):
        raise RuntimeError("OPENAI_API_KEY が未設定です。--openai-api-key または Secrets/環境変数で指定してください。")
    client = OpenAIClient(api_key=args.openai_api_key, base_url=args.openai_base_url)

    user_prompt = make_user_prompt()

    all_rows: List[dict] = []
    for idx, p in enumerate(image_paths, start=1):
        data_url, _ = load_and_downscale_for_vlm(p, max_side=args.max_side, jpeg_quality=args.jpeg_quality)
        print(f"[INFO] Processing tile {idx}/{len(image_paths)}: {os.path.basename(p)} (model=gpt-4o)")
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
        time.sleep(max(0, args.throttle_ms) / 1000.0)

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
