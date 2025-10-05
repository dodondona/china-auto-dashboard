#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
- /rank/1 を開いてフルページスクショ(タイル分割)を作成
- 画像を VLM (OpenAI gpt-4o / gpt-4o-mini) に渡して表データ(ranking rows)を抽出
- CSVに保存

使い方例:
  python vlm_rank_reader.py \
    --from-url https://www.autohome.com.cn/rank/1 \
    --out data/autohome_rank_2025-08.csv \
    --model gpt-4o-mini
"""

import os, io, re, csv, math, time, base64, json, argparse
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError
from openai import OpenAI

# ===== 読み取りプロンプト（元の仕様踏襲） =====
SYSTEM_PROMPT = """あなたは表の読み取りに特化した視覚アシスタントです。
画像は中国の自動車販売ランキングです。UI部品や広告は無視してください。
出力は JSON のみ。構造:
{
  "rows": [
    {"rank": <int|null>, "name": "<string>", "count": <int|null>}
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

def chunk_image_and_call_vlm(img_path: Path, client: OpenAI, model: str) -> list[dict]:
    """
    必要に応じて縦分割(タイル)して複数画像を一度に投げる。
    VLMへの投げ方は元ロジック踏襲。温度0で安定化。
    """
    from PIL import Image
    im = Image.open(img_path)
    H = im.height
    MAX_SLICE = 2200               # 1枚あたりの高さ上限（安定用）
    n = math.ceil(H / MAX_SLICE)
    imgs = []
    for i in range(n):
        top = i * MAX_SLICE
        bottom = min(H, (i+1)*MAX_SLICE)
        crop = im.crop((0, top, im.width, bottom))
        buf = io.BytesIO()
        crop.save(buf, format="PNG")
        imgs.append(base64.b64encode(buf.getvalue()).decode("ascii"))

    # 画像を順に食わせる
    messages = [
        {"role":"system","content": SYSTEM_PROMPT},
        {"role":"user","content": [{"type":"text","text":"次のスクリーンショット群から表を読み取ってください。"}] }
    ]
    for enc in imgs:
        messages[1]["content"].append({"type":"image_url","image_url":{"url": f"data:image/png;base64,{enc}"}})

    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        max_tokens=1500,
        messages=messages,
    )
    txt = resp.choices[0].message.content.strip()
    m = re.search(r'\{[^]*"rows"\s*:\s*\[[\s\S]*?\][^}]*\}', txt)
    payload = json.loads(m.group(0)) if m else json.loads(txt)
    rows = payload.get("rows", [])
    # 正規化
    out = []
    for r in rows:
        try:
            rk = int(r.get("rank")) if r.get("rank") is not None else None
        except Exception:
            rk = None
        name = (r.get("name") or "").strip()
        try:
            cnt = int(str(r.get("count")).replace(",","")) if r.get("count") not in (None,"") else None
        except Exception:
            cnt = None
        if rk or name or cnt:
            out.append({"rank": rk, "name": name, "count": cnt})
    return out

def goto_with_retries(page, url: str, timeout_ms: int = 120000):
    """
    できるだけ”今まで通り”の挙動を保ちつつ、到達性だけ強化。
    - 複数候補URLにリトライ（www/m）
    - wait_until=load / domcontentloaded を切替
    """
    candidates = [
        (url, "load"),
        (url, "domcontentloaded"),
    ]
    # www → m (軽いUIで速いことが多い)
    if "autohome.com.cn/rank/1" in url:
        candidates += [
            ("https://m.autohome.com.cn/rank/1", "load"),
            ("https://m.autohome.com.cn/rank/1", "domcontentloaded"),
        ]

    last_err = None
    for u, wait in candidates:
        try:
            page.goto(u, wait_until=wait, timeout=timeout_ms)
            return u
        except TimeoutError as e:
            last_err = e
            page.wait_for_timeout(1200)
            continue
    # ここまで失敗したらそのまま例外
    raise last_err or TimeoutError("goto retries exhausted")

def scroll_to_bottom(page, idle_ms=700, max_rounds=60):
    """
    無限スクロールを”増えなくなるまで×連続3回”で終了。
    """
    prev = -1
    stable = 0
    for i in range(max_rounds):
        page.mouse.wheel(0, 20000)
        page.wait_for_timeout(idle_ms)
        n = page.evaluate("() => document.querySelectorAll('[data-rank-num]').length")
        if n == prev:
            stable += 1
        else:
            stable = 0
        prev = n
        if stable >= 3:
            break
    return prev

def capture_fullpage_screenshot(url: str, out_png: Path) -> int:
    """
    直接サイトを開き、最下部までスクロールしてフルページのpngを保存。
    返り値: 見つかった行数 (目安)
    """
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(
            user_agent=UA_MOBILE,
            viewport={"width": 440, "height": 900},
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
        )
        # 軽量化（画像まではブロックしない。フォント/解析系はブロック）
        ctx.route("**/*", lambda route: route.abort() if any(
            x in route.request.url for x in [
                "googletagmanager", "analytics", "gtag", "baidu.com/hm", "umeng", "heatmap"
            ]) else route.continue_())
        ctx.set_default_navigation_timeout(120000)
        page = ctx.new_page()

        reached = goto_with_retries(page, url, timeout_ms=120000)
        page.wait_for_load_state("networkidle")
        rows = scroll_to_bottom(page, idle_ms=700, max_rounds=60)

        # モバイルUIは fixed ヘッダが重なることがあるので、一瞬スクロールしてから撮る
        page.evaluate("() => window.scrollTo(0, 0)")
        page.wait_for_timeout(200)
        page.screenshot(path=str(out_png), full_page=True)
        browser.close()
        return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--model", default="gpt-4o-mini")
    ap.add_argument("--screenshot", default="data/rank_fullpage.png")
    args = ap.parse_args()

    out_png = Path(args.screenshot)
    out_png.parent.mkdir(parents=True, exist_ok=True)

    print(f"📥 navigate: {args.from_url}")
    rows_seen = capture_fullpage_screenshot(args.from_url, out_png)
    print(f"   rows_seen≈{rows_seen}, screenshot: {out_png}")

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY が未設定です。")
    client = OpenAI(api_key=api_key)

    print("🧠 VLM 読み取り中...")
    rows = chunk_image_and_call_vlm(out_png, client, args.model)

    # rankがない/飛んでいる場合は補完（画像上の順で再採番）
    norm = []
    r_auto = 1
    for r in rows:
        rk = r["rank"] if r["rank"] else r_auto
        r_auto = rk + 1
        name = r["name"].strip()
        cnt = r["count"]
        norm.append({"rank": rk, "name": name, "count": cnt})

    # CSV出力
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["rank","name","count"])
        w.writeheader()
        w.writerows(sorted(norm, key=lambda x: x["rank"]))

    print(f"✅ saved: {args.out}")

if __name__ == "__main__":
    main()
