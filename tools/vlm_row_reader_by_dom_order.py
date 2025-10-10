#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_row_reader_by_dom_order.py
- /rank/1 を開き、[data-rank-num] の出現順で各行をスクショ
- その順番のまま LLM に渡し、brand/model/count に加えて rank(行頭の番号) も読み取る
- LLMが返した rank を優先し、欠損時のみ 1..N で補完
- 最終CSV: rank, brand, model, count（順序ズレ防止 & 番号の画面準拠）

使い方:
  python tools/vlm_row_reader_by_dom_order.py \
    --url https://www.autohome.com.cn/rank/1 \
    --out data/autohome_rank_YYYY-MM_vlmfix.csv \
    --model gpt-4o-mini
"""

import os, io, re, csv, json, time, base64, argparse
from pathlib import Path
from typing import List, Dict
from playwright.sync_api import sync_playwright, TimeoutError
from openai import OpenAI

UA_MOBILE = (
    "Mozilla/5.0 (Linux; Android 11; Pixel 5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Mobile Safari/537.36"
)

SYSTEM_PROMPT = """あなたは画像から自動車販売ランキングの各行を読み取るアシスタントです。
複数の「行画像」を、与えられた順番のまま解析し、各行の
  - rank（行頭に表示されているランキング番号の整数。必ず画像の数字を読み取る）
  - brand（ブランド名, 中国語）
  - model（車系名, 中国語。角括弧【】内があればそれを優先）
  - count（月販台数の整数, 無ければ空で良い）
を JSON 配列で返してください。

厳守事項:
- 返却配列の順序は「入力画像の順番」と完全一致させる（並べ替え禁止）
- 要素数も一致させる（不足/過剰禁止）
- rank は可能な限り画像上の数字を読み取って返す。読めない場合は null とする
- JSON 以外の文字は出力しない
出力例:
[
  {"rank":31,"brand":"比亚迪","model":"海豚","count":13968},
  {"rank":32,"brand":"上汽大众","model":"途岳","count":13693}
]
"""

def b64_of_bytes(data: bytes) -> str:
    import base64
    return base64.b64encode(data).decode("ascii")

def goto_with_retries(page, url: str, timeout_ms: int = 120000):
    candidates = [
        (url, "load"),
        (url, "domcontentloaded"),
    ]
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

def scroll_to_bottom(page, idle_ms=650, max_rounds=60):
    prev = -1
    stable = 0
    for _ in range(max_rounds):
        page.mouse.wheel(0, 20000)
        page.wait_for_timeout(idle_ms)
        n = page.evaluate("() => document.querySelectorAll('[data-rank-num]').length")
        if n == prev: stable += 1
        else: stable = 0
        prev = n
        if stable >= 3:
            break
    return prev

def capture_row_images(url: str, out_dir: Path) -> List[bytes]:
    out_dir.mkdir(parents=True, exist_ok=True)
    images: List[bytes] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(
            user_agent=UA_MOBILE,
            viewport={"width": 480, "height": 960},
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
        )
        page = ctx.new_page()
        goto_with_retries(page, url)
        page.wait_for_load_state("networkidle")
        scroll_to_bottom(page)

        rows = page.query_selector_all("[data-rank-num]")
        for idx, el in enumerate(rows, start=1):
            png = el.screenshot()  # bytes
            images.append(png)
            # デバッグ保存（任意）
            (out_dir / f"row_{idx:02d}.png").write_bytes(png)

        browser.close()
    return images

def infer_rows_with_llm(row_pngs: List[bytes], model: str) -> List[Dict]:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    content = [{"type":"text","text":"以下の行画像を、与えた順に解析してください。"}]
    for png in row_pngs:
        content.append({"type":"image_url","image_url":{"url": f"data:image/png;base64,{b64_of_bytes(png)}"}})

    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        max_tokens=3500,
        messages=[
            {"role":"system","content": SYSTEM_PROMPT},
            {"role":"user","content": content},
        ],
    )
    txt = resp.choices[0].message.content.strip()
    try:
        data = json.loads(txt)
    except Exception:
        start = txt.find("[")
        end = txt.rfind("]")+1
        data = json.loads(txt[start:end])
    if not isinstance(data, list):
        raise RuntimeError("LLM出力が配列ではありません")
    return data

def to_int_or_none(x):
    try:
        return int(str(x).strip().replace(",",""))
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="https://www.autohome.com.cn/rank/1")
    ap.add_argument("--out", required=True)
    ap.add_argument("--model", default="gpt-4o-mini")
    ap.add_argument("--tmpdir", default="data/vlm_rows")
    args = ap.parse_args()

    rows_png = capture_row_images(args.url, Path(args.tmpdir))
    if not rows_png:
        raise SystemExit("行を検出できませんでした。")

    llm_rows = infer_rows_with_llm(rows_png, args.model)

    # 行数合わせ（安全側）
    n = min(len(llm_rows), len(rows_png))
    llm_rows = llm_rows[:n]

    # rankは「LLMが読んだ数字」を優先。欠損時のみ 1..n で補完。
    out_rows = []
    for i, r in enumerate(llm_rows, start=1):
        rk = to_int_or_none(r.get("rank"))
        brand = (r.get("brand") or "").strip()
        model = (r.get("model") or "").strip()
        count = to_int_or_none(r.get("count"))
        out_rows.append({
            "rank": rk if rk is not None else i,
            "brand": brand,
            "model": model,
            "count": "" if count is None else count
        })

    # rankで安定ソート（LLMが31,32...と読めている想定、欠損補完分も自然に並ぶ）
    out_rows.sort(key=lambda x: int(x["rank"]))

    # CSV出力
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["rank","brand","model","count"])
        w.writeheader()
        w.writerows(out_rows)

    print(f"✅ VLM rows saved with RANK: {args.out}  (rows={len(out_rows)})")

if __name__ == "__main__":
    main()
