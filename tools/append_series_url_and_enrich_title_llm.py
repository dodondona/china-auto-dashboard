#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_and_enrich_title_llm.py
-----------------------------------------------------
- autohome.com.cn/rank/1 の「ランキング全体」を Playwright で開く
- 各 rank/series_url を **HTMLではなくリンクから抽出**（動的ロード完了後）
- 各 series_url にアクセスし <title> を取得
- title を LLM で解析し brand / model を推定
- rank, series_url, brand, model, count を CSV 出力

依存:
  pip install playwright openai pandas
  playwright install chromium
"""

import os, re, csv, time, json, argparse
import pandas as pd
from pathlib import Path
from playwright.sync_api import sync_playwright
from openai import OpenAI

UA_MOBILE = (
    "Mozilla/5.0 (Linux; Android 11; Pixel 5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Mobile Safari/537.36"
)

PROMPT_BRAND_MODEL = """以下は自動車の車系ページのタイトルです。
タイトルから「ブランド名」と「車系名（モデル名）」を推定してください。
中国語で出力してください。形式は必ずJSONで:
{"brand": "品牌名", "model": "车系名"}"""

def goto_with_retries(page, url: str, timeout_ms: int = 120000):
    tries = [url]
    if "www.autohome.com.cn" in url:
        tries.append(url.replace("www.autohome.com.cn", "m.autohome.com.cn"))
    for u in tries:
        try:
            page.goto(u, wait_until="load", timeout=timeout_ms)
            return u
        except Exception:
            page.wait_for_timeout(1000)
    raise RuntimeError("Cannot load page")

def scroll_to_bottom(page, idle_ms=650, max_rounds=40):
    prev_len = -1
    stable = 0
    for _ in range(max_rounds):
        page.mouse.wheel(0, 20000)
        page.wait_for_timeout(idle_ms)
        n = page.evaluate("() => document.querySelectorAll('[data-rank-num]').length")
        if n == prev_len:
            stable += 1
        else:
            stable = 0
        prev_len = n
        if stable >= 3:
            break
    return prev_len

def extract_rank_and_links(page):
    """動的ロード完了後、rank, series_url, count を抽出"""
    data = []
    items = page.query_selector_all("[data-rank-num]")
    for el in items:
        try:
            rank = int(el.get_attribute("data-rank-num"))
        except Exception:
            continue
        # ボタンの series-id
        sid_btn = el.query_selector("button[data-series-id]")
        sid = sid_btn.get_attribute("data-series-id") if sid_btn else None
        url = f"https://www.autohome.com.cn/{sid}/" if sid else None
        # 販売台数
        text = el.inner_text().strip()
        m = re.search(r"(\d{4,6})\s*车系销量", text)
        count = int(m.group(1)) if m else None
        data.append({"rank": rank, "series_url": url, "count": count})
    return data

def get_title_from_series_url(page, url):
    """個別ページを開いて<title>を取得"""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
        title = page.title()
        return title.strip()
    except Exception:
        return ""

def llm_parse_brand_model(client, model_name, title):
    """LLMでタイトル解析"""
    if not title:
        return {"brand": "", "model": ""}
    try:
        resp = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": PROMPT_BRAND_MODEL},
                {"role": "user", "content": title},
            ],
            temperature=0,
            max_tokens=200,
        )
        text = resp.choices[0].message.content.strip()
        m = re.search(r"\{.*\}", text)
        data = json.loads(m.group(0)) if m else {}
        brand = data.get("brand", "")
        model = data.get("model", "")
        return {"brand": brand, "model": model}
    except Exception:
        return {"brand": "", "model": ""}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", default="https://www.autohome.com.cn/rank/1")
    ap.add_argument("--output", required=True)
    ap.add_argument("--model", default="gpt-4o-mini")
    ap.add_argument("--max-series", type=int, default=60)
    args = ap.parse_args()

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=UA_MOBILE, viewport={"width":480,"height":960})
        page = ctx.new_page()
        print(f"🌐 Loading {args.rank_url}")
        goto_with_retries(page, args.rank_url)
        scroll_to_bottom(page)
        data = extract_rank_and_links(page)
        print(f"✅ Extracted {len(data)} ranks")
        subset = [d for d in data if d.get("series_url")][:args.max_series]

        # タイトル取得
        for d in subset:
            if not d["series_url"]:
                continue
            t = get_title_from_series_url(page, d["series_url"])
            d["title"] = t
            page.wait_for_timeout(300)
        browser.close()

    # LLM解析
    print("🤖 Parsing brand/model via LLM…")
    for d in subset:
        bm = llm_parse_brand_model(client, args.model, d.get("title",""))
        d.update(bm)

    df = pd.DataFrame(subset).sort_values("rank")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"✅ Saved: {out} ({len(df)} rows)")

if __name__ == "__main__":
    main()
