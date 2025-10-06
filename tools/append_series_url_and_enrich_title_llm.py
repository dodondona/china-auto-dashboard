#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_and_enrich_title_llm.py
-----------------------------------------------------
- autohome.com.cn/rank/1 を Playwright で開く
- ランキング各行を DOM から列挙し、各行の button[data-series-id] から series_url を作る
- rank は data-rank-num を優先。無い/読めない場合は「行の出現順」で補完
- 各 series_url を開いて <title> を取得
- title を LLM で解析し brand / model を推定
- rank / series_url / count / title / brand / model を CSV 出力

依存:
  pip install playwright openai pandas
  playwright install chromium
"""

import os
import re
import json
import time
import argparse
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from openai import OpenAI

UA_MOBILE = (
    "Mozilla/5.0 (Linux; Android 11; Pixel 5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Mobile Safari/537.36"
)

PROMPT_BRAND_MODEL = (
    "你将看到一个汽车车系页面的标题，请从标题中解析出【品牌名】和【车系名】。\n"
    "严格以 JSON 输出：{\"brand\":\"品牌名\",\"model\":\"车系名\"}\n"
    "若无法判断，留空字符串。"
)

def goto_with_retries(page, url: str, timeout_ms: int = 120000):
    """www と m を順に試す。"""
    candidates = [url]
    if "www.autohome.com.cn" in url:
        candidates.append(url.replace("www.autohome.com.cn", "m.autohome.com.cn"))
    last_err = None
    for u in candidates:
        try:
            page.goto(u, wait_until="load", timeout=timeout_ms)
            return u
        except Exception as e:
            last_err = e
            page.wait_for_timeout(1000)
    raise last_err or RuntimeError("Failed to open page")

def wait_rank_dom_ready(page, timeout_ms=60000):
    """[data-rank-num] を待つ（SPA対策）。"""
    try:
        page.wait_for_selector("[data-rank-num]", timeout=timeout_ms)
    except PWTimeout:
        # 一部構成で描画が遅い場合の緩和：少しスクロールしながら待つ
        for _ in range(10):
            page.mouse.wheel(0, 20000)
            page.wait_for_timeout(800)
            if page.query_selector("[data-rank-num]"):
                return
        raise

def scroll_to_bottom(page, idle_ms=650, max_rounds=40):
    """末尾までロード（無限スクロール対策）。"""
    prev = -1
    stable = 0
    for _ in range(max_rounds):
        page.mouse.wheel(0, 24000)
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

def safe_int(x):
    try:
        return int(str(x).strip())
    except Exception:
        return None

def extract_rank_and_links(page):
    """
    ランキング行を DOM から列挙し、rank / series_url / count を抽出。
    - rank: data-rank-num を優先、無ければ出現順で補完
    - series_url: button[data-series-id] → https://www.autohome.com.cn/{id}/
    - count: 行テキストから (\d{4,6}) 车系销量 を拾う
    """
    rows = []
    items = page.query_selector_all("[data-rank-num]")
    if not items:
        return rows

    for idx, el in enumerate(items, start=1):
        # rank
        rank_attr = el.get_attribute("data-rank-num")
        rank = safe_int(rank_attr) or idx

        # series id → url
        btn = el.query_selector("button[data-series-id]")
        sid = btn.get_attribute("data-series-id") if btn else None
        series_url = f"https://www.autohome.com.cn/{sid}/" if sid else None

        # count
        text = el.inner_text() or ""
        m = re.search(r"(\d{4,6})\s*车系销量", text)
        count = safe_int(m.group(1)) if m else None

        rows.append(
            {"rank": rank, "series_url": series_url, "count": count}
        )
    return rows

def get_title_from_series_url(page, url):
    """個別車系ページの <title> を取得。失敗時は空文字。"""
    if not url:
        return ""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=25000)
        # SPAページで title が遅れることがあるので短く待つ
        page.wait_for_timeout(500)
        return (page.title() or "").strip()
    except Exception:
        return ""

def llm_parse_brand_model(client, model_name, title):
    """LLMにタイトルを渡して brand/model を抽出。必ずキーを返す。"""
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
        out = (resp.choices[0].message.content or "").strip()

        # JSON 抽出（寛容に）
        m = re.search(r"\{.*\}", out, re.S)
        data = json.loads(m.group(0)) if m else {}
        brand = (data.get("brand") or "").strip()
        model = (data.get("model") or "").strip()
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
        ctx = browser.new_context(
            user_agent=UA_MOBILE,
            viewport={"width": 480, "height": 960},
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
        )
        page = ctx.new_page()

        # ランキングページを開く
        print(f"🌐 Loading {args.rank_url}")
        goto_with_retries(page, args.rank_url, timeout_ms=120000)
        # ランクDOMが現れるまで待つ（重要）
        wait_rank_dom_ready(page, timeout_ms=60000)
        # 全部読み込む
        n = scroll_to_bottom(page)
        print(f"🧩 detected rows: {n}")

        # rank / series_url / count を抽出（リンク基準）
        base_rows = extract_rank_and_links(page)
        # 万が一空なら、見た順でダミー採番しておく
        if not base_rows:
            items = page.query_selector_all("[data-rank-num]") or []
            base_rows = [{"rank": i, "series_url": None, "count": None} for i, _ in enumerate(items, start=1)]

        # seriesページの title を収集
        print("🔎 Fetching <title> from series_url ...")
        subset = sorted(base_rows, key=lambda r: r["rank"])[: args.max_series]
        for r in subset:
            r["title"] = get_title_from_series_url(page, r.get("series_url"))
            # ごく短い間隔でアクセス（過負荷回避）
            page.wait_for_timeout(250)

        browser.close()

    # LLMで brand/model を解析
    print("🤖 Parsing brand/model via LLM...")
    for r in subset:
        bm = llm_parse_brand_model(client, args.model, r.get("title", ""))
        r.update(bm)

    # ---- ここから堅牢化：rank 列の保証と安定ソート ----
    # 万一不正があっても rank を必ず持たせる
    rows_fixed = []
    auto = 1
    for r in subset:
        rk = safe_int(r.get("rank"))
        if rk is None:
            rk = auto
        rows_fixed.append({**r, "rank": rk})
        auto += 1

    df = pd.DataFrame(rows_fixed)

    # rank 列が無い/空の場合の最終ガード
    if "rank" not in df.columns or df["rank"].isna().all():
        print("⚠️ rank 列を補完します（出現順）")
        df["rank"] = range(1, len(df) + 1)

    df = df.sort_values(by="rank", ascending=True).reset_index(drop=True)

    # 保存
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"✅ Saved: {out}  (rows={len(df)})")

if __name__ == "__main__":
    main()
