#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_and_enrich_title_llm.py
-----------------------------------------------------
- autohome.com.cn/rank/1 を Playwright で開く
- ランキング各行を DOM から列挙し、button[data-series-id] から series_url を生成
- rank は data-rank-num があればそれを、無ければ「行の見た目の順位」や出現順で補完
- 各 series_url を開き <title> を取得
- title を LLM で解析して brand/model を推定
- rank / series_url / count / title / brand / model を CSV へ

依存:
  pip install playwright openai pandas
  playwright install chromium
"""

import os
import re
import json
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

def wait_rank_dom_ready(page, timeout_ms=120000):
    """
    ランキングの行が現れるまで待つ。
    data-rank-num が無い構成もあるので、複数セレクタで待機。
    """
    try:
        page.wait_for_selector("div.rank-num, em.rank, [data-rank-num], button[data-series-id]",
                               timeout=timeout_ms, state="visible")
    except PWTimeout as e:
        # デバッグ用にHTMLを保存
        Path("data").mkdir(parents=True, exist_ok=True)
        with open("data/debug_rankpage_error.html", "w", encoding="utf-8") as f:
            f.write(page.content())
        raise e

def scroll_to_bottom(page, idle_ms=700, max_rounds=50):
    """無限スクロールの末尾まで読む。"""
    prev = -1
    stable = 0
    for _ in range(max_rounds):
        page.mouse.wheel(0, 24000)
        page.wait_for_timeout(idle_ms)
        n = page.evaluate("() => document.querySelectorAll('button[data-series-id]').length")
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

def nearest_row_container(el):
    """行コンテナっぽい上位要素を返す（セレクタ揺れ対策）。"""
    c = el
    for _ in range(6):
        if c is None:
            break
        # 行内に見える典型的要素があればここを行とみなす
        if c.query_selector("button[data-series-id]") and (
            c.get_attribute("data-rank-num") or
            c.query_selector("div.rank-num, em.rank") or
            c.query_selector(".tw-text-lg.tw-font-medium")
        ):
            return c
        c = c.evaluate_handle("n => n.parentElement").as_element()
    return el

def parse_rank_from_container(container):
    """data-rank-num > 可視の順位 > None の順に取得。"""
    attr = container.get_attribute("data-rank-num")
    rk = safe_int(attr)
    if rk is not None:
        return rk
    badge = container.query_selector("div.rank-num, em.rank")
    if badge:
        txt = (badge.inner_text() or "").strip()
        rk = safe_int(re.sub(r"[^\d]", "", txt))
        if rk is not None:
            return rk
    return None

def parse_count_from_container(container):
    txt = (container.inner_text() or "").strip()
    m = re.search(r"(\d{4,6})\s*车系销量", txt)
    return safe_int(m.group(1)) if m else None

def extract_rank_and_links(page):
    """
    行を列挙し、rank / series_url / count を抽出。
    - ラインの基準は button[data-series-id]
    - rankは data-rank-num → 表示順位 → 出現順
    """
    buttons = page.query_selector_all("button[data-series-id]") or []
    rows = []
    for idx, btn in enumerate(buttons, start=1):
        sid = btn.get_attribute("data-series-id")
        series_url = f"https://www.autohome.com.cn/{sid}/" if sid else None
        cont = nearest_row_container(btn)
        rk = parse_rank_from_container(cont)
        if rk is None:
            rk = idx
        count = parse_count_from_container(cont)
        rows.append({"rank": rk, "series_url": series_url, "count": count})
    return rows

def get_title_from_series_url(page, url):
    """個別車系ページの<title>を取得。失敗時は空文字。"""
    if not url:
        return ""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=25000)
        page.wait_for_timeout(400)
        return (page.title() or "").strip()
    except Exception:
        return ""

def llm_parse_brand_model(client, model_name, title):
    """LLMにタイトルを渡して brand/model を抽出。"""
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
        m = re.search(r"\{.*\}", out, re.S)
        data = json.loads(m.group(0)) if m else {}
        return {
            "brand": (data.get("brand") or "").strip(),
            "model": (data.get("model") or "").strip(),
        }
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
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(
            user_agent=UA_MOBILE,
            viewport={"width": 480, "height": 960},
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
        )
        page = ctx.new_page()

        # ランキングページ
        print(f"🌐 Loading {args.rank_url}")
        goto_with_retries(page, args.rank_url, timeout_ms=120000)
        wait_rank_dom_ready(page, timeout_ms=120000)
        total = scroll_to_bottom(page)
        print(f"🧩 detected buttons(data-series-id): {total}")

        base_rows = extract_rank_and_links(page)
        if not base_rows:
            # デバッグダンプ
            Path("data").mkdir(parents=True, exist_ok=True)
            with open("data/debug_rankpage_empty.html", "w", encoding="utf-8") as f:
                f.write(page.content())
            # フェイルセーフ（空でもrankだけ採番）
            items = page.query_selector_all("[data-rank-num]") or []
            base_rows = [{"rank": i, "series_url": None, "count": None} for i, _ in enumerate(items, start=1)]

        # 各 series_url の <title> 取得
        print("🔎 Fetching <title> from series_url ...")
        subset = sorted(base_rows, key=lambda r: r["rank"])[: args.max_series]
        for r in subset:
            r["title"] = get_title_from_series_url(page, r.get("series_url"))
            page.wait_for_timeout(250)

        browser.close()

    # LLM で brand/model を解析
    print("🤖 Parsing brand/model via LLM...")
    for r in subset:
        r.update(llm_parse_brand_model(client, args.model, r.get("title", "")))

    # rank列の保証と安定ソート
    rows_fixed = []
    for i, r in enumerate(subset, start=1):
        rk = safe_int(r.get("rank")) or i
        rows_fixed.append({**r, "rank": rk})
    df = pd.DataFrame(rows_fixed)
    if "rank" not in df.columns or df["rank"].isna().all():
        df["rank"] = range(1, len(df) + 1)
    df = df.sort_values("rank").reset_index(drop=True)

    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"✅ Saved: {out}  (rows={len(df)})")

if __name__ == "__main__":
    main()
