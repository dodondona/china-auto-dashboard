#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Append series_url (and optionally count) to CSV by scraping /rank/1.

最小変更で既存パイプラインにはめ込めるドロップイン版:
- 引数は従来どおり(--rank-url --input --output --name-col --idle-ms --max-rounds 等)
- 名前ではなく rank をキーにマージ
- リンク抽出は button[data-series-id] を DOM 順で列挙（= 表示順がそのまま rank）
"""

import os
import re
import argparse
from pathlib import Path
import pandas as pd
from playwright.sync_api import sync_playwright

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", default="https://www.autohome.com.cn/rank/1")
    ap.add_argument("--input", required=True, help="input CSV (既存raw)")
    ap.add_argument("--output", required=True, help="output CSV")
    ap.add_argument("--name-col", default="model")  # 互換のため残すが使用しない
    ap.add_argument("--idle-ms", type=int, default=650)
    ap.add_argument("--max-rounds", type=int, default=40)
    # 互換: 未知引数が来てもエラーにしない
    args, _ = ap.parse_known_args()
    return args

UA_MOBILE = (
    "Mozilla/5.0 (Linux; Android 11; Pixel 5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Mobile Safari/537.36"
)

def wait_rank_ready(page, timeout_ms=120000):
    # data-rank-num が無くても、series-id ボタンは必ず出るのでこれで待つ
    page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
    page.wait_for_selector("button[data-series-id]", timeout=timeout_ms)

def scroll_to_bottom(page, idle_ms=650, max_rounds=40):
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

def parse_count_from_container(container):
    txt = (container.inner_text() or "").strip()
    m = re.search(r"(\d{4,6})\s*车系销量", txt)
    return safe_int(m.group(1)) if m else None

def nearest_row_container(el):
    c = el
    for _ in range(6):
        if c is None:
            break
        if c.query_selector("button[data-series-id]"):
            return c
        c = c.evaluate_handle("n => n.parentElement").as_element()
    return el

def scrape_rank_list(url, idle_ms, max_rounds):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(
            user_agent=UA_MOBILE,
            viewport={"width": 480, "height": 960},
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
        )
        page = ctx.new_page()
        page.goto(url, wait_until="load", timeout=180000)
        wait_rank_ready(page, timeout_ms=180000)
        total = scroll_to_bottom(page, idle_ms=idle_ms, max_rounds=max_rounds)

        buttons = page.query_selector_all("button[data-series-id]") or []
        rows = []
        for idx, btn in enumerate(buttons, start=1):
            sid = btn.get_attribute("data-series-id")
            series_url = f"https://www.autohome.com.cn/{sid}/" if sid else None
            cont = nearest_row_container(btn)
            count = parse_count_from_container(cont)
            rows.append({"rank": idx, "series_url": series_url, "count_from_web": count})

        browser.close()
    return rows

def main():
    args = parse_args()
    inp = Path(args.input)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(inp, encoding="utf-8-sig")
    if "rank" not in df.columns:
        # 万一 rank が無い raw でも、行順で採番
        df.insert(0, "rank", range(1, len(df) + 1))

    print(f"📥 input: {inp} ({len(df)} rows)")
    print(f"🌐 scraping: {args.rank_url}")

    web_rows = scrape_rank_list(args.rank_url, args.idle_ms, args.max_rounds)
    web = pd.DataFrame(web_rows)
    # 50件未満の場合もあるのでそのままマージ（rank基準・上書き）
    merged = df.merge(web, on="rank", how="left")

    # series_url 列名を従来通りに
    if "series_url_y" in merged.columns and "series_url_x" in merged.columns:
        merged["series_url"] = merged["series_url_x"].fillna(merged["series_url_y"])
        merged = merged.drop(columns=["series_url_x", "series_url_y"])
    elif "series_url" not in merged.columns and "series_url_y" in merged.columns:
        merged = merged.rename(columns={"series_url_y": "series_url"})

    # count は既存があれば温存、無ければWeb値で補完
    if "count" in merged.columns and "count_from_web" in merged.columns:
        merged["count"] = merged["count"].fillna(merged["count_from_web"])
        merged = merged.drop(columns=["count_from_web"])
    elif "count_from_web" in merged.columns and "count" not in merged.columns:
        merged = merged.rename(columns={"count_from_web": "count"})

    merged = merged.sort_values("rank").reset_index(drop=True)
    merged.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"✅ output: {out} ({len(merged)} rows)")

if __name__ == "__main__":
    main()
