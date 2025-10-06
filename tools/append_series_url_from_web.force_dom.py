#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Append series_url (and optionally count) to CSV by scraping /rank/1.

最小変更で既存パイプラインにはめ込めるドロップイン版:
- 引数は従来どおり(--rank-url --input --output --name-col --idle-ms --max-rounds)
- rank は DOM 出現順（=画面の並び）
- まず button[data-series-id] を使う。見つからない場合は <a href="//www.autohome.com.cn/<digits>/"> を正規表現で回収
- wait_for_selector は使わず、querySelectorAll の length をポーリング（可視化待ちを回避）
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
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--name-col", default="model")  # 互換目的（未使用）
    ap.add_argument("--idle-ms", type=int, default=650)
    ap.add_argument("--max-rounds", type=int, default=40)
    args, _ = ap.parse_known_args()
    return args

UA_MOBILE = (
    "Mozilla/5.0 (Linux; Android 11; Pixel 5) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Mobile Safari/537.36"
)

def safe_int(x):
    try:
        return int(str(x).strip())
    except Exception:
        return None

def poll_series_button_count(page, total_ms=180000, step_ms=800):
    """
    可視状態を待たず、DOMに現れた個数をポーリング。
    スクロールもしながら安定するまで待つ。
    """
    waited = 0
    stable = 0
    last = -1
    while waited < total_ms:
        n = page.evaluate("() => document.querySelectorAll('button[data-series-id]').length")
        if n > 0 and n == last:
            stable += 1
        else:
            stable = 0
        last = n
        if n > 0 and stable >= 2:   # 2回連続で変化なし＝安定
            return n
        page.mouse.wheel(0, 22000)
        page.wait_for_timeout(step_ms)
        waited += step_ms
    return 0

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

def scrape_by_buttons(page):
    """button[data-series-id] が使えるときの経路（以前と同じ挙動）。"""
    buttons = page.query_selector_all("button[data-series-id]") or []
    rows = []
    for idx, btn in enumerate(buttons, start=1):
        sid = btn.get_attribute("data-series-id")
        url = f"https://www.autohome.com.cn/{sid}/" if sid else None
        # count は必要ならここで行テキストから拾う（互換のため None でもOK）
        rows.append({"rank": idx, "series_url": url})
    return rows

def scrape_by_anchor_regex(page):
    """
    バックアップ経路:
    ページHTMLから <a href="//www.autohome.com.cn/<digits>[/#?]..."> を
    ドキュメント順でユニーク抽出（最初の50～60件がランキングの本体）。
    """
    html = page.content()
    # href は // から始まることがあるので https: を補う
    pattern = re.compile(r'href="(?:https:)?//www\.autohome\.com\.cn/(\d{3,7})/?(?:[?#"][^"]*)?"')
    seen, ids = set(), []
    for m in pattern.finditer(html):
        sid = m.group(1)
        if sid not in seen:
            seen.add(sid)
            ids.append(sid)
    rows = [{"rank": i, "series_url": f"https://www.autohome.com.cn/{sid}/"} for i, sid in enumerate(ids, start=1)]
    # デバッグ用ダンプ（念のため）
    Path("data").mkdir(parents=True, exist_ok=True)
    with open("data/debug_rankpage_fallback.html", "w", encoding="utf-8") as f:
        f.write(html)
    return rows

def scrape_rank_list(url, idle_ms, max_rounds):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )
        ctx = browser.new_context(
            user_agent=UA_MOBILE,
            viewport={"width": 480, "height": 960},
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            extra_http_headers={
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
            },
        )
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=180000)

        # 可視待ちをやめてポーリング
        _ = poll_series_button_count(page, total_ms=180000, step_ms=800)
        _ = scroll_to_bottom(page, idle_ms=idle_ms, max_rounds=max_rounds)

        # まずは「前と同じ」ボタン経路
        rows = scrape_by_buttons(page)

        # 0件だったらアンカー正規表現でバックアップ抽出
        if not rows:
            rows = scrape_by_anchor_regex(page)

        browser.close()
    return rows

def main():
    args = parse_args()
    inp = Path(args.input)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(inp, encoding="utf-8-sig")
    if "rank" not in df.columns:
        # raw が rank 無しでも安全に動くように
        df.insert(0, "rank", range(1, len(df) + 1))

    print(f"📥 input: {inp} ({len(df)} rows)")
    print(f"🌐 scraping: {args.rank_url}")

    web_rows = scrape_rank_list(args.rank_url, args.idle_ms, args.max_rounds)
    web = pd.DataFrame(web_rows)

    # rank でストレートに結合（名前は使わない＝取り違いを避ける）
    merged = df.merge(web, on="rank", how="left")

    # series_url 列の正規化
    if "series_url_y" in merged.columns and "series_url_x" in merged.columns:
        merged["series_url"] = merged["series_url_x"].fillna(merged["series_url_y"])
        merged = merged.drop(columns=["series_url_x", "series_url_y"])
    elif "series_url" not in merged.columns and "series_url_y" in merged.columns:
        merged = merged.rename(columns={"series_url_y": "series_url"})

    merged = merged.sort_values("rank").reset_index(drop=True)
    merged.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"✅ output: {out} ({len(merged)} rows)")

if __name__ == "__main__":
    main()
