#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_from_web.playwright_full.py  (robust, event-wait compatible)
- /rank/1 を Playwright で最下段までロード
- DOMから series_id を「出現順」で抽出（=順位）
- 入力CSVに series_url 列として付与
"""

import asyncio, re, argparse, pandas as pd
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

RANK_XHR_KEYWORD = "rank"  # “frontapi/rank/series” など 'rank' を含むXHRを待つ

async def _wait_any_response_with_keyword(page, keyword: str, timeout_ms: int = 60000):
    """
    page.wait_for_response が無い環境でも動くように、wait_for_event('response', ...) を使って待つ。
    """
    try:
        await page.wait_for_event(
            "response",
            predicate=lambda r: (r is not None) and (r.status == 200) and (keyword in r.url),
            timeout=timeout_ms
        )
    except PWTimeout:
        # XHRが見つからなくても後段のDOM待ちへフォールバック
        pass

async def wait_rank_filled(page, timeout_ms=60000):
    """
    ランキングXHRが返り、DOMにカードが出るまで待つ
    """
    # 1) XHR完了待ち（本文は使わない）
    await _wait_any_response_with_keyword(page, RANK_XHR_KEYWORD, timeout_ms=timeout_ms)

    # 2) DOMに最初のカードが現れるまで待つ（いずれかが見えればOK）
    sel_any = 'button[data-series-id], a[href*="//www.autohome.com.cn/"], [data-rank-num], div.rank-num'
    await page.wait_for_selector(sel_any, state="visible", timeout=timeout_ms)

async def extract_series_ids_from_dom(page):
    """
    DOMから series_id を出現順で抽出（button[data-series-id] と href両対応）
    """
    ids = []

    # 1) button[data-series-id]
    try:
        btns = await page.locator('button[data-series-id]').element_handles()
        for h in btns:
            sid = await h.get_attribute("data-series-id")
            if sid and sid.isdigit() and sid not in ids:
                ids.append(sid)
    except Exception:
        pass

    # 2) a[href*="//www.autohome.com.cn/xxxx/"]
    try:
        hrefs = await page.eval_on_selector_all(
            'a[href*="//www.autohome.com.cn/"]',
            "els => els.map(e => e.getAttribute('href'))"
        )
        for href in hrefs or []:
            if not href:
                continue
            m = re.search(r'//www\.autohome\.com\.cn/(\d{3,7})/', href)
            if m:
                sid = m.group(1)
                if sid not in ids:
                    ids.append(sid)
    except Exception:
        pass

    return ids

async def robust_scroll_to_bottom(page, rounds=40, idle_ms=600, min_delta=1, max_items=60):
    """
    下までスクロール。抽出件数が増えなくなるまで回す
    """
    prev = 0
    for i in range(rounds):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(idle_ms/1000)

        ids = await extract_series_ids_from_dom(page)
        cur = len(ids)
        print(f"  ⤷ round {i+1}: {cur}件 (+{cur-prev})")
        if cur >= max_items:
            return ids[:max_items]
        if (cur - prev) < min_delta and i >= 2:
            # 揺さぶり
            await page.evaluate("window.scrollTo(0, 0)")
            await asyncio.sleep(0.3)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(idle_ms/1000)
            ids2 = await extract_series_ids_from_dom(page)
            if len(ids2) - cur < min_delta:
                return ids2[:max_items]
        prev = cur
    return await extract_series_ids_from_dom(page)

async def run(rank_url, input_csv, output_csv, name_col, max_rounds, idle_ms, min_delta):
    Path("data").mkdir(exist_ok=True, parents=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-gpu"])
        page = await browser.new_page()
        print(f"🌐 開始: {rank_url}")
        await page.goto(rank_url, wait_until="networkidle")

        # ランキングが注入されるまで待つ（互換版）
        await wait_rank_filled(page, timeout_ms=60000)

        # しっかり下まで表示させる
        ids = await robust_scroll_to_bottom(
            page, rounds=max_rounds, idle_ms=idle_ms, min_delta=min_delta, max_items=60
        )

        if not ids:
            # デバッグ出力
            Path("data/_debug_rank_page.html").write_text(await page.content(), encoding="utf-8")
            await page.screenshot(path="data/_debug_rank_page.png", full_page=True)
            print("⚠️ 0件でした。data/_debug_rank_page.html / .png を確認してください。")

        await browser.close()

    # 入出力
    df = pd.read_csv(input_csv)
    n = min(len(df), len(ids))
    urls = [f"https://www.autohome.com.cn/{sid}/" for sid in ids[:n]]
    out = df.head(n).copy()
    out["series_url"] = urls
    out.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"✅ 抽出完了: {len(ids)}件 / 保存: {output_csv}（{n}行に付与）")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", default="https://www.autohome.com.cn/rank/1")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--name-col", default="model")
    ap.add_argument("--max-rounds", type=int, default=40)
    ap.add_argument("--idle-ms", type=int, default=600)
    ap.add_argument("--min-delta", type=int, default=1)
    args = ap.parse_args()
    asyncio.run(run(args.rank_url, args.input, args.output, args.name_col,
                    args.max_rounds, args.idle_ms, args.min_delta))

if __name__ == "__main__":
    main()
