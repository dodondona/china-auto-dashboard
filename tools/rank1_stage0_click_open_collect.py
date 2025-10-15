#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tools/rank1_stage0_click_open_collect.py

目的:
  Autohomeのランキングページを開き、ランキングの「車両（シリーズ）名」を
  ランキング順にクリック -> 別タブで開かれたページのURLを取得 -> CSV化する。

互換性:
  既存ワークフロー/呼び出しと互換を保つため、引数と出力CSVの列名は従来通り。
  出力: rank, series_id, series_url

使い方(ローカル):
  pip install playwright
  playwright install chromium
  python tools/rank1_stage0_click_open_collect.py --url "https://www.autohome.com.cn/..." --outdir data/rank1_click --max 100
"""
import os
import re
import csv
import time
import argparse
from pathlib import Path
from typing import List, Tuple, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# === セレクタ候補 ===
RANK_ITEM_LINK_SELECTOR_CANDIDATES: List[str] = [
    # シリーズ名リンクに広めに当てる（クラスはよく変わるため）
    'a[href^="https://www.autohome.com.cn/"]:not([href*="club"])',
    'a[href*="autohome.com.cn/"]:not([href*="club"])',
    "a:has(.tw-text-lg)",
    ".tw-text-base.tw-font-semibold >> xpath=ancestor::a[1]"
]

def extract_series_id(url: str) -> str:
    """
    シリーズIDをURLから抽出。/series/12345.html または /12345/ パターンに対応。
    """
    m = re.search(r"/series/(\d+)\.html", url)
    if m:
        return m.group(1)
    m = re.search(r"/(\d{4,6})(?:/|$)", url)
    return m.group(1) if m else ""

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def wait_and_scroll_all(page, min_count: int = 100, max_loops: int = 200, wait_ms: int = 220):
    """
    無限スクロール対策: 底までスクロールしつつ要素数の増加が止まるまで待つ
    """
    last_height = 0
    for _ in range(max_loops):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(wait_ms)
        # 高さ変化検知
        height = page.evaluate("document.body.scrollHeight")
        progressed = height > last_height
        last_height = height
        # 要素カウント
        best = 0
        for sel in RANK_ITEM_LINK_SELECTOR_CANDIDATES:
            try:
                c = page.locator(sel).count()
                if c > best:
                    best = c
            except Exception:
                pass
        if best >= min_count and not progressed:
            break

def pick_best_selector(page) -> Tuple[str, int]:
    best_sel, best_count = None, 0
    for sel in RANK_ITEM_LINK_SELECTOR_CANDIDATES:
        try:
            cnt = page.locator(sel).count()
            if cnt > best_count:
                best_sel, best_count = sel, cnt
        except Exception:
            continue
    if not best_sel or best_count == 0:
        raise RuntimeError("ランキング項目のリンクが見つかりません。セレクタを調整してください。")
    return best_sel, best_count

def collect_links_by_click(url: str, outdir: str, topk: Optional[int], wait_ms: int, max_scrolls: int, pre_wait: int):
    ensure_dir(outdir)
    csv_path = os.path.join(outdir, "series_urls.csv")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        if pre_wait > 0:
            page.wait_for_timeout(pre_wait)

        # 充分に読み込ませる
        wait_and_scroll_all(page, min_count=(topk or 100), max_loops=max_scrolls, wait_ms=wait_ms)
        best_sel, total = pick_best_selector(page)

        # ランキング順のElementHandleスナップショットを確保
        elements = page.locator(best_sel).all()
        items = elements[: (topk or len(elements))]

        results: List[Tuple[int, str, str]] = []

        for i, el in enumerate(items, start=1):
            # 表示テキスト（シリーズ名のはず）: デバッグ用途
            try:
                title_on_list = el.inner_text().strip()
            except Exception:
                title_on_list = ""

            # クリックして別タブを捕捉
            try:
                with ctx.expect_page() as new_page_info:
                    el.scroll_into_view_if_needed()
                    el.click()
                new_page = new_page_info.value
            except PWTimeout:
                # Ctrl+Click で再試行（強制新規タブ）
                try:
                    with ctx.expect_page() as new_page_info:
                        el.scroll_into_view_if_needed()
                        el.click(modifiers=["Control"])
                    new_page = new_page_info.value
                except Exception:
                    results.append((i, title_on_list, ""))
                    continue

            # 読み込み待ち & URL取得
            try:
                new_page.wait_for_load_state("domcontentloaded", timeout=30000)
            except PWTimeout:
                pass
            opened_url = new_page.url or ""
            # すぐ閉じる
            try:
                new_page.close()
            except Exception:
                pass

            # CSVは rank, series_id, series_url
            sid = extract_series_id(opened_url)
            results.append((i, sid, opened_url))

        # 出力
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["rank", "series_id", "series_url"])
            for row in results:
                w.writerow(row)

        print(f"[ok] collected {len(results)} links -> {csv_path}")
        ctx.close()
        browser.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="AutohomeランキングのURL")
    ap.add_argument("--outdir", required=True, help="CSVなどの出力先ディレクトリ")
    ap.add_argument("--max", type=int, default=50, help="上位N件（既定50）")
    ap.add_argument("--wait-ms", type=int, default=220)
    ap.add_argument("--max-scrolls", type=int, default=220)
    ap.add_argument("--pre-wait", type=int, default=1200)
    args = ap.parse_args()

    collect_links_by_click(
        url=args.url,
        outdir=args.outdir,
        topk=args.max,
        wait_ms=args.wait_ms,
        max_scrolls=args.max_scrolls,
        pre_wait=args.pre_wait,
    )

if __name__ == "__main__":
    main()
