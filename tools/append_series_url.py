#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url.py (popup-aware)
- ランキング /rank/1（など）から各行の series_url を取得して CSV に追記
- 車名クリックで「新しいタブ」が開く前提に対応（popup を expect して URL 取得）
- href が無い/右クリック禁止でも、クリック → 新タブURL取得 → 正規化
- #pvareaid やクエリは落として https://www.autohome.com.cn/<id>/ へ正規化
- 行セレクタが合わない場合はページ全体から /<id>/ パターンのリンクを拾うフォールバック
"""

import csv, re, sys, time, random, argparse
from urllib.parse import urljoin
from typing import List, Dict, Optional, Tuple
from playwright.sync_api import (
    sync_playwright, TimeoutError as PWTimeout, Page, BrowserContext
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120 Safari/537.36"
)

# ランキングの行候補（広めに）
ROW_SELECTORS = [
    "table.rank-list tbody tr",
    "div.rank-list table tbody tr",
    "table tbody tr",
    ".rank-list tbody tr",
    "div.rank-table tbody tr",
]

# 行内の車名セル／リンク候補（順に試す）
NAME_CELL_SELECTORS = [
    "td.name a", "td a", "td.name", "td:nth-child(2) a", "td:nth-child(2)",
]

# series_id 抽出（様々な表記に対応）
SERIES_ID_PATTERNS = [
    re.compile(r"/(\d{3,7})(?:/|$)"),
    re.compile(r"series[-/](\d{3,7})", re.I),
    re.compile(r"[?&#]seriesid=(\d{3,7})", re.I),
    re.compile(r"series_(\d{3,7})", re.I),
]

def extract_series_id(url: str) -> Optional[str]:
    if not url:
        return None
    for pat in SERIES_ID_PATTERNS:
        m = pat.search(url)
        if m:
            return m.group(1)
    return None

def normalize_series_url(url: str) -> str:
    """https://www.autohome.com.cn/<id>/ に丸める。ハッシュやクエリは無視。"""
    sid = extract_series_id(url or "")
    return f"https://www.autohome.com.cn/{sid}/" if sid else ""

def read_csv_rows(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv_rows(path: str, rows: List[Dict[str, str]]) -> None:
    if not rows: return
    fields = list(rows[0].keys())
    if "series_url" not in fields:
        fields.append("series_url")
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)

def find_rows(page: Page):
    for sel in ROW_SELECTORS:
        loc = page.locator(sel)
        if loc.count() > 0:
            return loc
    return None

def collect_series_links_fallback(page: Page, base_url: str, expect_n: int) -> List[str]:
    """テーブルが取れない場合の保険：ページ全体の <a> から /<id>/ を拾って上から順に返す"""
    js = """
    () => Array.from(document.querySelectorAll('a[href]')).map(a=>{
      const r=a.getBoundingClientRect();
      return {href:a.getAttribute('href'), y:r.top}
    })
    """
    anchors = page.evaluate(js) or []
    links: List[Tuple[float,str]] = []
    for a in anchors:
        href = a.get("href") or ""
        if href.startswith("//"): href = "https:" + href
        elif href.startswith("/"): href = urljoin(base_url, href)
        norm = normalize_series_url(href)
        if norm:
            links.append((a.get("y", 1e9), norm))
    links.sort(key=lambda t: t[0])
    uniq, seen = [], set()
    for _, u in links:
        if u not in seen:
            seen.add(u); uniq.append(u)
        if len(uniq) >= expect_n:
            break
    return uniq

def click_and_capture_series_url(
    context: BrowserContext, page: Page, clickable, base_url: str, timeout_ms: int = 15000
) -> str:
    """新しいタブで開く前提を重視して、popup を捕まえてURLを得る。"""
    try:
        clickable.scroll_into_view_if_needed(timeout=timeout_ms)
    except Exception:
        pass

    target_url = ""
    got_popup = False

    # クリック直前に popup の監視をセット
    with context.expect_page() as popup_wait:
        try:
            clickable.click(timeout=timeout_ms, force=True)
        except Exception:
            # 最終手段：bounding box クリック
            try:
                box = clickable.bounding_box()
                if box:
                    page.mouse.click(box["x"] + box["width"]/2, box["y"] + box["height"]/2)
                else:
                    clickable.click(timeout=timeout_ms, force=True)
            except Exception:
                pass
    try:
        popup = popup_wait.value
        got_popup = True
        popup.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
        # 画像なども読み込ませておくと URL 安定
        popup.wait_for_load_state("networkidle", timeout=timeout_ms)
        target_url = popup.url
        popup.close()
    except Exception:
        # まれに同タブで遷移 or SPA でURLだけ変化
        try:
            target_url = page.url
        except Exception:
            target_url = ""

    # 同タブ遷移していたらランキングに戻る
    if not got_popup:
        try:
            page.go_back(wait_until="load", timeout=timeout_ms)
        except Exception:
            page.goto(base_url, wait_until="domcontentloaded")

    return normalize_series_url(target_url)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", required=True, help="例: https://www.autohome.com.cn/rank/1")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    rows = read_csv_rows(args.input)
    if not rows:
        print("入力CSVが空です。", file=sys.stderr); sys.exit(1)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width":1280,"height":1700}
        )
        page = context.new_page()
        page.goto(args.rank_url, wait_until="networkidle", timeout=30000)

        rows_loc = find_rows(page)
        if rows_loc and rows_loc.count() > 0:
            n = min(rows_loc.count(), len(rows))
            for i in range(n):
                r = rows_loc.nth(i)
                url = ""
                # 1) まず href を試す（新タブでも href があればそのまま正規化）
                got = False
                for sel in NAME_CELL_SELECTORS:
                    loc = r.locator(sel).first
                    if loc.count() == 0: continue
                    try:
                        href = loc.get_attribute("href")
                        if href and href != "javascript:void(0)":
                            from urllib.parse import urljoin
                            url = normalize_series_url(urljoin(args.rank_url, href))
                            got = True
                            break
                    except Exception:
                        pass
                # 2) href が無い/void の場合はクリックで popup URL を取得
                if not got:
                    clickable = None
                    for sel in NAME_CELL_SELECTORS:
                        loc = r.locator(sel).first
                        if loc.count() > 0:
                            clickable = loc; break
                    if not clickable:
                        clickable = r  # 最後の保険
                    try:
                        url = click_and_capture_series_url(context, page, clickable, args.rank_url)
                    except Exception:
                        url = ""
                rows[i]["series_url"] = url
                time.sleep(random.uniform(0.08, 0.2))
        else:
            # テーブルが見つからない → ページ全体から /<id>/ を拾うフォールバック
            urls = collect_series_links_fallback(page, args.rank_url, expect_n=len(rows))
            for i in range(len(rows)):
                rows[i]["series_url"] = urls[i] if i < len(urls) else ""

        context.close(); browser.close()

    write_csv_rows(args.output, rows)
    print(f"✔ series_url 追記完了: {args.output}")

if __name__ == "__main__":
    main()
