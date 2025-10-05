#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_from_web.py (row-driven, strict matching)
- /rank/1 を開いて無限スクロール
- CSV 各行の「車名（+任意で販売台数）」を手がかりに、該当カードだけを探して a[href] を取得
- series_url を https://www.autohome.com.cn/<id>/ に正規化して追記
- 見つからない行は空のまま（順番埋めはしない）

使い方（Actions内）:
  python tools/append_series_url_from_web.py \
    --rank-url https://www.autohome.com.cn/rank/1 \
    --input data/autohome_raw_YYYY-MM.csv \
    --output data/autohome_raw_YYYY-MM_with_series.csv \
    --name-col model \
    --max-rounds 40 --idle-ms 700
"""

import re, csv, sys, argparse, time
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")

RE_SERIES_ID_IN_URL = re.compile(r'/(\d{3,7})(?:/|[?#])')

def normalize(s: str) -> str:
    if not s: return ""
    s = s.strip().lower()
    # よくある記号・全角/半角スペース・区切りを除去
    s = re.sub(r'[ \t\u3000・·•／/（）()\-\+]+', '', s)
    return s

def to_series_url(url_or_id: str) -> str:
    if not url_or_id: return ""
    m = RE_SERIES_ID_IN_URL.search(url_or_id)
    if m:
        sid = m.group(1)
    else:
        # 12345 のようなIDが渡ってきたケース
        sid = re.sub(r'\D', '', url_or_id)
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

def detect_name_col(fields: List[str], preferred: Optional[str]) -> str:
    if preferred and preferred in fields: return preferred
    for c in ["model_text","model","name","car","series_name","title"]:
        if c in fields: return c
    return fields[0]

def extract_href_from_element(el) -> Optional[str]:
    # 要素自身
    try:
        href = el.get_attribute("href")
        if href and href != "javascript:void(0)": return href
    except Exception:
        pass
    # 子孫の a[href]
    try:
        a = el.locator("a[href]").first
        if a.count() > 0:
            href = a.get_attribute("href")
            if href and href != "javascript:void(0)": return href
    except Exception:
        pass
    # 祖先→子孫の a[href]（カードのラッパーから）
    try:
        parent = el.locator("xpath=ancestor-or-self::*").first
        a2 = parent.locator("a[href]").first
        if a2.count() > 0:
            href = a2.get_attribute("href")
            if href and href != "javascript:void(0)": return href
    except Exception:
        pass
    return None

def find_card_and_href(page, name_norm: str) -> Optional[str]:
    """
    画面上から name_norm を含むテキストを持つ要素を探し、その近傍の a[href] を取る。
    """
    # a:has-text → テキスト → カード(container)の順にトライ
    locs = [
        page.locator(f'a:has-text("{name_norm}")'),
        page.get_by_text(name_norm, exact=False)
    ]
    for loc in locs:
        if loc.count() > 0:
            el = loc.first
            href = extract_href_from_element(el)
            if href: return href

    # 最後の保険：カードっぽい要素で has_text 検索
    cont = page.locator("li,div").filter(has_text=name_norm).first
    if cont and cont.count() > 0:
        href = extract_href_from_element(cont)
        if href: return href

    return None

def scroll_until_found(page, target_norm: str, max_rounds: int, idle_ms: int) -> Optional[str]:
    """
    無限スクロールしながら target_norm を探す。見つかったら href を返す。
    """
    # まず画面上を試す
    href = find_card_and_href(page, target_norm)
    if href: return href

    last_height = 0
    for _ in range(max_rounds):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(idle_ms)
        page.wait_for_load_state("networkidle")

        href = find_card_and_href(page, target_norm)
        if href: return href

        # スクロール位置が増えていない = 末尾まで到達の兆候
        height = page.evaluate("() => document.body.scrollHeight")
        if height == last_height:
            break
        last_height = height

    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", default="https://www.autohome.com.cn/rank/1")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--name-col", default=None)
    ap.add_argument("--max-rounds", type=int, default=40)
    ap.add_argument("--idle-ms", type=int, default=700)
    args = ap.parse_args()

    rows = read_csv_rows(args.input)
    if not rows:
        print("入力CSVが空です。"); sys.exit(1)
    name_col = detect_name_col(list(rows[0].keys()), args.name_col)

    print("[append_series_url] mode=ROW-DRIVEN (no order fallback)", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(user_agent=UA, viewport={"width":1280,"height":1700})
        # 軽量化：画像・動画・フォントはブロック
        context.route("**/*", lambda route: route.abort()
                      if route.request.resource_type in ["image","media","font"] else route.continue_())
        page = context.new_page()
        page.goto(args.rank_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_load_state("networkidle")

        # 行ごとに検索してURL取得
        for i, r in enumerate(rows):
            raw_name = r.get(name_col, "")
            name_norm = normalize(raw_name)
            url = ""

            if name_norm:
                try:
                    href = scroll_until_found(page, name_norm, max_rounds=args.max_rounds, idle_ms=args.idle_ms)
                    if href:
                        if href.startswith("//"): href = "https:" + href
                        elif href.startswith("/"): href = urljoin(args.rank_url, href)
                        url = to_series_url(href)
                except Exception:
                    url = ""

            r["series_url"] = url
            print(f"[{i+1}/{len(rows)}] {raw_name} -> {url or '(not found)'}", flush=True)

        context.close(); browser.close()

    write_csv_rows(args.output, rows)
    print(f"✔ 出力: {args.output}  （CSV {len(rows)}行）")
    print(f"  使用した車名列: {name_col}")

if __name__ == "__main__":
    main()
