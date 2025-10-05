#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url.py (name-driven popup capture)
- ランキング /rank/1 のカード型UIに対応
- CSV側の車名列（model_text 等）を手掛かりに画面上の該当テキストをクリック
- 新しいタブ (popup) を捕まえて遷移先URLを取得し、series_url に正規化して追記
- #pvareaid やクエリは無視して https://www.autohome.com.cn/<id>/ に丸める
"""

import csv, re, sys, time, random, argparse
from typing import List, Dict, Optional
from urllib.parse import urljoin
from playwright.sync_api import (
    sync_playwright, TimeoutError as PWTimeout, Page, BrowserContext
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120 Safari/537.36"
)

# series_id 抽出（さまざまな表記に対応）
SERIES_ID_PATS = [
    re.compile(r"/(\d{3,7})(?:/|$)"),
    re.compile(r"series[-/](\d{3,7})", re.I),
    re.compile(r"[?&#]seriesid=(\d{3,7})", re.I),
    re.compile(r"series_(\d{3,7})", re.I),
]

def extract_series_id(url: str) -> Optional[str]:
    if not url: return None
    for pat in SERIES_ID_PATS:
        m = pat.search(url)
        if m: return m.group(1)
    return None

def normalize_series_url(url: str) -> str:
    sid = extract_series_id(url or "")
    return f"https://www.autohome.com.cn/{sid}/" if sid else ""

def read_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv(path: str, rows: List[Dict[str, str]]) -> None:
    if not rows: return
    fields = list(rows[0].keys())
    if "series_url" not in fields:
        fields.append("series_url")
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)

def detect_name_col(fieldnames: List[str], preferred: Optional[str]) -> str:
    if preferred and preferred in fieldnames:
        return preferred
    for c in ["model_text", "model", "name", "car", "series_name", "title"]:
        if c in fieldnames: return c
    # 最後の手段：先頭列
    return fieldnames[0]

def scroll_page(page: Page, steps: int = 8, pause: float = 0.25):
    for _ in range(steps):
        page.mouse.wheel(0, 1400)
        page.wait_for_timeout(int(pause * 1000))

def click_and_capture_popup(context: BrowserContext, page: Page, clickable, base_url: str, timeout_ms: int = 12000) -> str:
    """車名テキストなどをクリック → 新タブURLを取得。失敗時は同タブURLも見る。"""
    try:
        clickable.scroll_into_view_if_needed(timeout=timeout_ms)
    except Exception:
        pass

    # クリック直前に popup 監視を仕掛ける
    with context.expect_page() as popup_wait:
        try:
            clickable.click(timeout=timeout_ms, force=True)
        except Exception:
            # 座標クリック（最終兵器）
            try:
                box = clickable.bounding_box()
                if box:
                    page.mouse.click(box["x"] + box["width"]/2, box["y"] + box["height"]/2)
                else:
                    clickable.click(timeout=timeout_ms, force=True)
            except Exception:
                pass

    # 新タブを受け取る（来なければ同タブでURL変化を確認）
    target_url = ""
    got_popup = False
    try:
        p2 = popup_wait.value
        got_popup = True
        p2.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
        p2.wait_for_load_state("networkidle", timeout=timeout_ms)
        target_url = p2.url
        p2.close()
    except Exception:
        try:
            target_url = page.url
        except Exception:
            target_url = ""

    # 同タブで変わってしまったら戻す
    if not got_popup:
        try:
            page.go_back(wait_until="load", timeout=timeout_ms)
        except Exception:
            page.goto(base_url, wait_until="domcontentloaded")

    return normalize_series_url(target_url)

def find_click_target_for_name(page: Page, name: str):
    """
    クリック対象を “名前ベース” で探す。
    - まず a:has-text("<name>")
    - なければ get_by_text("<name>")
    - それでもダメならカードらしき要素（li/div）で has_text してその中の a を優先
    """
    name = (name or "").strip()
    if not name: return None

    # 1) a:has-text
    loc = page.locator(f'a:has-text("{name}")')
    if loc.count() > 0:
        return loc.first

    # 2) get_by_text（Playwrightのテキスト検索）
    try:
        loc2 = page.get_by_text(name, exact=False)
        if loc2.count() > 0:
            return loc2.first
    except Exception:
        pass

    # 3) li/div カード内の a
    containers = page.locator("li, div")
    cont = containers.filter(has_text=name)
    if cont.count() > 0:
        a_inside = cont.first.locator("a")
        if a_inside.count() > 0:
            return a_inside.first
        return cont.first

    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", required=True, help="例: https://www.autohome.com.cn/rank/1")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--name-col", default=None, help="車名列（未指定なら自動推定）")
    args = ap.parse_args()

    rows = read_csv(args.input)
    if not rows:
        print("入力CSVが空です。", file=sys.stderr); sys.exit(1)

    name_col = detect_name_col(list(rows[0].keys()), args.name_col)
    print(f"[*] 使用する車名列: {name_col}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 1700}
        )
        page = context.new_page()
        page.goto(args.rank_url, wait_until="networkidle", timeout=30000)
        scroll_page(page, steps=4, pause=0.2)  # 初期ロード促進

        for i, r in enumerate(rows):
            name = (r.get(name_col) or "").strip()
            if not name:
                rows[i]["series_url"] = ""
                continue

            url = ""
            # 検索→スクロールを繰り返して見つける
            for _ in range(6):
                target = find_click_target_for_name(page, name)
                if target and target.count() > 0:
                    try:
                        url = click_and_capture_popup(context, page, target, args.rank_url)
                    except Exception:
                        url = ""
                    if url:
                        break
                # まだ見つからない/開けない → もう少しスクロールして再トライ
                page.mouse.wheel(0, 1400)
                page.wait_for_timeout(350)

            rows[i]["series_url"] = url
            print(f"  #{i+1} {name} -> {url or '(未取得)'}")
            time.sleep(random.uniform(0.06, 0.15))

        context.close(); browser.close()

    write_csv(args.output, rows)
    print(f"✔ 出力: {args.output}")

if __name__ == "__main__":
    main()
