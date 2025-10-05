#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url.py
Autohome ランキングページから車種ごとのリンクを取得して
CSV に series_url 列を追加する。

- href が無い場合でも、Playwrightでクリックして遷移URLを確定。
- 同タブ遷移・新タブ・SPA すべて対応。
- 元CSVは読み取り専用。出力は別ファイル。
"""

import csv, re, sys, time, random, argparse
from urllib.parse import urljoin
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page, BrowserContext

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
ROW_SELECTOR = "table.rank-list tbody tr, table tbody tr"

def normalize_series_url(url: str) -> str:
    m = re.search(r"https?://www\.autohome\.com\.cn/(\d{3,7})(?:/|$)", url or "")
    return f"https://www.autohome.com.cn/{m.group(1)}/" if m else ""

def click_and_capture_series_url(context: BrowserContext, page: Page, clickable, base_url: str, timeout_ms: int = 15000) -> str:
    """hrefが無い場合に実際にクリックして遷移URLを得る"""
    try:
        clickable.scroll_into_view_if_needed(timeout=timeout_ms)
    except Exception:
        pass

    target_url = ""
    got_popup = False
    popup_wait = context.expect_page()

    try:
        with page.expect_navigation(wait_until="load", timeout=timeout_ms):
            clickable.click(timeout=timeout_ms)
        target_url = page.url
    except Exception:
        # 新タブパターン
        try:
            popup = popup_wait.value
            got_popup = True
            popup.wait_for_load_state("networkidle", timeout=timeout_ms)
            target_url = popup.url
            popup.close()
        except Exception:
            target_url = page.url

    # 同タブなら戻る
    if not got_popup:
        try:
            page.go_back(wait_until="load", timeout=timeout_ms)
        except Exception:
            page.goto(base_url, wait_until="domcontentloaded")

    return normalize_series_url(target_url)

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
        w.writeheader()
        w.writerows(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", required=True)
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    rows = read_csv_rows(args.input)
    print(f"[1/3] 読み込み: {len(rows)} 行")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width":1280,"height":1600}
        )
        page = context.new_page()
        page.goto(args.rank_url, wait_until="networkidle", timeout=30000)

        all_rows = page.locator(ROW_SELECTOR)
        n = min(all_rows.count(), len(rows))
        print(f"[2/3] ランキング行検出: {n}件")

        for i in range(n):
            r = all_rows.nth(i)
            href = ""
            text = ""
            a = r.locator("td.name a").first
            try:
                if a.count() > 0:
                    href = a.get_attribute("href")
                    text = a.inner_text().strip()
            except Exception:
                pass

            if href:
                full = urljoin(args.rank_url, href)
                rows[i]["series_url"] = normalize_series_url(full)
            else:
                clickable = a if a.count() > 0 else r.locator("td.name").first
                try:
                    url = click_and_capture_series_url(context, page, clickable, args.rank_url)
                    rows[i]["series_url"] = url
                    print(f"  → #{i+1}: click取得 {url}")
                except Exception as e:
                    print(f"  × #{i+1} 取得失敗: {e}")
                    rows[i]["series_url"] = ""

            time.sleep(random.uniform(0.1, 0.3))

        context.close(); browser.close()

    write_csv_rows(args.output, rows)
    print(f"[3/3] 出力完了: {args.output}")

if __name__ == "__main__":
    main()
