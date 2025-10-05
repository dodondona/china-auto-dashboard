#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url.py
Autohomeのランキングページから各車種のリンク（series URL）を取得し、
既存CSVに 'series_url' 列を追記するだけのユーティリティ。

- 元のCSVは変更しない（別名で保存）
- 動的HTML対応（Playwright使用）
"""

import csv, re, sys, time, random, argparse
from urllib.parse import urljoin, urlparse
from pathlib import Path
from typing import List, Dict, Optional
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

ROW_SELECTORS = ["table.rank-list tbody tr", "table tbody tr"]
NAME_LINK_SELECTORS = ["td.name a", "td a", "a"]

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
        w.writeheader()
        w.writerows(rows)

def normalize_url(url: str) -> str:
    if not url: return ""
    m = re.search(r"/(\d{3,7})/", url)
    if not m:
        m = re.search(r"/(\d{3,7})(?:$|\?)", url)
    if m:
        return f"https://www.autohome.com.cn/{m.group(1)}/"
    pr = urlparse(url)
    return f"{pr.scheme}://{pr.netloc}{pr.path}"

def fetch_links(rank_url: str, max_rows: Optional[int] = None) -> List[Dict[str, str]]:
    result = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()
        page.goto(rank_url, wait_until="networkidle")
        rows = None
        for sel in ROW_SELECTORS:
            try:
                page.wait_for_selector(sel, timeout=15000)
                loc = page.locator(sel)
                if loc.count() > 0:
                    rows = loc
                    break
            except PWTimeout:
                continue
        if not rows:
            raise RuntimeError("行が見つかりません。セレクタ要調整。")

        n = rows.count() if max_rows is None else min(max_rows, rows.count())
        for i in range(n):
            row = rows.nth(i)
            link, text = "", ""
            for a_sel in NAME_LINK_SELECTORS:
                a = row.locator(a_sel).first
                try:
                    href = a.get_attribute("href")
                    tx = a.inner_text().strip()
                except Exception:
                    continue
                if href:
                    link = urljoin(rank_url, href)
                    text = tx
                    break
            result.append({"href": link or "", "text": text})
            time.sleep(random.uniform(0.05, 0.15))
        context.close(); browser.close()
    return result

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rank-url", required=True)
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    rows = read_csv(args.input)
    links = fetch_links(args.rank_url, len(rows))

    for i, row in enumerate(rows):
        href = links[i]["href"] if i < len(links) else ""
        row["series_url"] = normalize_url(href)
    write_csv(args.output, rows)
    print(f"✅ series_url追記完了: {args.output}")

if __name__ == "__main__":
    main()
