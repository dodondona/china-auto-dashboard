# tools/autohome_config_to_csv.py
# -*- coding: utf-8 -*-
"""
Autohome 参数配置ページをPlaywrightでロードし、HTMLテーブルをCSVに出力。
不可視文字（\xa0, \u200b, \ufeff）による文字欠け（例：前置→置）を防止済み。
テーブル描画完了待機（.style_row__XPu4s）を追加。
"""

import os
import re
import csv
import sys
import pathlib
from playwright.sync_api import sync_playwright

def normalize_value(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if s in {"—", "-", "–", "×", "无"}:
        return "0"
    if s in {"●", "•"}:
        return "1"
    if s == "○":
        return "opt"
    return s

def safe_text(el):
    """Playwright要素から安全にテキストを抽出（不可視文字除去＋strip）"""
    txt = el.inner_text().replace("\xa0", "").replace("\u200b", "").replace("\ufeff", "").strip()
    return normalize_value(txt)

def ensure_dir(p: pathlib.Path):
    p.mkdir(parents=True, exist_ok=True)

def dump_csv(path: pathlib.Path, rows):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerows(rows)

def series_id_from_url(url: str) -> str:
    m = re.search(r"/series/(\d+)", url)
    return m.group(1) if m else "series"

def main():
    urls = [u for u in sys.argv[1:] if u.startswith("http")]
    if not urls:
        urls = ["https://www.autohome.com.cn/config/series/7806.html#pvareaid=3454437"]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        page.set_default_timeout(45000)

        for url in urls:
            sid = series_id_from_url(url)
            print(f"Loading: {url}", flush=True)
            page.goto(url, wait_until="networkidle")

            # ★追加：JS描画完了を待機（参数配置テーブルが表示されるまで）
            try:
                page.wait_for_selector(".style_row__XPu4s", timeout=15000)
            except Exception:
                print("⚠️ Timeout: 表描画が確認できませんでしたが続行します。")

            # スクロールで遅延ロード要素を展開
            try:
                total_h = page.evaluate("() => document.body.scrollHeight")
                y = 0
                while y < total_h:
                    page.evaluate(f"() => window.scrollTo(0, {y})")
                    page.wait_for_timeout(100)
                    y += 800
                page.wait_for_timeout(300)
            except Exception:
                pass

            # テーブル・グリッド抽出
            tables = page.query_selector_all("table")
            grids = page.query_selector_all('[role=\"table\"], [role=\"grid\"]')
            objs = [("table", el) for el in tables] + [("grid", el) for el in grids]

            if not objs:
                print("No table found.")
                continue

            out_dir = pathlib.Path("output") / "autohome" / sid
            ensure_dir(out_dir)
            all_rows = []
            saved = 0

            for idx, (kind, root) in enumerate(objs, start=1):
                if kind == "table":
                    trs = root.query_selector_all("tr")
                    rows = []
                    for tr in trs:
                        tds = tr.query_selector_all("th,td")
                        row = [safe_text(td) for td in tds]
                        if any(row):
                            rows.append(row)
                else:
                    rows = []
                    child_rows = root.query_selector_all(":scope > *")
                    for r in child_rows:
                        cells = r.query_selector_all(":scope > *")
                        if not cells:
                            continue
                        row = [safe_text(td) for td in cells]
                        if any(row):
                            rows.append(row)

                if not rows:
                    continue

                width = max(len(r) for r in rows)
                rows = [r + [""] * (width - len(r)) for r in rows]
                saved += 1
                part_path = out_dir / f"table_{idx:02d}.csv"
                dump_csv(part_path, rows)
                print(f"✅ Saved: {part_path} ({len(rows)} rows)")
                all_rows.extend(rows)

            print(f"Found {saved} table(s)")
            if all_rows:
                merged_path = out_dir / f"config_{sid}.csv"
                dump_csv(merged_path, all_rows)
                print(f"✅ Saved: {merged_path} ({len(all_rows)} rows)")

        browser.close()

if __name__ == "__main__":
    main()
