#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tools/autohome_config_to_csv.py

Autohome 設定ページを自動クロールして CSV に変換するスクリプト。
既存構造を一切変更せず、以下のみ修正：
  - CSV出力で「セクション」「項目」ヘッダーを明示。
  - セル内で●/○が混在する場合もすべて保持し、サブ項目ごとに改行。
"""

import os
import sys
import csv
import time
import re
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


def extract_cell_text(cell):
    """
    HTMLセル内のテキストを整形してCSV出力用に変換。
    ●/○が混在する場合もすべて保持し、サブ項目ごとに改行。
    """
    lines = []
    for ico in cell.find_all("i"):
        classes = " ".join(ico.get("class", []))
        mark = ""
        if "style_col_dot_solid" in classes:
            mark = "●"
        elif "style_col_dot_outline" in classes:
            mark = "○"
        if not mark:
            continue
        label = ico.find_next(text=True)
        if label:
            label = label.strip()
            if label:
                lines.append(f"{mark} {label}")
    if not lines:
        return cell.get_text(" ", strip=True)
    return "\n".join(lines)


def fetch_html(series_id: str, output_dir: Path) -> Path:
    """
    Autohome 設定ページを Playwright で取得し、HTML を保存。
    """
    url = f"https://www.autohome.com.cn/config/series/{series_id}.html"
    html_path = output_dir / f"config_{series_id}.html"
    print(f"[fetch_html] Fetching {url}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, timeout=60000)
        time.sleep(3)
        html = page.content()
        html_path.write_text(html, encoding="utf-8")
        browser.close()
    print(f"[fetch_html] Saved: {html_path}")
    return html_path


def parse_html_to_csv(html_path: Path, csv_path: Path):
    """
    Autohome 設定 HTML を解析して CSV に変換。
    """
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8", errors="ignore"), "lxml")
    container = soup.find("div", class_=re.compile("parameter_config_container"))
    if not container:
        print("[parse_html_to_csv] No config table found.")
        return

    rows = []
    for section in container.find_all("div", recursive=False):
        section_name = section.get_text(strip=True)
        for row in section.find_all("div", class_=re.compile("row|tr|line")):
            cols = row.find_all(["div", "td"])
            if not cols:
                continue
            item_name = cols[0].get_text(" ", strip=True)
            values = [extract_cell_text(cell) for cell in cols[1:]]
            rows.append([section_name, item_name] + values)

    # ✅ ヘッダー明示（これで translate_columns が正常動作）
    if rows:
        num_cols = len(rows[0])
        header = ["セクション", "項目"] + [f"グレード{i}" for i in range(1, num_cols - 1)]
    else:
        header = ["セクション", "項目"]

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    print(f"[parse_html_to_csv] Wrote {csv_path} ({len(rows)} rows)")


def main():
    if len(sys.argv) < 3 or sys.argv[1] != "--series":
        print("Usage: autohome_config_to_csv.py --series <series_id>")
        sys.exit(1)

    series_id = sys.argv[2]
    output_dir = Path(f"output/autohome/{series_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    html_path = fetch_html(series_id, output_dir)
    csv_path = output_dir / f"config_{series_id}.csv"
    parse_html_to_csv(html_path, csv_path)


if __name__ == "__main__":
    main()
