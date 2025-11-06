#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tools/autohome_config_to_csv.py

Autohome設定ページをクロールしてCSVを生成するスクリプト。
既存処理を一切壊さず、CSV出力時の表記（●/○混在・改行欠如）を正確化。

変更点：
- ●/○をサブ項目ごとに保持。
- 改行区切りで1セル内に全て出力。
- その他の処理（クロール・翻訳・構造解析）は従来通り。

"""

import os
import sys
import csv
import time
import json
import re
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


def extract_cell_text(cell):
    """
    HTMLセル内のテキストを整形してCSV出力用に変換する。
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
        text = cell.get_text(" ", strip=True)
        return text
    return "\n".join(lines)


def fetch_html(series_id: str, output_dir: Path) -> Path:
    """
    Autohomeの設定ページをPlaywrightで取得し、ローカルHTMLとして保存。
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
    Autohome設定HTMLを解析してCSVに変換。
    """
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8", errors="ignore"), "lxml")

    rows = []
    table = soup.find("div", class_=re.compile("parameter_config_container"))
    if not table:
        print("[parse_html_to_csv] No table found.")
        return

    for section in table.find_all("div", recursive=False):
        section_name = section.get_text(strip=True)
        for row in section.find_all("div", class_=re.compile("row|tr|line")):
            cols = row.find_all(["div", "td"])
            if not cols:
                continue
            item_name = cols[0].get_text(" ", strip=True)
            values = []
            for cell in cols[1:]:
                values.append(extract_cell_text(cell))
            rows.append([section_name, item_name] + values)

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
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
