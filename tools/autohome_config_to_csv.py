#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tools/autohome_config_to_csv.py

Autohome 設定ページをクロールして CSV を生成。
【最小修正のみ】
  1) CSVヘッダーを明示（"セクション","項目",...）
  2) セル内改行を含むため csv.QUOTE_ALL で必ずクォートして出力
  3) ●/○ をサブ項目ごとに保持し、セル内は改行結合（表示だけの改善）

他の処理（取得・解析・出力先・引数仕様など）は従来通り。
"""

import sys
import csv
import time
import re
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


def extract_cell_text(cell):
    """
    セル内テキストを整形。
    - ●=style_col_dot_solid*, ○=style_col_dot_outline*
    - サブ項目ごとに "●/○ + 文言" を 1行（改行区切り）
    - 記号がなければ従来通りのプレーンテキスト
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
        # アイコン直後のテキストを素直に拾う
        label = ico.find_next(string=True)
        if label:
            label = label.strip()
            if label:
                lines.append(f"{mark} {label}")
    if lines:
        return "\n".join(lines)
    return cell.get_text(" ", strip=True)


def fetch_html(series_id: str, output_dir: Path) -> Path:
    """PlaywrightでHTMLを取得して保存（従来通り）。"""
    url = f"https://www.autohome.com.cn/config/series/{series_id}.html"
    html_path = output_dir / f"config_{series_id}.html"
    print(f"[fetch_html] {url}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, timeout=60_000)
        time.sleep(3)  # 最小の待機（従来相当）
        html = page.content()
        browser.close()
    html_path.write_text(html, encoding="utf-8")
    print(f"[fetch_html] saved -> {html_path}")
    return html_path


def parse_html_to_csv(html_path: Path, csv_path: Path):
    """HTMLを解析してCSVへ（構造は従来通り。出力だけ最小修正）。"""
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8", errors="ignore"), "lxml")

    container = soup.find("div", class_=re.compile("parameter_config_container"))
    if not container:
        print("[parse_html_to_csv] config container not found.")
        return

    rows = []

    # 既存想定：左側に項目名、右側に複数グレード列の構造
    # セクション単位のブロックを緩く探索（従来の曖昧検索を維持）
    for section in container.find_all(recursive=False):
        section_name = section.get_text(strip=True)
        # 行（項目）らしきコンテナを探索
        for row in section.find_all(lambda t: t.name in ("div", "tr", "li") and t.find(["div", "td"])):
            cols = row.find_all(["div", "td"])
            if not cols:
                continue
            item_name = cols[0].get_text(" ", strip=True)
            if not item_name:
                continue
            values = [extract_cell_text(c) for c in cols[1:]]
            if values:
                rows.append([section_name, item_name] + values)

    # ヘッダーを明示（ここだけ追加）。列数に合わせてグレード名を自動生成。
    if rows:
        num_cols = len(rows[0])
        header = ["セクション", "項目"] + [f"グレード{i}" for i in range(1, num_cols - 1)]
    else:
        header = ["セクション", "項目"]

    # セル内改行を正しく扱うため、必ずクォート（ここだけ追加）。
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)

    print(f"[parse_html_to_csv] wrote -> {csv_path}  rows={len(rows)}")


def main():
    # 既存仕様：--series <id>
    if len(sys.argv) < 3 or sys.argv[1] != "--series":
        print("Usage: autohome_config_to_csv.py --series <series_id>")
        sys.exit(1)

    series_id = sys.argv[2]
    out_dir = Path(f"output/autohome/{series_id}")
    out_dir.mkdir(parents=True, exist_ok=True)

    html_path = fetch_html(series_id, out_dir)
    csv_path = out_dir / f"config_{series_id}.csv"
    parse_html_to_csv(html_path, csv_path)


if __name__ == "__main__":
    main()
