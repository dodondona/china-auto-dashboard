#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tools/autohome_config_to_csv.py

既存構造・挙動はそのまま。
修正点は1箇所のみ：
  - セル内で●と○が混在しても正しく改行付きで出力されるように。
他の動作・引数・ファイル構造・ヘッダー・quote設定など一切変更なし。
"""

import os
import sys
import re
import csv
import json
import time
from pathlib import Path
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup


def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip()) if s else ""


def fetch_html(series_id: str, output_dir: Path) -> Path:
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


def cell_value(td):
    """セル内テキストの抽出（ここだけ修正：●/○混在対応）"""
    # HTML構造から solid / outline の iタグをすべて拾う
    icons = []
    for i_tag in td.select('[class*="style_col_dot_solid__"], [class*="style_col_dot_outline__"]'):
        classes = " ".join(i_tag.get("class", []))
        icons.append("●" if "solid" in classes else "○")

    txt = norm_space(td.get_text(" ", strip=True))

    # アイコンが存在する場合はすべて改行区切りで結合
    if icons:
        if txt:
            return "\n".join(f"{mark} {txt}" for mark in icons)
        else:
            return "\n".join(icons)

    # アイコンがない場合は従来どおり
    return txt if txt else "–"


def parse_div_layout_to_wide_csv(soup, csv_path: Path):
    """Autohomeのdiv構造をCSVに変換（既存構造）"""
    sections = soup.select("div[id^='config_data_']")
    rows = []

    for sec in sections:
        sec_name = norm_space(sec.select_one("h3").get_text()) if sec.select_one("h3") else ""
        for tr in sec.select("tr, div.row"):
            tds = tr.select("td, div.col")
            if not tds:
                continue
            item = norm_space(tds[0].get_text())
            values = [cell_value(td) for td in tds[1:]]
            rows.append([sec_name, item] + values)

    if not rows:
        print("[parse_div_layout_to_wide_csv] No data rows found.")
        return

    # 元の構造を壊さずにヘッダー維持
    header = ["セクション", "項目"] + [f"グレード{i}" for i in range(1, len(rows[0]) - 1)]

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    print(f"[parse_div_layout_to_wide_csv] Wrote {csv_path} ({len(rows)} rows)")


def parse_html_to_csv(html_path: Path, csv_path: Path):
    """HTMLを読み込み、divレイアウト解析関数を呼び出す"""
    html = Path(html_path).read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "lxml")
    parse_div_layout_to_wide_csv(soup, csv_path)


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
