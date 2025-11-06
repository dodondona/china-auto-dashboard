#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tools/autohome_config_to_csv.py

完全オリジナル構造を維持。
修正点は唯一：
  - cell_value(): 同一セルに複数の●/○がある場合、すべて改行付きで出力。
他は一切変更なし。
"""

import os, sys, re, csv, time
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
    """セル内テキスト抽出（ここだけ修正）。"""
    icons = []
    for i_tag in td.select('[class*="style_col_dot_solid__"], [class*="style_col_dot_outline__"]'):
        c = " ".join(i_tag.get("class", []))
        icons.append("●" if "solid" in c else "○")

    txt = norm_space(td.get_text(" ", strip=True))

    if icons:
        # ○や●が複数ある場合、改行区切りで出力
        if txt:
            return "\n".join(f"{mark} {txt}" for mark in icons)
        else:
            return "\n".join(icons)
    return txt if txt else "–"


def parse_html_to_csv(html_path: Path, csv_path: Path):
    """HTML構造を解析（オリジナル構造維持）"""
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8", errors="ignore"), "lxml")

    tables = soup.select("table")
    rows = []

    for table in tables:
        section_name = table.find_previous("h3")
        sec_name = norm_space(section_name.get_text()) if section_name else ""
        for tr in table.select("tr"):
            tds = tr.select("td")
            if not tds:
                continue
            item_name = norm_space(tds[0].get_text())
            values = [cell_value(td) for td in tds[1:]]
            if item_name:
                rows.append([sec_name, item_name] + values)

    if not rows:
        print("[parse_html_to_csv] No data rows found.")
        return

    header = ["セクション", "項目"] + [f"グレード{i}" for i in range(1, len(rows[0]) - 1)]
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
