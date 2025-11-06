#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Autohome Config HTML → CSV 変換スクリプト（完全版）
------------------------------------------------------------
- 各シリーズIDの config ページ (https://www.autohome.com.cn/config/series/xxx.html)
  を解析して CSV 出力する
- 出力先: output/autohome/{series_id}/config_{series_id}.csv
"""

import os
import re
import sys
import time
import csv
import json
import argparse
import requests
from pathlib import Path
from bs4 import BeautifulSoup

# ============================================================
# Utility functions
# ============================================================

def fetch_html(series_id: str) -> Path:
    """AutohomeのシリーズIDページからHTMLを取得して保存"""
    url = f"https://www.autohome.com.cn/config/series/{series_id}.html"
    print(f"[fetch_html] Fetching {url}")

    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, headers=headers, timeout=30)
    res.encoding = "utf-8"
    html_path = Path(f"output/autohome/{series_id}/config_{series_id}.html")
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(res.text, encoding="utf-8")
    print(f"[fetch_html] Saved: {html_path}")
    return html_path


# ============================================================
# cell_value() ←ここだけ修正版
# ============================================================
def cell_value(span) -> str:
    """
    <span> 要素を人間可読な文字列に変換
    - ●/○ アイコンを維持
    - HTML出現順を厳守
    """
    parts = []
    for node in span.children:
        # アイコン判定
        if getattr(node, "name", None) == "i":
            cls = " ".join(node.get("class", []))
            if "solid" in cls:
                parts.append("●")
            elif "outline" in cls:
                parts.append("○")
        else:
            # テキストノードはそのまま追加
            text = str(node).strip()
            if text:
                parts.append(text)
    # スペース区切りで結合（順序保持）
    return " ".join(parts).strip()


# ============================================================
# HTML解析処理
# ============================================================

def parse_html_to_rows(html_path: Path):
    """HTMLファイルを解析し、行データを返す"""
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "lxml")

    table_divs = soup.find_all("div", class_=re.compile("style_col_xf986"))
    rows = []

    for div in table_divs:
        title_div = div.find_previous_sibling("div", class_=re.compile("style_col_xf986"))
        section_name = title_div.get_text(strip=True) if title_div else ""
        span = div.find("span", class_=re.compile("tw-flex"))
        if not span:
            continue
        val = cell_value(span)
        rows.append({
            "セクション": section_name,
            "項目": val
        })
    return rows


# ============================================================
# メイン処理
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--series", required=True, help="Autohome series ID")
    args = parser.parse_args()
    sid = args.series

    out_dir = Path(f"output/autohome/{sid}")
    out_dir.mkdir(parents=True, exist_ok=True)

    html_path = fetch_html(sid)
    rows = parse_html_to_rows(html_path)

    if not rows:
        print("[parse_div_layout_to_wide_csv] No data rows found.")
        return

    csv_path = out_dir / f"config_{sid}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["セクション", "項目"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"[done] {csv_path} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
