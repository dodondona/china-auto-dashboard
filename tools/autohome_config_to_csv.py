#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
import json
import argparse
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# --------------------------------
# 定数
# --------------------------------
PC_URL = "https://www.autohome.com.cn/config/series/{series}.html#pvareaid=3454437"
MOBILE_URL = "https://car.m.autohome.com.cn/config/spec/{series}/"

# --------------------------------
# 正規化ユーティリティ
# --------------------------------
def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

# --------------------------------
# HTML解析
# --------------------------------
def extract_table(soup: BeautifulSoup):
    head = soup.select_one('[class*="style_table_title__"]')
    if not head:
        return None

    head_cells = [c for c in head.find_all(recursive=False) if getattr(c, "name", None)]

    def clean_model_name(t):
        t = norm_space(t)
        t = re.sub(r"^\s*钉在左侧\s*", "", t)
        t = re.sub(r"\s*对比\s*$", "", t)
        return norm_space(t)

    model_names = [clean_model_name(c.get_text(" ", strip=True)) for c in head_cells[1:]]
    n_models = len(model_names)

    def find_container_with(head_node):
        p = head_node
        for _ in range(12):
            p = p.parent
            if not p:
                break
            if p.find(class_=re.compile(r"style_table_title__")) and p.find(class_=re.compile(r"style_row__")):
                return p
        return head_node.parent

    container = find_container_with(head)
    if not container:
        return None

    def is_section_title(node):
        cls = " ".join(node.get("class", []))
        return "style_table_title__" in cls

    def get_section_from_title(node):
        sticky = node.find(class_=re.compile(r"table_title_col"))
        sec = norm_space(sticky.get_text(" ", strip=True) if sticky else node.get_text(" ", strip=True))
        # ✅ セクション名を簡潔化
        sec = re.sub(r"\s*标配.*$", "", sec)
        sec = re.sub(r"\s*选配.*$", "", sec)
        sec = re.sub(r"\s*- 无.*$", "", sec)
        return norm_space(sec)

    def is_data_row(node):
        cls = " ".join(node.get("class", []))
        return "style_row__" in cls

    def cell_value(td):
        is_solid = bool(td.select_one('[class*="style_col_dot_solid__"]'))
        is_outline = bool(td.select_one('[class*="style_col_dot_outline__"]'))
        txt = norm_space(td.get_text(" ", strip=True))
        if is_solid and not is_outline:
            return "●" if txt in ("", "●", "○") else f"● {txt}"
        if is_outline and not is_solid:
            return "○" if txt in ("", "●", "○") else f"○ {txt}"
        return txt if txt else "–"

    records = []
    current_section = ""
    children = [c for c in container.find_all(recursive=False) if getattr(c, "name", None)]

    for ch in children:
        if ch is head:
            continue
        if is_section_title(ch):
            current_section = get_section_from_title(ch)
            continue
        if is_data_row(ch):
            kids = [k for k in ch.find_all(recursive=False) if getattr(k, "name", None)]
            if not kids:
                continue
            left = norm_space(kids[0].get_text(" ", strip=True))
            cells = kids[1:1 + n_models]
            if len(cells) < n_models:
                cells = cells + [soup.new_tag("div")] * (n_models - len(cells))
            elif len(cells) > n_models:
                cells = cells[:n_models]
            vals = [cell_value(td) for td in cells]
            records.append([current_section, left] + vals)

    if not records:
        return None

    header = ["セクション", "項目"] + model_names
    return [header] + records


# --------------------------------
# メイン
# --------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", type=str, required=True, help="Autohome series id (e.g., 6814)")
    ap.add_argument("--outdir", type=str, default="output/autohome", help="Output base dir")
    ap.add_argument("--mobile", action="store_true", help="Use mobile site")
    args = ap.parse_args()

    series = args.series.strip()
    outdir = Path(args.outdir) / series
    outdir.mkdir(parents=True, exist_ok=True)
    url = (MOBILE_URL if args.mobile else PC_URL).format(series=series)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(locale="zh-CN")
        page = context.new_page()

        print(f"Loading: {url}")
        try:
            # ✅ ここだけ追加（リトライ1回）
            try:
                page.goto(url, wait_until="networkidle", timeout=120000)
            except Exception as e:
                print(f"⚠️ Timeout or error at first attempt: {e}. Retrying once...")
                page.goto(url, wait_until="networkidle", timeout=120000)

            html = page.content()
            soup = BeautifulSoup(html, "lxml")
            table = extract_table(soup)
            if not table:
                print(f"No config found for {series}")
                return

            import pandas as pd
            df = pd.DataFrame(table[1:], columns=table[0])
            csv_path = outdir / f"config_{series}.csv"
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            print(f"Saved: {csv_path}")

        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    main()
