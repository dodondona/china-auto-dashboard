# tools/autohome_config_to_csv.py
# -*- coding: utf-8 -*-
"""
Autohome 参数配置ページ（https://www.autohome.com.cn/config/series/<id>.html）
HTML埋め込みJSON(window.CONFIG)を解析してCSV出力する。
不可視文字（\xa0, \u200b, \ufeff）による欠落対策を追加。
"""

import os
import re
import sys
import json
import csv
import pathlib
import requests
import pandas as pd

def clean_text(s):
    """不可視文字・全角空白の除去"""
    if not isinstance(s, str):
        return s
    return (
        s.replace("\xa0", "")
         .replace("\u200b", "")
         .replace("\ufeff", "")
         .replace("\u3000", "")
         .strip()
    )

def extract_config_json(html):
    """window.CONFIG JSONを抽出"""
    m = re.search(r'window\.CONFIG\s*=\s*(\{.*?\});', html, re.S)
    if not m:
        raise ValueError("window.CONFIG not found")
    js = m.group(1)
    return json.loads(js)

def ensure_dir(p: pathlib.Path):
    p.mkdir(parents=True, exist_ok=True)

def dump_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

def parse_config_data(cfg):
    """CONFIG JSON から仕様比較表を行列化"""
    # JSON構造例: cfg['result']['paramtypeitems']
    param_groups = cfg.get("result", {}).get("paramtypeitems", [])
    header = []
    tables = []
    for g in param_groups:
        group_name = g.get("name")
        for pitem in g.get("paramitems", []):
            name = clean_text(pitem.get("name"))
            vals = [clean_text(v.get("value")) for v in pitem.get("valueitems", [])]
            row = [group_name, name] + vals
            tables.append(row)
    return tables

def main():
    urls = [u for u in sys.argv[1:] if u.startswith("http")]
    if not urls:
        urls = ["https://www.autohome.com.cn/config/series/7806.html"]

    for url in urls:
        print(f"Loading: {url}")
        sid_match = re.search(r"/series/(\d+)", url)
        sid = sid_match.group(1) if sid_match else "series"
        html = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}).text
        cfg = extract_config_json(html)
        rows = parse_config_data(cfg)

        out_dir = pathlib.Path("output") / "autohome" / sid
        ensure_dir(out_dir)
        out_path = out_dir / f"config_{sid}.csv"
        dump_csv(out_path, rows)
        print(f"✅ Saved: {out_path} ({len(rows)} rows)")

if __name__ == "__main__":
    main()
