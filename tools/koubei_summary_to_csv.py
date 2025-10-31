#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, re, json, zipfile
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup

"""
概要:
  autohome_reviews_<series_id>.zip 内の JSON または HTML を解析し、
  結果を autohome_reviews_<series_id>.csv に出力する。

変更点:
  - JSONが list 形式の場合（[ {...}, {...} ]）でも処理できるように修正。
  - data = json.load(f) の直後に list → dict 変換ガードを追加。
"""

def parse_json_from_zip(zip_path: Path):
    """ZIP内の .json ファイルを順に読み込み DataFrame を作成"""
    rows = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if not name.endswith(".json"):
                continue
            try:
                with zf.open(name) as f:
                    data = json.load(f)
                    # ✅ list形式の場合は先頭要素を使う
                    if isinstance(data, list) and len(data) > 0:
                        data = data[0]
                    if not isinstance(data, dict):
                        continue
                    rid = data.get("id") or Path(name).stem
                    title = data.get("title", "")
                    pros = "、".join(data.get("pros", []))
                    cons = "、".join(data.get("cons", []))
                    rows.append({
                        "id": rid,
                        "title": title,
                        "pros": pros,
                        "cons": cons
                    })
            except Exception as e:
                print(f"[warn] {name}: {e}")
                continue
    return pd.DataFrame(rows)


def parse_html_from_zip(zip_path: Path):
    """ZIP内の .html ファイルからデータ抽出（旧フォールバック）"""
    rows = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if not name.endswith(".html"):
                continue
            try:
                with zf.open(name) as f:
                    html = f.read().decode("utf-8", errors="ignore")
                soup = BeautifulSoup(html, "lxml")
                rid = Path(name).stem
                title_el = soup.select_one(".title")
                pros_el = soup.select(".tag-pros li")
                cons_el = soup.select(".tag-cons li")
                rows.append({
                    "id": rid,
                    "title": title_el.text.strip() if title_el else "",
                    "pros": "、".join([x.text.strip() for x in pros_el]),
                    "cons": "、".join([x.text.strip() for x in cons_el]),
                })
            except Exception as e:
                print(f"[warn] {name}: {e}")
                continue
    return pd.DataFrame(rows)


def main(zip_file: str):
    zip_path = Path(zip_file)
    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP not found: {zip_file}")

    df = parse_json_from_zip(zip_path)
    if df.empty:
        print("⚠️ No valid JSON data found in zip, trying HTML fallback")
        df = parse_html_from_zip(zip_path)

    if df.empty:
        raise RuntimeError("No valid JSON data found in zip")

    out_csv = f"autohome_reviews_{zip_path.stem.replace('autohome_reviews_', '')}.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"✅ CSV written: {out_csv} ({len(df)} rows)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tools/koubei_summary_to_csv.py <zip_path>")
        sys.exit(1)
    main(sys.argv[1])
