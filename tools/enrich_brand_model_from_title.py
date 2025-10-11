#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
series_url へアクセスして <title> を取得し、そこから brand / model を抽出。
抽出した brand/model/title_raw を CSV に追記/上書きする。

使い方:
python tools/enrich_brand_model_from_title.py \
  --input data/autohome_raw_2025-08_with_series.csv \
  --output data/autohome_raw_2025-08_with_brand.csv \
  --series-url-col series_url \
  --brand-col brand --model-col model --title-col title_raw

- キャッシュ無し（毎回HTTPアクセス）
- 取得失敗時は既存値を温存
"""

import argparse
import csv
import re
import time
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

TITLE_PAT = re.compile(r"【(.+?)】(.+?)_(.+?)报价_.+?汽车之家")

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--series-url-col", default="series_url")
    p.add_argument("--brand-col", default="brand")
    p.add_argument("--model-col", default="model")
    p.add_argument("--title-col", default="title_raw")
    p.add_argument("--sleep", type=float, default=0.6)  # サイト負荷配慮
    return p.parse_args()

def fetch_title(url: str, timeout: int = 20) -> Optional[str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        title = soup.title.string if soup.title else None
        if title:
            return title.strip()
    except Exception:
        return None
    return None

def parse_brand_model_from_title(title: str) -> Optional[tuple]:
    """
    例: 【星愿】吉利银河_星愿报价_星愿图片_汽车之家
         ^model   ^brand
    """
    m = TITLE_PAT.search(title or "")
    if not m:
        return None
    model_cn = m.group(1).strip()
    brand_cn = m.group(2).strip()
    return brand_cn, model_cn

def main():
    args = _parse_args()
    df = pd.read_csv(args.input)
    s_col = args.series_url_col
    b_col = args.brand_col
    m_col = args.model_col
    t_col = args.title_col

    if s_col not in df.columns:
        raise KeyError(f"'{s_col}' column not found in {args.input}")

    # 出力列を確保
    if t_col not in df.columns:
        df[t_col] = pd.NA

    for idx, row in df.iterrows():
        url = str(row.get(s_col) or "").strip()
        if not url or url == "NA":
            continue

        title = fetch_title(url)
        if title:
            df.at[idx, t_col] = title
            parsed = parse_brand_model_from_title(title)
            if parsed:
                brand_cn, model_cn = parsed
                # CSV既存値が空orNaNなら上書き（既存が信頼できるなら温存）
                if pd.isna(row.get(b_col)) or str(row.get(b_col)).strip() == "":
                    df.at[idx, b_col] = brand_cn
                if pd.isna(row.get(m_col)) or str(row.get(m_col)).strip() == "":
                    df.at[idx, m_col] = model_cn
        time.sleep(args.sleep)

    df.to_csv(args.output, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"► {args.input} -> {args.output}")

if __name__ == "__main__":
    main()
