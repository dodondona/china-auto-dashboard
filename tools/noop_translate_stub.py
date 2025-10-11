#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
noop_translate_stub.py

CSV に brand_ja / model_ja の空列があるかだけ確認し、
なければ空列を追加して上書き保存するだけのスタブ。
（翻訳は行いません）
"""
import argparse
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.csv, dtype=str).fillna("")
    for col in ["brand_ja", "model_ja"]:
        if col not in df.columns:
            df[col] = ""
    df.to_csv(args.csv, index=False, encoding="utf-8-sig")
    print(f"[ok] verified columns -> {args.csv}")

if __name__ == "__main__":
    main()
