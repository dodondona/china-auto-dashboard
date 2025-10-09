#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
translate_brand_model_llm.py
title_raw から brand / model / 日本語表記を LLM で補完。
Claude / GPT 双方対応。
"""

import os, sys, time, argparse, pandas as pd
from anthropic import Anthropic
from openai import OpenAI

def safe_read_csv(path, retries=3, delay=1.0):
    """ファイル存在・空データチェックを含む安全読込"""
    for i in range(retries):
        if not os.path.exists(path):
            print(f"[ERROR] Input file not found: {path}")
            time.sleep(delay)
            continue
        size = os.path.getsize(path)
        if size == 0:
            print(f"[WARN] File exists but empty ({size} bytes), retrying ({i+1}/{retries})...")
            time.sleep(delay)
            continue
        try:
            df = pd.read_csv(path)
            if df.empty or len(df.columns) == 0:
                print(f"[WARN] No data found in file, retrying ({i+1}/{retries})...")
                time.sleep(delay)
                continue
            print(f"[INFO] Loaded CSV ({len(df)} rows, {len(df.columns)} cols)")
            return df
        except pd.errors.EmptyDataError:
            print(f"[WARN] EmptyDataError while reading, retrying ({i+1}/{retries})...")
            time.sleep(delay)
            continue

    print(f"[FATAL] Failed to read valid CSV after {retries} attempts: {path}")
    sys.exit(1)

def llm_extract(text, provider, model):
    if not text.strip():
        return ("", "")
    prompt = f"""
次の車種タイトルから、ブランド名（brand）とモデル名（model）を抽出してください。
出力はJSON形式で：
{{"brand": "xxx", "model": "yyy"}}

タイトル: 「{text}」
"""
    if provider == "anthropic":
        client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        msg = client.messages.create(
            model=model,
            max_tokens=100,
            messages=[{"role": "user", "content": prompt}],
        )
        out = msg.content[0].text.strip()
    else:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        msg = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
        )
        out = msg.choices[0].message.content.strip()

    import json
    try:
        data = json.loads(out)
        return (data.get("brand", ""), data.get("model", ""))
    except Exception:
        return ("", "")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--provider", choices=["anthropic", "openai"], default="anthropic")
    ap.add_argument("--model", default="claude-3-5-sonnet-latest")
    args = ap.parse_args()

    df = safe_read_csv(args.input)

    # brand/model補完
    results = []
    for _, row in df.iterrows():
        title = str(row.get("title_raw", "") or "")
        brand, model = llm_extract(title, args.provider, args.model)
        row["brand_ja"] = brand
        row["model_ja"] = model
        results.append(row)

    out_df = pd.DataFrame(results)
    out_df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"Saved: {args.output}")

if __name__ == "__main__":
    main()
