#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, sys, pandas as pd
from typing import Tuple
import anthropic

def translate_pair(client, model: str, brand: str, model_name: str) -> Tuple[str, str]:
    prompt = f"""あなたは自動車のブランド・車種名の翻訳者です。
入力の中国語のブランド・車種を、日本語向け表記に変換してください。
ルール:
- ブランドは公式のカタカナ or 既存の日本語社名（例: 比亚迪→BYD、日产→日産、丰田→トヨタ、吉利→Geely）
- 車種は「漢字（Pinyin/英名）」の括弧付き、または公式グローバル名があればそれを優先
出力はJSONで: {{"brand_ja": "...", "model_ja": "..."}}
ブランド="{brand}" 車種="{model_name}"
"""
    resp = client.messages.create(
        model=model,
        max_tokens=200,
        messages=[{"role":"user","content":prompt}]
    )
    txt = resp.content[0].text.strip()
    try:
        import json
        j = json.loads(txt)
        return j.get("brand_ja", brand).strip(), j.get("model_ja", model_name).strip()
    except Exception:
        return brand, model_name

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--provider", choices=["anthropic"], default="anthropic")
    ap.add_argument("--model", default="claude-3-5-sonnet-20241022")
    args = ap.parse_args()

    if not os.path.exists(args.input) or os.path.getsize(args.input) == 0:
        cols = ["rank_seq","rank","brand","model","count","series_url","brand_conf","series_conf","title_raw","brand_ja","model_ja"]
        pd.DataFrame(columns=cols).to_csv(args.output, index=False, encoding="utf-8")
        print("Input CSV missing or empty. Wrote header-only output and skipped translation.")
        return

    # すべて文字列に変換
    df = pd.read_csv(args.input, dtype=str, encoding="utf-8").fillna("")
    if df.empty:
        df["brand_ja"], df["model_ja"] = [], []
        df.to_csv(args.output, index=False, encoding="utf-8")
        print("No rows to translate.")
        return

    client = anthropic.Client(api_key=os.environ.get("ANTHROPIC_API_KEY",""))
    brand_ja, model_ja = [], []

    for _, row in df.iterrows():
        b = str(row.get("brand", "") or "").strip()
        m = str(row.get("model", "") or "").strip()
        try:
            jb, jm = translate_pair(client, args.model, b, m)
        except Exception as e:
            print(f"Error translating {b}/{m}: {e}")
            jb, jm = b, m
        brand_ja.append(jb)
        model_ja.append(jm)

    df["brand_ja"], df["model_ja"] = brand_ja, model_ja
    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"Saved translated: {args.output}")

if __name__ == "__main__":
    main()
