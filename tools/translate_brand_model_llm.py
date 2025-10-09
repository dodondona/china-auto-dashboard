#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, sys, pandas as pd
from typing import Dict, Tuple

# Claude
import anthropic

def translate_pair(client, model: str, brand: str, model_name: str) -> Tuple[str, str]:
    prompt = f"""あなたは自動車のブランド・車種名の翻訳者です。
入力の中国語のブランド・車種を、日本語向け表記に変換してください。
ルール:
- ブランドは公式のカタカナ or 既存の日本語社名（例: 比亚迪→BYD、日产→日産、丰田→トヨタ、吉利→Geely）
- 車種は「漢字（Pinyin/英名）」の括弧付き、または公式グローバル名があればそれを優先（例: 秦PLUS→秦（Qin）PLUS、朗逸→Lavida、卡罗拉锐放→カローラクロス）
- 余計な記号や接尾辞は付けない

出力はJSONで:
{{"brand_ja": "...", "model_ja": "..."}} 

ブランド="{brand}" 車種="{model_name}"
"""
    resp = client.messages.create(
        model=model,
        max_tokens=200,
        messages=[{"role":"user","content":prompt}]
    )
    txt = resp.content[0].text.strip()
    # 極小パーサ（JSON以外は素直にそのままフォールバック）
    out_brand, out_model = "", ""
    try:
        import json
        j = json.loads(txt)
        out_brand = (j.get("brand_ja") or "").strip()
        out_model = (j.get("model_ja") or "").strip()
    except Exception:
        # 例外時は元文
        out_brand, out_model = brand, model_name
    return out_brand or brand, out_model or model_name

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--provider", choices=["anthropic"], default="anthropic")
    ap.add_argument("--model", default="claude-3-5-sonnet-20241022")
    args = ap.parse_args()

    if not os.path.exists(args.input) or os.path.getsize(args.input) == 0:
        # 入力が存在しない/空なら、空ヘッダのみで出力して成功終了
        cols = ["rank_seq","rank","brand","model","count","series_url","brand_conf","series_conf","title_raw","brand_ja","model_ja"]
        pd.DataFrame(columns=cols).to_csv(args.output, index=False, encoding="utf-8")
        print("Input CSV missing or empty. Wrote header-only output and skipped translation.")
        return

    df = pd.read_csv(args.input, dtype=str, encoding="utf-8")
    # ヘッダのみ（行0）ならそのまま書き出し
    if df.shape[0] == 0:
        df["brand_ja"] = []
        df["model_ja"] = []
        df.to_csv(args.output, index=False, encoding="utf-8")
        print("No rows to translate. Output header only.")
        return

    client = anthropic.Client(api_key=os.environ.get("ANTHROPIC_API_KEY",""))

    brand_ja, model_ja = [], []
    for _, row in df.iterrows():
        b = (row.get("brand") or "").strip()
        m = (row.get("model") or "").strip()
        try:
            jb, jm = translate_pair(client, args.model, b, m)
        except Exception as e:
            print(f"Error: {b} {m}: {e}")
            jb, jm = b, m
        brand_ja.append(jb)
        model_ja.append(jm)

    df["brand_ja"] = brand_ja
    df["model_ja"] = model_ja
    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"Saved: {args.output}")

if __name__ == "__main__":
    main()
