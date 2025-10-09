#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
append_series_url_from_html_llm.py
---------------------------------
AutohomeランキングHTMLをLLMに渡し、
順位・ブランド・車種・台数・URLのズレを「目視整合」で修正するスクリプト。

想定入力: data/autohome_raw_2025-09.csv（rank, model, count, series_url 等を含む）
想定出力: data/autohome_raw_2025-09_with_llmfix.csv

依存:
  pip install openai pandas
  export OPENAI_API_KEY="sk-xxxx"

使い方:
  python tools/append_series_url_from_html_llm.py \
    --input data/autohome_raw_2025-09.csv \
    --html data/autohome_rankpage_2025-09.html \
    --output data/autohome_raw_2025-09_with_llmfix.csv
"""

import os, re, json, argparse
import pandas as pd
from openai import OpenAI

client = OpenAI()

PROMPT_TEMPLATE = """以下は汽车之家(autohome.com.cn)の月間销量ランキングページHTMLの断片です。
HTML構造上のズレで rank, brand, model, series_url が間違っていることがあります。
あなたは人間の目視のように内容を見て、実際の画面上で正しく対応する rank と brand/model/series_url を再構成してください。

出力フォーマットは JSON 配列で、次の形式で返してください：
[
  {"rank": <整数>, "brand": "<ブランド名>", "model": "<車種名>", "count": <整数>, "series_url": "<URL>"}
]

制約:
- HTMLタグをそのまま解析してよいが、順位と車名がズレている場合は意味上正しい行に直すこと。
- 同ブランド内の連番車種を整合させる。
- 「【】」内の文字が車種名である場合、それを優先する。
- 数字（rank）順に並び替えて出力する。

HTML内容:
----------------
{html}
----------------
"""

def fix_with_llm(html_text: str):
    """LLMで修正版JSONを返す"""
    prompt = PROMPT_TEMPLATE.format(html=html_text[:30000])  # 3万文字まで安全
    resp = client.responses.create(
        model="gpt-4o-mini",
        input=prompt,
        temperature=0.2,
    )
    raw_text = resp.output_text.strip()
    try:
        json_text = re.search(r'\[.*\]', raw_text, re.S).group(0)
        data = json.loads(json_text)
        return pd.DataFrame(data)
    except Exception as e:
        print("⚠️ JSON解析失敗:", e)
        print(raw_text)
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="既存CSV（rank, brand, model, countなど）")
    ap.add_argument("--html", required=True, help="ランキングHTMLファイル")
    ap.add_argument("--output", required=True, help="出力CSVパス")
    args = ap.parse_args()

    df_in = pd.read_csv(args.input)
    with open(args.html, "r", encoding="utf-8") as f:
        html_text = f.read()

    print("👁️ LLMによる整合性チェックを実行中...")
    df_llm = fix_with_llm(html_text)

    if df_llm is None or df_llm.empty:
        print("❌ LLM補正に失敗しました。入力HTMLを確認してください。")
        return

    # rankでjoin（LLMがrank順を維持している前提）
    df_out = df_llm.merge(df_in, on="rank", how="left", suffixes=("", "_orig"))

    # 差分比較のための確認列
    df_out["brand_changed"] = df_out["brand"] != df_out["brand_orig"]
    df_out["model_changed"] = df_out["model"] != df_out["model_orig"]

    df_out.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"✅ 修正版CSVを出力しました: {args.output}")
    print(f"📝 修正行数: {sum(df_out['model_changed'])}")

if __name__ == "__main__":
    main()
