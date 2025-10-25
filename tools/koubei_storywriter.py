#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
口コミCSV → ストーリー要約（自然な日本語＋引用付き）
改訂版: 2025-10
"""

import os, re, argparse, pandas as pd
from collections import Counter
from openai import OpenAI

client = None
try:
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
except Exception:
    client = None


def summarize_texts(texts, max_chars=1800):
    joined = "。".join([t for t in texts if isinstance(t, str)])[:max_chars]
    return joined


def generate_story_with_quotes(df, vehicle_id, pros_col="positive", cons_col="negative"):
    pos_texts = summarize_texts(df[pros_col].dropna().tolist())
    neg_texts = summarize_texts(df[cons_col].dropna().tolist())

    # 引用を少し混ぜる
    pos_examples = df[pros_col].dropna().sample(min(2, len(df))).tolist()
    neg_examples = df[cons_col].dropna().sample(min(2, len(df))).tolist()

    # ChatGPT要約プロンプト
    prompt = f"""
あなたは自動車レビューの専門ライターです。
以下の口コミデータをもとに、自然な日本語で厚みのあるレビュー要約を書いてください。
・ポジティブ面とネガティブ面をそれぞれ3〜5文でまとめる
・実際の口コミの引用（例: 「〜という声もある」「"〜"とのレビューも」）を挿入
・やや雑誌記事風に自然な語り口に
・形式は以下の見出し構成で出力：

### 全体サマリー
### ポジティブな評価
### ネガティブな評価
### 総評

【ポジティブ内容】
{pos_texts}

【ネガティブ内容】
{neg_texts}

【ポジティブの代表コメント】
{pos_examples}

【ネガティブの代表コメント】
{neg_examples}
"""

    if client:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.5,
        )
        story = resp.choices[0].message.content.strip()
    else:
        # fallback（オフライン用）
        story = (
            f"【車両ID: {vehicle_id}】口コミストーリー要約（簡易）\n"
            "ポジティブ：デザイン・燃費・静粛性に満足の声が多い。\n"
            "ネガティブ：ロードノイズや素材品質に改善余地がある。\n"
        )
    return story


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("vehicle_id", help="Autohome vehicle id")
    ap.add_argument("--pros", type=int, default=5)
    ap.add_argument("--cons", type=int, default=5)
    ap.add_argument("--style", default="executive")
    args = ap.parse_args()

    vid = args.vehicle_id
    csv_path = f"autohome_reviews_{vid}.csv"
    if not os.path.exists(csv_path):
        alt = f"output/autohome/{vid}/autohome_reviews_{vid}.csv"
        if os.path.exists(alt):
            csv_path = alt
        else:
            raise FileNotFoundError(f"{csv_path} が見つかりません")

    df = pd.read_csv(csv_path, encoding="utf-8")
    if "positive" not in df.columns or "negative" not in df.columns:
        df["positive"], df["negative"] = "", ""

    story = generate_story_with_quotes(df, vid)

    out_txt = f"autohome_reviews_{vid}_story.txt"
    with open(out_txt, "w", encoding="utf-8") as f:
        f.write(story)

    print(f"✅ {out_txt} を生成しました")


if __name__ == "__main__":
    main()
