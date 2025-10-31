#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
koubei_storywriter.py
Autohome review summary and story generator.
"""
import os, sys, json, re, random
import pandas as pd
from pathlib import Path
from openai import OpenAI

# ====== 設定 ======
DEFAULT_MODEL = "gpt-4o-mini"
MAX_TOKENS = 2000

# ====== 関数 ======
def load_reviews(series_id: str):
    csv_path = detect_csv(series_id)
    df = pd.read_csv(csv_path)
    if "pros_ja" not in df.columns:
        raise ValueError("CSVに pros_ja 列がありません。")
    return df

def detect_csv(series_id: str) -> str:
    candidates = [
        f"autohome_reviews_{series_id}.csv",
        f"output/autohome/{series_id}/autohome_reviews_{series_id}.csv",
        f"output/autohome/{series_id}/reviews_{series_id}.csv",
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    raise FileNotFoundError(f"autohome_reviews_{series_id}.csv が見つかりません。")

def top_k(series, k: int):
    """上位k件（最大k件）。要素数が少ない場合はそのまま返す。"""
    items = [s for s in series if isinstance(s, str) and s.strip()]
    return items[:k] if len(items) >= k else items

def choose_representatives(texts, max_each=2):
    """代表コメントを最大max_each件だけ抽出。"""
    clean = [t.strip() for t in texts if isinstance(t, str) and t.strip()]
    if len(clean) <= max_each:
        return clean
    return random.sample(clean, max_each)

def summarize_with_llm(model: str, prompt: str) -> str:
    client = OpenAI()
    res = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=MAX_TOKENS,
        temperature=0.7,
    )
    return res.choices[0].message.content.strip()

def write_output(series_id, story_text: str):
    txt = f"autohome_reviews_{series_id}_story.txt"
    md = f"autohome_reviews_{series_id}_story.md"
    with open(txt, "w", encoding="utf-8") as f:
        f.write(story_text)
    with open(md, "w", encoding="utf-8") as f:
        f.write(story_text)
    print(f"[done] wrote: {txt}, {md}")

# ====== メイン処理 ======
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate Koubei story summary")
    parser.add_argument("series_id", help="Autohome series ID (e.g. 7806)")
    parser.add_argument("--pros", type=int, default=4, help="ポジティブ要点の最大数")
    parser.add_argument("--cons", type=int, default=4, help="ネガティブ要点の最大数")
    parser.add_argument("--quotes", type=int, default=2, help="各項目あたり代表コメントの最大数")
    parser.add_argument("--style", type=str, default="executive", help="出力スタイル（executive/friendlyなど）")
    args = parser.parse_args()

    series_id = args.series_id
    df = load_reviews(series_id)

    # pros/cons列を確認
    pros_all = [str(x) for x in df["pros_ja"].dropna().tolist() if x.strip()]
    cons_all = []
    if "cons_ja" in df.columns:
        cons_all = [str(x) for x in df["cons_ja"].dropna().tolist() if x.strip()]

    top_pros = top_k(pros_all, args.pros)
    top_cons = top_k(cons_all, args.cons)

    # 引用文
    pros_quotes = choose_representatives(pros_all, max_each=args.quotes)
    cons_quotes = choose_representatives(cons_all, max_each=args.quotes)

    # ====== 要約プロンプト ======
    prompt = f"""
あなたは自動車レビューの要約専門家です。
以下のポジティブ・ネガティブ要素を基に、{args.style}スタイルでまとめてください。
ポジティブ・ネガティブともに、存在する件数のみを扱って構いません（最大 {args.pros} / {args.cons} 件）。

【ポジティブ要点】:
{json.dumps(top_pros, ensure_ascii=False, indent=2)}

【ネガティブ要点】:
{json.dumps(top_cons, ensure_ascii=False, indent=2)}

【代表コメント（ポジティブ）】:
{json.dumps(pros_quotes, ensure_ascii=False, indent=2)}

【代表コメント（ネガティブ）】:
{json.dumps(cons_quotes, ensure_ascii=False, indent=2)}
"""

    try:
        story = summarize_with_llm(DEFAULT_MODEL, prompt)
    except Exception as e:
        print(f"[warn] LLM要約に失敗しました: {e}")
        story = "要約生成に失敗しました。"

    write_output(series_id, story)


if __name__ == "__main__":
    main()
