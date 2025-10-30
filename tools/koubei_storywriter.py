#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, os, json, re
from pathlib import Path
from openai import OpenAI

"""
Usage:
  python tools/koubei_storywriter.py <vehicle_id>
"""

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
VEHICLE_ID = sys.argv[1].strip() if len(sys.argv) >= 2 else ""
OUTDIR = Path(__file__).resolve().parent.parent
CSV_PATH = OUTDIR / f"autohome_reviews_{VEHICLE_ID}.csv"
TXT_PATH = OUTDIR / f"autohome_reviews_{VEHICLE_ID}_story.txt"

MODEL = "gpt-4.1-mini"
MAX_TOKENS = 600
TEMPERATURE = 0.8  # 若干上げて自然な表現へ

def make_prompt(csv_path: Path) -> str:
    import pandas as pd
    if not csv_path.exists():
        return "口コミデータがありません。"
    df = pd.read_csv(csv_path)
    pros = " / ".join(df.get("pros_ja", [])[:10].dropna().astype(str).tolist())
    cons = " / ".join(df.get("cons_ja", [])[:10].dropna().astype(str).tolist())
    return (
        f"【良い点】{pros}\n"
        f"【悪い点】{cons}\n\n"
        "上記の内容をもとに、この車の魅力・印象・特徴を自然な日本語で描写してください。"
        "レビューを要約するのではなく、“体験を語るように”ストーリー調でまとめてください。"
        "段落を分け、導入→走行感覚→内装→総評の流れを意識してください。"
        "誇張や広告的表現は禁止。感情は控えめに、現実感のある語り口でお願いします。"
    )

def generate_story(prompt: str) -> str:
    completion = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": "あなたは自動車レビュー記事を執筆する日本語ライターです。"},
            {"role": "user", "content": prompt}
        ],
        max_tokens=MAX_TOKENS,
        temperature=TEMPERATURE,
    )
    return completion.choices[0].message.content.strip()

def main():
    if not VEHICLE_ID:
        print("Vehicle ID required")
        return
    prompt = make_prompt(CSV_PATH)
    story = generate_story(prompt)
    TXT_PATH.write_text(story, encoding="utf-8")
    print(f"✅ Story saved to: {TXT_PATH}")

if __name__ == "__main__":
    main()
