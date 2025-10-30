#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, os, json, re
from pathlib import Path
from openai import OpenAI

"""
Usage:
  python tools/koubei_storywriter.py <vehicle_id> [--pros N] [--cons N] [--quotes N] [--style style_name]
"""

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
VEHICLE_ID = sys.argv[1].strip() if len(sys.argv) >= 2 else ""
OUTDIR = Path(__file__).resolve().parent.parent
CSV_PATH = OUTDIR / f"autohome_reviews_{VEHICLE_ID}.csv"
TXT_PATH = OUTDIR / f"autohome_reviews_{VEHICLE_ID}_story.txt"

# === モデルとパラメータ設定 ===
MODEL = "gpt-4.1-mini"
MAX_TOKENS = 500
TEMPERATURE = 0.7

def make_prompt(csv_path: Path) -> str:
    import pandas as pd
    if not csv_path.exists():
        return "口コミデータがありません。"
    df = pd.read_csv(csv_path)
    pros = " / ".join(df.get("pros_ja", [])[:10].dropna().astype(str).tolist())
    cons = " / ".join(df.get("cons_ja", [])[:10].dropna().astype(str).tolist())
    return f"この車の良い点: {pros}\n悪い点: {cons}\nこの情報をもとに自然な物語風にまとめてください。"

def generate_story(prompt: str) -> str:
    completion = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": "あなたは自動車レビュー記事を執筆する日本語ライターです。"},
            {"role": "user", "content": (
                "以下の情報をもとに、車の印象や特徴を自然で温かみのあるストーリー調にまとめてください。\n"
                "・最大400文字程度。\n"
                "・誇張せず、客観性を保ちつつも感情の流れを含めてください。\n"
                "・見出しや箇条書きは不要です。\n"
                "・人が語るような文体にしてください。\n\n"
                f"{prompt}"
            )}
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
