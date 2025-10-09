#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, time
import pandas as pd
from anthropic import Anthropic, APIStatusError

PROMPT = """あなたは自動車の車名翻訳アシスタントです。
出力はCSVの2列: brand_ja, model_ja のみ。ルールは厳守：
- 既知のグローバル英名はそれを優先（例: Tesla Model Y, VW Lavida, BYD Dolphin）。
- 中国ブランド名は基本ローマ字化せず中国語表記。日本ブランド名は日本語表記（トヨタ/日産/ホンダ 等）。
- BYD「海洋」シリーズは英名（Dolphin/Seal/Seagull/Sea Lion）。数字・接尾辞は半角（Seal 06, Seal 05 DM-i）。
- Qin/Song/Yuan など王朝シリーズは「漢字（Qin）」形式に接尾辞（秦（Qin）PLUS 等）。
- VW ブランドは「フォルクスワーゲン」。
- 余分な説明は出さない。CSVの2セルのみ。
入力:
brand="{brand}"
model="{model}"
"""

def ask_claude(client: Anthropic, model_id: str, brand: str, model: str):
    resp = client.messages.create(
        model=model_id,
        max_tokens=64,
        temperature=0,
        messages=[{"role":"user","content":PROMPT.format(brand=brand, model=model)}],
    )
    text = resp.content[0].text.strip()
    if "\n" in text:
        text = text.splitlines()[-1]
    parts = [p.strip() for p in text.split(",")]
    if len(parts) < 2:
        parts += [""]
    return parts[0], parts[1]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--provider", choices=["anthropic"], default="anthropic")
    ap.add_argument("--model", default="claude-3-5-sonnet-latest")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

    brand_ja_list, model_ja_list = [], []
    for _, row in df.iterrows():
        b, m = str(row.get("brand","")), str(row.get("model",""))
        model_id = args.model
        for attempt in range(2):
            try:
                bj, mj = ask_claude(client, model_id, b, m)
                break
            except APIStatusError as e:
                # モデルID 404 などは latest にフォールバック
                if getattr(e, "status_code", 0) == 404 and model_id != "claude-3-5-sonnet-latest":
                    model_id = "claude-3-5-sonnet-latest"
                    time.sleep(1)
                    continue
                raise
        brand_ja_list.append(bj)
        model_ja_list.append(mj)

    df["brand_ja"] = brand_ja_list
    df["model_ja"] = model_ja_list
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"Saved: {args.output}")

if __name__ == "__main__":
    main()
