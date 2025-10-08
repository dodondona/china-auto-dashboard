#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
translate_brand_model_llm.py

ChatGPT本体と同等の理解力で、中国語の自動車ブランド名・車種名を
グローバル市場での正式名称（または日本語カタカナ表記＋ピンイン）に翻訳するスクリプト。

ルール・優先順位：
  1. 公式グローバルサイト（byd.com / geely.com / etc.）での英名を最優先。
  2. 無ければ Autohome（autohome.com.cn）上のモデルページの英語表記を参照。
  3. さらに無ければ Wikipedia（英語版・中国語版の双方）を参照。
  4. どの情報源でも見つからない場合は LLM が合理的に推論し、
     - 既知のグローバル名がある場合 → そのまま採用
     - ない場合 → 中国語を日本語の漢字で書き、直後に（ピンイン）を付す
       例：宏光 → 宏光（Hongguang）

※キャッシュは毎回削除する前提。
※上記以外のロジック・処理構造には一切変更を加えない。
"""

import os
import json
import csv
import time
import shutil
import openai

# === 設定 ===
openai.api_key = os.getenv("OPENAI_API_KEY")

CACHE_DIR = "cache"
if os.path.exists(CACHE_DIR):
    shutil.rmtree(CACHE_DIR)  # ★追記：毎回キャッシュ削除
os.makedirs(CACHE_DIR, exist_ok=True)

INPUT_CSV = "data/autohome_raw_2025-08_with_brand.csv"
OUTPUT_CSV = "data/autohome_raw_2025-08_with_brand_ja.csv"

# === ChatGPT呼び出し関数 ===
def translate_with_llm(brand, model):
    prompt = f"""
あなたは中国自動車市場とグローバル市場の両方に詳しい翻訳アシスタントです。

以下のルールに従って、中国語の「ブランド名」「車種名」を
日本語のブランド名およびモデル名に変換してください。

### 方針
- ChatGPT本体と同等の理解力で、各モデルのグローバル名称を正確に判断してください。
- 必要に応じてWeb検索を行い、次の順に信頼できる情報源を参照してください：
  1. 公式グローバルサイト（例：byd.com, geely.com, toyota-global.com など）
  2. Autohome（autohome.com.cn）上の該当モデルページ
  3. Wikipedia（英語版・中国語版）
- 検索で得た正式英名がある場合はそれを採用してください。
- グローバル名が無い場合は、中国語を日本語漢字に変換し、
  直後に（ピンイン）を付けてください。
- 英数字や既知の英語名はそのまま残します。

### 書式
brand_ja, model_ja の順で回答してください。

### 入力
ブランド名: {brand}
モデル名: {model}

### 出力例
BYD, 秦（Qin）PLUS
五菱, 宏光（Hongguang）MINIEV
日産, シルフィー（Sylphy）
BYD, 海豹（Haibao）06新能源
"""

    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",  # 元のモデルを維持
            messages=[
                {"role": "system", "content": "あなたは自動車業界専門の翻訳アシスタントです。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print("LLM呼び出しエラー:", e)
        return ""

# === CSV処理 ===
rows = []
with open(INPUT_CSV, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        brand, model = row["brand"], row["model"]
        print(f"翻訳中: {brand} {model} ...")

        translated = translate_with_llm(brand, model)
        if "," in translated:
            parts = [p.strip() for p in translated.split(",", 1)]
            row["brand_ja"] = parts[0]
            row["model_ja"] = parts[1]
        else:
            row["brand_ja"] = translated
            row["model_ja"] = ""
        rows.append(row)
        time.sleep(1)

# === CSV書き出し ===
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print("✅ 翻訳完了:", OUTPUT_CSV)
