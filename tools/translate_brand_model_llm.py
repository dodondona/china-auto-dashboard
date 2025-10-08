#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
translate_brand_model_llm.py
-----------------------------------
中国語の brand / model から、日本語・グローバル統一表記を LLM によって生成するスクリプト。

変更点：
- ChatGPT（gpt-4o-mini）を使用
- 毎回キャッシュ削除（最新の翻訳指示を反映）
- プロンプトを強化し、ChatGPT本体と同等の理解力で
  グローバル販売名 / 日本語表記を判別
-----------------------------------
"""

import os
import csv
import json
import time
import openai

# ==== 設定 ====
INPUT_CSV = "data/autohome_raw_2025-08_with_brand.csv"
OUTPUT_CSV = "data/autohome_raw_2025-08_with_brand_ja.csv"
CACHE_FILE = "cache/brand_model_cache.json"

MODEL_NAME = "gpt-4o-mini"  # GPT-4o系を使用（ChatGPT本体と同等の理解力）

# ==== キャッシュ削除 ====
if os.path.exists(CACHE_FILE):
    print("🧹 キャッシュ削除中...")
    try:
        os.remove(CACHE_FILE)
        print("✅ キャッシュ削除完了")
    except Exception as e:
        print("⚠️ キャッシュ削除失敗:", e)
else:
    print("ℹ️ キャッシュファイルなし")

# ==== プロンプトテンプレート ====
PROMPT_TEMPLATE = """
ChatGPT本体と同等の理解力で、以下の中国語ブランド名と車種名を、
できる限りグローバル販売名または日本語正式表記に統一してください。

参照・優先順位：
1️⃣ メーカー公式のグローバル英語名（BYD, Geely, XPeng, Changan, Great Wall, NIO, SAICなど）
2️⃣ Autohome（汽车之家）の英語版表記
3️⃣ Wikipedia英語・日本語版の車種名
4️⃣ いずれにも存在しない場合のみ、中国語を日本語漢字に変換し、括弧内にピンイン（拼音）を併記してください。
　例：宏光 → 宏光（Hongguang）MINIEV

出力フォーマットとルール：
- ブランドはグローバルブランド表記（例：BYD、Geely、XPeng、トヨタ、ホンダ、日産）
- 車種は以下の形式で統一：
  「<中国語部分（必要なら日本語漢字）>（<ピンインまたは英語公式名>）<派生記号>」
  例：秦PLUS → 秦（Qin）PLUS
       海豹05 DM-i → 海豹（Haibao）05 DM-i
       宏光MINIEV → 宏光（Hongguang）MINIEV
       カムリ、シルフィ、アコードなど既存日本名がある場合はそのまま
- 直訳は使わない（例：「星愿」→“Star Wish”は不可）

出力は以下のJSON形式で：
{
  "brand_ja": "<ブランド>",
  "model_ja": "<モデル>"
}

# 出力例：
入力: 比亚迪, 海豹05 DM-i
出力: { "brand_ja": "BYD", "model_ja": "海豹（Haibao）05 DM-i" }

入力: 日产, 轩逸
出力: { "brand_ja": "日産", "model_ja": "シルフィ（Sylphy）" }

入力: 吉利银河, 星愿
出力: { "brand_ja": "Geely Galaxy", "model_ja": "星願（Xingyuan）" }

入力: 丰田, 卡罗拉锐放
出力: { "brand_ja": "トヨタ", "model_ja": "カローラクロス（Corolla Cross）" }
"""

# ==== OpenAIクライアント ====
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def translate_with_llm(brand, model):
    """LLMでブランド・モデルを翻訳"""
    prompt = PROMPT_TEMPLATE + f"\n\n入力: {brand}, {model}\n出力:"
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": "あなたは自動車業界の翻訳専門家です。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=400
        )
        text = response.choices[0].message.content.strip()
        if text.startswith("```"):
            text = text.strip("`").replace("json", "").strip()
        result = json.loads(text)
        return result.get("brand_ja", ""), result.get("model_ja", "")
    except Exception as e:
        print(f"⚠️ 翻訳失敗: {brand} {model} ({e})")
        return "", ""

# ==== 入出力処理 ====
output_rows = []
with open(INPUT_CSV, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        brand, model = row["brand"], row["model"]
        brand_ja, model_ja = translate_with_llm(brand, model)
        row["brand_ja"], row["model_ja"] = brand_ja, model_ja
        output_rows.append(row)
        print(f"✅ {brand} {model} → {brand_ja} / {model_ja}")
        time.sleep(1.2)  # API制限対策

fieldnames = list(output_rows[0].keys())
with open(OUTPUT_CSV, "w", newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(output_rows)

print(f"\n🎯 出力完了: {OUTPUT_CSV}")
