#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
translate_brand_model_llm.py
ブランド・車種名を LLM でグローバル名（英語 or 日本語漢字＋ピンイン）に翻訳する。

変更点：
- ChatGPT本体と同等の理解力で、公式→Autohome→Wikipedia→Fallbackの順に推定。
- キャッシュは毎回削除。
"""

import os
import csv
import json
import time
from openai import OpenAI

# ======= 設定 =======
INPUT_CSV = "data/autohome_raw_2025-08.csv"
OUTPUT_CSV = "data/autohome_raw_2025-08_with_brand_ja.csv"
CACHE_FILE = "cache/translate_cache.json"
MODEL = "gpt-4o-mini"
SLEEP_SEC = 2.0
# ====================

client = OpenAI()

# キャッシュ削除
if os.path.exists(CACHE_FILE):
    os.remove(CACHE_FILE)
    print("🗑 キャッシュを削除しました。")

# 翻訳プロンプト
PROMPT_TEMPLATE = """
ChatGPT本体と同等の理解力で、以下の中国語ブランド名と車種名を、
できる限りグローバル販売名（英語）または日本語での正式名称に翻訳してください。

優先順位：
1️⃣ メーカー公式の英語サイト（BYD, Geely, Changan, XPeng, NIO, Great Wall, SAICなど）に記載の英語名を最優先。
2️⃣ 次に、Autohome（汽车之家）またはGlobal Autohomeに記載の英語表記を参照。
3️⃣ それでも存在しない場合は、Wikipedia英語版・日本語版の記載を参考。
4️⃣ いずれにも存在しない場合は、中国語名を日本語漢字に変換し、括弧内にピンインを併記してください。
   例：宏光 → 宏光（Hongguang）MINIEV

ブランド名はグローバルブランド表記（例：比亚迪→BYD、日产→日産、丰田→トヨタ、五菱汽车→Wuling）。
車種名は実際の輸出モデル名を優先し、略語では返さないでください。

出力は以下のJSON形式のみで返してください：
{
  "brand_ja": "<ブランドの日本語または英語表記>",
  "model_ja": "<車種の翻訳結果>"
}
"""

def translate_with_llm(brand, model):
    """LLMに問い合わせてブランド・車種を翻訳"""
    prompt = f"{PROMPT_TEMPLATE}\n\n対象:\nブランド: {brand}\n車種: {model}\n"
    try:
        res = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "system", "content": "You are an automotive naming expert."},
                      {"role": "user", "content": prompt}],
            temperature=0.2,
        )
        text = res.choices[0].message.content.strip()
        if "{" in text:
            data = json.loads(text[text.index("{"): text.rindex("}") + 1])
            return data.get("brand_ja", ""), data.get("model_ja", "")
        else:
            return "", text
    except Exception as e:
        print("⚠️ 翻訳エラー:", e)
        return "", ""

def main():
    rows_out = []
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, 1):
            brand, model = row["brand"], row["model"]
            print(f"[{i}] 翻訳中: {brand} / {model}")
            brand_ja, model_ja = translate_with_llm(brand, model)
            row["brand_ja"] = brand_ja
            row["model_ja"] = model_ja
            rows_out.append(row)
            time.sleep(SLEEP_SEC)

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows_out[0].keys())
        writer.writeheader()
        writer.writerows(rows_out)
    print("✅ 出力完了:", OUTPUT_CSV)

if __name__ == "__main__":
    main()
