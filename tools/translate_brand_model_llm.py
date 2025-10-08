#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
translate_brand_model_llm.py (Claude 3.5 Sonnet対応版)

- ChatGPT本体と同等の理解力で、中国語ブランド名・車種名をグローバル／日本語表記に変換
- キャッシュは毎回削除
- Claude 3.5 Sonnet API使用
"""

import os, csv, json, re, shutil
from anthropic import Anthropic

# === パス設定 ===
CACHE_DIR = "cache"
INPUT_CSV = "data/autohome_raw_2025-08_with_brand.csv"
OUTPUT_CSV = "data/autohome_raw_2025-08_with_brand_ja.csv"

# === Claude APIクライアント ===
client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

# === キャッシュ削除 ===
if os.path.exists(CACHE_DIR):
    shutil.rmtree(CACHE_DIR)
os.makedirs(CACHE_DIR, exist_ok=True)

# === Claude翻訳関数 ===
def translate_with_claude(brand: str, model: str) -> dict:
    """
    Claude 3.5 Sonnetを使ってブランド・車種名を翻訳する
    """
    prompt = f"""
あなたは自動車分野の専門翻訳者です。
以下の中国語ブランド名と車種名を、ChatGPT本体と同等の理解力で、グローバル販売名および日本語表記に翻訳してください。

【ルール】
- ブランド名：
  - 世界共通ブランドは英語（例: 比亚迪→BYD、吉利→Geely、特斯拉→Tesla）
  - 日本ブランドは日本語カタカナ（例: 丰田→トヨタ、日产→日産）
- 車種名：
  - グローバル名が存在すればそれを採用（例: 海豹→Seal、海豚→Dolphin）
  - 存在しない場合は日本語漢字＋（ピンイン）形式（例: 星越→星越（Xingyue））
  - 海洋シリーズ（BYD 海豹・海豚・海狮など）は公式英語名を使用
  - 数字・アルファベットはそのまま残す
  - 不明な場合は原文を保持
- 出力は以下のJSON形式のみ：
{{
  "brand_ja": "...",
  "model_ja": "..."
}}

入力:
ブランド: {brand}
車種: {model}
"""
    try:
        resp = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=400,
            temperature=0,
            messages=[{"role": "user", "content": prompt}]
        )
        text = resp.content[0].text.strip()
        m = re.search(r"\{.*\}", text, re.S)
        if not m:
            raise ValueError("JSON抽出失敗")
        result = json.loads(m.group(0))
        return {
            "brand_ja": result.get("brand_ja", brand),
            "model_ja": result.get("model_ja", model),
        }
    except Exception as e:
        print(f"[Error] {brand} {model}: {e}")
        return {"brand_ja": brand, "model_ja": model}

# === CSV読み込みと翻訳 ===
rows = []
with open(INPUT_CSV, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        brand, model = row["brand"].strip(), row["model"].strip()
        result = translate_with_claude(brand, model)
        row["brand_ja"] = result["brand_ja"]
        row["model_ja"] = result["model_ja"]
        rows.append(row)

# === 出力 ===
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"✅ 翻訳完了: {OUTPUT_CSV}")
