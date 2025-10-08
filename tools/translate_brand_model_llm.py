#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
translate_brand_model_llm.py

・既存ロジックは維持
・毎回キャッシュ削除
・LLMで翻訳 → 最後に最小限の正規化（中国名/グローバル名のゆらぎ補正）
"""

import os
import csv
import time
import shutil
import re
import openai

openai.api_key = os.getenv("OPENAI_API_KEY")

CACHE_DIR = "cache"
if os.path.exists(CACHE_DIR):
    shutil.rmtree(CACHE_DIR)  # キャッシュ逐一削除
os.makedirs(CACHE_DIR, exist_ok=True)

INPUT_CSV  = "data/autohome_raw_2025-08_with_brand.csv"
OUTPUT_CSV = "data/autohome_raw_2025-08_with_brand_ja.csv"

# ---------------- LLM 呼び出し ----------------
def translate_with_llm(brand, model):
    prompt = f"""
あなたは自動車業界専門の翻訳アシスタントです。
以下のルールに従い、中国語の「ブランド」「モデル」を日本語/グローバル名へ整形します。

【参照優先度】
1) 公式グローバルサイト　2) Autohome　3) Wikipedia
見つかった正式英名を優先。ない場合のみ、中国語→日本語漢字＋（ピンイン）。

【書式規則】
- brand_ja, model_ja の順で出力（カンマ区切り）
- 既知の英数表記はそのまま
- 中国語を残す場合は「漢字（Pinyin）」の形
- 「漢字＋英字サフィックス」例：宏光MINIEV → 宏光（Hongguang）MINIEV
- BYD海洋シリーズは原則グローバル英名（Dolphin / Seal / Seagull / Sea Lion）
  ・海豹05 DM-i → Seal 05 DM-i
  ・海獅06 新能源 → Sea Lion 06
  ・“新能源”は英名化時は付けない

【入力】
ブランド: {brand}
モデル: {model}

【出力例】
BYD, 秦（Qin）PLUS
五菱, 宏光（Hongguang）MINIEV
日産, シルフィー（Sylphy）
BYD, Seal 06
"""
    try:
        resp = openai.chat.completions.create(
            model="gpt-4o-mini",  # 既存維持
            messages=[
                {"role": "system", "content": "You are a precise automotive translation assistant."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print("LLM呼び出しエラー:", e)
        return ""

# ============== 正規化ここから（最小限の後処理） ==============

# ブランド名の強制マップ（出力ゆらぎ抑止）
BRAND_FIX = {
    # 中国勢
    "吉利银河": "Geely Galaxy",
    "吉利汽车": "Geely",
    "吉利":     "Geely",
    "比亚迪":   "BYD",
    "五菱汽车": "Wuling",
    "五菱":     "Wuling",
    "奇瑞":     "Chery",
    "长安":     "Changan",
    "长安汽车": "Changan",
    "长安启源": "Changan Qiyuan",
    "零跑汽车": "Leapmotor",
    "零跑":     "Leapmotor",
    "红旗":     "Hongqi",
    "哈弗":     "Haval",
    "小鹏":     "XPeng",
    "小米汽车": "Xiaomi",

    # グローバル既知（日本語定着名優先）
    "大众":     "フォルクスワーゲン",
    "丰田":     "トヨタ",
    "日产":     "日産",
    "本田":     "ホンダ",
    "奥迪":     "アウディ",
    "奔驰":     "メルセデス・ベンツ",
    "宝马":     "BMW",
    "别克":     "ビュイック",
    "特斯拉":   "テスラ",
    "AITO":     "AITO",
}

# 主要モデルの強制マップ（誤りやすい箇所のみピンポイント）
MODEL_FORCE = {
    # BYD 海洋シリーズ（英名に寄せる）
    "海豚": "Dolphin",
    "海豹": "Seal",
    "海鸥": "Seagull",
    "海狮": "Sea Lion",

    # VW 系
    "朗逸": "Lavida",
    "速腾": "Sagitar",
    "迈腾": "Magotan",
    "帕萨特": "パサート",
    "途观": "Tiguan",
    "途观L": "Tiguan L",
    "探岳": "Tayron",
    "途岳": "Tharu",

    # Toyota
    "卡罗拉锐放": "Corolla Cross",
    "RAV4荣放": "RAV4",
    "锋兰达": "Frontlander",
    "凯美瑞": "カムリ",

    # Nissan
    "轩逸": "シルフィー（Sylphy）",

    # Chery
    "瑞虎8": "Tiggo 8",

    # Geely
    "博越L": "Boyue L",
    "星越L": "星越（Xingyue）L",

    # BYD 秦/宋 系
    "秦PLUS": "秦（Qin）PLUS",
    "秦L":    "秦（Qin）L",
    "宋PLUS新能源": "宋（Song）PLUS",
    "宋Pro新能源":  "宋（Song）Pro",

    # Wuling
    "宏光MINIEV": "宏光（Hongguang）MINIEV",

    # そのままでよいもの
    "雅阁": "アコード",
    "宝马3系": "3シリーズ",
    "奥迪A6L": "A6L",
    "奔驰C级": "Cクラス",
    "元PLUS": "Atto 3",   # グローバルはAtto 3（要望反映）
    "元UP":   "Yuan UP", # “UP”はグローバルでもUP表記
}

# 中国語→英名 化で数字や派生を整える（BYD海洋シリーズの派生）
def normalize_byd_ocean(model_zh, model_ja):
    # 海豚 → Dolphin
    if "海豚" in model_zh:
        return "Dolphin"

    # 海豹XX → Seal XX / Seal XX DM-i
    if "海豹" in model_zh:
        m = re.search(r"海豹\s*0?(\d+)\s*(DM-i)?", model_zh, re.I)
        if m:
            num = m.group(1)
            dmi = m.group(2)
            return f"Seal {num} DM-i" if dmi else f"Seal {num}"
        # 06新能源 のような場合
        m2 = re.search(r"海豹\s*0?(\d+)", model_zh)
        if m2:
            return f"Seal {m2.group(1)}"
        return "Seal"

    # 海狮XX → Sea Lion XX
    if "海狮" in model_zh or "海獅" in model_zh:
        m = re.search(r"(海狮|海獅)\s*0?(\d+)", model_zh)
        if m:
            return f"Sea Lion {m.group(2)}"
        return "Sea Lion"

    # 海鸥 → Seagull
    if "海鸥" in model_zh:
        return "Seagull"

    return model_ja

# “漢字＋英字サフィックス” のピンイン括弧付与（必要最低限）
PINYIN_MINI = {
    "宏光": "Hongguang",
    "逸动": "Yidong",
    "星愿": "Xingyuan",
    "星越": "Xingyue",
}

def inject_pinyin_parentheses(model_zh, model_ja):
    # 例：宏光MINIEV → 宏光（Hongguang）MINIEV
    for han, py in PINYIN_MINI.items():
        if model_zh.startswith(han) and re.search(r"[A-Za-z]", model_zh):
            # 既に括弧が入っていれば何もしない
            if f"{han}（" in model_ja or f"{han}(" in model_ja:
                return model_ja
            # サフィックス（MINIEVなど）を抽出
            suf = model_zh.replace(han, "")
            suf = suf.strip()
            return f"{han}（{py}）{suf}" if suf else f"{han}（{py}）"
    return model_ja

def normalize_brand_model(brand_zh, model_zh, brand_ja, model_ja):
    # --- ブランド修正 ---
    brand_ja = BRAND_FIX.get(brand_zh, brand_ja)

    # --- モデル強制（ピンポイント） ---
    if model_zh in MODEL_FORCE:
        model_ja = MODEL_FORCE[model_zh]

    # --- BYD 海洋シリーズ派生補正 ---
    if "比亚迪" in brand_zh or brand_ja == "BYD":
        model_ja = normalize_byd_ocean(model_zh, model_ja)
        # “新能源”は英名化時に基本付与しない
        model_ja = re.sub(r"\s*新能源", "", model_ja)

    # --- VW の取り違え補正（探岳/Tayron、途岳/Tharu、途观/Tiguan） ---
    if brand_ja in ("フォルクスワーゲン", "Volkswagen", "大众"):
        if "探岳" in model_zh:
            model_ja = "Tayron"
        if "途岳" in model_zh:
            model_ja = "Tharu"
        if "途观" in model_zh:
            # L が付けば L まで
            model_ja = "Tiguan L" if "L" in model_zh else "Tiguan"

    # --- ピンイン括弧の自動挿入（漢字＋英字サフィックス系） ---
    model_ja = inject_pinyin_parentheses(model_zh, model_ja)

    # --- “英字+数字”のスペース欠落補正（Seal06 → Seal 06 等） ---
    model_ja = re.sub(r"([A-Za-z])\s*(\d{2})\b", r"\1 \2", model_ja)

    return brand_ja, model_ja
# ============== 正規化ここまで ==============


# ---------------- CSV I/O ----------------
rows = []
with open(INPUT_CSV, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        brand_zh = row["brand"]
        model_zh = row["model"]

        # 既存：LLMに一任
        out = translate_with_llm(brand_zh, model_zh)
        if "," in out:
            bja, mja = [p.strip() for p in out.split(",", 1)]
        else:
            bja, mja = out.strip(), ""

        # 追加：最小限の正規化で崩れを補正
        bja, mja = normalize_brand_model(brand_zh, model_zh, bja, mja)

        row["brand_ja"] = bja
        row["model_ja"] = mja
        rows.append(row)
        time.sleep(0.8)  # レート配慮（既存相当）

# 書き出し
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print("✅ 完了:", OUTPUT_CSV)
