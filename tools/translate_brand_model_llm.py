#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLMで中国名→グローバル英名／日本語表記へ正規化（辞書最小・将来耐性重視）
- 辞書列挙に頼らず、一般化ルールで統一
- ピンインは「日本語新字体（Pinyin）」でシリーズ直後に併記（例: 宏光（Hongguang）MINIEV）
- .cache は外部でクリア運用（本スクリプトはプロンプトのみ更新）
"""

import argparse, json, os, sys, time
import pandas as pd
import regex as re2
from typing import Dict, List

LATIN_RE = re2.compile(r"^[\p{Latin}\p{Number}\s\-\+\/\.\(\)]+$")
HAS_CJK  = re2.compile(r"\p{Han}")

DEF_MODEL = "gpt-4o-mini"
BATCH = 50
RETRY = 3
SLEEP = 1.0

# =================== プロンプト（v7：一般化ルールを強化） ===================

PROMPT_BRAND = """
あなたは自動車ブランド名の正規化を行う変換器です。出力は厳密に JSON のみ。

【出力仕様】
- 形式: {"map": {"<入力>": "<出力>", ...}}
- 入力に含まれる全てのキーを必ず含めること。
- JSON以外の文字（説明・注釈・コードブロック・末尾カンマ）は禁止。

【基本方針（将来耐性を重視し、辞書に依存しない）】
A) でたらめ禁止。確信がない場合は、入力をそのまま返す。
B) 記号・英数字・スペースは保持。
C) 出力は単一文字列のみ。

【ブランドの優先順位ルール】
1) **グローバルで通用する英名が確立**している場合は、その英名で統一。
   例: BYD, NIO, Li Auto, XPeng, Zeekr, Xiaomi, Geely, Wuling, Haval, Chery, Hongqi, Leapmotor, AITO など
   - 合成ブランド（例: 吉利银河）は **Geely Galaxy** のように上位の英名＋ライン名を用いる。
2) **日本メーカー**は日本語カタカナ表記に統一。
   例: トヨタ, ホンダ, 日産, 三菱, マツダ, スバル, スズキ, ダイハツ, レクサス, いすゞ など
3) **欧米ブランドで日本で慣用のカタカナがある**場合はカタカナ。
   例: フォルクスワーゲン, メルセデス・ベンツ, ビュイック, アウディ, BMW（そのまま）
4) 上記に当てはまらず**中国語のみ**で国際表記が不明な場合は、**簡体字→日本語の新字体**に変換した漢字表記へ統一。
   - これは辞書ではなく一般原則。よくある例（網羅ではない）:
     红→紅, 录→録, 赵→趙, 苏→蘇, 汉→漢, 关→関, 亚→亜, 长→長, 机→機, 东→東, 风→風,
     国→国, 厂→厂（日本語では工場名等は文脈次第）, 丰→豊, 马→馬, 乌→烏, 鸟→鳥, 龙→竜/龍,
     门→門, 广→広, 台→台, 资→資, 车→車, 灯→灯, 线→線, 压→圧, 网→網, 备→備, 级→級 など。
   - 変換は自然な日本語の字形を優先（機種依存/異体字は避ける）。
5) **ピンインだけのブランド**に見える場合は、**漢字（Pinyin）** とする。
   例: 長安（Changan）
6) 出力は安定表記を意識し、**誤ったカナ化や当て字は避ける**。

理解したら、与えられた items（ブランド名配列）に対して JSON のみ返す。
"""

PROMPT_MODEL = """
あなたは自動車モデル名（車名/シリーズ名）の正規化を行う変換器です。出力は厳密に JSON のみ。

【出力仕様】
- 形式: {"map": {"<入力>": "<出力>", ...}}
- 入力に含まれる全てのキーを必ず含めること。
- JSON以外の文字（説明・注釈・コードブロック・末尾カンマ）は禁止。

【基本方針】
1) でたらめ禁止。確信がない場合は入力をそのまま返す。
2) 英数字・記号は保持（Model 3, DM-i, Pro, MAX, Plus, L, S 等）。
3) 先頭のブランド片（例: 本田CR-V）は除去し、**モデルのみ**にする。
4) 最終出力が**全てラテン文字**になる場合は**括弧（ピンイン）を付けない**。
   → 例: Qin PLUS, Seal 06, Dolphin, Lavida, Magotan など。

【優先順位（一般ルール）】
A) **グローバル英名が明確**な場合は英名を採用（括弧を付けない）。
   - BYD海洋: 海獅→Sea Lion, 海豹→Seal, 海豚→Dolphin, 海鷗→Seagull
   - 輸出名が定着: 朗逸→Lavida, 速腾→Sagitar, 迈腾→Magotan, 探岳→Tayron, 途岳→Tharu,
     途観L→Tiguan L, 锋兰达→Frontlander, 卡罗拉锐放→Corolla Cross, 缤越→Coolray,
     博越L→Atlas L, 瑞虎8→Tiggo 8, 艾瑞泽8→Arrizo 8, 昂科威Plus→Envision Plus,
     元UP→Dolphin Mini, 元PLUS→Atto 3, 雅阁→アコード, 凯美瑞→カムリ, 宝马3系→3 Series,
     银河A7→Galaxy A7, 宏光MINIEV→宏光（Hongguang）MINIEV（※例外：下記Cを適用）
B) **日本で長年使われる既定カタカナ**はカタカナを優先（例: シルフィー/アコード/カムリ/カローラ）。
C) **グローバル名が不明で中国語固有名**の場合は、**簡体字→日本語の新字体**へ変換し、
   さらに**シリーズ本体の直後**にピンインを全角括弧で併記する。
   - 形式: 《シリーズ漢字》（Pinyin）《接尾詞/サフィックス》
   - 例:
     - 星愿 → 星願（Xingyuan）
     - 逸动 → 逸動（Yidong）
     - 宏光MINIEV → 宏光（Hongguang）MINIEV
     - 星越L → 星越（Xingyue）L
     - 宋PLUS → 宋（Song）PLUS
     - 宋Pro → 宋（Song）Pro
   - 注意: **括弧の位置はシリーズ本体の直後**。接尾の「L/Pro/PLUS/DM-i 等」はそのまま後ろに続ける。
   - スペースは**入力の意図を尊重**（元が「星越L」なら出力も「星越（Xingyue）L」）。

【その他の注意】
- 誤カナ化や当て字は禁止。安定した国際表記を優先。
- 大文字小文字は一般的な表記に合わせる（例: PLUS, Pro, DM-i）。
- 地名やライン名を勝手に追加しない。
理解したら、与えられた items（モデル名配列）に対して JSON のみ返す。
"""

# =================== 以降は既存どおり（変更なし） ===================

def is_latin(x: str) -> bool:
    return isinstance(x, str) and LATIN_RE.match((x or "").strip()) is not None

def call_llm(items: List[str], prompt: str, model: str) -> Dict[str, str]:
    from openai import OpenAI
    client = OpenAI()
    user = prompt + "\nInput list (JSON array):\n" + json.dumps(items, ensure_ascii=False)
    for attempt in range(RETRY):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "Reply with strict JSON only. No prose."},
                    {"role": "user",   "content": user},
                ],
                temperature=0,
                response_format={"type": "json_object"},
            )
            txt = resp.choices[0].message.content.strip()
            obj = json.loads(txt)
            mp  = obj.get("map", {})
            return {x: mp.get(x, x) for x in items}
        except Exception:
            if attempt == RETRY - 1:
                raise
            time.sleep(SLEEP * (attempt + 1))
    return {x: x for x in items}

def chunked(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--brand-col", default="brand")
    ap.add_argument("--model-col", default="model")
    ap.add_argument("--brand-ja-col", default="brand_ja")
    ap.add_argument("--model-ja-col", default="model_ja")
    ap.add_argument("--model", default=DEF_MODEL)
    args = ap.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY is not set.", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(args.input)
    if args.brand_col not in df.columns or args.model_col not in df.columns:
        raise RuntimeError(f"Input must contain '{args.brand_col}' and '{args.model_col}'. columns={list(df.columns)}")

    # brand
    brands = sorted(set(str(x) for x in df[args.brand_col].dropna()))
    brand_map = {}
    for batch in chunked(brands, BATCH):
        brand_map.update(call_llm(batch, PROMPT_BRAND, args.model))

    # model
    models = sorted(set(str(x) for x in df[args.model_col].dropna()))
    model_map = {}
    for batch in chunked(models, BATCH):
        model_map.update(call_llm(batch, PROMPT_MODEL, args.model))

    # apply
    df[args.brand_ja_col] = df[args.brand_col].map(lambda x: brand_map.get(str(x), str(x)))
    df[args.model_ja_col] = df[args.model_col].map(lambda x: model_map.get(str(x), str(x)))

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"[OK] LLM-normalized: {args.input} -> {args.output} (rows={len(df)})")

if __name__ == "__main__":
    main()
