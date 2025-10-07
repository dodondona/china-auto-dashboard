#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLMで中国名→グローバル英名／日本語表記へ正規化（辞書最小・将来耐性重視）
- ChatGPT対話時と同等の意味理解プロンプトを導入
- 辞書列挙に頼らず、一般化ルールで統一
- ピンインは「日本語新字体（Pinyin）」でシリーズ直後に併記（例: 宏光（Hongguang）MINIEV）
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

# =================== プロンプト更新版（ChatGPT同等理解 + 現行ルール維持） ===================

PROMPT_BRAND = """
あなたは自動車ブランド名の翻訳・正規化を行う専門家です。
ChatGPT本体と同等の意味理解で、以下のルールに従い最も自然な表記を出力してください。
出力は厳密にJSON形式 {"map": {...}} のみで、他の文字や注釈は禁止。

【ブランド翻訳ルール】
1. グローバルで通用する英名がある場合 → その英名を採用（例: BYD, XPeng, Li Auto, Geely, Xiaomi）。
2. 日本ブランド → カタカナ（例: トヨタ, ホンダ, 日産, マツダ）。
3. 欧米ブランド → 日本での慣用カタカナ（例: フォルクスワーゲン, メルセデス・ベンツ, ビュイック, アウディ）。
4. 上記にない中国語ブランド → 簡体字→日本語新字体に変換（例: 红→紅, 长→長, 亚→亜）。
5. ブランドが派生ラインを持つ場合（例: 吉利银河）→ 上位英名＋ライン名（Geely Galaxy）。
6. ピンインのみのブランド → 漢字＋（Pinyin）形式（例: 長安（Changan））。
"""

PROMPT_MODEL = """
あなたは自動車モデル名の翻訳・正規化を行う専門家です。
ChatGPT本体と同等の意味理解で、ブランド・文脈を考慮して最も自然な表記を出力してください。
出力は厳密にJSON形式 {"map": {...}} のみで、他の文字や注釈は禁止。

【モデル翻訳ルール】
A. グローバル英名が存在する場合 → そのまま採用（例: Seal, Dolphin, Sea Lion, Atto 3, Lavida）。
B. 日本語で通用するモデル → カタカナ（例: シルフィー, カムリ, アコード）。
C. 中国語固有名でグローバル名不明 → 新字体＋ピンインを（）で併記し、サフィックスの前に置く。
   例:
     星願 → 星願（Xingyuan）
     宏光MINIEV → 宏光（Hongguang）MINIEV
     星越L → 星越（Xingyue）L
     宋PLUS → 宋（Song）PLUS
     宋Pro → 宋（Song）Pro
D. 英字を含む構成（例: Qin PLUS, Seal 06, Song PLUS）はスペース維持。
E. 海外輸出名が明確にある場合（例: 博越L→Atlas L, 缤越→Coolray）はそちらを優先。
F. ピンインは必ずシリーズ本体の直後に置く。スペースや大小文字は元入力を尊重。
"""

# =================== 以降は既存通り ===================

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
                    {"role": "user", "content": user},
                ],
                temperature=0,
                response_format={"type": "json_object"},
            )
            txt = resp.choices[0].message.content.strip()
            obj = json.loads(txt)
            mp = obj.get("map", {})
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
        raise RuntimeError(f"Input must contain '{args.brand_col}' and '{args.model_col}'.")

    brands = sorted(set(str(x) for x in df[args.brand_col].dropna()))
    brand_map = {}
    for batch in chunked(brands, BATCH):
        brand_map.update(call_llm(batch, PROMPT_BRAND, args.model))

    models = sorted(set(str(x) for x in df[args.model_col].dropna()))
    model_map = {}
    for batch in chunked(models, BATCH):
        model_map.update(call_llm(batch, PROMPT_MODEL, args.model))

    df[args.brand_ja_col] = df[args.brand_col].map(lambda x: brand_map.get(str(x), str(x)))
    df[args.model_ja_col] = df[args.model_col].map(lambda x: model_map.get(str(x), str(x)))

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"[OK] LLM-normalized: {args.input} -> {args.output} (rows={len(df)})")

if __name__ == "__main__":
    main()
