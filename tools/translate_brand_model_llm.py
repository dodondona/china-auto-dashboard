#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLMで中国名→グローバル英名／日本語表記へ正規化
- 辞書ではなくLLM推論で名称統一（国際表記＋日本語両対応）
- ピンインのみの結果は「日本語新字体（ピンイン）」形式に変換
- .cache は毎回削除（再実行で全件リトライ）
"""

import argparse, json, os, sys, time, shutil
import pandas as pd
import regex as re2
from typing import Dict, List

LATIN_RE = re2.compile(r"^[\p{Latin}\p{Number}\s\-\+\/\.\(\)]+$")
HAS_CJK  = re2.compile(r"\p{Han}")
DEF_MODEL = "gpt-4o-mini"
BATCH = 50
RETRY = 3
SLEEP = 1.0

# ======= PROMPTS =======

PROMPT_BRAND = """
あなたは自動車ブランド名の正規化を行う変換器です。出力は厳密にJSONのみで行ってください。

【出力仕様】
- {"map": {"<入力>": "<出力>", ...}} の形式。
- 全キーを含め、JSON以外の文字（注釈・コードブロック等）は禁止。

【ルール】
1) でたらめ禁止。確信がない場合は入力をそのまま返す。
2) 記号・英数字は保持。
3) グローバル英名が確立している場合は**英名で統一**。
4) 日本メーカーはカタカナ名（トヨタ、ホンダ、日産、マツダ、スバル、スズキ、ダイハツ）。
5) 中国ブランドで英名が確立している場合は英名表記：
   - 吉利→Geely
   - 吉利银河→Geely Galaxy
   - 五菱→Wuling
   - 比亚迪→BYD
   - 奇瑞→Chery
   - 长安→Changan
   - 哈弗→Haval
   - 零跑→Leapmotor
   - 红旗→Hongqi
   - AITO→AITO
   - 小鹏→XPeng
   - 小米汽车→Xiaomi
   - 奔驰→Mercedes-Benz
   - 奥迪→Audi
   - 宝马→BMW
   - 大众→フォルクスワーゲン
   - 丰田→トヨタ
   - 本田→ホンダ
   - 日产→日産
   - 别克→ビュイック
6) 上記以外の中国語ブランド名は、簡体字→日本語の新字体で自然置換。

例:
"比亚迪"→"BYD"
"长安启源"→"Changan Qiyuan"
"吉利银河"→"Geely Galaxy"
"红旗"→"紅旗" または "Hongqi"
"""

PROMPT_MODEL = """
あなたは自動車モデル名（車種名）の正規化を行います。JSONのみ返してください。

【出力仕様】
- {"map": {"<入力>": "<出力>", ...}} の形式。

【基本ルール】
1) でたらめ禁止。確信がない場合は入力をそのまま返す。
2) 英数字・記号はそのまま。
3) ブランドプレフィックス（例: "本田CR-V"）は除去し、モデルのみ。
4) 以下の優先順序に従う：

【グローバル表記優先リスト】
- 宏光MINIEV → 宏光（Hongguang）MINIEV
- 星愿 → 星願（Xingyuan）
- 海獅06新能源 → Sea Lion 06
- 海豹06新能源 → Seal 06
- 秦PLUS / 秦L → Qin PLUS / Qin L
- 海鸥 → Seagull
- 元UP → Dolphin Mini
- 元PLUS → Atto 3
- 宋PLUS新能源 → Song PLUS
- 宋Pro新能源 → Song Pro
- 海豹05 DM-i → Seal 05 DM-i
- 博越L → Atlas L
- 星越L → Xingyue L
- 朗逸 → Lavida
- 速腾 → Sagitar
- 迈腾 → Magotan
- 帕萨特 → Passat
- 途観L → Tiguan L
- 途岳 → Tharu
- 探岳 → Tayron
- 锋兰达 → Frontlander
- 卡罗拉锐放 → Corolla Cross
- 凯美瑞 → カムリ
- RAV4荣放 → RAV4
- 银河A7 → Galaxy A7
- 宏光 → 宏光（Hongguang）
- 逸動 → 逸動（Yidong）
- 瑞虎8 → Tiggo 8
- 艾瑞泽8 → Arrizo 8
- 哈弗大狗 → Big Dog
- 零跑C10 → C10
- 小鹏MONA M03 → Mona M03
- 雅阁 → アコード
- 宝马3系 → 3シリーズ
- 红旗H5 → 紅旗H5（Hongqi H5 でも可）
- 昂科威Plus → Envision Plus
- 缤越 → Coolray
- 缤果 → Bingo

【ピンインのみの結果】
中国語のままピンインに置換される場合は、
「日本語新字体（ピンイン）」の形式にしてください。
例: 星願（Xingyuan）, 宏光（Hongguang）

理解したら、与えられた items に対して JSON のみ返してください。
"""

# ======= CORE FUNCTIONS =======

def is_latin(x: str) -> bool:
    return isinstance(x, str) and LATIN_RE.match((x or "").strip()) is not None

def call_llm(items: List[str], prompt: str, model: str) -> Dict[str, str]:
    from openai import OpenAI
    client = OpenAI()
    user = prompt + "\nInput list:\n" + json.dumps(items, ensure_ascii=False)
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
            mp = json.loads(txt).get("map", {})
            return {x: mp.get(x, x) for x in items}
        except Exception as e:
            if attempt == RETRY - 1:
                raise
            time.sleep(SLEEP * (attempt + 1))
    return {x: x for x in items}

def chunked(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# ======= MAIN =======

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

    # --- always reset cache
    cache_dir = ".cache"
    if os.path.isdir(cache_dir):
        shutil.rmtree(cache_dir)
    os.makedirs(cache_dir, exist_ok=True)

    if not os.getenv("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY not set.", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(args.input)
    for col in [args.brand_col, args.model_col]:
        if col not in df.columns:
            raise RuntimeError(f"Missing column {col}")

    from tempfile import NamedTemporaryFile
    tmp = NamedTemporaryFile(delete=False).name

    # ---- brand ----
    brands = sorted(set(str(x) for x in df[args.brand_col].dropna()))
    brand_map = {}
    for batch in chunked(brands, BATCH):
        part = call_llm(batch, PROMPT_BRAND, args.model)
        brand_map.update(part)

    # ---- model ----
    models = sorted(set(str(x) for x in df[args.model_col].dropna()))
    model_map = {}
    for batch in chunked(models, BATCH):
        part = call_llm(batch, PROMPT_MODEL, args.model)
        model_map.update(part)

    # ---- apply ----
    df[args.brand_ja_col] = df[args.brand_col].map(lambda x: brand_map.get(str(x), str(x)))
    df[args.model_ja_col] = df[args.model_col].map(lambda x: model_map.get(str(x), str(x)))

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"[OK] Normalized {args.input} → {args.output} (rows={len(df)})")

if __name__ == "__main__":
    main()
