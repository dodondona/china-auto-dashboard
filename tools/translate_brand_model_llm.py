#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLMで中国名→表示用（日本語優先／グローバル名併用）に正規化（辞書最小）
- ブランド/モデルをユニーク抽出→LLMにバッチ問い合わせ
- 厳格JSONで受け取り、CJKが残った項目だけ再問い合わせ
- 既にLatinは素通し、なければ『漢字本体（Pinyin）+サフィックス』へフォールバック
- 実行ごとにキャッシュは削除（毎回クリーンにやり直す）
"""

import argparse, json, os, time, sys
from typing import Dict, List
import pandas as pd
import regex as re2  # pip install regex

LATIN_RE = re2.compile(r"^[\p{Latin}\p{Number}\s\-\+\/\.\(\)]+$")
HAS_CJK  = re2.compile(r"\p{Han}")

DEF_MODEL = "gpt-4o-mini"
BATCH = 50
RETRY = 3
SLEEP = 1.0

# --- 差し替え済みプロンプト（最小だが強い few-shot 付き） ---
PROMPT_BRAND = """
あなたはChatGPT本体と同等の理解力を持つ変換器です。入力は中国語/混在表記の自動車ブランド名です。
以下の規則に厳密に従い、日本語での最終表示用に統一してください。出力は JSON のみ。

【出力仕様】
- 返答は厳密に: {"map": {"<入力>": "<出力>", ...}}
- 入力に含まれる全てのキーを必ず含める。
- JSON以外の文字（説明/注釈/コードブロック/末尾カンマ）は禁止。

【共通ルール】
1) 捏造禁止。確信が持てない場合は**入力をそのまま返す**。
2) 出力は**単一文字列**のみ（説明・注釈を付けない）。
3) 日本で一般に定着したカタカナ名がある場合は**必ずカタカナ**にする。
   例: "丰田"→"トヨタ", "本田"→"ホンダ", "日产"→"日産",
       "大众"→"フォルクスワーゲン", "奥迪"→"アウディ",
       "别克"→"ビュイック", "奔驰"→"メルセデス・ベンツ"。
4) 中国系や新興でカタカナ慣用が弱い場合は**グローバル英名**を採用（BYD, XPeng, Li Auto, NIO, Zeekr, Geely, Wuling, Haval, Chery, Hongqi, Leapmotor, AITO, Xiaomi など）。
5) サブブランド「吉利银河」は **"Geely Galaxy"** を採用。
6) **取り違え禁止（重要）**：
   - "吉利"/"吉利汽车" は **Geely / ジーリー**（**Chery/奇瑞ではない**）
   - "奇瑞" は **Chery / チリ**（**Geely/吉利ではない**）

【Few-shot（厳守例）】
入力→出力:
- "丰田" → "トヨタ"
- "日产" → "日産"
- "大众" → "フォルクスワーゲン"
- "别克" → "ビュイック"
- "奔驰" → "メルセデス・ベンツ"
- "奥迪" → "アウディ"
- "红旗" → "紅旗"
- "吉利汽车" → "Geely"
- "吉利银河" → "Geely Galaxy"
- "奇瑞" → "Chery"
- "五菱汽车" → "Wuling"
- "小米汽车" → "Xiaomi"
- "零跑汽车" → "Leapmotor"
- "哈弗" → "Haval"
- "长安" → "長安"
- "长安启源" → "長安啓源"

理解したら、与えられた `items` についてJSONのみを返す。
"""

PROMPT_MODEL = """
あなたはChatGPT本体と同等の理解力を持つ変換器です。入力は中国語/混在表記の車名（シリーズ/モデル）です。
以下の規則に厳密に従い、日本語での最終表示用に統一してください。出力は JSON のみ。

【出力仕様】
- 返答は厳密に: {"map": {"<入力>": "<出力>", ...}}
- 入力に含まれる全てのキーを必ず含める。
- JSON以外の文字（説明/注釈/コードブロック/末尾カンマ）は禁止。

【絶対遵守】
A) **先頭のブランド片は必ず除去**（"本田CR-V"→"CR-V", "小米SU7"→"SU7"）。
B) **記号・英数字・大小・スペースは保存**（例: "Model 3", "DM-i", "Pro", "PLUS", "L", "EV", "PHEV", "05/06"）。
C) 出力は**単一文字列**のみ。

【優先順位】
1) **日本メーカーの既定カタカナ名を必ず採用**：
   - "轩逸"→"シルフィー", "凯美瑞"→"カムリ", "卡罗拉锐放"→"カローラクロス",
     "雅阁"→"アコード", "本田CR-V"→"CR-V", "RAV4荣放"→"RAV4"。
2) **国際的に定着した英名がある場合は英名**：
   - 大众系: Lavida（朗逸）, Magotan（迈腾）, Tayron（探岳）, Tharu（途岳）,
     Passat, Tiguan L, Atlas L（博越L）, Frontlander（锋兰达）
   - BYD系: Atto 3（元PLUS）, Seal 06（海豹06）, Sea Lion 06（海狮06）, Dolphin（海豚）, Dolphin Mini（元UP）
3) 2に該当しない**中国語固有名**は
   **『漢字本体（Pinyin）+ サフィックス』**（全角括弧）で統一：
   例: "星愿"→"星願（Xingyuan）", "宏光MINIEV"→"宏光（Hongguang）MINIEV",
       "星越L"→"星越（Xingyue）L", "秦PLUS"→"秦（Qin）PLUS",
       "秦L"→"秦（Qin）L", "宋PLUS"→"宋（Song）PLUS",
       "宋Pro"→"宋（Song）Pro", "海豹05 DM-i"→"海豹（Haibao）05 DM-i"。
4) **取り違え防止（重要）**：
   - "元UP" は **"Dolphin Mini"**（"元（Yuan）UP" ではない）
   - "朗逸" は **"Lavida"**
   - "迈腾" は **"Magotan"**
   - "锋兰达" は **"Frontlander"**
   - "缤越" は **"Coolray"**
5) **括弧位置の揺らぎ禁止**：
   - 常に「**本体（Pinyin）**」の直後にサフィックス（"PLUS", "Pro", "L", "DM-i", "MINIEV" 等）を続ける。
   - 例：×「宋(Song)PLUS」→ ○「宋（Song）PLUS」

【Few-shot（厳守例）】
入力→出力:
- "轩逸" → "シルフィー"
- "凯美瑞" → "カムリ"
- "卡罗拉锐放" → "カローラクロス"
- "雅阁" → "アコード"
- "RAV4荣放" → "RAV4"
- "本田CR-V" → "CR-V"
- "朗逸" → "Lavida"
- "迈腾" → "Magotan"
- "探岳" → "Tayron"
- "途岳" → "Tharu"
- "博越L" → "Atlas L"
- "锋兰达" → "Frontlander"
- "元PLUS" → "Atto 3"
- "元UP" → "Dolphin Mini"
- "海豚" → "Dolphin"
- "海狮06新能源" → "Sea Lion 06"
- "海豹06新能源" → "Seal 06"
- "宏光MINIEV" → "宏光（Hongguang）MINIEV"
- "秦PLUS" → "秦（Qin）PLUS"
- "秦L" → "秦（Qin）L"
- "星愿" → "星願（Xingyuan）"

理解したら、与えられた `items` についてJSONのみを返す。
"""

def is_latin(x: str) -> bool:
    return isinstance(x, str) and LATIN_RE.match((x or "").strip()) is not None

def load_cache(path: str) -> Dict[str, Dict[str, str]]:
    # 実行ごとにキャッシュ削除（要求どおり）
    if path and os.path.isfile(path):
        try:
            os.remove(path)
        except Exception:
            pass
    return {"brand": {}, "model": {}}

def save_cache(path: str, data: Dict[str, Dict[str, str]]):
    if not path:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

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
            # 返ってこなかったキーは恒等写像
            return {x: mp.get(x, x) for x in items}
        except Exception:
            if attempt == RETRY - 1:
                raise
            time.sleep(SLEEP * (attempt + 1))
    return {x: x for x in items}

def chunked(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def requery_nonlatin(map_in: Dict[str, str], prompt: str, model: str) -> Dict[str, str]:
    # CJKが残っていても、“日本語既定/英名優先/漢字（Pinyin）ルール”に該当し得るので
    # もう一度だけ該当キーを再問い合わせ
    bad = [k for k, v in map_in.items() if HAS_CJK.search(str(v or "")) and not is_latin(str(v or ""))]
    if not bad:
        return map_in
    fix = call_llm(bad, prompt, model)
    map_in.update(fix)
    return map_in

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--brand-col", default="brand")
    ap.add_argument("--model-col", default="model")
    ap.add_argument("--brand-ja-col", default="brand_ja")
    ap.add_argument("--model-ja-col", default="model_ja")
    ap.add_argument("--model", default=DEF_MODEL)
    ap.add_argument("--cache", default=".cache/global_map.json")
    args = ap.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY is not set.", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(args.input)
    if args.brand_col not in df.columns or args.model_col not in df.columns:
        raise RuntimeError(f"Input must contain '{args.brand_col}' and '{args.model_col}'. columns={list(df.columns)}")

    cache = load_cache(args.cache)

    # ----- brand -----
    brands = sorted(set(str(x) for x in df[args.brand_col].dropna()))
    need = [b for b in brands if b not in cache["brand"]]
    brand_map = dict(cache["brand"])
    for batch in chunked(need, BATCH):
        part = call_llm(batch, PROMPT_BRAND, args.model)
        brand_map.update(part)
        brand_map = requery_nonlatin(brand_map, PROMPT_BRAND, args.model)
        cache["brand"] = brand_map; save_cache(args.cache, cache)

    # ----- model -----
    models = sorted(set(str(x) for x in df[args.model_col].dropna()))
    need = [m for m in models if m not in cache["model"]]
    model_map = dict(cache["model"])
    for batch in chunked(need, BATCH):
        part = call_llm(batch, PROMPT_MODEL, args.model)
        model_map.update(part)
        model_map = requery_nonlatin(model_map, PROMPT_MODEL, args.model)
        cache["model"] = model_map; save_cache(args.cache, cache)

    # ----- apply -----
    df[args.brand_ja_col] = df[args.brand_col].map(lambda x: brand_map.get(str(x), str(x)))
    df[args.model_ja_col] = df[args.model_col].map(lambda x: model_map.get(str(x), str(x)))

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"[OK] LLM-normalized: {args.input} -> {args.output} (rows={len(df)})")

if __name__ == "__main__":
    main()
