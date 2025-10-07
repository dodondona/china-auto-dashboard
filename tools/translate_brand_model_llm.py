#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLMで中国名→グローバル英名に正規化（辞書最小）
- ブランド/モデルをユニーク抽出→LLMにバッチ問い合わせ
- 厳格JSONで受け取り、CJKが残った項目だけ再問い合わせ
- 既にLatinは素通し、なければピンイン(Title Case)へフォールバック
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

# --- プロンプト（短く・強い指示） ---
PROMPT_BRAND = """
あなたは自動車ブランド名の正規化を行う変換器です。入力は中国語や混在表記のブランド名です。
以下の規則に厳密に従い、日本語での最終表示用に統一してください。出力は JSON のみ。

【出力仕様】
- 返答は厳密に: {"map": {"<入力>": "<出力>", ...}}
- 入力に含まれる全てのキーを必ず含めること。
- JSON以外の文字（説明・注釈・コードブロック・末尾カンマ）は一切禁止。

【共通ルール】
1) でたらめ禁止。確信が持てない場合は**入力をそのまま返す**。
2) 記号・英数字・スペースは温存（例: "AITO", "BAIC", "Li Auto"）。
3) 出力は**単一文字列**のみ。括弧/注釈を付けない。

【ブランドの優先順序】
A) **グローバルで通用するラテン表記が明確**なら、その綴りをそのまま採用（例: "BYD", "NIO", "Li Auto", "XPeng", "Zeekr", "Xiaomi", "Volkswagen", "Audi", "BMW"）。
B) Aに該当せず、**日本で広く通用する日本語ブランド名**が明確な場合は日本語表記（例: "トヨタ", "ホンダ", "日産", "三菱", "マツダ", "スバル", "スズキ", "ダイハツ"）。※確信がなければ適用しない。
C) それ以外（中国語のみ等）で**国際的ラテン表記が不明**な場合は、**簡体字→日本語の字形（新字体）**に自然置換した漢字表記にする（例: "东风日产"→"東風日産", "红旗"→"紅旗", "长安"→"長安"）。
D) ジョイント・ベンチャー名（例: "东风日产", "一汽丰田"）は、A/B/Cの方針で**単一の最上位ブランド表示**に統一してよいが、確信なき場合はCを採用。

理解したら、与えられた `items` についてJSONのみを返す。
"""

PROMPT_MODEL = """
あなたは自動車のモデル（車名/シリーズ名）の正規化を行う変換器です。入力は中国語や混在表記のモデル名です。
以下の規則に厳密に従い、日本語での最終表示用に統一してください。出力は JSON のみ。

【出力仕様】
- 返答は厳密に: {"map": {"<入力>": "<出力>", ...}}
- 入力に含まれる全てのキーを必ず含めること。
- JSON以外の文字（説明・注釈・コードブロック・末尾カンマ）は一切禁止。

【共通ルール】
1) でたらめ禁止。確信が持てない場合は**入力をそのまま返す**。
2) 記号・英数字・スペースは温存（例: "Model 3", "AION S Plus", "001", "SU7", "e:HEV", "DM-i", "Pro", "MAX" などは変えない）。
3) 出力は**単一文字列**のみ。括弧/注釈を付けない。

【モデルの優先順序】
E) **グローバルで通用するラテン表記のモデル名**がある場合は、そのラテン表記をそのまま採用（例: "Model 3", "Han", "Seal", "001", "SU7", "Song PLUS", "AION S Plus"）。
F) **日本市場で長年に通用する日本メーカーの定番モデル名**はカタカナ表記を優先（例: シルフィー/Sylphy, アコード/Accord, カムリ/Camry, カローラ/Corolla, シビック/Civic, フィット/Fit, プリウス/Prius, アルファード/Alphard, ヤリス/Yaris）。※確信がなければ E を優先しラテン表記のまま。
G) 中国語の固有シリーズ名で**国際的ラテン表記が不明**な場合は、**簡体字→日本語の字形（新字体）**へ自然置換した漢字表記にする（例: "轩逸"→"軒逸", "星愿"→"星願", "海狮"→"海獅"）。
H) グレード/派生（"Pro", "MAX", "Plus", "DM-i", "EV", "PHEV" 等）は入力のまま維持。
I) 先頭に中国語ブランド片が付いている場合（例: "本田CR-V"）はブランド片を除去し、モデル名のみを残す（"CR-V"）。

理解したら、与えられた `items` についてJSONのみを返す。
"""

def is_latin(x: str) -> bool:
    return isinstance(x, str) and LATIN_RE.match((x or "").strip()) is not None

def load_cache(path: str) -> Dict[str, Dict[str, str]]:
    if not path or not os.path.isfile(path):
        return {"brand": {}, "model": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
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
            # 未返答は原文
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
    # 値にCJKが残っているキーだけ再問い合わせ
    bad = [k for k, v in map_in.items() if HAS_CJK.search(str(v or ""))]
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
    need   = [b for b in brands if not is_latin(b) and b not in cache["brand"]]
    brand_map = dict(cache["brand"])
    for batch in chunked(need, BATCH):
        part = call_llm(batch, PROMPT_BRAND, args.model)
        brand_map.update(part)
        brand_map = requery_nonlatin(brand_map, PROMPT_BRAND, args.model)
        cache["brand"] = brand_map; save_cache(args.cache, cache)
    for b in brands:
        if is_latin(b) and b not in brand_map:
            brand_map[b] = b

    # ----- model -----
    models = sorted(set(str(x) for x in df[args.model_col].dropna()))
    need   = [m for m in models if not is_latin(m) and m not in cache["model"]]
    model_map = dict(cache["model"])
    for batch in chunked(need, BATCH):
        part = call_llm(batch, PROMPT_MODEL, args.model)
        model_map.update(part)
        model_map = requery_nonlatin(model_map, PROMPT_MODEL, args.model)
        cache["model"] = model_map; save_cache(args.cache, cache)
    for m in models:
        if is_latin(m) and m not in model_map:
            model_map[m] = m

    # ----- apply -----
    df[args.brand_ja_col] = df[args.brand_col].map(lambda x: brand_map.get(str(x), str(x)))
    df[args.model_ja_col] = df[args.model_col].map(lambda x: model_map.get(str(x), str(x)))

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"[OK] LLM-normalized: {args.input} -> {args.output} (rows={len(df)})")

if __name__ == "__main__":
    main()
