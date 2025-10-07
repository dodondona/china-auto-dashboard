#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLMで中国名→グローバル英名または日本語名に正規化（辞書最小）
- ブランド/モデルをユニーク抽出→LLMにバッチ問い合わせ
- 厳格JSONで受け取り、CJKが残った項目だけ再問い合わせ
- ピンインは日本語漢字の直後に括弧付きで併記（例: 星願（Xingyuan））
- 毎回キャッシュ削除前提
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

# --- プロンプト定義（改訂版） ---

PROMPT_BRAND = """
あなたは自動車ブランド名の正規化を行う変換器です。入力は中国語や混在表記のブランド名です。
以下の規則に厳密に従い、日本語での最終表示用に統一してください。出力は JSON のみ。

【出力仕様】
- 厳密に次の形式: {"map": {"<入力>": "<出力>", ...}}
- 入力のすべてのキーを含めること。
- JSON以外の文字（説明文・注釈・コードブロック）は一切禁止。

【ルール】
1) でたらめ禁止。確信が持てない場合は入力をそのまま返す。
2) 記号・英数字・スペースは温存。
3) 出力は単一文字列。括弧や注釈は不要。

【ブランド変換規則】
A) グローバルで通用するラテン表記がある場合はその綴りを使う。
   例: "BYD", "NIO", "Li Auto", "XPeng", "Zeekr", "Xiaomi", "Volkswagen", "Audi", "BMW"
B) 日本で広く通用する自動車メーカー名はカタカナにする。
   例: "トヨタ", "ホンダ", "日産", "三菱", "マツダ", "スバル", "スズキ", "ダイハツ", "メルセデス・ベンツ", "フォルクスワーゲン", "ビュイック"
C) 中国語のみで国際表記がない場合は、簡体字→日本語の新字体へ変換。
   例: "红旗"→"紅旗", "长安"→"長安"
D) ピンイン表記しかないブランドは、漢字（ピンイン）の形式にする。
   例: "长安"→"長安（Changan）"
E) BYD「海洋シリーズ」（海豹/海獅/海豚/海鷗）はグローバル名が確立しているため、ブランドは常に"BYD"とする。
"""

PROMPT_MODEL = """
あなたは自動車のモデル名（車名/シリーズ名）の正規化を行う変換器です。
入力は中国語または混在表記のモデル名です。以下の規則に厳密に従い、日本語での最終表示用に統一してください。
出力は JSON のみ。

【出力仕様】
- 厳密に次の形式: {"map": {"<入力>": "<出力>", ...}}
- 入力のすべてのキーを含めること。
- JSON以外の文字（説明・注釈・コードブロック）は一切禁止。

【共通ルール】
1) 確信が持てない場合は入力をそのまま返す。
2) 記号・英数字・スペースはそのまま残す（例: "Model 3", "DM-i", "Pro"）。
3) 出力は単一文字列。

【モデル変換規則】
A) グローバルに通用するラテン表記のモデル名がある場合はそのまま採用。
   例: "Model 3", "Seal", "Atto 3", "Dolphin", "Song PLUS", "Coolray"
B) 日本で長年使われている日本メーカーのモデルはカタカナ表記。
   例: シルフィー, アコード, カムリ, カローラ, シビック, フィット, プリウス, アルファード, ヤリス
C) BYD海洋シリーズ（海豹/海獅/海豚/海鷗）は英語動物名に変換。
   例: 海豹→Seal, 海獅→Sea Lion, 海豚→Dolphin, 海鷗→Seagull
D) グローバル名がない場合で、ピンインが使われるモデルは「漢字（ピンイン）」形式にする。
   例: 星願→星願（Xingyuan）, 逸動→逸動（Yidong）, 宏光MINIEV→宏光（Hongguang）MINIEV
E) ブランド名が重複している場合（例: "本田CR-V"）はブランド名部分を削除してモデル名のみを残す。
F) グレード/派生記号（Pro, MAX, Plus, DM-iなど）はそのまま残す。
G) 缤越→Coolray, 博越→Atlas など輸出名がある場合はグローバル名を優先。
"""

def is_latin(x: str) -> bool:
    return isinstance(x, str) and LATIN_RE.match((x or "").strip()) is not None

def load_cache(path: str) -> Dict[str, Dict[str, str]]:
    # キャッシュ削除前提なので、常に空で開始
    return {"brand": {}, "model": {}}

def save_cache(path: str, data: Dict[str, Dict[str, str]]):
    pass  # キャッシュは保存しない

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

def requery_nonlatin(map_in: Dict[str, str], prompt: str, model: str) -> Dict[str, str]:
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
    args = ap.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY is not set.", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(args.input)
    if args.brand_col not in df.columns or args.model_col not in df.columns:
        raise RuntimeError(f"Input must contain '{args.brand_col}' and '{args.model_col}'. columns={list(df.columns)}")

    cache = {"brand": {}, "model": {}}

    # ----- brand -----
    brands = sorted(set(str(x) for x in df[args.brand_col].dropna()))
    brand_map = {}
    for batch in chunked(brands, BATCH):
        part = call_llm(batch, PROMPT_BRAND, args.model)
        brand_map.update(part)
        brand_map = requery_nonlatin(brand_map, PROMPT_BRAND, args.model)

    # ----- model -----
    models = sorted(set(str(x) for x in df[args.model_col].dropna()))
    model_map = {}
    for batch in chunked(models, BATCH):
        part = call_llm(batch, PROMPT_MODEL, args.model)
        model_map.update(part)
        model_map = requery_nonlatin(model_map, PROMPT_MODEL, args.model)

    # ----- apply -----
    df[args.brand_ja_col] = df[args.brand_col].map(lambda x: brand_map.get(str(x), str(x)))
    df[args.model_ja_col] = df[args.model_col].map(lambda x: model_map.get(str(x), str(x)))

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"[OK] LLM-normalized: {args.input} -> {args.output} (rows={len(df)})")

if __name__ == "__main__":
    main()
