#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLMで中国名→表示用名に正規化（辞書最小）
- ブランド/モデルをユニーク抽出→LLMにバッチ問い合わせ
- 厳格JSONで受け取り、CJKが残った項目だけ再問い合わせ
- 既にLatinは素通し、なければ漢字（必要に応じてピンイン併記）へフォールバック
- 実行毎にキャッシュを削除して常に最新ルールで再計算
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

# --- プロンプト（強化版。日本車カタカナ／ブランド片除去／ピンイン括弧の一貫性） ---

PROMPT_BRAND = """
あなたはChatGPT本体と同等の理解力を持つ変換器です。入力は中国語/混在表記の自動車ブランド名です。
以下の規則に厳密に従い、日本語での最終表示用に統一してください。出力は JSON のみ。

【出力仕様】
- 返答は厳密に: {"map": {"<入力>": "<出力>", ...}}
- 入力に含まれる全てのキーを必ず含める。
- JSON以外の文字（説明/注釈/コードブロック/末尾カンマ）は禁止。

【共通ルール】
1) 捏造禁止。確信が持てない場合は**入力をそのまま返す**。
2) 記号・英数字・スペースは保存。
3) 出力は**単一文字列**のみ（括弧や注釈を付けない）。

【ブランド優先順】
A) **日本メーカーは必ず日本語カタカナ表記**に統一：
   トヨタ, ホンダ, 日産, 三菱, マツダ, スバル, スズキ, ダイハツ, レクサス。
   例: "丰田"→"トヨタ"、"本田"→"ホンダ"、"日产"→"日産"。
B) A以外で**日本で一般に定着した日本語ブランド名**がある場合は日本語表記：
   例: "Volkswagen"→"フォルクスワーゲン", "Audi"→"アウディ", "Buick"→"ビュイック",
       "Mercedes-Benz"→"メルセデス・ベンツ", "Porsche"→"ポルシェ" など。
C) Bに該当せず、**グローバルで通用するラテン表記が明確**なら、その綴りを採用：
   例: BYD, XPeng, Li Auto, NIO, Zeekr, Geely, Wuling, Haval, Chery, Hongqi, Leapmotor, AITO, Xiaomi。
   例外：サブブランドとしての "吉利银河" は "Geely Galaxy" を採用。
D) それ以外（国際ラテン表記が不明）は**簡体字→日本語の新字体**へ自然置換：
   "红旗"→"紅旗"、"长安"→"長安" 等。
E) ジョイントベンチャー表記は最上位ブランドに統一してよいが、確信がなければD。

理解したら、与えられた `items` についてJSONのみを返す。
"""

PROMPT_MODEL = """
あなたはChatGPT本体と同等の理解力を持つ変換器です。入力は中国語/混在表記の車名（シリーズ名/モデル名）です。
以下の規則に厳密に従い、日本語での最終表示用に統一してください。出力は JSON のみ。

【出力仕様】
- 返答は厳密に: {"map": {"<入力>": "<出力>", ...}}
- 入力に含まれる全てのキーを必ず含める。
- JSON以外の文字（説明/注釈/コードブロック/末尾カンマ）は禁止。

【絶対遵守の共通ルール】
0) **モデル名の先頭に付いたブランド片は必ず除去**（"本田CR-V"→"CR-V"、"小米SU7"→"SU7"）。
1) **記号・英数字・スペース・大小は保存**（"Model 3", "DM-i", "Pro", "PLUS", "L", "EV", "PHEV", "06" など）。
2) 出力は**単一文字列**のみ（説明・注釈を付けない）。

【優先順（強度が高い順）】
A) **日本メーカーの既定カタカナ車名は必ずカタカナ**：
   例: "轩逸"→"シルフィー"（Sylphy）, "凯美瑞"→"カムリ", "卡罗拉锐放"→"カローラクロス",
       "雅阁"→"アコード", "本田CR-V"→"CR-V", "丰田RAV4荣放"→"RAV4"。
   ※ 英字記号名（RAV4等）はそのまま。
B) **中国以外OEMや中国市場の英語公式名が広く通用**：ラテン表記を採用。
   例: Lavida, Magotan, Tayron, Tharu, Frontlander, Atlas L, Atto 3, Seal 06 など。
C) **中国語固有シリーズ名**で国際ラテン表記が不明/保持したい固有漢字がある場合、
   **『漢字本体（Pinyin）+ サフィックス』**の形にする（全角括弧、直後に1回のみ）。
   - 派生/グレード等のサフィックスは入力通りを後置：PLUS, Pro, MAX, DM-i, EV, PHEV, L, 05/06 など。
   - 例：
     "星愿"→"星願（Xingyuan）"
     "宏光MINIEV"→"宏光（Hongguang）MINIEV"
     "星越L"→"星越（Xingyue）L"
     "秦PLUS"→"秦（Qin）PLUS"
     "秦L"→"秦（Qin）L"
     "宋PLUS"→"宋（Song）PLUS"
     "宋Pro"→"宋（Song）Pro"
     "海狮06新能源"→"海狮（Haishi）06新能源"
     "海豹05 DM-i"→"海豹（Haibao）05 DM-i"
D) **ブランド名の再付与は禁止**（モデル出力にブランドを繰り返さない）。

【品質ガード（再確認の観点）】
- 出力に中国語が残ってよいのは C の**漢字本体**のみ。ピンインは**全角括弧**で直後に1回だけ付す。
- 先頭ブランド片の取り残し（"小米SU7"→"SU7" 等）を禁止。
- "宋(Song)PLUS" など括弧位置の揺らぎは禁止。**必ず『本体（Pinyin）＋サフィックス』**の順。
- 日本車はローマ字を原則使わず**既定カタカナ**を優先（例外：RAV4等の公式英字名）。

理解したら、与えられた `items` についてJSONのみを返す。
"""

def is_latin(x: str) -> bool:
    return isinstance(x, str) and LATIN_RE.match((x or "").strip()) is not None

def load_cache(path: str) -> Dict[str, Dict[str, str]]:
    # 実行毎にキャッシュ削除（ファイルがあれば消す）
    if path and os.path.isfile(path):
        try:
            os.remove(path)
        except Exception:
            pass
    # 以降は空キャッシュで開始
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
            # 未応答キーは入力そのまま
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
    # 出力側に中国語（漢字）が“残ってはダメ”という意味ではなく、
    # 規則Cの「漢字本体（Pinyin）＋サフィックス」形式を満たさないラテン偏重や
    # 説明混入を再問い合わせで矯正する。
    # ここでは “ピンイン丸出しだけ” を検知して再問い合わせする。
    need_fix = []
    for k, v in map_in.items():
        s = str(v or "")
        if not s:
            need_fix.append(k)
            continue
        # JSONは満たしている前提。CJKゼロ＆元がCJKのみのときは要再問い合わせ
        if not HAS_CJK.search(s) and HAS_CJK.search(str(k)):
            need_fix.append(k)
    if not need_fix:
        return map_in
    fix = call_llm(need_fix, prompt, model)
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
