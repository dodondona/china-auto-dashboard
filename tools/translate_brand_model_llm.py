#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLMで中国名→グローバル英名に正規化（ルール＋代表例＋輸出名優先）
- ブランド/モデルをユニーク抽出→LLMにバッチ問い合わせ
- 厳格JSONで受け取り、CJKが残った項目だけ再問い合わせ
- キャッシュは毎回削除して再生成（プロンプト変更即反映）
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

# --- ブランド変換プロンプト ---
PROMPT_BRAND = """
あなたは自動車ブランド名の正規化を行う変換器です。
入力は中国語・英語・日本語が混ざったブランド名です。
以下の規則に厳密に従い、出力は JSON のみ。

【出力仕様】
- 厳密に: {"map": {"<入力>": "<出力>", ...}}
- 入力すべてを含め、JSON以外の文字（注釈・コードブロック等）禁止。
- 出力は単一文字列。

【ブランドの優先順序】
A) グローバルで通用する英語(ラテン)表記が明確なら、その綴りをそのまま採用（例: BYD, NIO, XPeng, Zeekr, Li Auto, Geely, Chery, Haval, Volkswagen, Audi, BMW, Mercedes-Benz）。
B) **日本メーカー（トヨタ、ホンダ、日産、三菱、マツダ、スバル、スズキ、ダイハツ）は必ずカタカナ表記**にする。
C) 上記以外で国際的ラテン表記が不明な場合は、簡体字→日本語の字形（新字体）へ自然置換（例: 红旗→紅旗、长安→長安、东风→東風）。
D) 中国ブランドでも、**輸出市場で統一されたグローバル英名が存在する場合はそちらを優先**（例: 比亚迪→BYD, 吉利→Geely, 奇瑞→Chery, 哈弗→Haval）。
E) 記号・英数字・スペースは保持。
F) ハルシネーション禁止。確信が持てない場合は入力をそのまま返すが、上記ルールに最大限従う。

【代表例】
- 比亚迪 → BYD
- 吉利汽车 → Geely
- 吉利银河 → Geely Galaxy
- 奇瑞 → Chery
- 哈弗 → Haval
- 红旗 → Hongqi
- 五菱汽车 → Wuling
- 长安 → Changan
- 岚图 → Voyah
- 深蓝 → Deepal
- 哪吒 → Neta
- 腾势 → Denza
- 智己 → IM Motors
- 小鹏 → XPeng
- 小米汽车 → Xiaomi
- 特斯拉 → Tesla
- 丰田 → トヨタ
- 本田 → ホンダ
- 日产 → 日産
- 大众 → Volkswagen
- 奔驰 → Mercedes-Benz
- 宝马 → BMW
- 别克 → Buick
- 奥迪 → Audi

これらの規則と例を参考に、与えられた `items` を同様に統一し、JSONのみ返答。
"""

# --- モデル変換プロンプト ---
PROMPT_MODEL = """
あなたは自動車のモデル（車名/シリーズ名）を正規化する変換器です。
入力は中国語・英語・日本語混在です。以下の規則に厳密に従い、JSONのみで返答。

【出力仕様】
- 厳密に: {"map": {"<入力>": "<出力>", ...}}
- 入力すべてを含めること。
- JSON以外の文字禁止。
- 出力は単一文字列。

【変換方針】
E) グローバルで通用するラテン表記があればそのまま採用（例: Model 3, Han, Seal, Dolphin, 001, SU7）。
F) **日本メーカーの定番モデルはカタカナ表記を優先**（例: カローラ, アコード, シビック, フィット, プリウス, シルフィー, カムリ）。
G) 中国語固有シリーズで国際ラテンが不明なら、簡体字→日本語字形（轩逸→軒逸, 星愿→星願, 海狮→海獅）。
H) グレード/派生（Pro, MAX, Plus, DM-i, EV, PHEV 等）は保持。
I) ブランド名混在（本田CR-V等）はモデルのみを残す（CR-V）。
J) **グローバル（輸出）向け英名が存在する場合は中国国内名より優先**。
   特に BYD, Geely, Chery, Haval などは以下に従う：
   - 元PLUS → Atto 3
   - 元UP → Dolphin Mini
   - 海豚 → Dolphin
   - 海豹 → Seal
   - 海狮 → Sea Lion
   - 宋PLUS → Song PLUS
   - 宋Pro → Song Pro
   - 唐 → Tang
   - 秦 → Qin
   - 汉 → Han
   - 星越L → Xingyue L
   - 博越L → Boyue L
   - 缤越 → Binyue
   - 瑞虎8 → Tiggo 8
   - 艾瑞泽8 → Arrizo 8
   - 朗逸 → Lavida
   - 速腾 → Sagitar
   - 探岳 → Tayron
   - 途岳 → Tharu
   - 迈腾 → Magotan
   - 奔驰C级 → C-Class
   - 宝马3系 → 3 Series
   - 红旗H5 → Hongqi H5
   - 五菱缤果 → Bingo
   - 哈弗大狗 → Big Dog
K) ハルシネーション禁止。確信が持てない場合は入力をそのまま返すが、上記のように可能な限りグローバル輸出名を採用。

上記ルール＋例に従い、与えられた `items` を統一してJSONのみ返答。
"""

def load_cache(path: str) -> Dict[str, Dict[str, str]]:
    # キャッシュは毎回削除
    if path and os.path.exists(path):
        try:
            os.remove(path)
            print(f"[INFO] Cache file {path} deleted for fresh run.")
        except Exception as e:
            print(f"[WARN] Cache delete failed: {e}")
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
    ap.add_argument("--cache", default=".cache/global_map.json")
    args = ap.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY is not set.", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(args.input)
    if args.brand_col not in df.columns or args.model_col not in df.columns:
        raise RuntimeError(f"Input must contain '{args.brand_col}' and '{args.model_col}'.")

    cache = load_cache(args.cache)

    # --- ブランド変換 ---
    brands = sorted(set(str(x) for x in df[args.brand_col].dropna()))
    brand_map = {}
    for batch in chunked(brands, BATCH):
        part = call_llm(batch, PROMPT_BRAND, args.model)
        brand_map.update(part)
        brand_map = requery_nonlatin(brand_map, PROMPT_BRAND, args.model)
        cache["brand"] = brand_map; save_cache(args.cache, cache)

    # --- モデル変換 ---
    models = sorted(set(str(x) for x in df[args.model_col].dropna()))
    model_map = {}
    for batch in chunked(models, BATCH):
        part = call_llm(batch, PROMPT_MODEL, args.model)
        model_map.update(part)
        model_map = requery_nonlatin(model_map, PROMPT_MODEL, args.model)
        cache["model"] = model_map; save_cache(args.cache, cache)

    # --- 適用 ---
    df[args.brand_ja_col] = df[args.brand_col].map(lambda x: brand_map.get(str(x), str(x)))
    df[args.model_ja_col] = df[args.model_col].map(lambda x: model_map.get(str(x), str(x)))

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"[OK] LLM-normalized (export/global-name priority): {args.input} -> {args.output} (rows={len(df)})")

if __name__ == "__main__":
    main()
