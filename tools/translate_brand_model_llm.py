#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLMで中国名→グローバル英名に正規化（辞書なし・ルール＋少量例で一般化）
- ブランド/モデルをユニーク抽出→LLMにバッチ問い合わせ
- 厳格JSONで受け取り、CJKが残った項目だけ再問い合わせ
- 毎回キャッシュを削除して新規に再生成（プロンプト変更即反映）
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

# --- ブランド用プロンプト（ルール＋代表例で一般化） ---
PROMPT_BRAND = """
あなたは自動車ブランド名の正規化を行う変換器です。入力は中国語や混在表記のブランド名です。
以下の規則に厳密に従い、日本語の最終表示用に統一してください。出力は JSON のみ。

【出力仕様】
- 返答は厳密に: {"map": {"<入力>": "<出力>", ...}}
- 入力に含まれる全てのキーを必ず含めること。
- JSON以外の文字（説明・注釈・コードブロック・末尾カンマ）は一切禁止。
- 出力は単一文字列のみ。括弧/注釈を付けない。

【基本方針】
1) 原則は「グローバルで通用する英語(ラテン)表記」を返す（例: BYD, NIO, Zeekr, Xiaomi, Volkswagen, Audi, BMW, Mercedes-Benz）。
2) それが不明な場合は「日本で広く通用する日本語ブランド名」を採用（例: トヨタ、ホンダ、日産、三菱、マツダ、スバル、スズキ、ダイハツ）。
3) それも不明な場合は「簡体字→日本語の字形（新字体）への自然置換」を行った漢字表記にする（例: 东风日产→東風日産、红旗→紅旗、长安→長安）。
4) 記号・英数字・スペースは温存（AITO, Li Auto など）。
5) ハルシネーションは禁止だが、確信が薄い場合でも「広く使われるグローバル綴りの推定」を優先し、漢字のままを避ける努力をする。

【代表例】（網羅ではない・辞書ではない。パターンを学習して一般化すること）
- 极氪 → Zeekr
- 岚图 → Voyah
- 哪吒 → Neta
- 深蓝 → Deepal
- 问界 → AITO
- 智己 → IM Motors
- 腾势 → DENZA
- 红旗 → Hongqi
- 吉利 → Geely
- 吉利银河 → Geely Galaxy
- 长安 → Changan
- 五菱汽车 → Wuling
- 奇瑞 → Chery
- 哈弗 → Haval
- 大众 → Volkswagen
- 奔驰 → Mercedes-Benz
- 宝马 → BMW
- 别克 → Buick
- 奥迪 → Audi
- 丰田 → トヨタ
- 本田 → ホンダ
- 日产 → 日産
- 东风风神 → Dongfeng Aeolus

上記の規則と代表例に従い、与えられた `items` を同様の基準で変換し、JSONのみを返す。
"""

# --- モデル用プロンプト（ルール＋代表例で一般化） ---
PROMPT_MODEL = """
あなたは自動車のモデル（車名/シリーズ名）の正規化を行う変換器です。入力は中国語や混在表記のモデル名です。
以下の規則に厳密に従い、日本語の最終表示用に統一してください。出力は JSON のみ。

【出力仕様】
- 返答は厳密に: {"map": {"<入力>": "<出力>", ...}}
- 入力に含まれる全てのキーを必ず含めること。
- JSON以外の文字（説明・注釈・コードブロック・末尾カンマ）は一切禁止。
- 出力は単一文字列のみ。括弧/注釈を付けない。

【基本方針】
E) グローバルで通用するラテン表記があればそれを採用（例: Model 3, Han, Seal, 001, SU7, Song PLUS, AION S Plus）。
F) 日本メーカーの定番モデルはカタカナ（例: シルフィー, アコード, カムリ, カローラ, シビック, フィット, プリウス, アルファード, ヤリス）。
G) 中国語固有シリーズで国際ラテンが不明なら、簡体字→日本語字形（新字体）へ自然置換（轩逸→軒逸, 星愿→星願, 海狮→海獅）。
H) グレード/派生（Pro, MAX, Plus, DM-i, EV, PHEV 等）は入力どおり維持。
I) 先頭にブランド片が付く混在（本田CR-V 等）はモデル名のみ（CR-V）を採用。
J) ハルシネーションは禁止だが、**確信が薄い場合でも、広く使われるグローバル綴りの推定を優先**し、漢字のままを避ける努力をする（例示に倣う）。

【代表例】（網羅ではない・辞書ではない。パターンを学習して一般化すること）
- 秦PLUS → Qin PLUS
- 秦L → Qin L
- 海豹 → Seal
- 海豹05 DM-i → Seal 05 DM-i
- 海豹06新能源 → Seal 06
- 海豚 → Dolphin
- 海狮06新能源 → Sea Lion 06
- 朗逸 → Lavida
- 速腾 → Sagitar
- 帕萨特 → Passat
- 探岳 → Tayron
- 途岳 → Tharu
- 星越L → Xingyue L
- 博越L → Boyue L
- 缤越 → Binyue
- 瑞虎8 → Tiggo 8
- 艾瑞泽8 → Arrizo 8
- 五菱缤果 → Bingo
- 哈弗大狗 → Big Dog
- 锋兰达 → Frontlander
- 卡罗拉锐放 → Corolla Cross
- 本田CR-V → CR-V
- 奔驰C级 → C-Class
- 宝马3系 → 3 Series
- 红旗H5 → Hongqi H5
- 问界M8 → M8
- 小米SU7 → SU7
- 银河A7 → A7

上記の規則と代表例に従い、与えられた `items` を同様の基準で変換し、JSONのみを返す。
"""

def load_cache(path: str) -> Dict[str, Dict[str, str]]:
    # キャッシュを毎回リセット
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

def requery_nonlatin(map_in: Dict[str, str], prompt: str, model: str) -> Dict[str, str]:
    # 出力にCJKが残ったキーのみ再問い合わせ（1回）
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

    # 🔁 キャッシュリセット
    cache = load_cache(args.cache)

    # ----- brand -----
    brands = sorted(set(str(x) for x in df[args.brand_col].dropna()))
    brand_map = {}
    for batch in chunked(brands, BATCH):
        part = call_llm(batch, PROMPT_BRAND, args.model)
        brand_map.update(part)
        brand_map = requery_nonlatin(brand_map, PROMPT_BRAND, args.model)
        cache["brand"] = brand_map; save_cache(args.cache, cache)

    # ----- model -----
    models = sorted(set(str(x) for x in df[args.model_col].dropna()))
    model_map = {}
    for batch in chunked(models, BATCH):
        part = call_llm(batch, PROMPT_MODEL, args.model)
        model_map.update(part)
        model_map = requery_nonlatin(model_map, PROMPT_MODEL, args.model)
        cache["model"] = model_map; save_cache(args.cache, cache)

    # ----- apply -----
    df[args.brand_ja_col] = df[args.brand_col].map(lambda x: brand_map.get(str(x), str(x)))
    df[args.model_ja_col] = df[args.model_col].map(lambda x: model_map.get(str(x), str(x)))

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"[OK] LLM-normalized (rules+examples, fresh): {args.input} -> {args.output} (rows={len(df)})")

if __name__ == "__main__":
    main()
