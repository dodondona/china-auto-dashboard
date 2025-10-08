#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
中国名→グローバル/日本語 正規化（LLM + 最小ルール後処理）
- 既存構造は維持（入出力・キャッシュ削除・LLMバッチ・厳格JSON）
- 毎回キャッシュを削除
- LLM出力の揺らぎを、最小のルールベースで確定修正（日本ブランド名、BYD海洋、VW中国専売名、宏光MINIEVの括弧位置 等）
"""

import argparse, json, os, time, sys
from typing import Dict, List
import pandas as pd
import regex as re2  # pip install regex

# =========== 既存の基本設定 ===========
LATIN_RE = re2.compile(r"^[\p{Latin}\p{Number}\s\-\+\/\.\(\)]+$")
HAS_CJK  = re2.compile(r"\p{Han}")

DEF_MODEL = "gpt-4o-mini"
BATCH = 50
RETRY = 3
SLEEP = 1.0

# =========== プロンプト（既存方針を堅持、先頭に“ChatGPT本体と同等の理解力で”を追記） ===========
PROMPT_BRAND = """
あなたは自動車ブランド名の正規化を行う変換器です。ChatGPT本体と同等の理解力で判断してください。入力は中国語や混在表記のブランド名です。
以下の規則に厳密に従い、日本語での最終表示用に統一してください。出力は JSON のみ。

【出力仕様】
- 返答は厳密に: {"map": {"<入力>": "<出力>", ...}}
- 入力に含まれる全てのキーを必ず含めること。
- JSON以外の文字（説明・注釈・コードブロック・末尾カンマ）は一切禁止。

【共通ルール】
1) でたらめ禁止。確信が持てない場合は**入力をそのまま返す**。
2) 記号・英数字・スペースは温存（例: "AITO", "BYD", "Geely", "XPeng"）。
3) 出力は**単一文字列**のみ。括弧/注釈を付けない。

【ブランドの優先順序】
A) **グローバルで通用するラテン表記が明確**なら、その綴りを採用（例: "BYD", "Geely", "XPeng", "AITO", "Xiaomi", "Volkswagen", "Audi", "BMW"）。
B) Aに該当せず、**日本で広く通用する日本語ブランド名**が明確な場合は日本語表記（例: "トヨタ", "ホンダ", "日産", "三菱", "マツダ", "スバル", "スズキ", "ダイハツ", "メルセデス・ベンツ", "アウディ", "フォルクスワーゲン", "ビュイック"）。※確信がなければ適用しない。
C) それ以外（中国語のみ等）で**国際的ラテン表記が不明**な場合は、**簡体字→日本語の字形（新字体）**に自然置換した漢字表記にする（例: "东风日产"→"東風日産", "红旗"→"紅旗"）。
D) ジョイント・ベンチャー名は、A/B/Cの方針で**単一の最上位ブランド表示**に統一してよいが、確信なき場合はCを採用。
"""

PROMPT_MODEL = """
あなたは自動車のモデル（車名/シリーズ名）の正規化を行う変換器です。ChatGPT本体と同等の理解力で判断してください。入力は中国語や混在表記のモデル名です。
以下の規則に厳密に従い、日本語での最終表示用に統一してください。出力は JSON のみ。

【出力仕様】
- 返答は厳密に: {"map": {"<入力>": "<出力>", ...}}
- 入力に含まれる全てのキーを必ず含めること。
- JSON以外の文字（説明・注釈・コードブロック・末尾カンマ）は一切禁止。

【共通ルール】
1) でたらめ禁止。確信が持てない場合は**入力をそのまま返す**。
2) 記号・英数字・スペースは温存（例: "Model 3", "AION S Plus", "001", "SU7", "e:HEV", "DM-i", "Pro", "MAX"）。
3) 出力は**単一文字列**のみ。括弧/注釈を付けない。

【モデルの優先順序】
E) **グローバルで通用するラテン表記のモデル名**がある場合は、そのラテン表記を採用（例: "Model 3", "Lavida", "Sagitar", "Magotan", "Tayron", "Tharu", "Tiguan L", "Dolphin", "Seal", "Seagull", "Sea Lion"）。
F) **日本市場で長年に通用する日本メーカーの定番モデル名**はカタカナ表記優先（例: シルフィー, カムリ, カローラ, シビック, アコード, RAV4）。※確信なければ E を優先。
G) 中国語の固有シリーズ名で**国際的ラテン表記が不明**な場合は、**簡体字→日本語の字形（新字体）**へ自然置換＋必要なら**括弧内にピンイン**を付与（例: "轩逸"→"シルフィー", "秦PLUS"→"秦（Qin）PLUS", "宏光MINIEV"→"宏光（Hongguang）MINIEV"）。
H) グレード/派生（"Pro", "MAX", "Plus", "DM-i", "EV", "PHEV" 等）は入力のまま維持。
I) 先頭に中国語ブランド片が付いている場合（例: "本田CR-V"）はブランド片を除去しモデル名のみを残す（"CR-V"）。
"""

# =========== ここからは元の処理系 ===========
def is_latin(x: str) -> bool:
    return isinstance(x, str) and LATIN_RE.match((x or "").strip()) is not None

def load_cache(path: str) -> Dict[str, Dict[str, str]]:
    # 起動毎に削除（要望通り）: 存在すれば消す
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

# =========== 最小限の後処理（ここが今回の“崩さない改善”の肝） ===========
PINYIN = {
    "秦": "Qin",
    "宋": "Song",
    "宏光": "Hongguang",
}

def postprocess_brand(name: str) -> str:
    if not name:
        return name
    # 日本で通るブランド名を確定
    # ※出力が英語になりがちなものを日本語へ揃える
    table = {
        "Toyota": "トヨタ", "丰田": "トヨタ", "豐田": "トヨタ", "トヨタ": "トヨタ",
        "Honda": "ホンダ", "本田": "ホンダ",
        "Nissan": "日産", "日产": "日産",
        "Volkswagen": "フォルクスワーゲン", "大众": "フォルクスワーゲン",
        "Buick": "ビュイック", "别克": "ビュイック",
        "Mercedes-Benz": "メルセデス・ベンツ", "奔驰": "メルセデス・ベンツ",
        "Audi": "アウディ", "奥迪": "アウディ",
        "BMW": "BMW", "宝马": "BMW",
        "Wuling": "五菱", "五菱": "五菱", "五菱汽车": "五菱",
        "BYD": "BYD", "比亚迪": "BYD",
        "Geely": "Geely", "吉利": "Geely", "吉利汽车": "Geely",
        "Geely Galaxy": "Geely Galaxy", "吉利银河": "Geely Galaxy",
        "XPeng": "XPeng", "小鹏": "XPeng",
        "Chery": "Chery", "奇瑞": "Chery",
        "Changan": "Changan", "长安": "Changan", "长安汽车": "Changan",
        "AITO": "AITO", "赛力斯": "AITO", "塞力斯": "AITO",
        "Xiaomi": "Xiaomi", "小米汽车": "Xiaomi", "小米": "Xiaomi",
        "Haval": "Haval", "哈弗": "Haval",
        "Hongqi": "紅旗", "红旗": "紅旗",
        "Leapmotor": "Leapmotor", "零跑汽车": "Leapmotor", "零跑": "Leapmotor",
    }
    return table.get(name, name)

def _ensure_paren(base_cn: str, tail: str) -> str:
    """秦/宋/宏光など：中国漢字＋（Pinyin）＋派生を強制"""
    py = PINYIN.get(base_cn)
    if not py:
        return base_cn + tail
    return f"{base_cn}（{py}）{tail}"

def postprocess_model(raw: str, brand_norm: str) -> str:
    if not raw:
        return raw
    s = raw.strip()

    # --- まずは既知の“英名で出すべき”モデルを確定 ---
    # BYD海洋シリーズ（英名固定）
    # 海豚 = Dolphin, 海豹 = Seal, 海鸥 = Seagull, 海狮 = Sea Lion
    s = re2.sub(r"^海豚(.*)$", r"Dolphin\1", s)
    s = re2.sub(r"^海豹(.*)$", r"Seal\1", s)
    s = re2.sub(r"^海鸥(.*)$", r"Seagull\1", s)
    s = re2.sub(r"^海狮(.*)$", r"Sea Lion\1", s)
    # 余計な“中国語＋英語”の二重表記を整理（例: RAV4（RAV4）→RAV4）
    s = re2.sub(r"^RAV4（RAV4）$", "RAV4", s)

    # VW中国専売（英名）
    vw_map = {
        "朗逸": "Lavida",
        "速腾": "Sagitar",
        "迈腾": "Magotan",
        "途岳": "Tharu",
        "探岳": "Tayron",
        "途观L": "Tiguan L",
        "帕萨特": "Passat",
    }
    if s in vw_map:
        s = vw_map[s]

    # トヨタの中国向け呼称
    toyota_map = {
        "卡罗拉锐放": "カローラクロス",
        "凯美瑞": "カムリ",
        "RAV4荣放": "RAV4",
        "锋兰达": "Frontlander",
    }
    if s in toyota_map:
        s = toyota_map[s]

    # 日本定番名
    jp_fix = {
        "轩逸": "シルフィー",
        "雅阁": "アコード",
        "本田CR-V": "CR-V",
    }
    if s in jp_fix:
        s = jp_fix[s]

    # 宏光MINIEV の括弧位置固定
    s = re2.sub(r"^宏光\s*MINI\s*EV$", "宏光（Hongguang）MINIEV", s, flags=re2.IGNORECASE)
    s = s.replace("宏光MINIEV", "宏光（Hongguang）MINIEV")

    # 秦 / 宋：漢字＋(Pinyin)＋派生を強制
    s = re2.sub(r"^秦\s*(PLUS|L)(.*)$", lambda m: _ensure_paren("秦", m.group(1)+m.group(2)), s)
    s = re2.sub(r"^宋\s*(PLUS|Pro)(.*)$", lambda m: _ensure_paren("宋", m.group(1)+m.group(2)), s)

    # 余計なメーカー名プレフィックス除去（例: 本田CR-V → CR-V）
    s = re2.sub(r"^(本田|ホンダ)\s*CR-?V$", "CR-V", s, flags=re2.IGNORECASE)

    return s

# =========== メイン処理 ===========
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

    # ----- apply + 最小後処理 -----
    def brand_norm(x: str) -> str:
        return postprocess_brand(brand_map.get(str(x), str(x)))

    def model_norm(x: str, bja: str) -> str:
        base = model_map.get(str(x), str(x))
        return postprocess_model(base, bja)

    df[args.brand_ja_col] = df[args.brand_col].map(brand_norm)
    df[args.model_ja_col]  = [
        model_norm(m, b) for m, b in zip(df[args.model_col], df[args.brand_ja_col])
    ]

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"[OK] LLM-normalized: {args.input} -> {args.output} (rows={len(df)})")

if __name__ == "__main__":
    main()
