#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, re, json, time, argparse
import pandas as pd
from pathlib import Path
from typing import Dict, Tuple

# ==== 1) 小さな既知マップ（ブランド中心） ====
BRAND_ALIASES = {
    # 中国系
    "吉利": "ジーリー",
    "吉利汽车": "ジーリー",
    "比亚迪": "BYD",
    "五菱": "五菱",             # 迷えば原文（漢字）でOK
    "奇瑞": "チェリー",
    "长安": "長安",
    "上汽大众": "フォルクスワーゲン",
    "上汽大眾": "フォルクスワーゲン",
    "上汽": "上汽",
    "广汽丰田": "トヨタ",       # 厳密に「広汽トヨタ」だが、簡易にトヨタ採用
    "广汽": "広汽",
    "小鹏": "シャオペン",
    "小鵬": "シャオペン",
    "蔚来": "ニオ",
    "理想": "リーオート",
    "问界": "アイト",           # AITO（アイト）。好みで英字のままも可
    "AITO": "アイト",

    # 欧米・日系
    "大众": "フォルクスワーゲン",
    "奥迪": "アウディ",
    "丰田": "トヨタ",
    "日产": "日産",
    "本田": "ホンダ",
    "梅赛德斯-奔驰": "メルセデス・ベンツ",
    "奔驰": "メルセデス・ベンツ",
    "宝马": "BMW",
    "特斯拉": "テスラ",
    "雪佛兰": "シボレー",
    "别克": "ビュイック",
    "奥迪": "アウディ",
    "保时捷": "ポルシェ",
    "红旗": "紅旗",
}

# 車種の記号・サフィックス類は基本「翻訳しない」
MODEL_PROTECT_TOKENS = (
    r"(?i)\b(plus|pro|max|dm-i|dm|ev|hev|phev|mhev|se|gt|gl|gs|l|s|x|rs)\b",
)
MODEL_PROTECT_RE = re.compile("|".join(MODEL_PROTECT_TOKENS))

# すでに英数字が中心なら翻訳しない（例：Model 3 / SU7 / RAV4）
MOSTLY_LATIN = re.compile(r"^[A-Za-z0-9\-\s\+\.]+$")

# かなを含むか（LLMが日本語化したかを見る）
HAS_KANA = re.compile(r"[ぁ-ゟ゠-ヿ]")

# キャッシュ
CACHE_PATH = Path("cache/ja_alias.json")

def load_cache() -> Dict[str, str]:
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cache(cache: Dict[str, str]):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

# ==== LLM 呼び出し（OpenAI互換） ====
def call_llm(prompt: str, model: str = "gpt-4o-mini") -> str:
    import openai
    client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
        max_tokens=64,
    )
    return resp.choices[0].message.content.strip()

def conservative_brand_ja(brand: str, model_name: str, cache: Dict[str, str], llm_model: str) -> str:
    key = f"B::{brand}"
    if key in cache:
        return cache[key]

    # 1) 既知マップ優先
    for k, v in BRAND_ALIASES.items():
        if k == brand or k in brand:
            cache[key] = v
            return v

    # 2) 英字・既にカタカナ → そのまま
    if MOSTLY_LATIN.match(brand) or HAS_KANA.search(brand):
        cache[key] = brand
        return brand

    # 3) LLM：広く使われる日本語表記があるときだけ日本語に。なければ原文（漢字）のまま
    prompt = f"""以下は自動車ブランドの表記です。日本語市場で広く定着した呼称がある場合のみ日本語（カタカナ等）で1語で返答。ない場合は原文のまま返す。
- 不要な説明は禁止。出力は1語のみ。
- あいまいな場合は原文のまま。

対象: {brand}
"""
    try:
        out = call_llm(prompt, model=llm_model)
        # 出力が不適切なら原文にフォールバック
        if not out or len(out) > 20 or "\n" in out:
            out = brand
    except Exception:
        out = brand

    cache[key] = out
    return out

def conservative_model_ja(model_name: str, brand_ja: str, cache: Dict[str, str], llm_model: str) -> str:
    key = f"M::{brand_ja}::{model_name}"
    if key in cache:
        return cache[key]

    # 1) 既に英字中心 or 守るべきトークンを含む → そのまま
    if MOSTLY_LATIN.match(model_name) or MODEL_PROTECT_RE.search(model_name):
        cache[key] = model_name
        return model_name

    # 2) すでにカナ混在 → そのまま（=訳済み扱い）
    if HAS_KANA.search(model_name):
        cache[key] = model_name
        return model_name

    # 3) LLM：通称が明確にある場合のみカタカナ。なければ原文のまま（漢字）
    prompt = f"""以下は自動車の車種名です。日本のメディアで広く使われるカタカナ通称が明確な場合のみカタカナで1語で返答。見当たらない場合は原文そのまま返す。
- 出力は1語のみ。不要な説明や補足は出力しない。
- “Plus / Pro / DM-i / EV / L / MAX などのサフィックスは英字のまま”。
- 例：RAV4→RAV4、Model 3→Model 3、海豚→海豚（訳さない）、卡罗拉锐放→カローラクロス

車種: {model_name}
"""
    try:
        out = call_llm(prompt, model=llm_model)
        # ガードレール
        if not out or len(out) > 30 or "\n" in out:
            out = model_name
        # LLMが余計な説明したら原文
        if " " in out and len(out.split()) > 3:
            out = model_name
    except Exception:
        out = model_name

    cache[key] = out
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--model", default="gpt-4o-mini")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    if not {"brand", "model"}.issubset(df.columns):
        raise SystemExit("input CSVに brand / model 列が必要です。")

    cache = load_cache()

    brand_ja_list, model_ja_list = [], []
    for _, row in df.iterrows():
        brand = str(row["brand"]).strip()
        model_name = str(row["model"]).strip()

        b_ja = conservative_brand_ja(brand, model_name, cache, args.model)
        m_ja = conservative_model_ja(model_name, b_ja, cache, args.model)

        brand_ja_list.append(b_ja)
        model_ja_list.append(m_ja)

        # 低速すぎる環境向けの軽いスロットル
        time.sleep(0.05)

    df["brand_ja"] = brand_ja_list
    df["model_ja"] = model_ja_list
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")

    save_cache(cache)
    print(f"✅ 翻訳済みCSV: {args.output}  （brand_ja / model_ja 追加）")

if __name__ == "__main__":
    main()
