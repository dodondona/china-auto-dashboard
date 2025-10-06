#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse, json, os, re, time, hashlib
from pathlib import Path
import pandas as pd
from openai import OpenAI

# 簡体→日本語漢字の近似（フォールバック）
try:
    from opencc import OpenCC
    cc = OpenCC('s2tjp')
except Exception:
    cc = None

def norm_space(s: str) -> str:
    return re.sub(r'\s+', ' ', s or '').strip()

# 👉 ここがコア：最小限の「カタカナ優先ルール」
JP_BRAND_CANON = {
    # brand_ja 正規化候補
    "トヨタ": {"トヨタ","Toyota","TOYOTA"},
    "ホンダ": {"ホンダ","Honda","HONDA"},
    "日産":   {"日産","Nissan","NISSAN"},
}
# モデル名の「英語グローバル名 → カタカナ」最小セット
JP_MODEL_KATA = {
    # Nissan
    "Sylphy": "シルフィ",
    "Serena": "セレナ",
    "X-Trail": "エクストレイル",
    "March": "マーチ",
    # Honda
    "Accord": "アコード",
    "Civic": "シビック",
    "Fit": "フィット",
    "Vezel": "ヴェゼル",
    # Toyota
    "Camry": "カムリ",
    "Corolla": "カローラ",
    "Corolla Cross": "カローラクロス",
    "Yaris": "ヤリス",
    "Alphard": "アルファード",
    "Voxy": "ヴォクシー",
    "Noah": "ノア",
    "Crown": "クラウン",
    "Land Cruiser": "ランドクルーザー",
    "Land Cruiser Prado": "ランドクルーザープラド",
    "RAV4": "RAV4",  # これのみ英記が一般的
}

# brandが日本メーカーかどうかチェック
def is_jp_brand(brand_ja: str) -> bool:
    b = norm_space(brand_ja)
    for k, variants in JP_BRAND_CANON.items():
        if b in variants or b == k:
            return True
    return b in {"トヨタ","ホンダ","日産"}

def kata_override(brand_ja: str, model_en: str) -> str:
    """日本メーカーの場合、英語モデル名の一部をカタカナに置換（最小ルール）"""
    if not is_jp_brand(brand_ja):
        return model_en
    m = norm_space(model_en)
    # 最長一致を先に
    for key in sorted(JP_MODEL_KATA.keys(), key=len, reverse=True):
        if key.lower() == m.lower():
            return JP_MODEL_KATA[key]
    return m

PROMPT = """あなたは自動車名の正規化アシスタントです。以下の制約で出力してください。

【目的】
- 入力は中国サイトから得た「ブランド名」「モデル名」「ページタイトル」です。
- 出力は JSON のみで、キーは brand_ja と model_ja です。

【変換ルール】
1) モデル名は「グローバル正式名称（英語）」が一般に存在するならそれを採用。
   例: 海豹→Seal, 海豚→Dolphin, 海鸥→Seagull, 元PLUS→Atto 3, 轩逸→Sylphy, 凯美瑞→Camry 等
2) 見つからない場合のみ、原語の簡体字を「日本語の漢字体系に近い字形」で返す。
3) ブランド名は一般的な日本語表記（カタカナ or 英文既成社名）を優先。
   例: BYD, テスラ, フォルクスワーゲン, トヨタ, ホンダ, 日産, メルセデス・ベンツ, BMW 等
4) 余計な語や注釈は一切つけず、厳密に JSON だけを返す。

【入力】
brand(raw): {brand}
model(raw): {model}
title: {title}

【出力】
{{"brand_ja":"...","model_ja":"..."}}
"""

def llm_translate(client: OpenAI, model: str, brand: str, model_name: str, title: str) -> dict:
    prompt = PROMPT.format(brand=brand, model=model_name, title=title)
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0,
            messages=[
                {"role": "system", "content": "Return ONLY JSON with keys brand_ja and model_ja."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=120,
        )
        txt = resp.choices[0].message.content.strip()
        m = re.search(r'\{.*\}', txt, flags=re.S)
        if not m:
            return {}
        obj = json.loads(m.group(0))
        return {
            "brand_ja": norm_space(obj.get("brand_ja", "")),
            "model_ja": norm_space(obj.get("model_ja", "")),
        }
    except Exception:
        return {}

def fallback_jp(text: str) -> str:
    t = norm_space(text)
    if not t:
        return t
    if re.fullmatch(r'[A-Za-z0-9\-\s\+\.]+', t):
        return t
    if cc:
        try:
            return norm_space(cc.convert(t))
        except Exception:
            pass
    return t

def make_key(brand: str, model: str, title: str) -> str:
    s = json.dumps([brand, model, title], ensure_ascii=False)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--model", default="gpt-4o-mini")
    ap.add_argument("--cache", default="data/.translate_brand_model_ja.cache.json")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    if "title_raw" not in df.columns:
        df["title_raw"] = ""

    # キャッシュ
    cache_path = Path(args.cache)
    cache = {}
    if cache_path.exists():
        try:
            cache = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY",""))

    brand_ja_list, model_ja_list = [], []

    for _, row in df.iterrows():
        brand_raw = str(row.get("brand","") or "")
        model_raw = str(row.get("model","") or "")
        title     = str(row.get("title_raw","") or "")
        key = make_key(brand_raw, model_raw, title)

        got = cache.get(key)
        if not got:
            got = llm_translate(client, args.model, brand_raw, model_raw, title)
            time.sleep(0.2)
            cache[key] = got
            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass

        b_ja = got.get("brand_ja","") if isinstance(got, dict) else ""
        m_ja = got.get("model_ja","") if isinstance(got, dict) else ""

        # 最低限検証
        if not b_ja:
            b_ja = fallback_jp(brand_raw)
        if not m_ja:
            m_ja = fallback_jp(model_raw)

        # ✅ 日本メーカーなら主要モデルだけカタカナ優先
        m_ja = kata_override(b_ja, m_ja)

        brand_ja_list.append(b_ja)
        model_ja_list.append(m_ja)

    df["brand_ja"] = [norm_space(x) for x in brand_ja_list]
    df["model_ja"] = [norm_space(x) for x in model_ja_list]

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"✅ 翻訳完了: {args.output}  ({len(df)} rows)")

if __name__ == "__main__":
    main()
