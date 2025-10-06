#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
brand / model の日本語列を追加するトランスレータ
方針:
 1) LLMに「グローバル名称（英語公称）があるならそれを返す。なければ簡体字→日本語の漢字へ」
 2) 英数字・記号はそのまま維持
 3) 厳密JSONで brand_ja, model_ja を返させ、検証に失敗したら OpenCC でフォールバック
 4) タイトル（title_raw）も渡して文脈を補強
使い方:
  python tools/translate_brand_model_ja.py \
    --input data/autohome_raw_YYYY-MM_with_brand.csv \
    --output data/autohome_raw_YYYY-MM_with_brand_ja.csv \
    --model gpt-4o-mini
"""

import argparse, json, os, sys, time, re, hashlib
import pandas as pd
from pathlib import Path

# OpenAI SDK (>=1.x)
from openai import OpenAI

# フォールバック用：簡体字→日本語の漢字（主に常用字＋書記体系）へ近似変換
# 完全ではないが、中国語簡体→日本語（s2tjp）で「化け」はだいぶ減る
try:
    from opencc import OpenCC
    cc = OpenCC('s2tjp')  # Simplified Chinese to Japanese Kanji
except Exception:
    cc = None

def norm_space(s: str) -> str:
    return re.sub(r'\s+', ' ', s or '').strip()

PROMPT = """あなたは自動車名の正規化アシスタントです。以下の制約で出力してください。

【目的】
- 入力は中国サイトから得た「ブランド名」「モデル名」「ページタイトル」です。
- 出力は JSON のみで、キーは brand_ja と model_ja です。

【変換ルール】
1) モデル名は「グローバル正式名称（英語）」が一般に存在するならそれを採用してください。
   例:  海豹→Seal, 海豚→Dolphin, 海鸥→Seagull, 元PLUS→Atto 3, 轩逸→Sylphy, 凯美瑞→Camry, 雅阁→Accord 等
   （英数字・ハイフン等の記号はそのまま）
2) グローバル正式名称が見つからない場合のみ、原語の簡体字を「日本語の漢字体系に近い字形」に変換して返してください。
   （カタカナ化は避け、英数字はそのまま、略称や創作はしない）
3) ブランド名は一般的な日本語表記（カタカナ or 英文正式社名）を優先してください。
   例: BYD, テスラ, フォルクスワーゲン, トヨタ, ホンダ, 日産, メルセデス・ベンツ, BMW など。
   迷う場合は英文既成社名（BYD, Tesla, Volkswagen, Toyota, Honda, Nissan, Mercedes-Benz, BMW 等）
4) 余計な語や注釈は一切つけず、厳密に JSON だけを返してください。

【入力】
brand(raw): {brand}
model(raw): {model}
title: {title}

【出力形式例】
{{"brand_ja":"トヨタ","model_ja":"Camry"}}
"""

def llm_translate(client: OpenAI, model: str, brand: str, model_name: str, title: str) -> dict:
    """LLMに問い合わせて brand_ja, model_ja を得る。失敗時は空を返す。"""
    prompt = PROMPT.format(brand=brand, model=model_name, title=title)
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0,
            messages=[
                {"role": "system", "content": "You are a precise normalizer that returns ONLY JSON with required keys."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=120,
        )
        txt = resp.choices[0].message.content.strip()
        # JSON以外を排除（例: ```json ... ```）
        m = re.search(r'\{.*\}', txt, flags=re.S)
        if not m:
            return {}
        obj = json.loads(m.group(0))
        out = {
            "brand_ja": norm_space(obj.get("brand_ja", "")),
            "model_ja": norm_space(obj.get("model_ja", "")),
        }
        # 最低限の検証：空や過剰長、変な接頭語をはねる
        for k in list(out.keys()):
            if not out[k] or len(out[k]) > 60:
                out[k] = ""
        return out
    except Exception:
        return {}

def fallback_jp(text: str) -> str:
    """OpenCCがあれば簡体→日本語漢字へ。なければ原文を軽く整形。"""
    t = norm_space(text)
    if not t:
        return t
    if re.fullmatch(r'[A-Za-z0-9\-\s\+\.]+', t):
        return t  # 英数字はそのまま
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
    for col in ("brand", "model"):
        if col not in df.columns:
            print(f"❌ '{col}' 列が見つかりません: {args.input}")
            sys.exit(1)
    if "title_raw" not in df.columns:
        # なくても動くようにする
        df["title_raw"] = ""

    # キャッシュ読み込み
    cache_path = Path(args.cache)
    if cache_path.exists():
        try:
            cache = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            cache = {}
    else:
        cache = {}

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY", ""))

    brand_ja_list, model_ja_list = [], []

    for i, row in df.iterrows():
        brand = str(row.get("brand", "") or "")
        model_name = str(row.get("model", "") or "")
        title = str(row.get("title_raw", "") or "")

        key = make_key(brand, model_name, title)
        got = cache.get(key)

        if not got:
            # LLM問い合わせ
            got = llm_translate(client, args.model, brand, model_name, title)
            # レート控えめ
            time.sleep(0.2)
            cache[key] = got
            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass

        b_ja = got.get("brand_ja", "") if isinstance(got, dict) else ""
        m_ja = got.get("model_ja", "") if isinstance(got, dict) else ""

        # フォールバック: 空や中国語丸残りっぽい場合は OpenCC で簡体→日本語漢字へ
        if not b_ja:
            b_ja = fallback_jp(brand)
        if not m_ja:
            # グローバル英字が入っていればそのまま通る。無ければ字形変換。
            m_ja = fallback_jp(model_name)

        brand_ja_list.append(b_ja)
        model_ja_list.append(m_ja)

    df["brand_ja"] = brand_ja_list
    df["model_ja"] = model_ja_list

    # 最後に軽く正規化：余計な二重空白など
    for col in ("brand_ja", "model_ja"):
        df[col] = df[col].map(norm_space)

    out = args.output
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"✅ 翻訳完了: {out}  ({len(df)} rows)")

if __name__ == "__main__":
    main()
