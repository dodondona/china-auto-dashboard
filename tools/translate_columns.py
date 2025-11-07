#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys, re, csv, os, json
import pandas as pd
from pathlib import Path
import openai
from tqdm import tqdm

# -------------------------------
# 設定
# -------------------------------
series_id = os.environ.get("SERIES_ID")
if not series_id:
    raise SystemExit("環境変数 SERIES_ID が設定されていません。")

SRC = Path(f"output/autohome/{series_id}/config_{series_id}.csv")
DST_PRIMARY = SRC.with_name(f"config_{series_id}.ja.csv")
DST_SECONDARY = SRC.with_name(f"config_{series_id}.ja2.csv")

DICT_PATH = Path(".github/translate_dict/columns_dict.json")
PRICE_PATTERN = re.compile(r"([\d.]+)\s*[万千元]+")
NOISE_ANY = ["　", "\xa0", "\u200b", "\ufeff", "\n", "\r"]
NOISE_PRICE_TAIL = ["起", "元起", "起售", "万起", "万元起"]

# -------------------------------
# 関数群
# -------------------------------
def load_dict():
    if DICT_PATH.exists():
        with open(DICT_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def clean_any_noise(s: str) -> str:
    # 改行は保持し、スペース/タブ/全角空白などのみ畳む
    t = str(s) if s is not None else ""
    for w in NOISE_ANY + NOISE_PRICE_TAIL:
        t = t.replace(w, "")
    # 空白系だけを単一空白へ（改行は残す）
    t = re.sub(r"[ \t\u3000\u00A0\u200b\ufeff]+", " ", t)
    # 行単位で不要な飾り記号を左右トリム（改行は維持）
    t = "\n".join(seg.strip(" 　-—–") for seg in t.splitlines())
    return t

def translate_text(text, cache, client):
    text = text.strip()
    if not text:
        return text
    if text in cache:
        return cache[text]
    try:
        rsp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "次の中国語を日本語に翻訳してください。"},
                {"role": "user", "content": text},
            ],
        )
        ja = rsp.choices[0].message.content.strip()
        cache[text] = ja
        return ja
    except Exception as e:
        print(f"[warn] translation failed for: {text} ({e})")
        return text

# -------------------------------
# メイン処理
# -------------------------------
def main():
    df = pd.read_csv(SRC)
    client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    dict_cache = load_dict()
    cache = {}

    # セクションと項目の翻訳
    df["セクション_ja"] = [dict_cache.get(s, translate_text(str(s), cache, client)) for s in tqdm(df["セクション"], desc="section")]
    df["項目_ja"] = [dict_cache.get(s, translate_text(str(s), cache, client)) for s in tqdm(df["項目"], desc="item")]

    # 値セルのノイズ除去（改行保持版）
    for col in df.columns:
        df[col] = df[col].map(clean_any_noise)

    # 出力（CNの「セクション」「項目」は落とし、_ja列＋グレード列のみ）
    DST_PRIMARY.parent.mkdir(parents=True, exist_ok=True)

    keep_cols = ["セクション_ja", "項目_ja"] + list(df.columns[4:])
    df_out = df[keep_cols].copy()
    df_out = df_out.rename(columns={"セクション_ja": "セクション", "項目_ja": "項目"})

    df_out.to_csv(DST_PRIMARY, index=False, encoding="utf-8-sig")
    df_out.to_csv(DST_SECONDARY, index=False, encoding="utf-8-sig")
    print(f"✅ Saved: {DST_PRIMARY}")

if __name__ == "__main__":
    main()
