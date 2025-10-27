from __future__ import annotations
import os, sys, re, json, time
from pathlib import Path
import pandas as pd
from openai import OpenAI
from unidecode import unidecode
from opencc import OpenCC

# ====== 入出力 ======
SERIES_ID = os.environ.get("SERIES_ID", "").strip()
CSV_IN = os.environ.get("CSV_IN", "").strip()
CSV_OUT = os.environ.get("CSV_OUT", "").strip()
DST_PRIMARY = os.environ.get("DST_PRIMARY", "").strip()
DST_SECONDARY = os.environ.get("DST_SECONDARY", "").strip()

# ====== OpenAI ======
api_key = os.environ.get("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError("OPENAI_API_KEY is not set")
client = OpenAI(api_key=api_key)

# ====== 設定 ======
BATCH_SIZE = 60
RETRIES = 3
SLEEP_BASE = 1.2
PRICE_RE_JA = re.compile(r"^(?:メーカー(?:希|推)望小売価格|メーカー推奨価格|ディーラー販売価格)(?:[（(]元[）)])?$")

def normalize_zh(text: str) -> str:
    cc = OpenCC("t2s")
    return cc.convert(text)

def should_skip_translation(value: str) -> bool:
    if value is None:
        return True
    s = str(value).strip()
    if s == "" or s.lower() == "nan":
        return True
    if re.fullmatch(r"[-\d\.\,]+", s):
        return True
    if re.fullmatch(r"\d{4}款.*", s):
        return False
    if re.fullmatch(r"\d{4}(\s*年)?", s):
        return True
    return False

def translate_via_openai(text: str, lang="ja") -> str:
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "あなたは中国語→日本語翻訳アシスタントです。できるだけ簡潔に翻訳してください。"},
            {"role": "user", "content": text},
        ],
    )
    return resp.choices[0].message.content.strip()

def safe_translate(text: str) -> str:
    if should_skip_translation(text):
        return text
    for i in range(RETRIES):
        try:
            return translate_via_openai(text)
        except Exception as e:
            print(f"[warn] {e}")
            time.sleep(SLEEP_BASE * (2 ** i))
    return text

def translate_df(df: pd.DataFrame) -> pd.DataFrame:
    df["セクション_ja"] = df["セクション"].apply(safe_translate)
    df["項目_ja"] = df["項目"].apply(safe_translate)
    return df

def normalize_price(value: str) -> str:
    if pd.isna(value):
        return value
    s = str(value).strip()
    s = s.replace("計算機", "").replace("询底价", "")
    s = re.sub(r"\s+", " ", s)
    return s

def process_csv(in_path: str, out_path: str):
    df = pd.read_csv(in_path)
    df["項目"] = df["項目"].astype(str).map(lambda s: normalize_zh(s))
    df["値"] = df["値"].astype(str).map(lambda s: normalize_price(s))
    df = translate_df(df)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"✅ Saved {out_path}")

if __name__ == "__main__":
    src = CSV_IN or (f"output/autohome/{SERIES_ID}/config_{SERIES_ID}.csv" if SERIES_ID else "")
    dst = CSV_OUT or DST_PRIMARY or (f"output/autohome/{SERIES_ID}/config_{SERIES_ID}.ja.csv")
    if not src or not Path(src).exists():
        print(f"Source not found: {src}")
        sys.exit(1)
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    process_csv(src, dst)
