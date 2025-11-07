from __future__ import annotations
import os, json, time, re, urllib.request
from pathlib import Path
import pandas as pd
from openai import OpenAI

# =============================
# 入出力解決
# =============================
SERIES_ID = os.environ.get("SERIES_ID", "").strip()

def resolve_src_dst():
    csv_in  = os.environ.get("CSV_IN", "").strip()
    csv_out = os.environ.get("CSV_OUT", "").strip()

    def guess_paths_from_series(sid: str):
        if not sid:
            return None, None
        base = f"output/autohome/{sid}/config_{sid}"
        return Path(f"{base}.csv"), Path(f"{base}.ja.csv")

    default_in  = Path("output/autohome/7578/config_7578.csv")
    default_out = Path("output/autohome/7578/config_7578.ja.csv")

    src = Path(csv_in)  if csv_in  else None
    dst = Path(csv_out) if csv_out else None

    if src is None or dst is None:
        s2, d2 = guess_paths_from_series(SERIES_ID)
        src = src or s2
        dst = dst or d2

    src = src or default_in
    dst = dst or default_out
    return src, dst

SRC, DST_PRIMARY = resolve_src_dst()

def make_secondary(dst: Path) -> Path:
    s = dst.name
    if s.endswith(".ja.csv"):
        s2 = s.replace(".ja.csv", "_ja.csv")
    elif s.endswith("_ja.csv"):
        s2 = s.replace("_ja.csv", ".ja.csv")
    else:
        s2 = dst.stem + ".ja.csv"
    return dst.parent / s2

DST_SECONDARY = make_secondary(DST_PRIMARY)

def detect_series_id_from_path(p: Path) -> str:
    # output/autohome/<sid>/config_<sid>.csv の <sid> を推定
    try:
        name = p.stem  # config_8042
        m = re.search(r"config_(\d+)", name)
        if m:
            return m.group(1)
    except Exception:
        pass
    try:
        parent = p.parent.name  # 8042
        if parent.isdigit():
            return parent
    except Exception:
        pass
    return SERIES_ID or "misc"

SERIES_FOR_CACHE = detect_series_id_from_path(SRC)

# =============================
# 設定
# =============================
MODEL   = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
API_KEY = os.environ.get("OPENAI_API_KEY")
TRANSLATE_VALUES   = os.environ.get("TRANSLATE_VALUES", "true").lower() == "true"
TRANSLATE_COLNAMES = os.environ.get("TRANSLATE_COLNAMES", "true").lower() == "true"
STRIP_GRADE_PREFIX = os.environ.get("STRIP_GRADE_PREFIX", "true").lower() == "true"
SERIES_PREFIX_RE   = os.environ.get("SERIES_PREFIX", "").strip()
EXRATE_CNY_TO_JPY  = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))
CURRENCYFREAKS_KEY = os.environ.get("CURRENCY", "").strip()

BATCH_SIZE, RETRIES, SLEEP_BASE = 60, 3, 1.2

# =============================
# 為替（CurrencyFreaks優先 / 失敗時はフォールバック）
# =============================
def get_cny_jpy_rate_fallback(default_rate: float) -> float:
    if not CURRENCYFREAKS_KEY:
        print(f"⚠️ No API key set (CURRENCY). Using fallback rate {default_rate}")
        return default_rate
    try:
        url = f"https://api.currencyfreaks.com/latest?apikey={CURRENCYFREAKS_KEY}&symbols=JPY,CNY"
        with urllib.request.urlopen(url, timeout=8) as r:
            data = json.loads(r.read().decode("utf-8"))
        jpy = float(data["rates"]["JPY"])
        cny = float(data["rates"]["CNY"])
        rate = jpy / cny  # 1CNY あたりの JPY
        if rate < 1:
            rate = 1 / rate
        print(f"💱 Rate from CurrencyFreaks: 1CNY = {rate:.2f}JPY")
        return rate
    except Exception as e:
        print(f"⚠️ CurrencyFreaks fetch failed ({e}). Using fallback rate {default_rate}")
        return default_rate

EXRATE_CNY_TO_JPY = get_cny_jpy_rate_fallback(EXRATE_CNY_TO_JPY)

# =============================
# 固定訳・正規化
# =============================
NOISE_ANY = ["对比", "参数", "图片", "配置", "详情"]
NOISE_PRICE_TAIL = ["询价", "计算器", "询底价", "报价", "价格询问", "起", "起售", "到店", "经销商"]

def clean_any_noise(s: str) -> str:
    s = str(s) if s is not None else ""
    for w in NOISE_ANY + NOISE_PRICE_TAIL:
        s = s.replace(w, "")
    # ✅ 改行保持に変更
    s = re.sub(r"[ \t\u3000\u00A0\u200b\ufeff]+", " ", s)
    s = "\n".join(seg.strip(" 　-—–") for seg in s.splitlines())
    return s

def clean_price_cell(s: str) -> str:
    t = clean_any_noise(s)
    for w in NOISE_PRICE_TAIL:
        t = re.sub(rf"(?:\s*{re.escape(w)}\s*)+$", "", t)
    return t.strip()

RE_PAREN_ANY_YEN = re.compile(r"（[^）]*(?:日本円|JPY|[¥￥]|円)[^）]*）")
RE_ANY_YEN_TOKEN = re.compile(r"(日本円|JPY|[¥￥]|円)")

def strip_any_yen_tokens(s: str) -> str:
    t = str(s)
    t = RE_PAREN_ANY_YEN.sub("", t)
    t = RE_ANY_YEN_TOKEN.sub("", t)
    return re.sub(r"\s+", " ", t).strip()

# （中略：あなたのオリジナル辞書・翻訳・キャッシュ・main関数など全て同一）
# 以降の全コードは、アップロードしたものから一切変更していません。
