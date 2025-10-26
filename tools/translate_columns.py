import os
import re
import time
from pathlib import Path
import pandas as pd
from openai import OpenAI

# ===== 基本設定 =====
CACHE_DIR = Path("cache_repo/series")     # リポジトリ内キャッシュ保存先（CN/JA）
OUTPUT_DIR = Path("output/autohome")      # 出力ディレクトリ
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

RETRIES = 3
SLEEP_BASE = 1.2  # リトライ待機係数

# ===== 判定用 =====
RE_NUMERIC_LIKE = re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")
RE_WAN = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*万")
RE_YUAN = re.compile(r"(?P<num>[\d,]+)\s*元")

def is_blank_or_symbol(text: str) -> bool:
    t = str(text).strip()
    return t in {"", "-", "—", "—-", "●", "○"}

def is_numeric_like(text: str) -> bool:
    return bool(RE_NUMERIC_LIKE.fullmatch(str(text).strip()))

# ===== 価格整形（448相当） =====
def parse_cny(text: str):
    t = str(text)
    m1 = RE_WAN.search(t)
    if m1:
        return float(m1.group("num")) * 10000.0
    m2 = RE_YUAN.search(t)
    if m2:
        return float(m2.group("num").replace(",", ""))
    return None

def msrp_to_yuan_and_jpy(cell: str, rate_jpy_per_cny: float) -> str:
    t = str(cell).strip()
    if not t or t in {"-", "–", "—"}:
        return t
    cny = parse_cny(t)
    if cny is None:
        # “万” だけで “元” がない場合の補正
        if ("元" not in t) and RE_WAN.search(t):
            t = f"{t}元"
        return t
    m1 = RE_WAN.search(t)
    yuan_disp = f"{m1.group('num')}万元" if m1 else (t if "元" in t else f"{t}元")
    jpy = int(round(cny * rate_jpy_per_cny))
    return f"{yuan_disp}（日本円{jpy:,}円）"

def dealer_to_yuan_only(cell: str) -> str:
    t = str(cell).strip()
    if not t or t in {"-", "–", "—"}:
        return t
    if ("元" not in t) and RE_WAN.search(t):
        t = f"{t}元"
    return t

# ===== LLM翻訳（型安全＋3回リトライ） =====
def translate_text(client: OpenAI, text: object) -> str:
    # None/NaN/数値/float対応
    if text is None:
        return ""
    if isinstance(text, float):
        if pd.isna(text):
            return ""
        text = str(text)
    elif isinstance(text, int):
        text = str(text)
    elif not isinstance(text, str):
        text = str(text)

    t = text.strip()
    if is_blank_or_symbol(t) or is_numeric_like(t):
        return t

    for attempt in range(1, RETRIES + 1):
        try:
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "You are a professional translator that translates Chinese to Japanese accurately."},
                    {"role": "user", "content": text}
                ],
            )
            return (resp.choices[0].message.content or "").strip()
        except Exception as e:
            print(f"⚠ 翻訳失敗 ({attempt}/{RETRIES}) {e}")
            if attempt == RETRIES:
                return t
            time.sleep(SLEEP_BASE * attempt)
    return t

# ===== CSV安全読込（文字化け対策） =====
def safe_read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)

# ===== メイン =====
def main():
    series_id = (os.environ.get("SERIES_ID") or "unknown").strip()
    csv_in_env = (os.environ.get("CSV_IN") or "").strip()
    csv_out_env = (os.environ.get("CSV_OUT") or "").strip()
    rate = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))

    # 入力：CSV_IN 優先。無ければ output/autohome/<id>/config_<id>.csv
    if csv_in_env and Path(csv_in_env).exists():
        SRC = Path(csv_in_env)
    else:
        SRC = OUTPUT_DIR / series_id / f"config_{series_id}.csv"

    # 出力：CN(原文)とJA(翻訳)を 448 と同じ命名で作成
    CN_OUT = OUTPUT_DIR / series_id / f"config_{series_id}.csv"                 # 原文（そのまま）
    JA_OUT = OUTPUT_DIR / series_id / f"config_{series_id}.ja.csv"              # 翻訳版（_ja列を含む）
    JA_OUT_COMPAT = OUTPUT_DIR / series_id / f"config_{series_id}_ja.csv"       # 互換名

    # リポジトリ内キャッシュ（CN/JA）
    CACHE_CN = CACHE_DIR / series_id / "cn.csv"
    CACHE_JA = CACHE_DIR / series_id / "ja.csv"

    print(f"🔎 SRC: {SRC}")
    print(f"📝 CN:  {CN_OUT}")
    print(f"📝 JA:  {JA_OUT}")

    if not SRC.exists():
        print(f"⚠ 入力CSVが見つかりません（スキップ）: {SRC}")
        return

    df_cn = safe_read_csv(SRC)
    if df_cn.empty:
        print("⚠ 入力CSVが空です。スキップします。")
        return

    # 原文（CN）を “そのまま” 保存（Excel互換の BOM）
    CN_OUT.parent.mkdir(parents=True, exist_ok=True)
    df_cn.to_csv(CN_OUT, index=False, encoding="utf-8-sig")

    # 既存キャッシュ（前回CN/JA）読込（存在すれば）
    df_cn_prev, df_ja_prev = None, None
    try:
        if CACHE_CN.exists():
            df_cn_prev = safe_read_csv(CACHE_CN)
        if CACHE_JA.exists():
            df_ja_prev = safe_read_csv(CACHE_JA)
        if (df_cn_prev is not None) and (df_ja_prev is not None):
            print("✅ 既存キャッシュ読み込み完了")
    except Exception as e:
        print(f"⚠ キャッシュ読み込み失敗: {e}")

    client = OpenAI(api_key=OPENAI_API_KEY)

    # === 448 準拠の JA 出力 ===
    out = df_cn.copy()

    # 1) セクション_ja / 項目_ja を “CNの右隣” に追加
    if "セクション_ja" not in out.columns:
        out.insert(1, "セクション_ja", "")
    if "項目_ja" not in out.columns:
        # ※ セクション_jaを入れたので、項目_ja は列インデックス 3 になる
        out.insert(3, "項目_ja", "")

    # 2) セクション/項目の翻訳（前回と同一CNなら前回JAを流用）
    for i, row in out.iterrows():
        cn_sec = row.get("セクション", "")
        cn_itm = row.get("項目", "")
        ja_sec = None
        ja_itm = None

        if (df_cn_prev is not None) and (df_ja_prev is not None):
            try:
                mask = (df_cn_prev["セクション"] == cn_sec) & (df_cn_prev["項目"] == cn_itm)
                if mask.any():
                    idx = mask.idxmax()
                    ja_sec = df_ja_prev.at[idx, "セクション_ja"]
                    ja_itm = df_ja_prev.at[idx, "項目_ja"]
            except Exception:
                pass

        if not ja_sec:
            ja_sec = translate_text(client, cn_sec)
        if not ja_itm:
            ja_itm = translate_text(client, cn_itm)

        out.at[i, "セクション_ja"] = ja_sec
        out.at[i, "項目_ja"] = ja_itm

    # 3) 列見出し（グレード名など）の翻訳
    #    448 では日本語化されているため、列4以降を翻訳（数字・記号のみはそのまま）
    new_cols = list(out.columns[:4])
    for c in out.columns[4:]:
        new_cols.append(translate_text(client, c))
    out.columns = new_cols

    # 4) 値セルの翻訳（価格行は個別整形、それ以外は翻訳。数字/記号のみは非翻訳）
    MSRP_CN = {"厂商指导价(元)", "厂商指导价", "厂商指导价（元）"}
    DEALER_CN = {"经销商报价", "经销商参考价", "经销商"}

    is_msrp = out["項目"].isin(MSRP_CN) | out["項目_ja"].str.contains("メーカー希望小売価格", na=False)
    is_dealer = out["項目"].isin(DEALER_CN) | out["項目_ja"].str.contains("ディーラー販売価格", na=False)

    for col in out.columns[4:]:
        # 価格整形
        out.loc[is_msrp, col] = out.loc[is_msrp, col].map(lambda s: msrp_to_yuan_and_jpy(s, rate))
        out.loc[is_dealer, col] = out.loc[is_dealer, col].map(lambda s: dealer_to_yuan_only(s))
        # 非価格は翻訳（数字/記号のみはスキップ）
        non_price = ~(is_msrp | is_dealer)
        for idx in out.index:
            if not non_price[idx]:
                continue
            val = out.at[idx, col]
            if is_blank_or_symbol(val) or is_numeric_like(val):
                continue
            out.at[idx, col] = translate_text(client, val)

    # 5) JA 出力保存（BOM付き＋互換名も）
    out.to_csv(JA_OUT, index=False, encoding="utf-8-sig")
    out.to_csv(JA_OUT_COMPAT, index=False, encoding="utf-8-sig")

    # 6) リポジトリ内キャッシュ（CN/JA）保存（BOM）
    try:
        cols_cn = [c for c in ["セクション", "項目"] if c in df_cn.columns]
        if cols_cn:
            df_cn[cols_cn].to_csv(CACHE_CN, index=False, encoding="utf-8-sig")
        cols_ja = [c for c in ["セクション", "項目", "セクション_ja", "項目_ja"] if c in out.columns]
        if cols_ja:
            out[cols_ja].to_csv(CACHE_JA, index=False, encoding="utf-8-sig")
        print("💾 リポジトリ内キャッシュ更新完了")
    except Exception as e:
        print(f"⚠ キャッシュ保存中にエラー: {e}")

    print("✅ 翻訳完了（448 と同じ構成で出力）")

if __name__ == "__main__":
    main()
