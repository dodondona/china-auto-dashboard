import os
import re
import time
from pathlib import Path
import pandas as pd
from openai import OpenAI

# ===== 基本設定 =====
CACHE_DIR = Path("cache_repo/series")     # リポジトリ内キャッシュ保存先（CN/JA）
OUTPUT_DIR = Path("output/autohome")      # 既存の出力ディレクトリ
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

RETRIES = 3
SLEEP_BASE = 1.2  # リトライ待機係数

# ===== 安全な文字・数値判定 =====
RE_NUMERIC_LIKE = re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")

def is_blank_or_symbol(text: str) -> bool:
    t = str(text).strip()
    return t in {"", "-", "—", "—-", "●", "○"}

def is_numeric_like(text: str) -> bool:
    return bool(RE_NUMERIC_LIKE.fullmatch(str(text).strip()))

# ===== LLM翻訳（型安全＋3回リトライ） =====
def translate_text(client: OpenAI, text):
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
                model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
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

# ===== メイン =====
def main():
    series_id = (os.environ.get("SERIES_ID") or "unknown").strip()
    csv_in_env = (os.environ.get("CSV_IN") or "").strip()
    csv_out_env = (os.environ.get("CSV_OUT") or "").strip()

    # 入力：CSV_IN 優先。無ければ output/autohome/<id>/config_<id>.csv
    if csv_in_env and Path(csv_in_env).exists():
        SRC = Path(csv_in_env)
    else:
        SRC = OUTPUT_DIR / series_id / f"config_{series_id}.csv"

    # 出力：指定があれば尊重、無ければ既定パス
    if csv_out_env:
        DST_PRIMARY = Path(csv_out_env)
        dst_dir = DST_PRIMARY.parent
        DST_SECONDARY = dst_dir / DST_PRIMARY.name.replace(".ja.csv", "_ja.csv")
    else:
        DST_PRIMARY = OUTPUT_DIR / series_id / f"config_{series_id}.ja.csv"
        DST_SECONDARY = OUTPUT_DIR / series_id / f"config_{series_id}_ja.csv"

    # リポジトリ内キャッシュ（CN/JA）
    CACHE_CN = CACHE_DIR / series_id / "cn.csv"
    CACHE_JA = CACHE_DIR / series_id / "ja.csv"

    print(f"🔎 SRC: {SRC}")
    print(f"📝 DST(primary): {DST_PRIMARY}")
    print(f"📝 DST(secondary): {DST_SECONDARY}")

    if not SRC.exists():
        print(f"⚠ 入力CSVが見つかりません（スキップ）: {SRC}")
        return

    # 入力は UTF-8 BOM を優先（Excel作成CSVも含め崩れない）
    def safe_read_csv(path: Path) -> pd.DataFrame:
        for enc in ("utf-8-sig", "utf-8"):
            try:
                return pd.read_csv(path, encoding=enc)
            except Exception:
                continue
        # どうしてもダメな場合は自動判定
        return pd.read_csv(path)

    df = safe_read_csv(SRC)
    if df.empty:
        print("⚠ 入力CSVが空です。スキップします。")
        return

    # キャッシュ読込（存在すれば）
    CACHE_CN.parent.mkdir(parents=True, exist_ok=True)
    CACHE_JA.parent.mkdir(parents=True, exist_ok=True)
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

    # 出力列を追加
    if "セクション_ja" not in df.columns:
        df["セクション_ja"] = ""
    if "項目_ja" not in df.columns:
        df["項目_ja"] = ""

    # 1行ずつ翻訳（キャッシュ行一致なら流用）
    for i, row in df.iterrows():
        sec = row.get("セクション", "")
        itm = row.get("項目", "")
        sec_j, itm_j = None, None

        if (df_cn_prev is not None) and (df_ja_prev is not None):
            try:
                mask = (df_cn_prev["セクション"] == sec) & (df_cn_prev["項目"] == itm)
                if mask.any():
                    idx = mask.idxmax()
                    sec_j = df_ja_prev.at[idx, "セクション_ja"]
                    itm_j = df_ja_prev.at[idx, "項目_ja"]
            except Exception:
                pass

        if not sec_j:
            sec_j = translate_text(client, sec)
        if not itm_j:
            itm_j = translate_text(client, itm)

        df.at[i, "セクション_ja"] = sec_j
        df.at[i, "項目_ja"] = itm_j

    # 出力（Excelで崩れない BOM 付き）
    DST_PRIMARY.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(DST_PRIMARY, index=False, encoding="utf-8-sig")
    df.to_csv(DST_SECONDARY, index=False, encoding="utf-8-sig")

    # リポジトリ内キャッシュを UTF-8 BOM で保存（毎回上書き）
    try:
        # CN側は中国語原文を保存するため、最低限必要列に限定（存在チェック）
        cols_cn = [c for c in ["セクション", "項目"] if c in df.columns]
        if cols_cn:
            df[cols_cn].to_csv(CACHE_CN, index=False, encoding="utf-8-sig")
        # JA側は翻訳済み列を含めて保存
        cols_ja = [c for c in ["セクション", "項目", "セクション_ja", "項目_ja"] if c in df.columns]
        if cols_ja:
            df[cols_ja].to_csv(CACHE_JA, index=False, encoding="utf-8-sig")
        print("💾 リポジトリ内キャッシュ更新完了")
    except Exception as e:
        print(f"⚠ キャッシュ保存中にエラー: {e}")

    print("✅ 翻訳完了")

if __name__ == "__main__":
    main()
