import os
import pandas as pd
from pathlib import Path
from openai import OpenAI
import json

# ===== 設定 =====
CACHE_DIR = Path("cache_repo/series")
OUTPUT_DIR = Path("output/autohome")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# ===== 翻訳関数 =====
def translate_text(client, text, src_lang="zh", tgt_lang="ja"):
    if not text or text.strip() in ["", "-", "—", "—-", "●"]:
        return text
    if text.isdigit():
        return text
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a professional translator that translates Chinese to Japanese accurately."},
            {"role": "user", "content": text}
        ]
    )
    return resp.choices[0].message.content.strip()

# ===== メイン =====
def main():
    series_id = os.environ.get("SERIES_ID") or "unknown"
    CSV_IN = os.environ.get("CSV_IN")

    # 🔸 以前の仕様に戻す：CSV_IN が優先、なければ autohome フォルダ
    if CSV_IN and Path(CSV_IN).exists():
        SRC = Path(CSV_IN)
    else:
        SRC = OUTPUT_DIR / series_id / f"config_{series_id}.csv"

    DST_PRIMARY = OUTPUT_DIR / series_id / f"config_{series_id}.ja.csv"
    DST_SECONDARY = OUTPUT_DIR / series_id / f"config_{series_id}_ja.csv"
    CACHE_CN = CACHE_DIR / series_id / "cn.csv"
    CACHE_JA = CACHE_DIR / series_id / "ja.csv"

    print(f"🔎 SRC: {SRC}")
    print(f"📝 DST(primary): {DST_PRIMARY}")
    print(f"📝 DST(secondary): {DST_SECONDARY}")

    if not Path(SRC).exists():
        print(f"⚠ 入力CSVが見つかりません（スキップ）: {SRC}")
        return

    df = pd.read_csv(SRC)
    if df.empty:
        print("⚠ 入力CSVが空です。スキップします。")
        return

    CACHE_CN.parent.mkdir(parents=True, exist_ok=True)
    CACHE_JA.parent.mkdir(parents=True, exist_ok=True)

    # 既存キャッシュ読み込み
    df_cn_prev, df_ja_prev = None, None
    if CACHE_CN.exists() and CACHE_JA.exists():
        try:
            df_cn_prev = pd.read_csv(CACHE_CN)
            df_ja_prev = pd.read_csv(CACHE_JA)
            print("✅ 既存キャッシュ読み込み完了")
        except Exception as e:
            print(f"⚠ キャッシュ読み込み失敗: {e}")

    client = OpenAI(api_key=OPENAI_API_KEY)
    df["セクション_ja"] = ""
    df["項目_ja"] = ""

    for i, row in df.iterrows():
        sec = row.get("セクション", "")
        itm = row.get("項目", "")
        sec_j, itm_j = None, None

        # 既存キャッシュに一致行があれば流用
        if df_cn_prev is not None and df_ja_prev is not None:
            mask = (df_cn_prev["セクション"] == sec) & (df_cn_prev["項目"] == itm)
            if mask.any():
                idx = mask.idxmax()
                sec_j = df_ja_prev.at[idx, "セクション_ja"]
                itm_j = df_ja_prev.at[idx, "項目_ja"]

        # 未翻訳 or 差分がある場合のみAPI翻訳
        if not sec_j or not itm_j:
            sec_j = translate_text(client, sec)
            itm_j = translate_text(client, itm)

        df.at[i, "セクション_ja"] = sec_j
        df.at[i, "項目_ja"] = itm_j

    df.to_csv(DST_PRIMARY, index=False)
    df.to_csv(DST_SECONDARY, index=False)

    # キャッシュ更新
    df.to_csv(CACHE_CN, index=False, columns=["セクション", "項目"])
    df.to_csv(CACHE_JA, index=False, columns=["セクション", "項目", "セクション_ja", "項目_ja"])
    print("💾 キャッシュ更新完了")
    print("✅ 翻訳完了")

if __name__ == "__main__":
    main()
