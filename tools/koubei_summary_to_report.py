#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV/TXT（pros/cons/sentiment）→ “読める”日本語レポートを自動生成
・API呼び出しなし（高速）
・割合・件数を明示
・エグゼクティブサマリー/推奨読者/注意点まで自動
使い方:
  python tools/koubei_summary_to_report.py 7806
  （ID省略可：最新ファイルを自動検出）
"""
import os, sys, re, glob
import pandas as pd

# ---------- 基本ユーティリティ ----------
def detect_files(vehicle_id: str|None):
    if vehicle_id:
        csv = f"autohome_reviews_{vehicle_id}.csv"
        txt  = f"autohome_reviews_{vehicle_id}_summary.txt"
        if not os.path.exists(csv) or not os.path.exists(txt):
            raise FileNotFoundError(f"必要ファイルが見つかりません: {csv}, {txt}")
        return csv, txt
    csvs = sorted(glob.glob("autohome_reviews_*.csv"), key=os.path.getmtime, reverse=True)
    txts = sorted(glob.glob("autohome_reviews_*_summary.txt"), key=os.path.getmtime, reverse=True)
    if not csvs or not txts:
        raise FileNotFoundError("入力CSV/TXTが見つかりません。")
    # 同じIDのペアを優先
    for c in csvs:
        m = re.search(r"autohome_reviews_(\d+)\.csv$", c)
        if not m: continue
        vid = m.group(1)
        t = f"autohome_reviews_{vid}_summary.txt"
        if t in txts or os.path.exists(t):
            return c, t
    return csvs[0], txts[0]

def ratio(n, total):
    return 0.0 if total <= 0 else round(n/total*100, 1)

def split_terms(series):
    s = series.dropna().astype(str).str.split(" / ").explode().str.strip()
    return s[s!=""]

def top_k(series, k=5):
    if series is None: return pd.Series(dtype=int)
    return split_terms(series).value_counts().head(k)

def col_exists(df, *cols):
    return all(c in df.columns for c in cols)

# ---------- 本体 ----------
def main():
    vehicle_id = sys.argv[1].strip() if len(sys.argv) >= 2 else None
    csv_path, txt_path = detect_files(vehicle_id)
    df = pd.read_csv(csv_path)

    # どの列を使うか自動判定
    if col_exists(df, "pros_ja","cons_ja","sentiment"):
        pros_col, cons_col = "pros_ja", "cons_ja"
        lang = "ja"
    elif col_exists(df, "pros_zh","cons_zh","sentiment"):
        pros_col, cons_col = "pros_zh", "cons_zh"
        lang = "zh"
    elif col_exists(df, "pros","cons","sentiment"):
        pros_col, cons_col = "pros", "cons"
        # かな/カナがあれば日本語寄りとみなす
        text = " ".join(df["pros"].dropna().astype(str).head(40).tolist())
        lang = "ja" if re.search(r"[ぁ-ゟ゠-ヿ]", text) else "zh"
    else:
        # どれも無ければ空で生成
        pros_col = cons_col = None
        lang = "ja"

    total = len(df)
    pos = int((df["sentiment"].astype(str).str.lower()=="positive").sum()) if "sentiment" in df.columns else 0
    mix = int((df["sentiment"].astype(str).str.lower()=="mixed").sum()) if "sentiment" in df.columns else 0
    neg = int((df["sentiment"].astype(str).str.lower()=="negative").sum()) if "sentiment" in df.columns else 0

    pros_top = top_k(df[pros_col]) if pros_col else pd.Series(dtype=int)
    cons_top = top_k(df[cons_col]) if cons_col else pd.Series(dtype=int)

    # 文面用ヘルパ
    def join_items(vc):
        items = [f"「{t}」（{cnt}件・{ratio(cnt,total)}%）" for t, cnt in vc.items()]
        if not items: return ""
        if len(items)==1: return items[0]
        return "、".join(items)

    # エグゼクティブサマリー
    if total == 0:
        senti_line = "レビュー件数が極めて少なく、全体傾向は不明です。"
    else:
        # 最多カテゴリで言い回しを変える
        max_tag = max([("positive",pos),("mixed",mix),("negative",neg)], key=lambda x:x[1])[0]
        if max_tag=="positive":
            senti_line = f"全体として**好意的な声が多く**（Positive {pos}件・{ratio(pos,total)}%）、次いでMixed {mix}件（{ratio(mix,total)}%）、Negative {neg}件（{ratio(neg,total)}%）でした。"
        elif max_tag=="mixed":
            senti_line = f"全体として**評価はやや分かれ**（Mixed {mix}件・{ratio(mix,total)}%）、Positive {pos}件（{ratio(pos,total)}%）、Negative {neg}件（{ratio(neg,total)}%）が続きました。"
        else:
            senti_line = f"全体として**否定的な声が目立ち**（Negative {neg}件・{ratio(neg,total)}%）、Mixed {mix}件（{ratio(mix,total)}%）、Positive {pos}件（{ratio(pos,total)}%）が続きました。"

    pros_line = "（該当データなし）" if pros_top.empty else join_items(pros_top)
    cons_line = "（該当データなし）" if cons_top.empty else join_items(cons_top)

    # “誰に向く/どこが注意”を定型で補う（軽いルール）
    who_line = ""
    if not pros_top.empty:
        if any("取り回し" in t or "駐車" in t or "小さ" in t for t in pros_top.index.astype(str)):
            who_line += "都市部の短距離移動や駐車頻度が高い人には向いています。"
        if any("燃費" in t or "維持費" in t or "コスパ" in t or "価格" in t for t in pros_top.index.astype(str)):
            who_line += "コスト重視のユーザーから支持されています。"
    warn_line = ""
    if not cons_top.empty:
        if any("航続" in t or "距離" in t for t in cons_top.index.astype(str)):
            warn_line += "長距離走行の頻度が高い人は航続面を要確認。"
        if any("加速" in t or "パワー" in t for t in cons_top.index.astype(str)):
            warn_line += "動力性能は試乗での確認がおすすめ。"
        if any("静粛" in t or "内装" in t for t in cons_top.index.astype(str)):
            warn_line += "質感や静粛性はグレード差に注意。"

    # レポート本文
    header = f"【車両ID: {re.search(r'autohome_reviews_(\\d+)\\.csv$', os.path.basename(csv_path)).group(1)}】口コミサマリー（文章版）"
    body_lines = [
        "■ エグゼクティブサマリー",
        senti_line,
        "",
        "■ よく挙がったポジティブ要素",
        pros_line,
        "",
        "■ よく挙がったネガティブ要素",
        cons_line,
        "",
        "■ こんな人に向いています",
        (who_line or "（特記なし）"),
        "",
        "■ 注意したいポイント",
        (warn_line or "（特記なし）"),
        "",
        "※ 比率は本レポート対象の要約行を母数に算出。取得ページや時期により変動します。"
    ]
    txt = header + "\n\n" + "\n".join(body_lines) + "\n"
    md  = f"# {header}\n\n" + "\n".join(body_lines) + "\n"

    vid = re.search(r"autohome_reviews_(\d+)\\.csv$", os.path.basename(csv_path)).group(1)
    with open(f"autohome_reviews_{vid}_report.txt","w",encoding="utf-8") as f:
        f.write(txt)
    with open(f"autohome_reviews_{vid}_report.md","w",encoding="utf-8") as f:
        f.write(md)
    print("✅ narrative report generated:", f"autohome_reviews_{vid}_report.txt")

if __name__ == "__main__":
    main()
