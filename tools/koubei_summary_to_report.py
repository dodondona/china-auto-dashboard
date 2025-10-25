#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Autohome 口コミサマリー → 文章風レポート生成

入力:
  - リポジトリ直下にある:
      autohome_reviews_<ID>.csv
      autohome_reviews_<ID>_summary.txt
    （ja/zh どちらのモードでもOK）

出力:
  - autohome_reviews_<ID>_report.txt
  - autohome_reviews_<ID>_report.md

使い方:
  python tools/koubei_summary_to_report.py 7806
  （IDを省略した場合は最新のCSV/TXTを自動推定）
"""
import os, sys, re, glob
import pandas as pd
from collections import Counter

# -------- ユーティリティ --------
def detect_files(vehicle_id: str|None):
    if vehicle_id:
        csv = f"autohome_reviews_{vehicle_id}.csv"
        txt  = f"autohome_reviews_{vehicle_id}_summary.txt"
        if not os.path.exists(csv) or not os.path.exists(txt):
            raise FileNotFoundError(f"必要ファイルが見つかりません: {csv}, {txt}")
        return csv, txt
    # vehicle_id未指定なら最新を使う
    csvs = sorted(glob.glob("autohome_reviews_*.csv"), key=os.path.getmtime, reverse=True)
    txts = sorted(glob.glob("autohome_reviews_*_summary.txt"), key=os.path.getmtime, reverse=True)
    if not csvs or not txts:
        raise FileNotFoundError("入力CSV/TXTが見つかりません。先に要約処理を実行してください。")
    # 同じIDのペアを優先
    for c in csvs:
        m = re.search(r"autohome_reviews_(\d+)\.csv$", c)
        if not m: continue
        vid = m.group(1)
        t = f"autohome_reviews_{vid}_summary.txt"
        if t in txts or os.path.exists(t):
            return c, t
    return csvs[0], txts[0]

def split_terms(series):
    s = series.dropna().astype(str).str.split(" / ").explode().str.strip()
    return s[s!=""]

def top_k(series, k=5):
    return split_terms(series).value_counts().head(k)

def ratio_fmt(n, total):
    if total <= 0: return "0%"
    return f"{(n/total*100):.0f}%"

# -------- センチメントとTOP抽出 --------
def read_summary_txt(path):
    # センチメント件数をTXTから拾う（保険）
    pos = mix = neg = None
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    # いくつかの見出しパターンに対応
    mpos = re.search(r"positive\s+(\d+)", content, re.I)
    mmix = re.search(r"mixed\s+(\d+)", content, re.I)
    mneg = re.search(r"negative\s+(\d+)", content, re.I)
    if mpos: pos = int(mpos.group(1))
    if mmix: mix = int(mmix.group(1))
    if mneg: neg = int(mneg.group(1))
    return pos, mix, neg

def detect_mode_by_columns(df: pd.DataFrame):
    if {"pros_ja","cons_ja"}.issubset(df.columns):
        return "ja"
    if {"pros_zh","cons_zh"}.issubset(df.columns):
        return "zh"
    # 旧列名の互換
    if {"pros","cons"}.issubset(df.columns):
        # 中身から判定（かなカナで簡易判定）
        text = " ".join(df["pros"].dropna().astype(str).head(30).tolist())
        if re.search(r"[ぁ-ゟ゠-ヿ]", text):
            return "ja_legacy"
        return "zh_legacy"
    # 何も無ければ空モード
    return "unknown"

# -------- 文章テンプレ生成 --------
def build_narrative(vehicle_id, mode, pros_top, cons_top, senti_counts):
    pos, mix, neg = senti_counts
    total = sum(x for x in (pos, mix, neg) if x is not None) or 0

    # センチメントの文
    if total == 0:
        senti_line = "全体傾向は不明です（要約件数が不足）。"
    else:
        # 最大カテゴリ
        triples = [("好意的", pos), ("賛否両論", mix), ("否定的", neg)]
        winner = max(triples, key=lambda x: (x[1] or 0))
        if winner[0] == "好意的":
            senti_line = f"全体としては**好意的な評価**が多く（positive {ratio_fmt(pos,total)}）、次いで賛否両論（mixed {ratio_fmt(mix,total)}）、否定的（negative {ratio_fmt(neg,total)}）の順でした。"
        elif winner[0] == "賛否両論":
            senti_line = f"全体としては**評価が分かれる**傾向で（mixed {ratio_fmt(mix,total)}）、好意的（positive {ratio_fmt(pos,total)}）と否定的（negative {ratio_fmt(neg,total)}）が続きます。"
        else:
            senti_line = f"全体としては**否定的な評価**が目立ち（negative {ratio_fmt(neg,total)}）、賛否両論（mixed {ratio_fmt(mix,total)}）と好意的（positive {ratio_fmt(pos,total)}）が続きます。"

    # TOPを文章化
    def join_top(idx_series):
        items = [f"「{term}」" for term in idx_series.index.tolist()]
        if not items: return ""
        if len(items) == 1: return items[0]
        if len(items) == 2: return "と".join(items)
        return "、".join(items[:-1]) + "、" + items[-1]

    pos_line = cons_line = ""
    if len(pros_top) > 0:
        pos_line = f"ポジティブ面では {join_top(pros_top)} が挙げられることが多く、日常利用における満足ポイントとして言及されました。"
    if len(cons_top) > 0:
        cons_line = f"一方で、ネガティブ面では {join_top(cons_top)} への指摘が目立ち、購入時の留意点として挙げられています。"

    header = f"【車両ID: {vehicle_id}】口コミの文章風サマリー"
    body = [
        senti_line,
        pos_line,
        cons_line,
        "総じて、上記の傾向はサンプルの範囲に依存するため、ページ数や時期を広げると比率が変動する可能性があります。"
    ]
    # 空行除去
    body = [s for s in body if s]
    md = f"# {header}\n\n" + "\n\n".join(body) + "\n"
    txt = header + "\n\n" + "\n\n".join(body) + "\n"
    return txt, md

def main():
    vehicle_id = sys.argv[1].strip() if len(sys.argv) >= 2 else None
    csv_path, txt_path = detect_files(vehicle_id)

    df = pd.read_csv(csv_path)
    mode = detect_mode_by_columns(df)

    # センチメントの件数（CSV優先、なければTXT）
    pos = (df["sentiment"].str.lower()=="positive").sum() if "sentiment" in df.columns else None
    mix = (df["sentiment"].str.lower()=="mixed").sum() if "sentiment" in df.columns else None
    neg = (df["sentiment"].str.lower()=="negative").sum() if "sentiment" in df.columns else None
    if None in (pos, mix, neg):
        # TXTから補完
        p2, m2, n2 = read_summary_txt(txt_path)
        pos = pos if pos is not None else p2
        mix = mix if mix is not None else m2
        neg = neg if neg is not None else n2
    senti_counts = (pos or 0, mix or 0, neg or 0)

    # Pros/Cons トップを抽出
    if mode in ("ja", "ja_legacy"):
        pros_col = "pros_ja" if "pros_ja" in df.columns else ("pros" if "pros" in df.columns else None)
        cons_col = "cons_ja" if "cons_ja" in df.columns else ("cons" if "cons" in df.columns else None)
    elif mode in ("zh", "zh_legacy"):
        pros_col = "pros_zh" if "pros_zh" in df.columns else ("pros" if "pros" in df.columns else None)
        cons_col = "cons_zh" if "cons_zh" in df.columns else ("cons" if "cons" in df.columns else None)
    else:
        # 列が見つからない場合は空で処理
        pros_col = cons_col = None

    pros_top = top_k(df[pros_col]) if pros_col in df.columns else pd.Series(dtype=int)
    cons_top = top_k(df[cons_col]) if cons_col in df.columns else pd.Series(dtype=int)

    # IDを推定（ファイル名から）
    vid = re.search(r"autohome_reviews_(\d+)\.csv$", os.path.basename(csv_path))
    vid = vid.group(1) if vid else (vehicle_id or "unknown")

    # 文章化
    txt, md = build_narrative(vid, mode, pros_top, cons_top, senti_counts)

    # 出力
    report_txt = f"autohome_reviews_{vid}_report.txt"
    report_md  = f"autohome_reviews_{vid}_report.md"
    with open(report_txt, "w", encoding="utf-8") as f:
        f.write(txt)
    with open(report_md, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"✅ レポート出力: {report_txt}, {report_md}")

if __name__ == "__main__":
    main()
