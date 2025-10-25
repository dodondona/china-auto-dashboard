#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, re, glob
import pandas as pd

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
    if series is None: return pd.Series(dtype=int)
    return split_terms(series).value_counts().head(k)

def ratio_fmt(n, total):
    if total <= 0: return "0%"
    return f"{(n/total*100):.0f}%"

def read_summary_txt(path):
    pos = mix = neg = None
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    mpos = re.search(r"positive\s+(\d+)", content, re.I)
    mmix = re.search(r"mixed\s+(\d+)", content, re.I)
    mneg = re.search(r"negative\s+(\d+)", content, re.I)
    if mpos: pos = int(mpos.group(1))
    if mmix: mix = int(mix.group(1)) if isinstance(mmix, int) else int(mmix.group(1))
    if mneg: neg = int(mneg.group(1))
    return pos, mix, neg

def detect_columns(df: pd.DataFrame):
    if {"pros_ja","cons_ja"}.issubset(df.columns):
        return "ja", df["pros_ja"], df["cons_ja"]
    if {"pros_zh","cons_zh"}.issubset(df.columns):
        return "zh", df["pros_zh"], df["cons_zh"]
    if {"pros","cons"}.issubset(df.columns):
        return "legacy", df["pros"], df["cons"]
    return "unknown", None, None

def build_narrative(vehicle_id, pros_top, cons_top, pos, mix, neg):
    total = (pos or 0) + (mix or 0) + (neg or 0)
    # センチメント行
    if total == 0:
        senti_line = "全体傾向は不明です（要約件数が不足）。"
    else:
        # 最大カテゴリ
        triples = [("好意的", pos or 0), ("賛否両論", mix or 0), ("否定的", neg or 0)]
        winner = max(triples, key=lambda x: x[1])[0]
        if winner == "好意的":
            senti_line = f"全体としては**好意的な評価**が多く（positive {ratio_fmt(pos,total)}）、次いで賛否両論（mixed {ratio_fmt(mix,total)}）、否定的（negative {ratio_fmt(neg,total)}）の順でした。"
        elif winner == "賛否両論":
            senti_line = f"全体としては**評価が分かれる**傾向で（mixed {ratio_fmt(mix,total)}）、好意的（positive {ratio_fmt(pos,total)}）と否定的（negative {ratio_fmt(neg,total)}）が続きます。"
        else:
            senti_line = f"全体としては**否定的な評価**が目立ち（negative {ratio_fmt(neg,total)}）、賛否両論（mixed {ratio_fmt(mix,total)}）と好意的（positive {ratio_fmt(pos,total)}）が続きます。"

    def join_top(idx_series):
        items = [f"「{term}」" for term in idx_series.index.tolist()]
        if not items: return ""
        if len(items) == 1: return items[0]
        if len(items) == 2: return "と".join(items)
        return "、".join(items[:-1]) + "、" + items[-1]

    pos_line = cons_line = ""
    if pros_top is not None and len(pros_top) > 0:
        pos_line = f"ポジティブ面では {join_top(pros_top)} を評価する声が多く、日常利用における満足点として挙げられています。"
    if cons_top is not None and len(cons_top) > 0:
        cons_line = f"一方で、ネガティブ面では {join_top(cons_top)} に不満を感じる人が一定数おり、購入時の留意点となっています。"

    if not pos_line and not cons_line:
        cons_line = "具体的な長所・短所の頻出項目は抽出できませんでしたが、センチメント分布から全体の傾向を把握できます。"

    header = f"【車両ID: {vehicle_id}】口コミの文章風サマリー"
    body = [s for s in [senti_line, pos_line, cons_line,
                        "なお、これらの傾向はサンプル範囲に依存するため、取得ページや時期を広げると比率が変動する可能性があります。"] if s]
    md = f"# {header}\n\n" + "\n\n".join(body) + "\n"
    txt = header + "\n\n" + "\n\n".join(body) + "\n"
    return txt, md

def main():
    vehicle_id = sys.argv[1].strip() if len(sys.argv) >= 2 else None
    csv_path, txt_path = detect_files(vehicle_id)
    df = pd.read_csv(csv_path)

    mode, pros_col, cons_col = detect_columns(df)
    pos = (df["sentiment"].str.lower()=="positive").sum() if "sentiment" in df.columns else 0
    mix = (df["sentiment"].str.lower()=="mixed").sum() if "sentiment" in df.columns else 0
    neg = (df["sentiment"].str.lower()=="negative").sum() if "sentiment" in df.columns else 0
    if (pos+mix+neg)==0:
        p2=m2=n2=None
        try:
            p2,m2,n2 = read_summary_txt(txt_path)
        except Exception:
            pass
        pos=pos or (p2 or 0); mix=mix or (m2 or 0); neg=neg or (n2 or 0)

    pros_top = top_k(pros_col) if pros_col is not None else pd.Series(dtype=int)
    cons_top = top_k(cons_col) if cons_col is not None else pd.Series(dtype=int)

    vid = re.search(r"autohome_reviews_(\d+)\.csv$", os.path.basename(csv_path))
    vid = vid.group(1) if vid else (vehicle_id or "unknown")

    txt, md = build_narrative(vid, pros_top, cons_top, pos, mix, neg)

    with open(f"autohome_reviews_{vid}_report.txt","w",encoding="utf-8") as f:
        f.write(txt)
    with open(f"autohome_reviews_{vid}_report.md","w",encoding="utf-8") as f:
        f.write(md)
    print("✅ narrative report generated")

if __name__ == "__main__":
    main()
