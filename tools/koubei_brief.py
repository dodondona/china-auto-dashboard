#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV → 極短サマリー（ポジ/ネガを各2〜3文）を生成
・API不要（速い）
・日本語/中国語どちらの列でも自動対応（pros_ja/cons_ja など）
出力:
  autohome_reviews_<ID>_brief.txt
  autohome_reviews_<ID>_brief.md

使い方:
  python tools/koubei_brief.py 5714 --k 3
"""
import os, sys, re, glob, argparse
import pandas as pd

def detect_csv(vehicle_id: str|None):
    if vehicle_id:
        p = f"autohome_reviews_{vehicle_id}.csv"
        if not os.path.exists(p): raise FileNotFoundError(p)
        return p
    csvs = sorted(glob.glob("autohome_reviews_*.csv"), key=os.path.getmtime, reverse=True)
    if not csvs: raise FileNotFoundError("CSVが見つかりません")
    return csvs[0]

def detect_cols(df: pd.DataFrame):
    # 優先度: ja → zh → legacy
    if {"pros_ja","cons_ja"}.issubset(df.columns): return "ja","pros_ja","cons_ja"
    if {"pros_zh","cons_zh"}.issubset(df.columns): return "zh","pros_zh","cons_zh"
    if {"pros","cons"}.issubset(df.columns):
        text = " ".join(df["pros"].dropna().astype(str).head(20).tolist())
        lang = "ja" if re.search(r"[ぁ-ゟ゠-ヿ]", text) else "zh"
        return lang,"pros","cons"
    return "ja", None, None

def split_terms(series):
    if series is None: return pd.Series(dtype=str)
    s = series.dropna().astype(str).str.split(" / ").explode().str.strip()
    s = s[s!=""]
    return s

def top_k_terms(df, col, k=3, total=1):
    s = split_terms(df[col]) if col in df.columns else pd.Series(dtype=str)
    if s.empty: return []
    vc = s.value_counts().head(k)
    out = []
    for term, cnt in vc.items():
        pct = round(cnt/total*100, 1) if total else 0.0
        out.append((str(term), int(cnt), pct))
    return out

def ratio(n, total): 
    return 0.0 if total<=0 else round(n/total*100, 1)

def build_positive_block(items):
    # 2〜3文でまとめる（件数や割合は控えめに）
    if not items: 
        return ["ポジティブ面の頻出要素は明確ではありません。"]
    lead = f"多くのユーザーは{items[0][0]}を評価しており、"
    if len(items) >= 3:
        tail = f"{items[1][0]}や{items[2][0]}も好意的に受け止められています。"
        s1 = lead + tail
        s2 = "日常利用における満足度を押し上げるポイントとして言及が目立ちます。"
        return [s1, s2]
    elif len(items) == 2:
        tail = f"{items[1][0]}にも満足の声が集まっています。"
        s1 = lead + tail
        s2 = "使い勝手の良さが総合的な評価につながっている印象です。"
        return [s1, s2]
    else:  # 1件
        s1 = f"ポジティブ面では、{items[0][0]}への評価が中心です。"
        s2 = "特に日常シーンでの使い勝手に寄与していると見られます。"
        return [s1, s2]

def build_negative_block(items):
    if not items:
        return ["ネガティブ面で目立つ指摘は限定的です。"]
    lead = f"一方で、{items[0][0]}に対する不満が挙がり、"
    if len(items) >= 3:
        tail = f"{items[1][0]}や{items[2][0]}も課題として指摘されています。"
        s1 = lead + tail
        s2 = "購入時には上記の点を重視して、試乗や仕様確認で納得感を得ることが推奨されます。"
        return [s1, s2]
    elif len(items) == 2:
        tail = f"{items[1][0]}も改善要望として見られます。"
        s1 = lead + tail
        s2 = "利用環境やグレードによって体感差が出るため、事前確認が無難です。"
        return [s1, s2]
    else:  # 1件
        s1 = f"ネガティブ面では、{items[0][0]}が主な懸念点です。"
        s2 = "用途に応じて許容できるか、購入前に確認しておくと安心です。"
        return [s1, s2]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("vehicle_id", nargs="?", help="Autohome vehicle id（例: 5714）")
    ap.add_argument("--k", type=int, default=3, help="上位項目の採用数(2〜3推奨)")
    args = ap.parse_args()

    csv_path = detect_csv(args.vehicle_id)
    df = pd.read_csv(csv_path)

    lang, pros_col, cons_col = detect_cols(df)
    total = len(df)
    s = df["sentiment"].astype(str).str.lower() if "sentiment" in df.columns else pd.Series(dtype=str)
    pos = int((s=="positive").sum()) if not s.empty else 0
    mix = int((s=="mixed").sum()) if not s.empty else 0
    neg = int((s=="negative").sum()) if not s.empty else 0

    k_use = max(2, min(args.k, 3))  # 2〜3に丸める
    pros_top = top_k_terms(df, pros_col, k=k_use, total=total) if pros_col else []
    cons_top = top_k_terms(df, cons_col, k=k_use, total=total) if cons_col else []

    # 1行サマリー（全体傾向）
    if total == 0:
        headline = "レビュー件数が少なく、全体傾向は判断困難です。"
    else:
        max_tag, max_val = max([("positive",pos),("mixed",mix),("negative",neg)], key=lambda x: x[1])
        if max_tag == "positive":
            headline = f"全体として好意的な評価が相対的に多く（Positive {ratio(pos,total)}%）、次いでMixed、Negativeの順です。"
        elif max_tag == "mixed":
            headline = f"全体として評価はやや分かれ（Mixed {ratio(mix,total)}%）、PositiveとNegativeが続きます。"
        else:
            headline = f"全体として否定的な評価が目立ち（Negative {ratio(neg,total)}%）、MixedとPositiveが続きます。"

    # 2〜3文のブロックを生成
    pos_block = build_positive_block(pros_top)
    neg_block = build_negative_block(cons_top)

    # 車両ID
    vm = re.search(r"autohome_reviews_(\d+)\.csv$", os.path.basename(csv_path))
    vid = vm.group(1) if vm else (args.vehicle_id or "unknown")

    header = f"【車両ID: {vid}】口コミ 超要約（2〜3文 × ポジ/ネガ）"
    txt = [
        header, "", 
        "■ 全体サマリー", headline, "",
        "■ ポジティブ面（2〜3文）", *pos_block, "",
        "■ ネガティブ面（2〜3文）", *neg_block, "",
        "※ 本結果は取得範囲の要約に基づきます。ページ数や時期により変動します。"
    ]
    txt = "\n".join(txt)
    md  = "# " + header + "\n\n" + "\n\n".join([
        "### 全体サマリー\n" + headline,
        "### ポジティブ面（2〜3文）\n" + " ".join(pos_block),
        "### ネガティブ面（2〜3文）\n" + " ".join(neg_block),
        "_※ 本結果は取得範囲の要約に基づきます。ページ数や時期により変動します。_"
    ]) + "\n"

    out_txt = f"autohome_reviews_{vid}_brief.txt"
    out_md  = f"autohome_reviews_{vid}_brief.md"
    with open(out_txt, "w", encoding="utf-8") as f: f.write(txt)
    with open(out_md, "w", encoding="utf-8") as f: f.write(md)
    print(f"✅ brief generated: {out_txt}, {out_md}")

if __name__ == "__main__":
    main()
