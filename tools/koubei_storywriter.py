#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV → ストーリー要約（自然文・オフライン）
・OpenAI/外部API 依存なし
・既存の“良かった”出力の調子を維持
"""

import os
import re
import argparse
import pandas as pd

def detect_csv(vehicle_id: str):
    cand = [
        f"autohome_reviews_{vehicle_id}.csv",
        f"output/autohome/{vehicle_id}/autohome_reviews_{vehicle_id}.csv",
    ]
    for p in cand:
        if os.path.exists(p):
            return p
    raise FileNotFoundError(f"CSV not found for vehicle_id={vehicle_id}")

def detect_cols(df: pd.DataFrame):
    if {"pros_ja","cons_ja"}.issubset(df.columns): return "pros_ja","cons_ja","ja"
    if {"pros_zh","cons_zh"}.issubset(df.columns): return "pros_zh","cons_zh","zh"
    if {"pros","cons"}.issubset(df.columns):
        text = " ".join(df["pros"].dropna().astype(str).head(30).tolist())
        lang = "ja" if re.search(r"[ぁ-ゟ゠-ヿ]", text) else "zh"
        return "pros","cons",lang
    # なければ本文 text から推定（zh とみなす）
    if "text" in df.columns:
        return None,None,"zh"
    return None,None,"zh"

def split_terms(series: pd.Series):
    if series is None:
        return pd.Series(dtype=str)
    s = series.dropna().astype(str).str.split(" / ").explode().str.strip()
    s = s[s!=""]
    return s

def top_terms(series: pd.Series, k=5):
    s = split_terms(series)
    if s.empty:
        return []
    vc = s.value_counts().head(k)
    return list(vc.index)

def quick_sentiment_breakdown(df: pd.DataFrame):
    if "sentiment" not in df.columns:
        return 0,0,0,len(df)
    s = df["sentiment"].astype(str).str.lower()
    pos = int((s=="positive").sum())
    mix = int((s=="mixed").sum())
    neg = int((s=="negative").sum())
    return pos, mix, neg, len(df)

def ratio(n, total):
    return 0.0 if total<=0 else round(n/total*100,1)

def build_section(title, lines):
    out = [f"### {title}"]
    for ln in lines:
        if ln.strip():
            out.append(ln.strip())
    return "\n".join(out) + "\n\n"

def generate_story(df: pd.DataFrame, vid: str):
    pros_col, cons_col, lang = detect_cols(df)
    pros_terms = top_terms(df[pros_col]) if pros_col else []
    cons_terms = top_terms(df[cons_col]) if cons_col else []

    pos, mix, neg, total = quick_sentiment_breakdown(df)

    # 全体サマリー
    if total == 0:
        overall = "取得されたレビューが少なく、全体傾向の判断は困難です。"
    else:
        # 最多カテゴリで傾向をひとこと
        order = sorted([("positive",pos),("mixed",mix),("negative",neg)], key=lambda x: x[1], reverse=True)
        top_tag = order[0][0]
        if top_tag == "positive":
            overall = f"全体として好意的な評価が相対的に多く（Positive {ratio(pos,total)}%）、日常的な使いやすさや価格対効果に満足する声が目立ちます。"
        elif top_tag == "mixed":
            overall = f"全体として評価はやや分かれ（Mixed {ratio(mix,total)}%）、良い点と改善希望が併存しています。"
        else:
            overall = f"全体として否定的な評価が相対的に多く（Negative {ratio(neg,total)}%）、静粛性や装備面での改善要望が見られます。"

    # ポジティブ側
    if pros_terms:
        if len(pros_terms) == 1:
            pros_text = f"ポジティブ面では、{pros_terms[0]}への評価が中心です。日常利用での満足度を底上げする要素として挙げられています。"
        elif len(pros_terms) == 2:
            pros_text = f"多くのユーザーが{pros_terms[0]}を評価しており、{pros_terms[1]}にも満足の声が集まっています。使い勝手の良さが総合的な評価につながっている印象です。"
        else:
            pros_text = f"多くのユーザーは{pros_terms[0]}を高く評価し、{pros_terms[1]}や{pros_terms[2]}も好意的に受け止めています。価格とのバランスに納得感があり、日常シーンでの使い心地が支持されています。"
    else:
        pros_text = "ポジティブ面の頻出要素は明確ではありませんが、一定の満足感はうかがえます。"

    # ネガティブ側
    if cons_terms:
        if len(cons_terms) == 1:
            cons_text = f"ネガティブ面では、{cons_terms[0]}が主な懸念点です。用途や条件によって体感差が出るため、事前確認が推奨されます。"
        elif len(cons_terms) == 2:
            cons_text = f"一方で、{cons_terms[0]}への不満が挙がり、{cons_terms[1]}も改善要望として見られます。長距離や季節による影響を考慮したうえでの判断が無難です。"
        else:
            cons_text = f"一方で、{cons_terms[0]}に対する不満が挙がり、{cons_terms[1]}や{cons_terms[2]}も課題として指摘されています。致命的というより“惜しい”というトーンが多く、試乗や仕様確認で納得感を得るのが安心です。"
    else:
        cons_text = "ネガティブ面で目立つ指摘は限定的です。"

    story = []
    story.append(f"【車両ID: {vid}】口コミストーリー要約\n")
    story.append(build_section("全体サマリー", [overall]))
    story.append(build_section("ポジティブな評価", [pros_text]))
    story.append(build_section("ネガティブな評価", [cons_text]))
    story.append("※ 本要約は取得した口コミの頻出語・記述内容に基づいて自動生成しています。")
    return "\n".join(story)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("vehicle_id", help="Autohome vehicle id")
    args = ap.parse_args()

    csv_path = detect_csv(args.vehicle_id)
    vid_match = re.search(r"autohome_reviews_(\d+)\.csv$", os.path.basename(csv_path))
    vid = vid_match.group(1) if vid_match else args.vehicle_id

    df = pd.read_csv(csv_path)
    story = generate_story(df, vid)

    out_txt = f"autohome_reviews_{vid}_story.txt"
    with open(out_txt, "w", encoding="utf-8") as f:
        f.write(story)

    print(f"✅ {out_txt} を生成（OpenAI 不使用）")

if __name__ == "__main__":
    main()
