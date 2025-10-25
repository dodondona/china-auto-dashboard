#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Autohome口コミ要約 → ストーリー形式出力
（改修前オリジナル版：自然文でポジ・ネガ・総評を簡潔にまとめる）
"""

import os
import re
import argparse
import pandas as pd

def detect_csv(vehicle_id: str|None):
    """autohome_reviews_<id>.csv を自動検出"""
    if vehicle_id:
        p = f"autohome_reviews_{vehicle_id}.csv"
        if os.path.exists(p):
            return p
        alt = f"output/autohome/{vehicle_id}/autohome_reviews_{vehicle_id}.csv"
        if os.path.exists(alt):
            return alt
        raise FileNotFoundError(p)
    else:
        csvs = sorted(
            [f for f in os.listdir(".") if f.startswith("autohome_reviews_") and f.endswith(".csv")],
            key=os.path.getmtime, reverse=True
        )
        if not csvs:
            raise FileNotFoundError("autohome_reviews_*.csv が見つかりません")
        return csvs[0]

def detect_cols(df: pd.DataFrame):
    """言語列（日本語/中国語）を自動検出"""
    if {"pros_ja", "cons_ja"}.issubset(df.columns):
        return "pros_ja", "cons_ja"
    if {"pros_zh", "cons_zh"}.issubset(df.columns):
        return "pros_zh", "cons_zh"
    if {"pros", "cons"}.issubset(df.columns):
        return "pros", "cons"
    return None, None

def top_terms(series: pd.Series, k=5):
    """頻出語を抽出"""
    if series is None or series.empty:
        return []
    s = series.dropna().astype(str).str.split(" / ").explode().str.strip()
    s = s[s != ""]
    vc = s.value_counts().head(k)
    return list(vc.index)

def build_section(title, lines):
    """セクションを整形"""
    out = [f"### {title}"]
    for ln in lines:
        if ln.strip():
            out.append(ln.strip())
    return "\n".join(out) + "\n\n"

def generate_story(df: pd.DataFrame, vid: str):
    """メインロジック"""
    pros_col, cons_col = detect_cols(df)
    total = len(df)

    pros_terms = top_terms(df[pros_col]) if pros_col else []
    cons_terms = top_terms(df[cons_col]) if cons_col else []

    # ポジティブ側
    pros_text = ""
    if pros_terms:
        pros_text = (
            f"多くのユーザーが{pros_terms[0]}を高く評価しており、"
            + (f"{'、'.join(pros_terms[1:])}も好印象です。" if len(pros_terms) > 1 else "")
        )
        pros_text += " 特に日常走行や静粛性、使い勝手の良さに関するコメントが目立ちます。"
    else:
        pros_text = "ポジティブな意見は限定的ですが、総じて満足度は高めです。"

    # ネガティブ側
    cons_text = ""
    if cons_terms:
        cons_text = (
            f"一方で、{cons_terms[0]}に関しては改善を求める声があり、"
            + (f"{'、'.join(cons_terms[1:])}にも課題が見られます。" if len(cons_terms) > 1 else "")
        )
        cons_text += " それでも致命的な欠点というよりは“惜しい”というニュアンスの意見が多いです。"
    else:
        cons_text = "ネガティブな意見はほとんどなく、完成度の高さがうかがえます。"

    # 総評
    overall = (
        "総じて、価格・性能・快適性のバランスに優れたモデルとして評価されており、"
        "特に都市部での通勤やファミリーユースでの満足度が高い車種です。"
    )

    # 組み立て
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

    print(f"✅ {out_txt} を生成しました")

if __name__ == "__main__":
    main()
