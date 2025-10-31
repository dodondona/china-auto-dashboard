#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, re, json, time
import pandas as pd
from pathlib import Path
from openai import OpenAI

# =========================================================
# 概要:
#   autohome_reviews_<id>.csv をもとに、要約文を生成
#   出力は autohome_reviews_<id>_story.txt / .md
# =========================================================

def detect_csv(series_id: str) -> Path:
    """ルート直下の autohome_reviews_<id>.csv を検出"""
    candidates = list(Path(".").glob(f"autohome_reviews_{series_id}.csv"))
    if not candidates:
        raise FileNotFoundError(f"autohome_reviews_{series_id}.csv not found in root")
    return candidates[0]


def build_prompt(payload, style):
    """プロンプト構築"""
    pros, cons, reps, meta = payload["pros"], payload["cons"], payload["representatives"], payload["meta"]

    tone = {
        "formal": "フォーマルで、ビジネスレポート調にまとめてください。",
        "friendly": "親しみやすく、自然な日本語でまとめてください。",
    }.get(style, "フォーマルで、ビジネスレポート調にまとめてください。")

    pros_block = "\n".join([f"- {p}" for p in pros]) or "（該当なし）"
    cons_block = "\n".join([f"- {c}" for c in cons]) or "（該当なし）"
    reps_block = "\n".join([f"- {r}" for r in reps]) or "（該当なし）"

    user = (
        f"文体ガイド: {tone}\n"
        "出力要件:\n"
        "1) 導入の1段落（全体傾向：肯定/否定のバランスを1〜2文）\n"
        "2) ポジティブの要点（2〜4点）\n"
        "3) ネガティブの要点（2〜4点）\n"
        "4) 向いているユーザー像と、購入時の注意点を1段落\n"
        "5) 最後に但し書き（サンプル範囲・時期により変動）\n"
        "6) すべて日本語。適度に接続詞を入れて自然に。\n"
        "7) 代表コメントは必要に応じて“例：〜”の形で軽く引用可。\n"
        "8) 全体の分量は、これまでよりやや厚め（倍程度を目安）にし、"
        "箇条書きでは各項目を2文前後で補足してください。\n"
        "9) 各段落では背景や理由づけを少し加え、自然な流れを作ってください。\n"
        "10) **重要**: 全体の長さは日本語でおよそ1,500〜2,000字を目安にし、"
        "箇条書きの各項目は3〜4文で具体的に説明してください。短く要約しすぎないでください。\n\n"
        f"メタ情報:\n{meta}\n"
        f"ポジティブ上位:\n{pros_block}\n\n"
        f"ネガティブ上位:\n{cons_block}\n\n"
        f"代表コメント（必要に応じて活用）:\n{reps_block}\n"
    )
    return user


def ask_model(client, system, user):
    """OpenAIモデル呼び出し"""
    comp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.4,
        max_tokens=2200,  # ← 900→1300→2200 に拡大（分量確保）
    )
    return comp.choices[0].message.content.strip()


def make_payload(df: pd.DataFrame):
    """CSVから入力データを整形"""
    def safe(v):
        return str(v).strip() if pd.notna(v) else ""
    pros = [safe(x) for x in df["pros_ja"].dropna().head(30).tolist()]
    cons = [safe(x) for x in df["cons_ja"].dropna().head(30).tolist()]
    reps = [safe(x) for x in df["title"].dropna().head(10).tolist()]
    meta = f"レビュー数: {len(df)}件"
    return {"pros": pros, "cons": cons, "representatives": reps, "meta": meta}


def main(series_id: str, style: str = "formal"):
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY", ""))

    csv_path = detect_csv(series_id)
    print(f"[detect] found {csv_path}")
    df = pd.read_csv(csv_path)

    payload = make_payload(df)
    prompt = build_prompt(payload, style)

    system = "あなたは中国自動車の口コミを分析する日本語レポート作成者です。全体傾向を踏まえ、統一感ある要約を生成してください。"

    story = ask_model(client, system, prompt)

    txt_path = Path(f"autohome_reviews_{series_id}_story.txt")
    md_path = Path(f"autohome_reviews_{series_id}_story.md")
    txt_path.write_text(story, encoding="utf-8")
    md_path.write_text(story, encoding="utf-8")

    print(f"✅ story generated: {txt_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tools/koubei_storywriter.py <series_id> [style]")
        sys.exit(1)
    series_id = sys.argv[1]
    style = sys.argv[2] if len(sys.argv) > 2 else "formal"
    main(series_id, style)
