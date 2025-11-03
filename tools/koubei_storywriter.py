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
        "1) 導入部では、当該車種の全体的な評価傾向を客観的に述べてください。\n"
        "   （例：『全体的には肯定的な評価が多いが、一部に課題も指摘されている』など）\n"
        "2) ポジティブな要点を「〜と評価されている」「〜が好評を得ている」の形で記述。\n"
        "3) ネガティブな要点を「〜との指摘がある」「〜が課題とされている」の形で記述。\n"
        "4) 各項目では理由や背景を簡潔に添え、評論家レポートのような中立的文体に。\n"
        "5) 適度に代表的な口コミを引用し、ユーザーの実感を補足として挿入してください。\n"
        "   （例：『“○○が便利だった”との声も多い』や『“△△が不満”とのコメントも見られる』など）\n"
        "6) 最後の段落では、評価傾向を踏まえた市場ポジション・開発示唆を簡潔に述べること。\n"
        "7) 文体は「〜と評価されている」「〜が指摘されている」など中立表現で統一（です／ます禁止）。\n"
        "8) すべて日本語Markdown形式で、見出し（###）と箇条書きを維持すること。\n\n"
        "【出力フォーマット例】\n"
        "```\n"
        "導入文（1〜2段落、全体傾向の要約）\n\n"
        "### ポジティブな評価点\n"
        "- 項目1（〜と評価されている。“○○が良い”との声もある）\n"
        "- 項目2（〜が好評を得ている。“△△が魅力的”といった意見も見られる）\n\n"
        "### ネガティブな評価点\n"
        "- 項目1（〜との指摘がある。“××が不便”といった口コミも散見される）\n"
        "- 項目2（〜が課題とされている。“□□に不満”との意見が複数ある）\n\n"
        "総括（1段落、評価傾向を踏まえた市場ポジション・開発示唆など）\n"
        "```\n\n"
        f"メタ情報:\n{meta}\n"
        f"ポジティブ上位:\n{pros_block}\n\n"
        f"ネガティブ上位:\n{cons_block}\n\n"
        f"代表コメント（引用候補）:\n{reps_block}\n"
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
        temperature=0.2,  # ← 安定性を高める（箇条書き崩れ防止）
        max_tokens=1300,
    )
    return comp.choices[0].message.content.strip()


def make_payload(df: pd.DataFrame):
    """CSVから入力データを整形"""
    def safe(v):
        return str(v).strip() if pd.notna(v) else ""

    # ✅ フォールバック処理を追加（旧版と同じ）
    pros_col = "pros_ja" if "pros_ja" in df.columns else "pros"
    cons_col = "cons_ja" if "cons_ja" in df.columns else "cons"

    pros = [safe(x) for x in df[pros_col].dropna().head(30).tolist()]
    cons = [safe(x) for x in df[cons_col].dropna().head(30).tolist()]
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

    # フォーマットをより安定させるため、Markdownを強調
    system = (
        "あなたは日本語Markdownレポート作成者です。"
        "常に###見出しと箇条書きを用いて構成し、自然で整然としたMarkdown形式を維持してください。"
    )

    story = ask_model(client, system, prompt)

    # 出力先ディレクトリ
    outdir = Path(f"output/koubei/{series_id}")
    outdir.mkdir(parents=True, exist_ok=True)

    txt_path = outdir / "story.txt"
    md_path = outdir / "story.md"
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
