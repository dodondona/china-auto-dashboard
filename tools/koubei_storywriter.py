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
    "出力冒頭にタイトルや見出し（例：「〇〇モデルの評価レポート」など）を付けないでください。\n"
    "1) 導入部では、当該車種の全体的な評価傾向を客観的かつ丁寧に述べてください。\n"
    "   （例：『全体的には肯定的な評価が多い一方で、一部に改善を求める声もあります。』など）\n"
    "2) ポジティブな評価点は **4〜6項目** 挙げてください。項目数は、該当する評価の数から判断してください。それぞれ「**項目名：** 内容説明＋代表的な口コミ」の形式にしてください。\n"
    "   各項目は2〜3文構成とし、背景や理由を添えて丁寧に記述してください。\n"
    "3) ネガティブな評価点は **3〜5項目** 挙げてください。項目数は、該当する評価の数から判断してください。同様に「**項目名：** 内容説明＋代表的な口コミ」の形式でまとめてください。\n"
    "   文末は『〜と指摘されています』『〜が課題とされています』などの中立的な表現を用いてください。\n"
    "4) 各項目には、代表的な口コミを1〜3文程度で引用または要約してください。\n"
    "   （例：“○○が便利だった”“△△が不満だった”“□□が高品質だった”といった声があります）\n"
    "5) 代表コメントは自然な文中引用として活用し、単なる羅列にしないでください。\n"
    "6) 各項目では、代表的な口コミを1〜2件引用し、その背景や理由を2〜3文で補足してください。\n"
    "   特に、使用シーン（例：長距離走行・市街地・家族利用など）を交えることで厚みを持たせてください。\n"
    "7) 各段落の分量は現在よりも1.5〜2倍程度に増やし、実際の使用感や具体的事例を含めて詳述してください。\n"
    "8) 文体は『です・ます調』で統一し、レポートとして自然で丁寧な文にしてください。\n"
    "9) 出力は日本語Markdown形式で、必ず見出し（###）と太字の項目名を維持してください。\n"
    "10) 「その他のコメント」では、上記のポジティブ・ネガティブ項目と内容が重複しないようにしてください。同じテーマ（例：外観・内装・燃費など）は繰り返さないでください。\n"
    "11) 評価や印象を断定的に書かないでください。常に『〜という声があります』『〜と評価されています』のように、ユーザーの意見として表現してください。\n"
    "12) 『上記の主要項目には含まれないが〜』という説明文は出力に含めないでください。\n\n"
    "【出力フォーマット例】\n"
    "```\n"
    "導入文（1〜2段落、全体傾向の要約）\n\n"
    "### ポジティブな評価点（4〜6項目）\n"
    "- **項目名：** 内容説明（2〜3文程度）。理由や背景を添え、口コミを交えて具体的に記述してください。\n"
    "  （例：“○○が快適でした”“△△が魅力的でした”といった声があります）\n\n"
    "### ネガティブな評価点（3〜5項目）\n"
    "- **項目名：** 課題内容（2〜3文程度）。背景や原因を説明し、代表コメントを引用してください。\n"
    "  （例：“□□が不便でした”“△△に不満でした”との指摘があります）\n\n"
    "### その他のコメント（例示）\n"
    "（以下はフォーマット例です。実際の出力時には繰り返さないでください）\n"
    "（例：“○○のデザインが個性的”“△△の収納が意外に便利”といった声があります）\n"
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
        temperature=0.2,
        max_tokens=1300,
    )
    return comp.choices[0].message.content.strip()


def clean_report(text: str) -> str:
    """タイトル行と末尾のまとめ文を削除"""
    text = re.sub(r'^[^\n]*モデルの評価レポート\s*\n*', '', text)
    text = re.sub(r'このように、.*?(?:。\s*)?$', '', text)
    return text.strip()


def make_payload(df: pd.DataFrame):
    """CSVから入力データを整形"""
    def safe(v):
        return str(v).strip() if pd.notna(v) else ""
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
    system = (
        "あなたは日本語Markdownレポート作成者です。"
        "常に###見出しと箇条書きを用いて構成し、自然で整然としたMarkdown形式を維持してください。"
    )
    story = ask_model(client, system, prompt)
    story = clean_report(story)
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
