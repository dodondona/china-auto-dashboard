# -*- coding: utf-8 -*-
# ==========================================================
# tools/koubei_storywriter.py
#
# 目的:
#   Autohome 口コミ（Koubei）CSV から要点を抽出し、
#   GPTにMarkdown形式（見出し＋箇条書き）で要約させる。
#   ポジティブ／ネガティブを分けて出力。
#
# 出力:
#   output/koubei/{series_id}/story.txt
#   output/koubei/{series_id}/story.md
#
# 依存:
#   openai==1.*
#   pandas
# ==========================================================

import os
import sys
import pandas as pd
from pathlib import Path
from openai import OpenAI

# ----------------------------------------------------------
# 安全に文字列化
# ----------------------------------------------------------
def safe(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

# ----------------------------------------------------------
# プロンプト生成（Markdown形式で）
# ----------------------------------------------------------
def make_prompt(pros_text, cons_text):
    prompt = f"""以下は中国自動車市場に関する自動車ユーザーの口コミデータです。
ポジティブな点とネガティブな点をそれぞれ箇条書きで整理し、
日本語でMarkdown形式（### 見出し＋箇条書き）でわかりやすく要約してください。

### ポジティブな点
{pros_text}

### ネガティブな点
{cons_text}
"""
    return prompt

# ----------------------------------------------------------
# データフレームからpayload生成
# ----------------------------------------------------------
def make_payload(df: pd.DataFrame) -> str:
    # 翻訳済み列があれば優先
    pros_col = "pros_ja" if "pros_ja" in df.columns else "pros"
    cons_col = "cons_ja" if "cons_ja" in df.columns else "cons"

    pros_list = df[pros_col].dropna().head(30).tolist()
    cons_list = df[cons_col].dropna().head(30).tolist()

    pros_text = "\n".join([f"- {safe(x)}" for x in pros_list])
    cons_text = "\n".join([f"- {safe(x)}" for x in cons_list])

    return make_prompt(pros_text, cons_text)

# ----------------------------------------------------------
# メイン処理
# ----------------------------------------------------------
def main(series_id: str, style: str = "story"):
    """
    series_id: Autohomeの車種ID
    style: 'story' 固定（将来拡張用）
    """
    csv_path = f"autohome_reviews_{series_id}.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSVが見つかりません: {csv_path}")

    print(f"[detect] found {csv_path}")
    df = pd.read_csv(csv_path)

    # プロンプト作成
    prompt = make_payload(df)

    # OpenAI API呼び出し
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    model = "gpt-4o-mini"
    print(f"[model] {model}")

    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ],
        temperature=0.7,
    )

    text = response.choices[0].message.content.strip()

    # 出力先ディレクトリ
    outdir = Path(f"output/koubei/{series_id}")
    outdir.mkdir(parents=True, exist_ok=True)

    txt_path = outdir / "story.txt"
    md_path = outdir / "story.md"

    txt_path.write_text(text, encoding="utf-8")
    md_path.write_text(text, encoding="utf-8")

    print(f"[done] Saved: {txt_path} / {md_path}")

# ----------------------------------------------------------
# CLIエントリポイント
# ----------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tools/koubei_storywriter.py <series_id> [style]")
        sys.exit(1)
    series_id = sys.argv[1]
    style = sys.argv[2] if len(sys.argv) > 2 else "story"
    main(series_id, style)
