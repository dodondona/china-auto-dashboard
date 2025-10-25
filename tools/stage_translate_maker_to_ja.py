# -*- coding: utf-8 -*-
# .github/scripts/stage_translate_maker_to_ja.py
#
# 目的:
#   - 既存CSVの 'manufacturer' 列（中国語）を日本語訳して 'manufacturer_ja' 列を追加
#   - まず辞書で変換、辞書に無い場合のみ LLM（OpenAI）で補完（APIキーがあれば）
#   - 元CSVは変更せず、*_with_maker_ja.csv を同ディレクトリに生成
#
# 使い方（例）:
#   python .github/scripts/stage_translate_maker_to_ja.py csv/*.csv public/*.csv
#
# 必要に応じて環境変数:
#   - OPENAI_API_KEY       ... あれば未知メーカーをLLMで訳す
#   - OPENAI_BASE_URL      ... (任意) 互換エンドポイントを使う場合
#   - OPENAI_MODEL         ... 既定: gpt-4o-mini（軽量で十分）
#
# 出力:
#   - 入力と同じ場所に *_with_maker_ja.csv を作成

import os
import sys
import re
import json
from pathlib import Path
import pandas as pd

# -- リアルタイム出力（Actions向け） ----------------------------
try:
    sys.stdout.reconfigure(line_buffering=True)  # type: ignore
except Exception:
    pass

# -- 変換辞書（必要に応じて追記してください） --------------------
DICT_ZH_TO_JA = {
    # 中国系
    "比亚迪": "BYD",
    "上汽": "上海汽車（SAIC）",
    "上汽集团": "上海汽車（SAIC）",
    "一汽": "第一汽車（FAW）",
    "东风": "東風（Dongfeng）",
    "广汽": "広州汽車（GAC）",
    "北汽": "北京汽車（BAIC）",
    "长安": "長安（Changan）",
    "长城": "長城（Great Wall）",
    "吉利": "吉利（Geely）",
    "奇瑞": "奇瑞（Chery）",
    "红旗": "紅旗（Hongqi）",
    "蔚来": "蔚来（NIO）",
    "小鹏": "小鵬（Xpeng）",
    "理想": "理想（Li Auto）",
    "上汽通用": "上汽通用（SAIC-GM）",
    "上汽通用五菱": "上汽通用五菱（SGMW／五菱）",
    "五菱": "五菱（Wuling）",

    # 米欧系
    "特斯拉": "テスラ",
    "大众": "フォルクスワーゲン",
    "上汽大众": "上汽-フォルクスワーゲン",
    "一汽-大众": "一汽-フォルクスワーゲン",
    "奥迪": "アウディ",
    "宝马": "BMW",
    "奔驰": "メルセデス・ベンツ",
    "雪佛兰": "シボレー",
    "凯迪拉克": "キャデラック",
    "标致": "プジョー",
    "雪铁龙": "シトロエン",
    "雷诺": "ルノー",
    "沃尔沃": "ボルボ",
    "保时捷": "ポルシェ",
    "捷豹": "ジャガー",
    "路虎": "ランドローバー",
    "菲亚特": "フィアット",
    "阿尔法·罗密欧": "アルファロメオ",

    # 韓国
    "现代": "ヒョンデ（現代）",
    "起亚": "キア",

    # 日本
    "丰田": "トヨタ",
    "一汽丰田": "一汽トヨタ",
    "广汽丰田": "広汽トヨタ",
    "本田": "ホンダ",
    "东风本田": "東風ホンダ",
    "广汽本田": "広汽ホンダ",
    "日产": "日産",
    "东风日产": "東風日産",
    "马自达": "マツダ",
    "三菱": "三菱",
    "铃木": "スズキ",
    "斯巴鲁": "スバル",
    "雷克萨斯": "レクサス",
    "英菲尼迪": "インフィニティ",
    "讴歌": "アキュラ",
}

# 長い名称（「上汽大众」「一汽-大众」など）を優先一致させるため降順で評価
DICT_KEYS_SORTED = sorted(DICT_ZH_TO_JA.keys(), key=len, reverse=True)

# -- LLM（任意） -------------------------------------------------
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "").strip() or "https://api.openai.com/v1"
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "").strip() or "gpt-4o-mini"

def translate_with_llm_ja(name_zh: str) -> str | None:
    """辞書に無いメーカー名を LLM で日本語に訳す（APIキーがある場合のみ）"""
    if not OPENAI_API_KEY:
        return None
    try:
        import requests
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        prompt = (
            "以下は中国語の自動車メーカー名です。日本語の一般的な呼称（カタカナ or 慣用表記）で1語～数語に簡潔に訳してください。"
            "国営/合弁の説明は不要です。略称（BYDなど）が広く使われる場合は括弧で補ってください。\n"
            f"メーカー名: {name_zh}\n"
            "出力はメーカー名のみ。"
        )
        payload = {
            "model": OPENAI_MODEL,
            "messages": [
                {"role": "system", "content": "You are a translator. Output ONLY the translated manufacturer name in Japanese."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.0,
        }
        r = requests.post(f"{OPENAI_BASE_URL}/chat/completions", headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        text = data["choices"][0]["message"]["content"].strip()
        # 保険: 改行や余分な引用符を除去
        text = re.sub(r"[\r\n\"“”]", "", text).strip()
        return text or None
    except Exception as e:
        print(f"⚠️ LLM translation failed for '{name_zh}': {e}")
        return None

# -- 変換ロジック -------------------------------------------------
def to_japanese_manufacturer(name_zh: str) -> str:
    """辞書で変換。無ければ部分一致・LLMを順に試す。最終手段は原文返し。"""
    if not isinstance(name_zh, str) or not name_zh.strip():
        return ""

    name = name_zh.strip()

    # 完全一致
    if name in DICT_ZH_TO_JA:
        return DICT_ZH_TO_JA[name]

    # 部分一致（長いキー優先）
    for key in DICT_KEYS_SORTED:
        if key in name:
            return DICT_ZH_TO_JA[key]

    # LLMにフォールバック（任意）
    ja = translate_with_llm_ja(name)
    if ja:
        return ja

    # どうしても無ければ原文を返す
    return name

# -- メイン処理 ---------------------------------------------------
def process_csv(csv_path: Path) -> Path | None:
    print(f"\n=== Processing {csv_path} ===")
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"⚠️ cannot read CSV: {e}")
        return None

    if "manufacturer" not in df.columns:
        print("ℹ️ skip (no 'manufacturer' column)")
        return None

    # 既存列を壊さず、manufacturer_ja を新設/更新
    ja_list = []
    unique_cache = {}  # 同一名の重複翻訳を避ける
    for val in df["manufacturer"].astype(str).fillna(""):
        if val in unique_cache:
            ja_list.append(unique_cache[val])
            continue
        ja = to_japanese_manufacturer(val)
        unique_cache[val] = ja
        ja_list.append(ja)

    df["manufacturer_ja"] = ja_list

    out_path = csv_path.with_name(csv_path.stem + "_with_maker_ja.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"✅ saved: {out_path}  rows={len(df)}")
    return out_path

def main():
    if len(sys.argv) < 2:
        print("Usage: python .github/scripts/stage_translate_maker_to_ja.py <csv1> [<csv2> ...]")
        sys.exit(1)

    # 引数はファイル or グロブを許容
    targets: list[Path] = []
    for arg in sys.argv[1:]:
        p = Path(arg)
        if p.exists() and p.suffix.lower() == ".csv":
            targets.append(p)
        else:
            for f in Path().glob(arg):
                if f.suffix.lower() == ".csv":
                    targets.append(f)

    if not targets:
        print("No CSV files matched.")
        sys.exit(0)

    for path in targets:
        process_csv(path)

if __name__ == "__main__":
    main()
