#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
translate_columns.py  (cacheless + dict-first + batch translation)

目的:
  - Autohome の設定CSVを日本語化し、最終出力(.ja.csv)を生成する。
  - ファイルキャッシュは一切使わない/作らない（毎回フル処理でも高速）。
  - コスト節約のため:
      * セクション/項目は辞書優先（APIを呼ばない）
      * 重複語を in-memory で集約し、まとめてバッチ翻訳
      * 数値・記号・ダッシュ等は翻訳スキップ
  - 既存仕様を壊さない:
      * 出力列は「セクション_ja」「項目_ja」+ グレード列（ヘッダは翻訳しない）
      * 価格等の表記ルールや用語は変更しない
      * YAMLのヒア構文等は不要（ここは純Python）

環境変数:
  - CSV_IN (必須)
  - DST_PRIMARY もしくは CSV_OUT（どちらか必須）
  - DST_SECONDARY（任意）
  - OPENAI_API_KEY（任意）: 未設定なら恒等（翻訳しない）
  - OPENAI_MODEL（任意・既定: gpt-4o-mini）
  - BATCH_SIZE（任意・既定: 80）
  - TIMEOUT_SEC（任意・既定: 25）

入出力:
  入力: 先頭2列が「セクション」「項目」、以降がグレード列（CN）
  出力: 「セクション_ja」「項目_ja」＋グレード列（ヘッダそのまま、中身を必要に応じ翻訳）
"""

from __future__ import annotations
import os
import re
from pathlib import Path
from typing import Dict, List
import pandas as pd

# ====== 環境変数 ======
SRC = os.environ.get("CSV_IN", "").strip()
DST_PRIMARY = os.environ.get("DST_PRIMARY", "").strip()
CSV_OUT = os.environ.get("CSV_OUT", "").strip()
DST_SECONDARY = os.environ.get("DST_SECONDARY", "").strip()

if not SRC:
    raise SystemExit("CSV_IN が未設定です。")

if not DST_PRIMARY:
    if CSV_OUT:
        DST_PRIMARY = CSV_OUT
    else:
        raise SystemExit("DST_PRIMARY か CSV_OUT のいずれかを設定してください。")

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "80"))
TIMEOUT_SEC = int(os.environ.get("TIMEOUT_SEC", "25"))
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"

# ====== ユーティリティ ======
def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8-sig")

def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")

def ensure_required_columns(df: pd.DataFrame) -> None:
    need = ["セクション", "項目"]
    for col in need:
        if col not in df.columns:
            raise ValueError(f"入力CSVに必須列 {col} が見当たりません。列名: {list(df.columns)}")

def is_trivial_no_translate(s: str) -> bool:
    """数値/記号/ダッシュ系は翻訳不要"""
    if s is None:
        return True
    x = str(s).strip()
    if not x:
        return True
    if re.fullmatch(r"[-—–·\.\/\s]+", x):
        return True
    # 数値（万/千/k 含む）、単位付き（元/円/¥）
    if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?(?:\s*[万千kK])?(?:\s*[元円¥])?", x):
        return True
    return False

# ====== 固定辞書（まずここを優先） ======
# 必要に応じて増やせます。既存ルールを壊さない安全な語のみ。
SECTION_DICT: Dict[str, str] = {
    "基本参数": "基本情報",
    "车身": "車体",
    "车身参数": "車体寸法",
    "外部配置": "外装装備",
    "内部配置": "内装装備",
    "座椅配置": "シート",
    "安全配置": "安全装備",
    "主/被动安全": "主/受動安全",
    "操控配置": "走行/操縦",
    "智驾辅助": "運転支援",
    "驾驶辅助": "運転支援",
    "灯光配置": "ライト",
    "多媒体配置": "マルチメディア",
    "空调/冰箱": "空調/冷蔵",
    "动力系统": "パワートレイン",
    "发动机": "エンジン",
    "电机": "モーター",
    "变速箱": "トランスミッション",
    "底盘转向": "シャシー/ステアリング",
    "车轮制动": "ホイール/ブレーキ",
    "保修政策": "保証",
    "整车质保": "車両保証",
}

ITEM_DICT: Dict[str, str] = {
    "厂商指导价": "メーカー希望小売価格",
    "经销商报价": "ディーラー販売価格",
    "排量(L)": "排気量(L)",
    "最大功率(kW)": "最大出力(kW)",
    "最大扭矩(N·m)": "最大トルク(N·m)",
    "变速箱类型": "変速機形式",
    "前悬架类型": "フロントサスペンション",
    "后悬架类型": "リアサスペンション",
    "驱动方式": "駆動方式",
    "车身结构": "ボディ構造",
    "长*宽*高(mm)": "全長×全幅×全高(mm)",
    "轴距(mm)": "ホイールベース(mm)",
    "整车质保": "車両保証",
    "主/副驾驶安全气囊": "運転席/助手席エアバッグ",
    "前/后排头部气囊(气帘)": "前/後席カーテンエアバッグ",
    "车道保持辅助系统": "レーンキープアシスト",
    "自适应巡航": "アダプティブクルーズ",
    "并线辅助": "ブラインドスポットモニター",
    "自动驻车": "オートホールド",
    "电动天窗": "電動サンルーフ",
    "全景天窗": "パノラマサンルーフ",
    "LED日间行车灯": "LEDデイタイムランニングライト",
    "大灯自动开闭": "オートライト",
    "车内中控锁": "集中ドアロック",
    "无钥匙进入系统": "キーレスエントリー",
    "无钥匙启动系统": "プッシュスタート",
    "多功能方向盘": "マルチファンクションステアリング",
    "定速巡航": "クルーズコントロール",
    "座椅材质": "シート素材",
    "主驾驶座椅调节": "運転席シート調整",
    "副驾驶座椅调节": "助手席シート調整",
    "前/后排座椅加热": "前/後席シートヒーター",
    "自动空调": "オートエアコン",
    "后座出风口": "後席エアアウトレット",
}

# ====== OpenAI: バッチ翻訳（未設定なら恒等） ======
def batch_translate_unique(values: List[str]) -> Dict[str, str]:
    """
    重複排除→バッチ翻訳→dict で返す。
    OPENAI_API_KEY が無ければ恒等返し。
    """
    # 事前フィルタ＆ユニーク化
    uniq: List[str] = []
    for v in values:
        x = (v or "").strip()
        if not x or is_trivial_no_translate(x):
            continue
        if x not in uniq:
            uniq.append(x)

    if not uniq or not OPENAI_API_KEY:
        # 恒等返し
        return {u: u for u in uniq}

    import requests
    out_map: Dict[str, str] = {}
    for i in range(0, len(uniq), BATCH_SIZE):
        chunk = uniq[i:i+BATCH_SIZE]
        # プロンプトは短文/項目想定・最小限
        sys_prompt = (
            "以下の各項目を日本語に短く自然に訳し、順番どおりに1行ずつ出力してください。"
            "数値・単位・記号は維持してください。余計な説明は一切不要です。"
        )
        user_payload = "\n".join(f"- {s}" for s in chunk)
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": OPENAI_MODEL,
                    "messages": [
                        {"role": "system", "content": sys_prompt},
                        {"role": "user", "content": user_payload},
                    ],
                    "temperature": 0.2,
                },
                timeout=TIMEOUT_SEC,
            )
            r.raise_for_status()
            text = r.json()["choices"][0]["message"]["content"].strip()
            # 出力は1行1訳を期待。万一箇条書き記号が返っても削ぎ落す。
            lines = [ln.strip(" -•\t") for ln in text.splitlines() if ln.strip()]
            if len(lines) != len(chunk):
                # 行数ズレは安全に補完
                # （壊さない：不足分は原文で埋める）
                while len(lines) < len(chunk):
                    lines.append(chunk[len(lines)])
                if len(lines) > len(chunk):
                    lines = lines[:len(chunk)]
        except Exception:
            # 失敗時は原文返し（壊さない）
            lines = chunk

        for src, ja in zip(chunk, lines):
            out_map[src] = (ja or src).strip()

    return out_map

# ====== メイン ======
def main():
    df = read_csv(Path(SRC))
    ensure_required_columns(df)

    # 出力器の骨格（ヘッダは既存仕様を維持）
    out = pd.DataFrame(index=df.index)
    out["セクション"] = df["セクション"]
    out["項目"] = df["項目"]
    out["セクション_ja"] = ""
    out["項目_ja"] = ""

    # グレード列（ヘッダは翻訳しない = そのまま）
    grade_cols = [c for c in df.columns if c not in ["セクション", "項目"]]
    for c in grade_cols:
        out[c] = df[c]

    # --- 1) セクション/項目: 辞書 → in-memory → APIバッチ ---
    # まず辞書適用
    sec_src = df["セクション"].astype(str).tolist()
    item_src = df["項目"].astype(str).tolist()

    sec_ja = [SECTION_DICT.get(s, None) for s in sec_src]
    item_ja = [ITEM_DICT.get(s, None) for s in item_src]

    # 未確定だけをAPI候補に集約（短期キャッシュ: seen）
    seen: Dict[str, str] = {}
    need_sec = [s for s, ja in zip(sec_src, sec_ja) if not ja and not is_trivial_no_translate(s)]
    need_item = [s for s, ja in zip(item_src, item_ja) if not ja and not is_trivial_no_translate(s)]

    # バッチ翻訳
    trans_map = batch_translate_unique(need_sec + need_item)
    seen.update(trans_map)

    # 確定（辞書 > seen > 恒等）
    out["セクション_ja"] = [
        SECTION_DICT.get(s) or seen.get(s) or (s if is_trivial_no_translate(s) else s)
        for s in sec_src
    ]
    out["項目_ja"] = [
        ITEM_DICT.get(s) or seen.get(s) or (s if is_trivial_no_translate(s) else s)
        for s in item_src
    ]

    # --- 2) グレード列のセル中身（必要に応じ：今回はコスト節約のため最小限） ---
    # 既存仕様を壊さないため、ここでは“必須そうな語”のみを翻訳候補にする。
    # （完全自動で全セルを訳すとコストとリスクが上がる。要件に応じて拡張可）
    # ここでは、ひらがな/カタカナ/漢字が混在しうるため、数値と記号だけスキップし、
    # それ以外は重複をまとめてバッチで処理する（ただし列ヘッダは触らない）。
    grade_values: List[str] = []
    for col in grade_cols:
        series = df[col].astype(str)
        for v in series:
            if not v or is_trivial_no_translate(v):
                continue
            grade_values.append(v.strip())

    # 既存 seen を活かして不足分だけ翻訳
    pending = [v for v in grade_values if v not in seen]
    if pending:
        seen.update(batch_translate_unique(pending))

    # 反映（原文維持ポリシー：訳があれば使う、なければ原文）
    for col in grade_cols:
        src_series = df[col].astype(str)
        out[col] = [
            seen.get(x.strip(), x) if not is_trivial_no_translate(x) else x
            for x in src_series
        ]

    # --- 3) 最終出力（CN列は出さない＝既存仕様）
    final_out = out[["セクション_ja", "項目_ja"] + grade_cols].copy()

    # --- 4) 保存 ---
    write_csv(final_out, Path(DST_PRIMARY))
    if DST_SECONDARY:
        write_csv(final_out, Path(DST_SECONDARY))

    print(f"✅ Wrote: {DST_PRIMARY}")
    if DST_SECONDARY:
        print(f"✅ Wrote: {DST_SECONDARY}")

if __name__ == "__main__":
    main()
