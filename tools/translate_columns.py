#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
translate_columns.py

目的:
  - Autohome の設定CSVを日本語化し、最終出力(.ja.csv)を生成する。
  - 差分再利用: 「前回のCNスナップショット(cache/<id>/cn.csv)」と「前回の最終出力(.ja.csv)」を突き合わせ、
    変更されていないセルは前回のJAをコピー、変更セルのみ翻訳する。
  - cache は CN スナップショットのみを保存（JAの別枠キャッシュは保存しない）。
  - 後方互換: 過去に cache/<id>/ja.csv がある場合は参照は可能（保存はしない）。

入出力(環境変数):
  - CSV_IN         : 入力CSV(必須)
  - CSV_OUT        : 互換エイリアス。なければ DST_PRIMARY を参照
  - DST_PRIMARY    : 最終出力(推奨)。CSV_OUTが未設定ならこちらを必須とみなす
  - DST_SECONDARY  : 追加出力(任意)。指定されていれば同一内容を書き出す
  - SERIES_ID      : キャッシュ保存先のサブフォルダ名に使用 (cache/<SERIES_ID>/)

前提となるCSV構造(最低限):
  - 列: 「セクション」「項目」+ 複数のグレード列 (CN表示)
  - 最終出力では: 「セクション_ja」「項目_ja」を追加し、グレード列の中身はJA化、列見出しも可能ならJAへ

翻訳について:
  - 本スクリプトは差分再利用を最優先。翻訳器はあくまでフォールバック。
  - OPENAI_API_KEY が設定されている場合に限り、簡易のOpenAI API呼び出しをサポート（オプション）。
    未設定/失敗時は、恒等(=原文返し)でフォールバックします。
  - 実運用の翻訳は既存の上流ステップ/別スクリプトに任せてOK。ここでは“壊さないこと”を最優先。

注意:
  - 既存パイプラインとの互換を重視し、列名・エンコーディング(utf-8-sig)・例外時挙動を保守的に実装。
"""

from __future__ import annotations
import os
import re
import csv
import json
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd

# ----------------------------
# 環境変数とパス解決
# ----------------------------
SRC = os.environ.get("CSV_IN", "").strip()
DST_PRIMARY = os.environ.get("DST_PRIMARY", "").strip()
CSV_OUT = os.environ.get("CSV_OUT", "").strip()  # 互換
DST_SECONDARY = os.environ.get("DST_SECONDARY", "").strip()
SERIES_ID = os.environ.get("SERIES_ID", "").strip()

if not SRC:
    raise SystemExit("CSV_IN が未設定です。")

if not DST_PRIMARY:
    # 互換: CSV_OUT 優先。無ければエラー
    if CSV_OUT:
        DST_PRIMARY = CSV_OUT
    else:
        raise SystemExit("DST_PRIMARY か CSV_OUT のいずれかを設定してください。")

# cache/<id>/cn.csv
def infer_series_id() -> str:
    if SERIES_ID:
        return SERIES_ID
    # 入力CSVパスから series_id を推定（数字連続を優先）
    name = Path(SRC).stem
    m = re.search(r"(\d{3,})", name)
    if m:
        return m.group(1)
    # ディレクトリ名等からも試す
    m2 = re.search(r"(\d{3,})", str(Path(SRC).parent))
    if m2:
        return m2.group(1)
    return "unknown"

_SERIES = infer_series_id()
CACHE_DIR = Path("cache") / _SERIES
CN_SNAP = CACHE_DIR / "cn.csv"
# 後方互換: 旧来のJAキャッシュ(参照のみ) ※新規保存はしない
JA_CACHE_LEGACY = CACHE_DIR / "ja.csv"

# ----------------------------
# ユーティリティ
# ----------------------------
def read_csv(path: Path) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return None

def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")

def norm_cn_cell(x: str) -> str:
    """CNセル比較用に正規化（空白統一・全角空白除去・改行等の空白化）"""
    if x is None:
        return ""
    s = str(x)
    s = s.replace("\u3000", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def same_shape_and_headers(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    if a is None or b is None:
        return False
    if a.shape != b.shape:
        return False
    return list(a.columns) == list(b.columns)

def ensure_required_columns(df: pd.DataFrame) -> None:
    need = ["セクション", "項目"]
    for col in need:
        if col not in df.columns:
            raise ValueError(f"入力CSVに必須列 {col} が見当たりません。列名: {list(df.columns)}")

# ----------------------------
# 翻訳器（最小限/フォールバック安全）
# ----------------------------
_OPENAI_READY = bool(os.environ.get("OPENAI_API_KEY", "").strip())
def translate_text_ja(s: str) -> str:
    """安全第一: 既訳再利用が効かなかった時の最終フォールバック。
       基本は恒等返し（壊さない）。OPENAI_API_KEY がある場合のみAPI試行。
    """
    s = str(s or "").strip()
    if not s:
        return s
    if not _OPENAI_READY:
        return s  # 恒等
    try:
        # ここは“使えるなら使う”に留める。API仕様は変わりやすいので最小限。
        import requests
        key = os.environ["OPENAI_API_KEY"].strip()
        endpoint = os.environ.get("OPENAI_API_BASE", "https://api.openai.com/v1/chat/completions")
        model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
        headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        prompt = f"次の中国語（または英語）を日本語に簡潔に訳してください：\n{s}"
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
        }
        r = requests.post(endpoint, headers=headers, json=payload, timeout=20)
        r.raise_for_status()
        data = r.json()
        cand = data["choices"][0]["message"]["content"].strip()
        return cand or s
    except Exception:
        return s  # 失敗しても壊さない

# ----------------------------
# メイン処理
# ----------------------------
def main():
    # 1) 入力読込
    df = read_csv(Path(SRC))
    if df is None:
        raise SystemExit(f"入力CSVが読めません: {SRC}")
    ensure_required_columns(df)

    # 2) 既存キャッシュ/前回出力の取得
    prev_cn_df = read_csv(CN_SNAP)
    prev_out_df = read_csv(Path(DST_PRIMARY))  # 前回の最終出力(.ja.csv)
    # 後方互換: 旧来の cache/<id>/ja.csv を参照（存在時のみ）
    prev_ja_df = read_csv(JA_CACHE_LEGACY)

    # 差分再利用フラグ
    enable_reuse = (prev_cn_df is not None) and same_shape_and_headers(df, prev_cn_df) and (
        (prev_out_df is not None) or (prev_ja_df is not None)
    )

    # 3) 出力器の骨格: out_full をCN列で初期化 → 後で見出し/中身をJAに置換
    #    列構成: [セクション, 項目, セクション_ja, 項目_ja] + grade列(CNヘッダのまま)
    out_full = pd.DataFrame(index=df.index)
    out_full["セクション"] = df["セクション"]
    out_full["項目"] = df["項目"]
    out_full["セクション_ja"] = ""
    out_full["項目_ja"] = ""
    grade_cols: List[str] = [c for c in df.columns if c not in ["セクション", "項目"]]
    for c in grade_cols:
        out_full[c] = df[c]

    # 4) 既訳再利用マップ（セクション/項目）
    sec_map_old: Dict[str, str] = {}
    item_map_old: Dict[str, str] = {}

    def build_maps_from_prev_out():
        # prev_out_df: [セクション_ja, 項目_ja, grade...], CN列は無い前提
        # 行対応は prev_cn_df と df が形状一致なので、同じ index 順で比較OK
        if prev_out_df is None or prev_cn_df is None:
            return
        if ("セクション_ja" not in prev_out_df.columns) or ("項目_ja" not in prev_out_df.columns):
            return
        # セクション
        for cur, old_cn, old_ja in zip(df["セクション"].astype(str),
                                       prev_cn_df["セクション"].astype(str),
                                       prev_out_df["セクション_ja"].astype(str)):
            if norm_cn_cell(cur) == norm_cn_cell(old_cn):
                sec_map_old[str(cur).strip()] = str(old_ja).strip() or str(cur).strip()
        # 項目
        for cur, old_cn, old_ja in zip(df["項目"].astype(str),
                                       prev_cn_df["項目"].astype(str),
                                       prev_out_df["項目_ja"].astype(str)):
            if norm_cn_cell(cur) == norm_cn_cell(old_cn):
                item_map_old[str(cur).strip()] = str(old_ja).strip() or str(cur).strip()

    def build_maps_from_legacy_cache():
        if prev_ja_df is None or prev_cn_df is None:
            return
        if ("セクション_ja" not in prev_ja_df.columns) or ("項目_ja" not in prev_ja_df.columns):
            return
        for cur, old_cn, old_ja in zip(df["セクション"].astype(str),
                                       prev_cn_df["セクション"].astype(str),
                                       prev_ja_df["セクション_ja"].astype(str)):
            if norm_cn_cell(cur) == norm_cn_cell(old_cn):
                sec_map_old[str(cur).strip()] = str(old_ja).strip() or str(cur).strip()
        for cur, old_cn, old_ja in zip(df["項目"].astype(str),
                                       prev_cn_df["項目"].astype(str),
                                       prev_ja_df["項目_ja"].astype(str)):
            if norm_cn_cell(cur) == norm_cn_cell(old_cn):
                item_map_old[str(cur).strip()] = str(old_ja).strip() or str(cur).strip()

    if enable_reuse:
        if prev_out_df is not None:
            build_maps_from_prev_out()
        elif prev_ja_df is not None:
            build_maps_from_legacy_cache()

    # 5) セクション/項目のJA埋め
    def map_or_translate(d: Dict[str, str], src: str) -> str:
        src = str(src or "").strip()
        if not src:
            return src
        if src in d:
            return d[src]
        # 既訳が無ければ最終手段で翻訳器（恒等フォールバック）
        ja = translate_text_ja(src)
        d[src] = ja
        return ja

    out_full["セクション_ja"] = df["セクション"].map(lambda x: map_or_translate(sec_map_old, x))
    out_full["項目_ja"]       = df["項目"].map(lambda x: map_or_translate(item_map_old, x))

    # 6) グレード列の「列見出し（ヘッダ）」のJA再利用
    #    prev_out_df があれば、そのヘッダ(セクション_ja/項目_jaを含む)を踏襲するのが安全。
    if enable_reuse and (prev_out_df is not None):
        # prev_out_df: [セクション_ja, 項目_ja, <grade_ja>...]
        # out_full   : [セクション, 項目, セクション_ja, 項目_ja, <grade_cn>...]
        fixed = list(out_full.columns)[:4]  # ["セクション", "項目", "セクション_ja", "項目_ja"]
        ja_grade_headers = list(prev_out_df.columns)[2:]  # セクション_ja/項目_ja の後ろがグレード列
        if len(ja_grade_headers) == len(grade_cols):
            out_full.columns = fixed + ja_grade_headers
        # もし数が合わなければそのまま（安全優先）
    else:
        # ヘッダ翻訳を強行しない（安全運用）。必要ならここに独自辞書や正規化を差し込む。
        pass

    # 7) グレード列の「セルの中身」再利用/翻訳
    #    prev_out_df があれば「変更なしセル」は prev_out_df から流用。
    if enable_reuse and (prev_cn_df is not None):
        # prev_out_df が優先 / なければ legacy
        ja_source = prev_out_df if prev_out_df is not None else prev_ja_df
        # インデックスで同じ行、列位置は:
        # - prev_out_df には CN列が無いので、CN→JAの列シフトが必要
        # - prev_ja_df(legacy) は out_full と同じ列だった想定（互換的に扱う）
        for i in range(len(df)):
            for j, col in enumerate(grade_cols, start=0):
                cur = norm_cn_cell(df.iat[i, 2 + j])  # df: [セクション, 項目, grade0, grade1...]
                old = norm_cn_cell(prev_cn_df.iat[i, 2 + j])
                out_col_idx = 4 + j  # out_full: [sec, item, sec_ja, item_ja, grade...]
                if cur == old and ja_source is not None:
                    try:
                        if ja_source is prev_out_df:
                            # prev_out_df は CN列が無いぶん、2列左に詰まっている
                            out_full.iat[i, out_col_idx] = ja_source.iat[i, out_col_idx - 2]
                        else:
                            # legacy: 列構造が out_full と一致していた想定
                            out_full.iat[i, out_col_idx] = ja_source.iat[i, out_col_idx]
                        continue
                    except Exception:
                        # ずれがあれば翻訳へフォールバック
                        pass
                # 変更あり or 参照失敗 → 翻訳（恒等フォールバック）
                out_full.iat[i, out_col_idx] = translate_text_ja(df.iat[i, 2 + j])
    else:
        # 再利用不可（初回/列変動など） → すべて翻訳（恒等フォールバック）
        for i in range(len(df)):
            for j, col in enumerate(grade_cols, start=0):
                out_full.iat[i, 4 + j] = translate_text_ja(df.iat[i, 2 + j])

    # 8) 最終出力（CN列は出力しない = 軽量版）
    final_out = out_full[["セクション_ja", "項目_ja"] + list(out_full.columns[4:])].copy()

    # 9) 書き出し
    write_csv(final_out, Path(DST_PRIMARY))
    if DST_SECONDARY:
        write_csv(final_out, Path(DST_SECONDARY))

    # 10) キャッシュ保存（CNのみ）/ JAキャッシュは保存しない
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    # 入力そのものをスナップショット（CN）
    write_csv(df, CN_SNAP)

    print(f"✅ Wrote: {DST_PRIMARY}")
    if DST_SECONDARY:
        print(f"✅ Wrote: {DST_SECONDARY}")
    print(f"📦 Repo cache CN: {CN_SNAP}")
    # 旧来: JAキャッシュは保存しません（参照のみ）
    # print(f"📦 Repo cache JA: {JA_CACHE_LEGACY} (not saved anymore)")

if __name__ == "__main__":
    main()
