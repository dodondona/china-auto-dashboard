#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
translate_brand_model_llm.py
- 中国語のブランド/モデル名を Official(site) → Wikipedia/Wikidata の順（オプション）で解決
- 既存CSVに brand_ja / model_ja を付与して出力
- さらに、輸出名（地域で一般的に使われる別名）が判明する場合は model_export 列に付与（任意の最小セット）
"""

import os
import json
import time
import argparse
import re
import pandas as pd
from tqdm import tqdm

# ========================= Wikipedia / Wikidata ==============================

def lookup_wikipedia(term: str):
    """zh Wikipediaから言語間リンク（ja優先、なければen）を取得"""
    import wikipediaapi
    # WikimediaのUAポリシーに従ってUser-Agentを明示
    wiki = wikipediaapi.Wikipedia(
        language='zh',
        user_agent='china-auto-dashboard/1.0 (https://github.com/dodondona/china-auto-dashboard; contact: github-actions)'
    )
    p = wiki.page(term)
    if not p.exists():
        return None
    links = p.langlinks
    if 'ja' in links:
        return links['ja'].title
    if 'en' in links:
        return links['en'].title
    return None

def lookup_wikidata(term: str):
    """Wikidataで zh 検索→ja/enラベルを取得（ja優先）"""
    import requests
    try:
        s = requests.get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbsearchentities",
                "language": "zh",
                "format": "json",
                "search": term,
                "type": "item",
                "limit": 5,
            },
            timeout=10,
        ).json()
        if not s.get("search"):
            return None
        qid = s["search"][0]["id"]
        ent = requests.get(
            f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
            timeout=10,
        ).json()["entities"][qid]
        labels = ent.get("labels", {})
        return labels.get("ja", labels.get("en", {})).get("value")
    except Exception:
        return None

def resolve_with_optional_wikidata(term: str, use_wd=True, sleep_sec=0.1):
    """Wikipedia→Wikidataの順に解決（ja>en>元のzh）"""
    if not term or str(term).strip() == "":
        return {"ja": term}
    ja = lookup_wikipedia(term)
    if not ja and use_wd:
        ja = lookup_wikidata(term)
    time.sleep(sleep_sec)
    return {"ja": ja or term}

# ========================= ユーティリティ（最小） ============================

BAD_TITLES = (
    "曖昧さ回避", "disambiguation", "主題歌", "アルバム", "ドラマ", "映画", "楽曲"
)

def is_ascii_word(s: str) -> bool:
    try:
        str(s).encode("ascii")
        return True
    except Exception:
        return False

def sanitize_title(brand_zh: str, model_zh: str, title: str) -> str | None:
    """Wikipedia由来タイトルの軽い健全化（変な題名や曖昧ページを弾く）"""
    if not title:
        return None
    t = str(title).strip()
    low = t.lower()

    # 曖昧さ回避/非自動車ワードを排除
    if any(k in t for k in BAD_TITLES) or "(disambiguation)" in low or "（曖昧さ回避）" in t:
        return None

    # Teslaは英字モデルを優先
    if brand_zh in {"特斯拉", "テスラ", "Tesla"}:
        m = re.search(r"\bModel\s+[3YSX]\b", t, re.IGNORECASE)
        if m:
            return m.group(0)

    # 元のモデル名が英字なら、それを尊重（括弧付きは除外）
    if is_ascii_word(model_zh):
        if "(" in t and ")" in t:
            return None
        return model_zh

    return t

# ---- 輸出名の最小マップ（必要最低限のみ。増やす場合はここに追加） ----
# できる限り規則化し、固定の個別辞書は縮小
EXPORT_NAME_RULES = [
    # BYD
    (re.compile(r"^Yuan\s*PLUS\b", re.I), "Atto 3"),
    (re.compile(r"^Seagull\b", re.I), "Dolphin Mini"),   # 地域により
    # Geely
    (re.compile(r"^Binyue\b", re.I), "Coolray"),
    (re.compile(r"^Boyue\s*L?\b", re.I), "Monjaro"),     # L/非LともMonjaro地域あり
    (re.compile(r"^Xingyue\s*L?\b", re.I), "Monjaro"),
    # Haval
    (re.compile(r"\bBig\s*Dog\b", re.I), "Dargo"),
]

def guess_export_name(brand_en: str, model_en: str) -> str | None:
    """輸出名が一般に流通している場合の最小推定（ルールベース）"""
    if not model_en:
        return None
    name = str(model_en).strip()
    for pat, export in EXPORT_NAME_RULES:
        if pat.search(name):
            return export
    # VW/トヨタ等は中国・欧州で固有名が固定化しており輸出名差が小さいためデフォルトNone
    return None

# =============================== Main =======================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="入力CSV")
    ap.add_argument("--output", required=True, help="出力CSV")
    ap.add_argument("--brand-col", default="brand")
    ap.add_argument("--model-col", default="model")
    ap.add_argument("--brand-ja-col", default="brand_ja")
    ap.add_argument("--model-ja-col", default="model_ja")
    ap.add_argument("--model-export-col", default="model_export", help="輸出名を出力する列名（存在すれば上書き）")
    ap.add_argument("--cache", default=".cache/global_map.json")
    ap.add_argument("--sleep", type=float, default=0.1)
    ap.add_argument("--use-wikidata", action="store_true", help="Wikidata補完を有効化")
    ap.add_argument("--use-official", action="store_true",
                    help="公式サイトCSE補完を有効化（環境変数 GOOGLE_API_KEY / GOOGLE_CSE_ID が必要）")
    ap.add_argument("--official-first", action="store_true",
                    help="--use-official と併用時、公式 → Wikipedia/Wikidata の順で解決")
    args = ap.parse_args()

    print(f"Translating: {args.input} -> {args.output}")

    # 出力/キャッシュディレクトリ
    if os.path.dirname(args.output):
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
    if os.path.dirname(args.cache):
        os.makedirs(os.path.dirname(args.cache), exist_ok=True)

    # キャッシュ
    cache = {}
    if os.path.exists(args.cache):
        with open(args.cache, "r", encoding="utf-8") as f:
            try:
                cache = json.load(f)
            except Exception:
                cache = {}

    # 入力
    df = pd.read_csv(args.input)
    brands = df[args.brand_col].dropna().unique().tolist()
    models = df[args.model_col].dropna().unique().tolist()

    brand_map, model_map = {}, {}

    # ----------------------------- Brand -------------------------------------
    # ブランドは従来通り：Wikipedia/Wikidataで十分安定（公式検索は基本不要）
    for b in tqdm(brands, desc="brand"):
        key = f"brand::{b}"
        if key in cache:
            brand_map[b] = cache[key]
            continue
        res = resolve_with_optional_wikidata(b, use_wd=args.use_wikidata, sleep_sec=args.sleep)
        brand_map[b] = res["ja"]
        cache[key] = brand_map[b]

    # ----------------------------- Model -------------------------------------
    for m in tqdm(models, desc="model"):
        key = f"model::{m}"
        if key in cache:
            model_map[m] = cache[key]
            continue

        ja = None
        if args.use_official and args.official_first:
            # 公式 → Wiki/Wikidata
            try:
                from tools.official_lookup import find_official_english
                guessed = find_official_english("", m)
                if guessed:
                    ja = guessed
            except Exception:
                ja = None
            if not ja:
                res = resolve_with_optional_wikidata(m, use_wd=args.use_wikidata, sleep_sec=args.sleep)
                ja = res["ja"]
        else:
            # 従来どおり Wiki/Wikidata → 公式
            res = resolve_with_optional_wikidata(m, use_wd=args.use_wikidata, sleep_sec=args.sleep)
            ja = res["ja"]
            if args.use_official and (not ja or ja == m):
                try:
                    from tools.official_lookup import find_official_english
                    guessed = find_official_english("", m)
                    if guessed:
                        ja = guessed
                except Exception:
                    pass

        model_map[m] = ja or m
        cache[key] = model_map[m]
        time.sleep(args.sleep)

    # ----------------------------- 書き戻し ----------------------------------
    df[args.brand_ja_col] = df[args.brand_col].map(lambda x: brand_map.get(str(x), str(x)))

    def _model_name(row):
        brand_zh = str(row[args.brand_col])
        model_zh = str(row[args.model_col])
        cand = model_map.get(model_zh, model_zh)

        # Wikipediaの変な題名を軽く修正（公式優先でも最終保険）
        fixed = sanitize_title(brand_zh, model_zh, cand)
        if fixed and fixed != model_zh:
            return fixed

        # 公式を行文脈で再トライ（ブランドを渡す）
        if args.use_official and (not cand or cand == model_zh):
            try:
                from tools.official_lookup import find_official_english
                guessed = find_official_english(brand_zh, model_zh)
                if guessed:
                    return guessed
            except Exception:
                pass

        # 英字原文を尊重
        if is_ascii_word(model_zh):
            return model_zh

        return cand

    df[args.model_ja_col] = df.apply(_model_name, axis=1)

    # ----------------------------- 輸出名列 ----------------------------------
    # 推定された英字(model_ja)から、一般的な輸出名がある場合のみ別列に出力
    def _export_name(row):
        brand_en = str(row[args.brand_ja_col])  # brand_ja は多くが英字化される想定
        model_en = str(row[args.model_ja_col])
        exp = guess_export_name(brand_en, model_en)
        return exp if exp else ""

    df[args.model_export_col] = df.apply(_export_name, axis=1)

    # 出力・キャッシュ保存
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    with open(args.cache, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    print(f"✅ Done. Saved to {args.output}")

if __name__ == "__main__":
    main()
