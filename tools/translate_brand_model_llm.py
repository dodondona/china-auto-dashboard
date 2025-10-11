#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Translate brand/model to global names with priority:
  1) Official sites via Google Custom Search (if GOOGLE_API_KEY & GOOGLE_CSE_ID are set)
  2) Wikipedia zh -> en interlanguage link
  3) LLM (OpenAI) fallback

No persistent cache is used.

CLI:
  python tools/translate_brand_model_llm.py \
    --input data/autohome_raw_2025-08_with_brand.csv \
    --output data/autohome_raw_2025-08_with_brand_ja.csv \
    --brand-col brand --model-col model \
    --brand-ja-col brand_ja --model-ja-col model_ja \
    --model gpt-4o

Required columns in input: brand, model (or names you pass by args)
Outputs (adds/overwrites):
  - brand_ja           : グローバル/日本語側のブランド表記（英字優先・JP既知はカタカナのまま）
  - model_ja           : グローバルなモデル英名（輸出名）。なければ妥当な英語表記
  - model_official_en  : Official ステップで得た英名（あれば）
  - source_model       : "official" / "wikipedia" / "llm" / "none"
"""

import argparse
import os
import re
import time
from typing import Dict, Optional, Tuple, List

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ===== Utilities =========================================================

def norm_space(s: str) -> str:
    return re.sub(r'\s+', ' ', s).strip()

def pick_english_like_token(txt: str) -> Optional[str]:
    """
    タイトルやスニペットから「モデル名っぽい英字」候補を抽出。
    例: "BYD Seal 06 – New Energy" -> "Seal 06"
    """
    if not txt:
        return None
    # 英字で始まり英字/数字/空白/ハイフンが続くフレーズを広めに拾う
    # ただし一般語の "Category", "File", "Order" 等は後で除外
    tokens = re.findall(r'[A-Z][A-Za-z0-9\- ]{1,40}', txt)
    if not tokens:
        return None
    # ノイズ除去
    bad = {'Category', 'File', 'Order', 'Electric Cars', 'SUVs', 'Cars', 'COROLLA', 'CAMRY', 'A6', 'CR', 'BMW'}
    cleaned = []
    for t in tokens:
        t = t.strip(' -')
        if len(t) < 2:
            continue
        if t in bad:
            continue
        cleaned.append(t)
    return cleaned[0] if cleaned else None

def katakana_brand_friendly(brand_en: str) -> str:
    """
    ブランドの日本語既定表記（最低限）。“辞書”を避けるため、ごく一部だけ。
    最終的には英字ブランドをそのまま返すことを基本とし、JPで一般的な訳が明確なもののみ置換。
    """
    if not brand_en:
        return brand_en
    mapping = {
        'Toyota': 'トヨタ',
        'Nissan': '日産',
        'Honda': 'ホンダ',
        'BMW': 'BMW',
        'Mercedes-Benz': 'メルセデス・ベンツ',
        'Volkswagen': 'フォルクスワーゲン',
        'Audi': 'アウディ',
        'Buick': 'ビュイック',
        'Geely': 'Geely',
        'Geely Galaxy': 'Geely Galaxy',
        'BYD': 'BYD',
        'Wuling': 'Wuling',
        'Hongqi': '紅旗',
        'XPeng': 'XPeng',
        'Chery': 'Chery',
        'Changan': 'Changan',
        'Haval': 'Haval',
        'Tesla': 'テスラ',
        'AITO': 'AITO',
        'Leapmotor': 'Leapmotor',
        'Xiaomi Auto': 'Xiaomi Auto',
    }
    return mapping.get(brand_en, brand_en)

# ===== Brand official domains (entry points) ============================
# ここは辞書ではなく「クローリング開始URLのリスト」という位置付け
BRAND_OFFICIAL_SITES: Dict[str, List[str]] = {
    # 欧米・日系
    'Toyota': ['https://www.toyota-global.com', 'https://www.toyota.com.cn', 'https://www.toyota.com'],
    'Nissan': ['https://www.nissan-global.com', 'https://www.nissan.com.cn', 'https://www.nissanusa.com'],
    'Honda': ['https://global.honda', 'https://www.honda.com.cn', 'https://automobiles.honda.com'],
    'Volkswagen': ['https://www.vw.com', 'https://www.vw.com.cn', 'https://www.volkswagen-newsroom.com'],
    'BMW': ['https://www.bmw.com', 'https://www.bmw.com.cn'],
    'Mercedes-Benz': ['https://www.mercedes-benz.com', 'https://www.mercedes-benz.com.cn'],
    'Audi': ['https://www.audi.com', 'https://www.audi.cn'],
    'Buick': ['https://www.buick.com.cn', 'https://www.buick.com'],
    'Tesla': ['https://www.tesla.com'],

    # 中国系
    'BYD': ['https://www.byd.com', 'https://www.byd.com/en', 'https://www.byd.com/jp', 'https://www.bydauto.com.cn'],
    'Geely': ['https://global.geely.com', 'https://www.geely.com'],
    'Geely Galaxy': ['https://galaxy.geely.com'],
    'Wuling': ['https://www.sgmw.com.cn', 'https://wuling.id'],
    'Hongqi': ['https://www.hongqi-auto.com', 'https://www.faw-hongqi.com.cn'],
    'XPeng': ['https://heyxpeng.com', 'https://en.xiaopeng.com', 'https://www.xiaopeng.com'],
    'Chery': ['https://www.cheryinternational.com', 'https://www.chery.cn'],
    'Changan': ['https://www.changan.com.cn', 'https://global.changan.com.cn'],
    'Haval': ['https://www.haval-global.com', 'https://www.haval.com.cn'],
    'Leapmotor': ['https://www.leapmotor.com'],
    'AITO': ['https://www.aitoauto.com'],
    'Xiaomi Auto': ['https://www.mi.com', 'https://www.mi.com/auto'],
}

# ===== Google CSE (Official) ===========================================

def cse_search_official(brand_en: str, model_zh: str) -> Optional[str]:
    """
    Google CSEでブランド公式サイトを site: で絞り、中国語モデル名で検索。
    戻り値: モデル英名候補（公式の英語表記をできるだけ抽出）
    """
    api_key = os.environ.get("GOOGLE_API_KEY")
    cse_id = os.environ.get("GOOGLE_CSE_ID")
    if not api_key or not cse_id:
        return None

    sites = BRAND_OFFICIAL_SITES.get(brand_en, [])
    search_bases = []
    if sites:
        for s in sites:
            domain = re.sub(r'^https?://', '', s).strip('/')
            search_bases.append(f'site:{domain}')
    # 公式サイトが未登録なら全Webで検索（ただしノイズが増える）
    if not search_bases:
        search_bases = ['']

    q_variants = []
    # 中国語モデル名 + 公式サイト
    for base in search_bases:
        base_sp = (base + ' ') if base else ''
        q_variants.append(f'{base_sp}"{model_zh}"')
        q_variants.append(f'{base_sp}{model_zh} {brand_en}')
    # ブランド英名 + 中国語モデル名
    q_variants.append(f'{brand_en} {model_zh}')

    for q in q_variants[:4]:  # 無駄打ちしすぎない
        try:
            url = "https://www.googleapis.com/customsearch/v1"
            resp = requests.get(url, params={"key": api_key, "cx": cse_id, "q": q, "num": 3}, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            items = data.get("items") or []
            for it in items:
                title = norm_space(it.get("title", ""))
                snippet = norm_space(it.get("snippet", ""))
                link = it.get("link", "")
                # URLスラッグから候補
                slug = None
                try:
                    path = re.sub(r'^https?://[^/]+', '', link)
                    segs = [s for s in path.split('/') if s]
                    for s in segs[::-1]:
                        if re.search(r'[a-zA-Z]', s) and not re.match(r'^\d+$', s):
                            slug = re.sub(r'[-_]', ' ', s)
                            break
                except Exception:
                    pass
                # タイトル/スニペットから英名候補
                cand = pick_english_like_token(title) or pick_english_like_token(snippet) or (slug.title() if slug else None)
                if cand:
                    return cand
        except Exception:
            continue
    return None

# ===== Wikipedia ========================================================

WIKI_AGENT = "china-auto-dashboard-bot/1.0 (contact: your-email@example.com)"
WIKI_API = "https://zh.wikipedia.org/w/api.php"

def wikipedia_zh_to_en_title(query_zh: str) -> Optional[str]:
    """
    zh で検索 → 最上位ページ → 言語リンクから en タイトル取得。
    """
    try:
        # 1) search
        r = requests.get(WIKI_API, params={
            "action": "query",
            "list": "search",
            "srsearch": query_zh,
            "srlimit": 1,
            "format": "json"
        }, headers={"User-Agent": WIKI_AGENT}, timeout=15)
        if r.status_code != 200:
            return None
        js = r.json()
        hits = js.get("query", {}).get("search", [])
        if not hits:
            return None
        page_title_zh = hits[0]["title"]

        # 2) langlinks to en
        r2 = requests.get(WIKI_API, params={
            "action": "query",
            "prop": "langlinks",
            "titles": page_title_zh,
            "lllanguages": "en",
            "lllimit": 50,
            "format": "json"
        }, headers={"User-Agent": WIKI_AGENT}, timeout=15)
        if r2.status_code != 200:
            return None
        js2 = r2.json()
        pages = js2.get("query", {}).get("pages", {})
        for _, p in pages.items():
            lls = p.get("langlinks", []) or []
            for ll in lls:
                if ll.get("lang") == "en":
                    return ll.get("*")  # English page title
        # enが無い場合は zh タイトルをローマ字化せず、そのまま（最後のLLMに回す）
        return None
    except Exception:
        return None

# ===== LLM (OpenAI) =====================================================

def llm_translate_brand_model(brand_zh: str, model_zh: str, model_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    LLMでブランド英名とモデル英名（輸出名・グローバル名）を推定。
    """
    import json
    import openai  # openai>=1.0 style
    openai.api_key = os.environ.get("OPENAI_API_KEY")
    if not openai.api_key:
        return None, None

    system = (
        "You are an automotive product nomenclature expert. "
        "Given a car brand (in Chinese) and model (in Chinese), output the global English brand name "
        "and the official/export English model name used outside China. If the model has no English export name, "
        "return a faithful English rendering that matches how the brand markets it domestically (e.g., Qin PLUS -> Qin PLUS). "
        "Return JSON with keys: brand_en, model_en."
    )
    user = f"brand_zh: {brand_zh}\nmodel_zh: {model_zh}"

    try:
        resp = openai.ChatCompletion.create(  # for compat with older SDKs used on Actions
            model=model_name,
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": user}],
            temperature=0.0,
            timeout=30
        )
        content = resp["choices"][0]["message"]["content"]
        m = re.search(r'\{.*\}', content, flags=re.S)
        if not m:
            return None, None
        data = json.loads(m.group(0))
        be = data.get("brand_en")
        me = data.get("model_en")
        return (be.strip() if be else None, me.strip() if me else None)
    except Exception:
        return None, None

# ===== Resolution pipeline ==============================================

def resolve_brand_en(brand_zh: str, llm_model: str) -> str:
    """
    ブランド英名の標準化：Official→Wiki→LLM の順で。
    ただしブランドはモデルより揺れが少ないため、最終的に英字ブランドを返し、JP有名どころはカタカナに置換。
    """
    # 1) LLM 少量（ブランドは短いので負荷小）
    brand_en = None
    be1, _ = llm_translate_brand_model(brand_zh, "", llm_model)
    if be1:
        brand_en = be1

    # 2) Wikipedia: brand 単体検索 → en title
    if not brand_en:
        en = wikipedia_zh_to_en_title(brand_zh)
        if en:
            brand_en = en

    # 3) 最後のフォールバック：そのまま
    if not brand_en:
        brand_en = brand_zh

    # 表示はできるだけ既知の日本語表記へ
    return katakana_brand_friendly(brand_en)

def resolve_model_en(brand_en: str, model_zh: str, llm_model: str) -> Tuple[Optional[str], Optional[str], str]:
    """
    モデル英名の決定: Official(CSE) -> Wikipedia -> LLM
    戻り値: (model_ja/global_en, model_official_en, source)
    """
    # 1) Official（CSE）
    off = cse_search_official(brand_en, model_zh)
    if off:
        return off, off, "official"

    # 2) Wikipedia
    wiki_en = wikipedia_zh_to_en_title(model_zh)
    if wiki_en:
        return wiki_en, None, "wikipedia"

    # 3) LLM
    be, me = llm_translate_brand_model(brand_en, model_zh, llm_model)
    if me:
        return me, None, "llm"

    # 4) どうしても出ない場合は中国語のまま
    return model_zh, None, "none"

# ===== Main =============================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--brand-col", dest="brand_col", default="brand")
    ap.add_argument("--model-col", dest="model_col", default="model")
    ap.add_argument("--brand-ja-col", dest="brand_ja_col", default="brand_ja")
    ap.add_argument("--model-ja-col", dest="model_ja_col", default="model_ja")
    ap.add_argument("--model", dest="llm_model", default="gpt-4o-mini")
    ap.add_argument("--sleep", type=float, default=0.1)
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    for col in [args.brand_col, args.model_col]:
        if col not in df.columns:
            raise RuntimeError(f"Input must contain '{col}'. columns={list(df.columns)}")

    # 出力列を用意
    if args.brand_ja_col not in df.columns:
        df[args.brand_ja_col] = ""
    if args.model_ja_col not in df.columns:
        df[args.model_ja_col] = ""
    if "model_official_en" not in df.columns:
        df["model_official_en"] = ""
    if "source_model" not in df.columns:
        df["source_model"] = ""

    brands = df[args.brand_col].fillna("").astype(str).unique().tolist()
    # ブランド英名の解決（重複最適化）
    resolved_brand_en: Dict[str, str] = {}
    for b in tqdm(brands, desc="brand"):
        if not b:
            continue
        try:
            # まず LLM/Wiki で英名（=brand_ja）へ（既知はカタカナ化）
            be_display = resolve_brand_en(b, args.llm_model)
            resolved_brand_en[b] = be_display if be_display else b
            time.sleep(args.sleep)
        except Exception:
            resolved_brand_en[b] = b

    # 行ごとにモデル解決
    model_cache: Dict[Tuple[str, str], Tuple[str, Optional[str], str]] = {}
    rows = []
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="model"):
        brand_zh = str(row[args.brand_col]) if pd.notna(row[args.brand_col]) else ""
        model_zh = str(row[args.model_col]) if pd.notna(row[args.model_col]) else ""
        brand_disp = resolved_brand_en.get(brand_zh, brand_zh)

        key = (brand_disp, model_zh)
        if key not in model_cache:
            try:
                m_ja, m_off, src = resolve_model_en(brand_disp if brand_disp else brand_zh, model_zh, args.llm_model)
                model_cache[key] = (m_ja, m_off, src)
                time.sleep(args.sleep)
            except Exception:
                model_cache[key] = (model_zh, None, "none")
        m_ja, m_off, src = model_cache[key]

        row_out = row.copy()
        row_out[args.brand_ja_col] = brand_disp
        row_out[args.model_ja_col] = m_ja or ""
        row_out["model_official_en"] = m_off or ""
        row_out["source_model"] = src
        rows.append(row_out)

    out_df = pd.DataFrame(rows, columns=df.columns.tolist() + [args.brand_ja_col, args.model_ja_col, "model_official_en", "source_model"])
    out_df.to_csv(args.output, index=False)
    print(f"Wrote: {args.output}")

if __name__ == "__main__":
    main()
