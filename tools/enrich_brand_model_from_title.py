#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
enrich_brand_model_from_title.py

目的:
- 入力CSVの brand / model を、公式サイト/Wikipedia/LLM の優先順で英語表記に整備
- 公式: Google Custom Search JSON API (公式サイト用のCX) で検索し、モデル名を抽出
- Wikipedia: CSE (Wikipedia用のCX) または enwiki API で補完
- LLM: OPENAI_API_KEY があれば最後の砦として翻訳・整形
- キャッシュは一切使わない（都度問い合わせ）
- HTML/メタ語の誤検出 ("Form", "Cookie" 等) を強力に除外

使い方:
python tools/enrich_brand_model_from_title.py \
  --input data/autohome_raw_2025-08_with_brand.csv \
  --output data/autohome_raw_2025-08_with_brand_ja.csv \
  --brand-col brand --model-col model \
  --brand-ja-col brand_ja --model-ja-col model_ja \
  --llm-model gpt-4o-mini

環境変数:
- GOOGLE_CSE_API_KEY          : Google Custom Search API key
- GOOGLE_CSE_CX_OFFICIAL      : 公式サイト向け CSE エンジンID (複数あるならカンマ区切りでOK)
- GOOGLE_CSE_CX_WIKIPEDIA     : Wikipedia向け CSE エンジンID (任意。無ければenwiki REST使用)
- OPENAI_API_KEY              : OpenAI API key (任意; LLM使う場合)

出力列(既存保持＋追記):
- brand_ja, model_ja (入力に存在すればそのまま維持)
- model_official_en : 最終的に採用した英語モデル名
- source_model      : "official" / "wikipedia" / "llm" / "current"（何も変えなかった場合）

注意:
- ネットワーク前提 (CSE/Wiki/LLM)。キー未設定時は利用可能なソースだけで処理。
- 既存列に上書きはせず、新規列へ書き込む。
"""

import argparse
import csv
import html
import json
import os
import re
import time
from html import unescape
from typing import Optional, List, Tuple

import pandas as pd
import requests
from tqdm import tqdm

# ---------- 正規表現・ノイズ語設定 ----------

BRANDS_RE = r"\b(BYD|Toyota|Volkswagen|VW|Honda|Nissan|Tesla|Geely|Chery|Changan|XPeng|Xpeng|AITO|Haval|Buick|BMW|Mercedes|Mercedes\-Benz|Audi|Hongqi|Wuling|AITO|Seres|Aion|Leapmotor|Lynk\s*&\s*Co|Jetour|Zeekr|Lexus|Skoda|Porsche|Peugeot|Renault|Mazda|Mitsubishi|Subaru|Volvo|Cadillac|Chevrolet|Ford|Hyundai|Kia|GAC|FAW|Dongfeng)\b"

# HTML/ナビ・一般語・月名・略号などをノイズとして弾く
MODEL_NOISE = re.compile(
    r"(Category|File|Untitled|Download|Spec|Brochure|Price|FAQ|News|Press|Dealer|Stock|Update|"
    r"Form|Forms|Years|Cookie|Privacy|Policy|Login|Register|"
    r"Annual|Report|Image|Alt|Other|Overview|Home|Official|Electric|Cars?|Vehicle|SUVs?|Sedan|Hatchback|Wagon|MPV|"
    r"Page|Index|All|Global|China|CN|EN|JP|KR|DE|FR|IT|ES|"
    r"\b[A-Za-z]{1,2}\d{1,2}\b|"
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b|"
    r"\b(Ⅰ|Ⅱ|III|IV|V|VI|VII|VIII|IX|X)\b)"
    , re.I
)

# ブランド固有の既知モデル（最低限の白リスト; あればマッチ優先）
KNOWN_MODELS = {
    "BYD": ["Qin", "Qin PLUS", "Qin L", "Seal", "Seal 06", "Seal 05 DM-i", "Dolphin", "Yuan", "Yuan PLUS", "Song", "Song PLUS", "Song Pro", "Tang", "Han", "Frigate 07", "Seagull", "Sea Lion 06"],
    "Toyota": ["Camry", "Corolla Cross", "Corolla", "RAV4"],
    "Volkswagen": ["Sagitar", "Lavida", "Magotan", "Passat", "Tiguan L", "Tayron", "Tharu"],
    "Tesla": ["Model 3", "Model Y", "Model S", "Model X"],
    "Geely": ["Boyue L", "Xingyue L", "Binyue", "Galaxy A7"],
    "Geely Galaxy": ["E8", "L6", "L7", "A7", "E5"],
    "Honda": ["CR-V", "Accord", "Civic"],
    "Chery": ["Tiggo 8", "Arrizo 8"],
    "XPeng": ["MONA M03", "G6", "P7", "G9"],
    "AITO": ["M5", "M7", "M8"],
    "BMW": ["3 Series", "5 Series"],
    "Buick": ["Envision Plus"],
    "Mercedes-Benz": ["C-Class", "E-Class"],
    "Wuling": ["Hongguang MINIEV", "Binguo", "Bingo"],
    "Changan": ["Eado", "CS75 PLUS", "Lumin"],
    "Haval": ["Big Dog"],
}

# ---------- 汎用ユーティリティ ----------

def http_get_json(url: str, params: dict = None, headers: dict = None, timeout: int = 20) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None

def safe_text(x: Optional[str]) -> str:
    if x is None:
        return ""
    return unescape(str(x)).strip()

def split_by_delimiters(text: str) -> List[str]:
    # title の左右に区切りがあることが多い
    parts = re.split(r"\||–|—|:|·|-", text)
    return [p.strip() for p in parts if p and p.strip()]

def looks_like_noise(word: str) -> bool:
    if not word:
        return True
    if MODEL_NOISE.search(word):
        return True
    # 無意味な一語や数字のみ
    if len(word) < 2:
        return True
    if re.fullmatch(r"\d+(\.\d+)?", word):
        return True
    return False

def brand_key_for_known(brand: str) -> str:
    b = brand or ""
    b = b.strip()
    if b in KNOWN_MODELS:
        return b
    # 正規化
    if b.lower() in ("vw", "volkswagen"):
        return "Volkswagen"
    if b.lower() in ("byd",):
        return "BYD"
    if b.lower() in ("geely galaxy", "galaxy", "吉利银河"):
        return "Geely Galaxy"
    if b.lower() in ("mercedes-benz", "mercedes", "benz"):
        return "Mercedes-Benz"
    if b.lower() in ("xpeng", "小鹏", "x peng"):
        return "XPeng"
    if b.lower() in ("wuling", "五菱"):
        return "Wuling"
    if b.lower() in ("changan", "長安", "长安"):
        return "Changan"
    return b

def prefer_known_model(brand: str, candidates: List[str]) -> Optional[str]:
    key = brand_key_for_known(brand)
    known = KNOWN_MODELS.get(key, [])
    # 完全一致優先
    for c in candidates:
        for k in known:
            if c.lower() == k.lower():
                return k
    # 前方一致など緩め
    for c in candidates:
        for k in known:
            if c.lower() in k.lower() or k.lower() in c.lower():
                return k
    return None

# ---------- 抽出ロジック ----------

def extract_model_from_title_snippet(raw: str, brand: str) -> Optional[str]:
    """
    公式CSE/Wikiの title/snippet からモデルらしい単語を抽出。
    - ブランドと同列のパイプ・コロン区切りの左側語を優先
    - ノイズ語は除外
    """
    t = safe_text(raw)
    if not t:
        return None
    # 区切りで左優先
    parts = split_by_delimiters(t)
    # ブランド名を落として候補化
    brand_pat = re.compile(BRANDS_RE, re.I)
    cands = []
    for p in parts[:2]:  # 左側2ブロックを主対象
        p2 = brand_pat.sub("", p).strip()
        # 大文字始まり語/数字・記号を含むモデルパターン
        # 例: "Model 3", "Qin PLUS", "Tiggo 8", "C-Class", "Seal 05 DM-i"
        m = re.findall(r"[A-Z][A-Za-z0-9\-]*(?:\s(?:[A-Z0-9][A-Za-z0-9\-]*))*", p2)
        for w in m:
            w = w.strip()
            if w and not looks_like_noise(w):
                cands.append(w)

    # 知見モデルを優先
    if cands:
        known = prefer_known_model(brand, cands)
        if known:
            return known
        return cands[0]

    # だめなら全文から拾う（最後の手段）
    t2 = brand_pat.sub("", t)
    m2 = re.findall(r"[A-Z][A-Za-z0-9\-]*(?:\s(?:[A-Z0-9][A-Za-z0-9\-]*))*", t2)
    for w in m2:
        w = w.strip()
        if w and not looks_like_noise(w):
            return w
    return None

# ---------- データ取得: CSE / Wikipedia / LLM ----------

def cse_query(api_key: str, cx: str, q: str, num: int = 5) -> List[dict]:
    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": api_key, "cx": cx, "q": q, "num": num}
    js = http_get_json(url, params=params)
    if not js:
        return []
    return js.get("items", [])

def try_official(brand: str, model_cn: str) -> Optional[str]:
    api_key = os.getenv("GOOGLE_CSE_API_KEY", "").strip()
    cx_list = [s.strip() for s in os.getenv("GOOGLE_CSE_CX_OFFICIAL", "").split(",") if s.strip()]
    if not api_key or not cx_list:
        return None
    query = f"{brand} {model_cn}"
    for cx in cx_list:
        items = cse_query(api_key, cx, query, num=5)
        for it in items:
            title = safe_text(it.get("title"))
            snippet = safe_text(it.get("snippet"))
            # タイトル優先 → だめならスニペット
            for text in (title, snippet):
                m = extract_model_from_title_snippet(text, brand)
                if m:
                    return m
        time.sleep(0.25)  # 軽いレート制御
    return None

def try_wikipedia(brand: str, model_cn: str) -> Optional[str]:
    # 1) CSE (Wikipedia CX) があれば利用
    api_key = os.getenv("GOOGLE_CSE_API_KEY", "").strip()
    cx_wiki = os.getenv("GOOGLE_CSE_CX_WIKIPEDIA", "").strip()
    query = f"{brand} {model_cn}"
    if api_key and cx_wiki:
        items = cse_query(api_key, cx_wiki, query, num=5)
        for it in items:
            title = safe_text(it.get("title"))
            snippet = safe_text(it.get("snippet"))
            for text in (title, snippet):
                m = extract_model_from_title_snippet(text, brand)
                if m:
                    return m

    # 2) enwiki API (簡易)
    # https://en.wikipedia.org/w/api.php?action=opensearch&search=...
    try:
        url = "https://en.wikipedia.org/w/api.php"
        params = {
            "action": "opensearch",
            "search": f"{brand} {model_cn}",
            "limit": 5,
            "namespace": 0,
            "format": "json",
        }
        js = http_get_json(url, params=params, timeout=15)
        if js and len(js) >= 2 and isinstance(js[1], list):
            for title in js[1]:
                m = extract_model_from_title_snippet(title, brand)
                if m:
                    return m
    except Exception:
        pass
    return None

def try_llm(brand: str, model_cn: str, llm_model: Optional[str]) -> Optional[str]:
    """
    LLMに「中国語の車名を英語のグローバル表記へ、正式モデル名のみ返す」と指示。
    """
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key or not llm_model:
        return None
    import requests as r

    system = "You are a precise automotive data normalizer. Return ONLY the official global English model name (no brand, no extra words). If unknown, return just the best transliteration (e.g., 'Xingyue L')."
    user = f"Brand: {brand}\nChinese Model: {model_cn}\nOutput: English official model name only."

    try:
        resp = r.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            data=json.dumps({
                "model": llm_model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": 0.1,
            }),
            timeout=40,
        )
        if resp.status_code == 200:
            js = resp.json()
            text = js["choices"][0]["message"]["content"].strip()
            # 余計な語が混ざっていれば削る
            text = re.sub(r"[\n\r]+", " ", text)
            text = re.sub(r"^['\"“”‘’\[\(]+|['\"“”‘’\]\)]+$", "", text).strip()
            if text and not looks_like_noise(text):
                return text
    except Exception:
        return None
    return None

# ---------- メイン処理 ----------

def process_row(brand: str, model_cn: str, title_raw: str, llm_model: Optional[str]) -> Tuple[str, str]:
    """
    1) official → 2) wikipedia → 3) LLM → 4) 現状維持 の順で model_official_en を決定
    source_model には採用ソースを格納
    """
    brand = safe_text(brand)
    model_cn = safe_text(model_cn)
    title_raw = safe_text(title_raw)

    # まずタイトルからヒント（あれば）
    hint = extract_model_from_title_snippet(title_raw, brand) if title_raw else None

    # 1) official (CSE)
    m = try_official(brand, model_cn if model_cn else hint or "")
    if m:
        return m, "official"

    # 2) wikipedia
    m = try_wikipedia(brand, model_cn if model_cn else hint or "")
    if m:
        return m, "wikipedia"

    # 3) LLM
    m = try_llm(brand, model_cn if model_cn else hint or "", llm_model)
    if m:
        return m, "llm"

    # 4) 現状維持（中国語モデルをそのままローマ字/英数化はしない。入力値をそのまま返す）
    return model_cn, "current"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="入力CSVパス")
    ap.add_argument("--output", required=True, help="出力CSVパス")
    ap.add_argument("--brand-col", default="brand", help="ブランド列名")
    ap.add_argument("--model-col", default="model", help="モデル(中国語)列名")
    ap.add_argument("--brand-ja-col", default="brand_ja", help="ブランド日本語列 (存在すれば維持)")
    ap.add_argument("--model-ja-col", default="model_ja", help="モデル日本語列 (存在すれば維持)")
    ap.add_argument("--title-col", default="title_raw", help="タイトル列 (ヒントとして使用; 任意)")
    ap.add_argument("--llm-model", default=None, help="LLMモデル (例: gpt-4o-mini / gpt-4o)")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    # 必須列チェック
    for col in [args.brand-col if False else args.brand_col, args.model_col]:
        if col not in df.columns:
            raise SystemExit(f"必要列がありません: {col}")

    title_col = args.title_col if args.title_col in df.columns else None

    # 出力列を準備（なければ作成）
    if "model_official_en" not in df.columns:
        df["model_official_en"] = ""
    if "source_model" not in df.columns:
        df["source_model"] = ""

    # 進捗表示
    it = tqdm(df.itertuples(index=False), total=len(df), desc="model")

    # 行処理
    out_models = []
    out_sources = []
    for row in it:
        brand = getattr(row, args.brand_col)
        model_cn = getattr(row, args.model_col)
        title_val = getattr(row, title_col) if title_col else ""
        m, src = process_row(brand, model_cn, title_val, args.llm_model)
        out_models.append(m)
        out_sources.append(src)
        # 軽いレート制御（CSEクォータ対策）
        time.sleep(0.05)

    df["model_official_en"] = out_models
    df["source_model"] = out_sources

    # brand_ja/model_jaは**上書きしない**（入力のまま維持）
    # CSV書き出し
    df.to_csv(args.output, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Wrote: {args.output}")

if __name__ == "__main__":
    main()
