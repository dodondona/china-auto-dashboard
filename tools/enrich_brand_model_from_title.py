#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
title_raw（Autohomeの車系ページ<title>）から brand / model を抽出し、
さらに「official → Wikipedia → LLM」の順でグローバル名を確定する。

出力カラム:
- brand_ja: 公式/英語名からの対訳（ブランドは既知の対訳表を最小限ローカライズ、ただし辞書固定ではない）
- model_ja: 中国語モデル名のかな訳はせず、英語モデルが出た場合はそのまま、無ければ元を返す
- model_official_en: 公式サイトで確認できたモデル英語名（無ければ空）
- source_model: "official" / "wikipedia" / "llm" / "current"

ポイント:
- キャッシュ完全無効（毎回問い合わせ）
- CSEは“公式サイトのみ”に限定。ドメインはコード内の allowlist で制御（必要に応じて編集可）
- Wikipedia: wikipediaapi + 明示UA
- LLM: OpenAI（gpt-4o-mini を既定、変えたい場合は引数）
- 「Jun 7」などのゴミを弾くフィルタを強化
"""

import argparse
import csv
import json
import os
import re
import time
from html import unescape
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ==========================
# 公式サイト CSE 検索ドメイン
# ==========================
OFFICIAL_DOMAINS = [
    # BYD
    "byd.com", "bydauto.com.cn", "byd-europe.com", "byd.co", "byd-auto.net",
    # Tesla
    "tesla.com",
    # Volkswagen
    "volkswagen.com", "vw.com", "vw.com.cn", "saic-volkswagen.com",
    # Toyota
    "toyota-global.com", "toyota.com.cn", "toyota.com",
    # Geely
    "geely.com", "geely.com.cn", "geelyauto.com", "geely.com/intl",
    # Wuling (SAIC-GM-Wuling)
    "sgmw.com.cn", "wuling.com",
    # Honda
    "global.honda", "honda.com.cn", "honda.com",
    # Changan
    "changan.com.cn", "changan.com", "qiyuan.auto", "changan-global.com",
    # XPeng
    "xiaopeng.com", "heyxpeng.com", "xpeng.com",
    # AITO / Seres
    "aitoauto.com", "seres.cn", "seres.com",
    # Haval / Great Wall
    "haval.com", "haval-global.com", "gwm-global.com", "gwm.com.cn",
    # Chery
    "cheryinternational.com", "chery.cn", "chery.com",
    # Buick
    "buick.com.cn", "buick.com",
    # Mercedes-Benz
    "mercedes-benz.com", "mercedes-benz.com.cn", "mbchina.com",
    # Audi
    "audi.com", "audi.cn",
    # Hongqi
    "hongqi.faw.cn", "hongqi-auto.com",
    # Xiaomi
    "xiaomi.com", "mi.com", "xiaomiev.com",
]

# ==========================
# OpenAI (LLM) 最後の保険
# ==========================
def llm_guess_openai(brand_cn: str, model_cn: str, title_raw: str, model_name: str) -> str | None:
    """
    公式,Wikipediaで確定しなかったときの最終手段。
    “車名としてのグローバル英語名”のみ返すよう強いプロンプト。
    """
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        return None
    import openai  # openai>=1.0 も可。古い v0 系でも api互換で使える書き方にする
    try:
        client = openai.OpenAI(api_key=api_key)
    except Exception:
        return None

    sys = (
        "You are an automotive naming expert. "
        "Return ONLY the official global English model name if it exists (e.g., 'Seal', 'Corolla Cross'). "
        "If unknown, return just the best concise English alias. No extra words, no explanation."
    )
    user = f"Chinese brand: {brand_cn}\nChinese model: {model_cn}\nPage title: {title_raw}\nTask: give the global English model name only."

    try:
        resp = client.chat.completions.create(
            model=model_name,
            messages=[{"role":"system","content":sys},{"role":"user","content":user}],
            temperature=0.1,
        )
        txt = resp.choices[0].message.content.strip()
        # ノイズ除去
        txt = re.sub(r"[\u3000\s]+", " ", txt).strip()
        txt = re.sub(r"[\"'“”‘’]", "", txt)
        if 2 <= len(txt) <= 50:
            return txt
    except Exception:
        return None
    return None

# ==========================
# Wikipedia（zh→en）最小実装
# ==========================
def wikipedia_guess(brand_cn: str, model_cn: str) -> str | None:
    try:
        import wikipediaapi
    except Exception:
        return None

    ua = "china-auto-dashboard/1.0 (https://github.com/dodondona/china-auto-dashboard)"
    wiki_zh = wikipediaapi.Wikipedia(user_agent=ua, language="zh")
    wiki_en = wikipediaapi.Wikipedia(user_agent=ua, language="en")

    # まず zh で “brand_cn model_cn” でページがあるかを試す
    query_terms = [
        f"{brand_cn} {model_cn}",
        f"{model_cn} {brand_cn}",
        f"{model_cn}",
    ]
    for q in query_terms:
        page = wiki_zh.page(q)
        if page.exists():
            # 言語リンクに英語があれば英語タイトル、無ければ zh タイトル（ローマ字無し）
            langlinks = page.langlinks
            if "en" in langlinks:
                en_title = langlinks["en"].title
                # モデル名っぽい短い語のみ採用
                if 2 <= len(en_title) <= 50 and not re.search(r"Category|File|List of", en_title, re.I):
                    return en_title
            else:
                # zhのタイトルがモデル英字を含んでる場合のみ採用
                t = page.title
                if re.search(r"[A-Za-z]{2,}", t):
                    return t
    # 何も無ければ None
    return None

# ==========================
# 公式サイト CSE
# ==========================
def cse_official_model(brand_cn: str, model_cn: str, series_url: str | None) -> str | None:
    api_key = os.getenv("GOOGLE_API_KEY", "")
    cse_id = os.getenv("GOOGLE_CSE_ID", "")
    if not api_key or not cse_id:
        return None

    # 公式ドメインに強制制限
    site_filter = " OR ".join([f"site:{d}" for d in OFFICIAL_DOMAINS])

    # クエリ候補
    q_candidates = []
    # 1) 中国語モデル優先
    if model_cn:
        q_candidates.append(f'"{model_cn}" {site_filter}')
    # 2) ブランド+モデル
    if brand_cn and model_cn:
        q_candidates.append(f'"{brand_cn}" "{model_cn}" {site_filter}')
    # 3) series_url の ID もヒントに
    if series_url:
        m = re.search(r"/(\d{3,6})/", str(series_url))
        if m:
            q_candidates.append(f'"{m.group(1)}" {site_filter}')

    s = requests.Session()
    s.headers.update({"User-Agent": "china-auto-dashboard/1.0 (+https://github.com/dodondona/china-auto-dashboard)"})

    for q in q_candidates:
        try:
            r = s.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": api_key, "cx": cse_id, "q": q, "num": 5, "hl": "en"},
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue

        items = data.get("items", []) or []
        for it in items:
            link = it.get("link", "")
            title = unescape(it.get("title", "")).strip()
            snippet = unescape(it.get("snippet", "")).strip()

            # ドメインチェック
            if not any(d in link for d in OFFICIAL_DOMAINS):
                continue

            # モデル名候補を title/snippet から抽出（ノイズ除去）
            cand = pick_model_from_text(title) or pick_model_from_text(snippet)
            if cand:
                return cand

    return None

MODEL_NOISE = re.compile(
    r"(Category|File|Untitled|Download|Spec|Brochure|Price|KV|KG|MM|ID|FAQ|News|Press|Release|Dealer|Stock|Update|"
    r"\b[A-Za-z]{2}\s\d{1,2}\b)", re.I
)

def pick_model_from_text(text: str) -> str | None:
    """
    タイトル/スニペットから“らしい”モデル英語名を拾う。
    - 単語列で “BYD Seal”, “Corolla Cross”, “Tayron”, “Magotan” 等を想定
    - ノイズ（Category, File, 日付のようなもの）は弾く
    """
    if not text:
        return None
    t = unescape(text)
    t = re.sub(r"[\u3000\s]+", " ", t).strip()

    if MODEL_NOISE.search(t):
        return None

    # 大文字単語の連結 or 先頭大文字語（簡易）
    # 例: BYD Seal, Corolla Cross, Magotan, Tayron, Binyue ...
    m = re.search(r"([A-Z][a-z0-9]+(?:\s[A-Z][a-z0-9]+)*)", t)
    if m:
        cand = m.group(1).strip()
        if 2 <= len(cand) <= 40:
            return cand
    return None

# ==========================
# title_raw → brand, model（CN) の抽出
# ==========================
TITLE_CN_PAT = re.compile(r"【(?P<model>[^】]+)】(?P<brand>[^_\s]+)_")

def extract_cn_brand_model_from_title(title_raw: str) -> tuple[str, str]:
    """
    Autohome典型:  【海豚】比亚迪_海豚报价_海豚图片_汽车之家
                  → model_cn=海豚, brand_cn=比亚迪
    """
    if not title_raw:
        return "", ""
    t = unescape(title_raw)
    m = TITLE_CN_PAT.search(t)
    if m:
        model_cn = m.group("model").strip()
        brand_cn = m.group("brand").strip()
        return brand_cn, model_cn
    # ダメなら保守的に分割
    t = re.sub(r"\s+", " ", t)
    model_cn = t[:20]
    return "", model_cn

# ==========================
# 日本語ブランド表記（最小限）
# ==========================
BRAND_JA_MAP_MIN = {
    "比亚迪": "BYD",
    "特斯拉": "テスラ",
    "大众": "フォルクスワーゲン",
    "丰田": "トヨタ",
    "吉利汽车": "Geely",
    "吉利银河": "Geely Galaxy",
    "五菱汽车": "Wuling",
    "本田": "ホンダ",
    "长安": "長安自動車",
    "长安启源": "Changan Qiyuan",
    "小鹏": "Xpeng",
    "AITO": "AITO",
    "哈弗": "ハバル",
    "奇瑞": "Chery",
    "别克": "ビュイック",
    "奔驰": "メルセデス・ベンツ",
    "奥迪": "アウディ",
    "红旗": "紅旗",
    "小米汽车": "Xiaomi Auto",
    "宝马": "BMW",
}

def brand_ja_name(brand_cn: str) -> str:
    return BRAND_JA_MAP_MIN.get(brand_cn, brand_cn)

# ==========================
# メイン
# ==========================
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--brand-col", default="brand")
    ap.add_argument("--model-col", default="model")
    ap.add_argument("--title-col", default="title_raw")
    ap.add_argument("--series-url-col", default="series_url")
    ap.add_argument("--brand-ja-col", default="brand_ja")
    ap.add_argument("--model-ja-col", default="model_ja")
    ap.add_argument("--model-official-col", default="model_official_en")
    ap.add_argument("--source-col", default="source_model")
    ap.add_argument("--openai-model", default="gpt-4o-mini")
    ap.add_argument("--sleep", type=float, default=0.6, help="API呼び出しの間隔(秒)")
    # 完全にノーキャッシュ（指定無し）
    return ap.parse_args()

def main():
    args = parse_args()
    df = pd.read_csv(args.input)

    # 出力列の初期化（存在すれば上書き）
    for col in (args.brand_ja_col, args.model_ja_col, args.model_official_col, args.source_col):
        if col not in df.columns:
            df[col] = ""

    s = requests.Session()
    s.headers.update({"User-Agent": "china-auto-dashboard/1.0 (+https://github.com/dodondona/china-auto-dashboard)"})


    for idx, row in df.iterrows():
        brand_cn = str(row.get(args.brand_col, "")).strip()
        model_cn = str(row.get(args.model_col, "")).strip()
        title_raw = str(row.get(args.title_col, "")).strip()
        series_url = str(row.get(args.series_url_col, "")).strip()

        # title から brand_cn / model_cn を補正（欠落や取り違いの保険）
        b2, m2 = extract_cn_brand_model_from_title(title_raw)
        if b2 and (not brand_cn or len(b2) > len(brand_cn)):
            brand_cn = b2
        if m2 and (not model_cn or len(m2) > len(model_cn)):
            model_cn = m2

        brand_ja = brand_ja_name(brand_cn)

        model_official = None
        source = "current"

        # 1) 公式
        try:
            model_official = cse_official_model(brand_cn, model_cn, series_url)
            if model_official:
                source = "official"
        except Exception:
            model_official = None

        # 2) Wikipedia
        if not model_official:
            try:
                wiki = wikipedia_guess(brand_cn, model_cn)
                if wiki:
                    model_official = wiki
                    source = "wikipedia"
            except Exception:
                pass

        # 3) LLM（OpenAI）
        if not model_official:
            llm = llm_guess_openai(brand_cn, model_cn, title_raw, args.openai_model)
            if llm:
                model_official = llm
                source = "llm"

        # 4) それでも空なら中国語のまま
        if not model_official:
            model_official = model_cn or ""

        # model_ja は、英語名が出たらそれをそのまま / そうでなければ中国語
        model_ja = model_official if re.search(r"[A-Za-z]", model_official) else model_cn

        df.at[idx, args.brand_ja_col] = brand_ja
        df.at[idx, args.model_ja_col] = model_ja
        df.at[idx, args.model_official_col] = model_official
        df.at[idx, args.source_col] = source

        if args.sleep > 0:
            time.sleep(args.sleep)

    df.to_csv(args.output, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Wrote: {args.output}")

if __name__ == "__main__":
    main()
