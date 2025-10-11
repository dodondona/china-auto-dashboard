# -*- coding: utf-8 -*-
"""
Google Custom Search JSON API を使って「公式サイト」だけを検索し、
シリーズ/モデルの英語名を抽出する簡易ルックアップ。

必要な環境変数:
- GOOGLE_API_KEY
- GOOGLE_CSE_ID

ドメインは下の OFFICIAL_DOMAINS を必要に応じて編集。
"""

import os
import re
import json
import urllib.parse
from typing import List, Optional

import requests

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID")

# 公式サイトドメイン（必要に応じて追記）
OFFICIAL_DOMAINS: List[str] = [
    # BYD
    "byd.com", "bydauto.com.cn", "byd.com.cn",
    # Geely / Galaxy
    "geely.com", "geely.com/global", "geely.com.cn", "geelygalaxy.geely.com",
    # Wuling / SAIC-GM-Wuling
    "sgmw.com.cn", "wuling-global.com", "wuling.com",
    # Tesla
    "tesla.com",
    # Toyota China / Global
    "toyota.com.cn", "toyota-global.com", "toyota.com",
    # Volkswagen China / Global
    "vw.com.cn", "volkswagen.com", "volkswagen.com.cn",
    # Nissan / Honda / Audi / Mercedes / BMW / Buick
    "nissan.com.cn", "nissan-global.com",
    "honda.com.cn", "global.honda", "honda.com",
    "audi.cn", "audi.com",
    "mercedes-benz.com.cn", "mercedes-benz.com",
    "bmw.com.cn", "bmw.com",
    "buick.com.cn", "buick.com",
    # Chery / Haval / Hongqi / Changan / XPeng / Leapmotor / Xiaomi / AITO
    "chery.cn", "cheryinternational.com",
    "haval.com.cn", "haval-global.com",
    "hongqi.com", "fhmg.com.cn", "hongqi-global.com",
    "changan.com.cn", "changan.com",
    "xpeng.com",
    "leapmotor.com",
    "auto.mi.com", "xiaomi.com",
    "aito.auto", "seres.cn",
]

# NGワード（タイトル/スニペットに含まれていたら候補から除外）
BAD_TOKENS = {
    "Category", "Categories", "404", "Untitled", "PDF",
    "File", "新闻", "资讯", "新闻中心", "News", "招聘", "Join us",
}

# 品番/数字記号などを無視してモデルらしさを拾うための簡易パターン
MODEL_PAT = re.compile(r"\b([A-Za-z][A-Za-z0-9\-\s]{1,30})\b")

# BYDなど一部の中英対応のヒント
BYD_HINT = {
    "秦": "Qin",
    "海豹": "Seal",
    "海豚": "Dolphin",
    "海鸥": "Seagull",
    "海狮": "Sealion",
    "元": "Yuan",
    "宋": "Song",
    "唐": "Tang",
    "汉": "Han",
}

def _is_official(link: str) -> bool:
    try:
        host = urllib.parse.urlparse(link).netloc.lower()
    except Exception:
        return False
    return any(dom in host for dom in OFFICIAL_DOMAINS)

def _call_cse(query: str) -> List[dict]:
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        return []
    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": GOOGLE_API_KEY, "cx": GOOGLE_CSE_ID, "q": query}
    try:
        r = requests.get(url, params=params, timeout=12)
        if r.status_code != 200:
            return []
        data = r.json()
        return data.get("items", [])
    except Exception:
        return []

def _extract_model_from_text(text: str) -> Optional[str]:
    # 短い英語片を拾ってNGワード除外
    if not text:
        return None
    # 強制的に長いブランド名などを避けやすくする簡易処理
    for token in BAD_TOKENS:
        if token.lower() in text.lower():
            return None
    # ほどほどに取りやすい最初の候補を返す
    m = MODEL_PAT.findall(text)
    if not m:
        return None
    # 文字数でそれっぽいものを選ぶ
    m = [t.strip() for t in m if 2 <= len(t.strip()) <= 30]
    return m[0] if m else None

def find_official_english(brand_cn: str, model_cn: str) -> Optional[str]:
    """
    公式サイトだけを対象にして、シリーズ/モデルの英名をなるべく抽出する。
    戻り値: model_en（抽出できないとき None）
    """
    # BYD などはヒントで強化
    hint = None
    for k, v in BYD_HINT.items():
        if k in model_cn:
            hint = v
            break

    # 複数クエリで当てにいく
    queries = []
    if hint:
        queries.append(f'{brand_cn} {model_cn} {hint}')
    queries.append(f'{brand_cn} {model_cn} site:({ " OR ".join(OFFICIAL_DOMAINS) })')
    queries.append(f'{brand_cn} {model_cn} model site:({ " OR ".join(OFFICIAL_DOMAINS) })')
    queries.append(f'{brand_cn} {model_cn} official site')

    for q in queries:
        items = _call_cse(q)
        for it in items:
            link = it.get("link", "")
            title = it.get("title", "")
            snippet = it.get("snippet", "")
            if not link or not _is_official(link):
                continue

            # タイトル優先で抽出
            cand = _extract_model_from_text(title)
            if not cand:
                cand = _extract_model_from_text(snippet)

            # BYD ヒントがあれば優遇
            if hint and cand and cand.lower().startswith(hint.lower()):
                return cand

            # 雑なNG除外
            if cand and all(b not in cand for b in BAD_TOKENS):
                return cand

    return None
