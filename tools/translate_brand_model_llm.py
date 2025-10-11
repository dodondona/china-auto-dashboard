#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import argparse
from typing import Dict, List, Optional, Tuple
import requests
import pandas as pd
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# -------------------------
# CLI
# -------------------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="input CSV")
    p.add_argument("--output", required=True, help="output CSV")
    p.add_argument("--brand-col", default="brand")
    p.add_argument("--model-col", default="model")
    p.add_argument("--brand-ja-col", default="brand_ja")
    p.add_argument("--model-ja-col", default="model_ja")
    p.add_argument("--cache", default=".cache/global_map.json")
    p.add_argument("--sleep", type=float, default=0.8)
    p.add_argument("--model", dest="llm_model", default="gpt-4o")
    return p.parse_args()

# -------------------------
# Env & constants
# -------------------------
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CSE_ID  = os.getenv("GOOGLE_CSE_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # 任意（ja翻訳用）
UA = "Mozilla/5.0 (compatible; ModelNameResolver/1.0; +https://example.org/bot) "
TIMEOUT = 15

STOPWORDS = {
    "category","file","news","blogs","suvs","electric cars","image bank","models",
    "purchase","configuration","dealer","thank you","about","technology","byd","toyota",
    "volkswagen","audi","honda","bmw","mercedes-benz","nissan","geely","wuling","xpeng",
    "leapmotor","haval","buick","hongqi","chery"
}

# ブランド -> 公式ドメイン（辞書は"モデル名"ではなく"公式サイトの入口"だけ）
OFFICIAL_SITES: Dict[str, List[str]] = {
    "比亚迪": ["byd.com","byd.com.cn"],
    "比亞迪": ["byd.com","byd.com.cn"],
    "BYD": ["byd.com","byd.com.cn"],
    "丰田": ["toyota.com.cn","gac-toyota.com.cn","ftms.com.cn"],
    "トヨタ": ["toyota.com.cn","gac-toyota.com.cn","ftms.com.cn"],
    "大众": ["vw.com.cn","faw-vw.com","svw-volkswagen.com"],
    "フォルクスワーゲン": ["vw.com.cn","faw-vw.com","svw-volkswagen.com"],
    "日产": ["nissan.com.cn","dongfeng-nissan.com.cn","dfmc.com.cn"],
    "日産": ["nissan.com.cn","dongfeng-nissan.com.cn","dfmc.com.cn"],
    "特斯拉": ["tesla.com","tesla.cn"],
    "吉利汽车": ["geely.com","geely.com.cn","xy.geely.com","galaxy.geely.com","global.geely.com"],
    "吉利银河": ["xy.geely.com","galaxy.geely.com","geely.com"],
    "吉利": ["geely.com","geely.com.cn","global.geely.com"],
    "奇瑞": ["chery.cn","chery.com","cheryinternational.com"],
    "哈弗": ["haval.com.cn","haval-global.com"],
    "红旗": ["hongqi-auto.com"],
    "红旗汽车": ["hongqi-auto.com"],
    "长安": ["changan.com.cn","changan.com"],
    "长安启源": ["changan.com.cn","changan.com"],
    "小鹏": ["xpeng.com"],
    "小米汽车": ["auto.mi.com","xiaomi.com","mi.com"],
    "AITO": ["aitoauto.com","huawei.com"],  # 実態上 AITO/Wenjie はここに出る
    "奥迪": ["audi.cn","faweaudi.com","saic-audi.cn"],
    "本田": ["honda.com.cn","gac-honda.com.cn","dongfeng-honda.com"],
    "宝马": ["bmw.com.cn","bmw.com"],
    "别克": ["buick.com.cn","saic-gm.com"],
    "五菱汽车": ["sgmw.com.cn","wuling-global.com"],
    "零跑汽车": ["leapmotor.com"],
    "奔驰": ["mercedes-benz.com.cn","mb.zungfu.com.cn","mercedes-benz.com"],
    # ほか足りない場合はCSE側のドメインホワイトリストでカバー
}

# -------------------------
# Utilities
# -------------------------
def http_get(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
        if r.ok and "text/html" in r.headers.get("content-type",""):
            r.encoding = r.apparent_encoding or r.encoding
            return r.text
    except Exception:
        return None
    return None

def is_model_like_url(url: str, model_cn: str) -> bool:
    u = url.lower()
    score = 0
    # URL ルール
    for key in ("/model", "/models", "/product", "/products", "/vehicle", "车型", "車型"):
        if key in u:
            score += 1
    # モデル名（漢字）を含むか
    if model_cn and model_cn in url:
        score += 1
    return score >= 1

def extract_text_candidates(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    texts = []
    # JSON-LD
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.get_text(strip=True))
            if isinstance(data, dict):
                for k in ("name","alternateName"):
                    v = data.get(k)
                    if isinstance(v, str):
                        texts.append(v)
            elif isinstance(data, list):
                for obj in data:
                    if isinstance(obj, dict):
                        for k in ("name","alternateName"):
                            v = obj.get(k)
                            if isinstance(v, str):
                                texts.append(v)
        except Exception:
            pass
    # og:title / title / h1
    m = soup.find("meta", attrs={"property":"og:title"})
    if m and m.get("content"):
        texts.append(m["content"])
    if soup.title and soup.title.string:
        texts.append(soup.title.string)
    for h in soup.find_all(["h1","h2"]):
        t = h.get_text(" ", strip=True)
        if t:
            texts.append(t)
    # 周辺に英名を置きがちな UI 要素
    for sel in ["[class*='model']","[class*='title']","[class*='name']","[class*='hero']"]:
        for el in soup.select(sel):
            t = el.get_text(" ", strip=True)
            if t:
                texts.append(t)
    # 余計な空白を整理
    texts = [re.sub(r"\s+", " ", t).strip() for t in texts if t.strip()]
    return list(dict.fromkeys(texts))  # uniq

def is_bad_en_token(tok: str) -> bool:
    t = tok.lower().strip(" -")
    if not t:
        return True
    if t in STOPWORDS:
        return True
    # 単なるブランド名だけ
    if t in {"byd","toyota","volkswagen","audi","honda","bmw","mercedes-benz","nissan","geely","wuling","xpeng","leapmotor","haval","buick","hongqi","chery","geely galaxy"}:
        return True
    # 汎用語の連結形
    if len(t) <= 2:
        return True
    return False

EN_TOKEN_RE = re.compile(r"\b([A-Za-z][A-Za-z0-9\- ]+[A-Za-z0-9])\b")

def pick_english_model(texts: List[str], model_cn: str) -> Optional[str]:
    """
    texts から「英語として妥当な車名」を抽出。
    - ラテン文字列を拾い、Stopwords を除外
    - 2〜3語までを優先（例：COROLLA CROSS, TIGGO 8, A6L, CS75 PLUS, YUAN PLUS）
    - モデル漢字の近傍に括弧で英名がある形も拾う
    """
    # 括弧併記: 〇〇（COROLLA CROSS）
    for t in texts:
        if model_cn and (model_cn in t):
            m = re.search(r"[（(]([A-Za-z0-9][A-Za-z0-9 \-]+)[)）]", t)
            if m:
                cand = m.group(1).strip()
                if not is_bad_en_token(cand):
                    return cand

    # ラテン候補を収集
    cands: List[str] = []
    for t in texts:
        for m in EN_TOKEN_RE.finditer(t):
            tok = m.group(1).strip()
            if is_bad_en_token(tok):
                continue
            # 語数を抑制（例: 長い説明文を避ける）
            if len(tok.split()) <= 4:
                cands.append(tok)
    # 頻度・短さ優先 + 車名らしさ
    freq: Dict[str,int] = {}
    for c in cands:
        freq[c] = freq.get(c, 0) + 1
    ranked = sorted(freq.items(), key=lambda kv: (-kv[1], len(kv[0])))
    return ranked[0][0] if ranked else None

def domain_in_whitelist(url: str, brand: str) -> bool:
    if not brand:
        return True
    host = urlparse(url).netloc.lower()
    allow = OFFICIAL_SITES.get(brand, [])
    # ブランド名のバリエーションも見る
    if not allow:
        # 大まかなホワイトリスト（CSE 側に登録済みであれば自然とここに落ちない）
        allow = []
    return any(h in host for h in allow)

def cse_search(q: str, site_domains: List[str], num=5) -> List[Dict]:
    if not (GOOGLE_API_KEY and GOOGLE_CSE_ID):
        return []
    base = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": GOOGLE_API_KEY,
        "cx": GOOGLE_CSE_ID,
        "q": q,
        "num": num,
        "lr": "lang_zh-CN"
    }
    r = requests.get(base, params=params, headers={"User-Agent": UA}, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return data.get("items", [])

def choose_best_cse_item(items: List[Dict], model_cn: str, brand: str) -> Optional[str]:
    best = None
    best_score = -1
    for it in items:
        url = it.get("link","")
        title = it.get("title","")
        snippet = it.get("snippet","")
        if not url:
            continue
        if not domain_in_whitelist(url, brand):
            continue
        score = 0
        U = url.lower()
        T = (title or "").lower()
        S = (snippet or "").lower()
        # モデル名一致
        if model_cn and model_cn in title:
            score += 4
        if model_cn and model_cn in snippet:
            score += 2
        if is_model_like_url(U, model_cn):
            score += 2
        # 英字車名の典型パターンっぽい語
        if re.search(r"\b(corolla cross|frontlander|lavida|sagitar|magotan|tayron|tharu|camry|rav4|arrizo|tiggo|binyue|boyue|xingyue|yuan|qin|song|seal|sealion|seagull|dolphin|accor|a6l|cs75|sylphy|monjaro)\b", T+S):
            score += 1
        if score > best_score:
            best, best_score = url, score
    return best

def resolve_official_en(brand: str, model_cn: str) -> Tuple[Optional[str], Optional[str], float]:
    """
    公式サイトから英語モデル名を推定。
    戻り: (model_official_en, source_url, confidence)
    """
    brand_key = brand.strip() if brand else ""
    site_domains = OFFICIAL_SITES.get(brand_key, [])
    q = f"{brand} {model_cn}"
    items = cse_search(q, site_domains, num=6)
    # 1st try: そのままのクエリ
    url = choose_best_cse_item(items, model_cn, brand_key)
    # 2nd: “site:” を含めた明示（CSE は内部でホワイトリストを持つが、保険）
    if not url and site_domains:
        q2 = f"{brand} {model_cn} site:{site_domains[0]}"
        items2 = cse_search(q2, site_domains, num=6)
        url = choose_best_cse_item(items2, model_cn, brand_key)

    if not url:
        return None, None, 0.0

    html = http_get(url)
    if not html:
        return None, url, 0.3

    texts = extract_text_candidates(html)
    en = pick_english_model(texts, model_cn)

    # 言語切替リンクを自動探索（/en or ?lang=en）
    if not en:
        alt_links = re.findall(r'href=["\']([^"\']+(?:/en|\?lang=en)[^"\']*)["\']', html, flags=re.I)
        alt_links = [l for l in alt_links if urlparse(l).netloc or l.startswith("/")]
        if alt_links:
            alt = alt_links[0]
            if not urlparse(alt).netloc:
                # 相対 → 絶対
                base = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
                alt = base + alt
            html2 = http_get(alt)
            if html2:
                texts2 = extract_text_candidates(html2)
                en = pick_english_model(texts2, model_cn)

    # フォールバック: JSON-LD alternateName のみでもOK
    if not en:
        # 最弱のフォールバック：ピンイン化は避けたいが、全く無いよりはまし
        # ここでは“L/PLUS/PRO/EV/DM-i/数字”はそのまま残す簡易ローマナイズ
        # （辞書ではなく、記号維持＋漢字部分は無視 or 既出のラテンを採用）
        if re.search(r"[A-Za-z0-9]", model_cn):
            en = model_cn  # 既にラテン含み（例：A6L, DM-i など）
        else:
            en = None  # 「辞書化を避けたい」方針に合わせ、最終手段も抑制

    conf = 0.9 if en else 0.3
    return en, url, conf

# -------------------------
# LLM（ja翻訳）：最小限
# -------------------------
def translate_to_ja(text: str) -> str:
    # OPENAI_API_KEY が無ければ原文返し（ワークフローは通る）
    if not OPENAI_API_KEY or not text:
        return text
    try:
        import openai  # 公式SDK v1系でも互換的に使える簡易呼び出し
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        prompt = f"自動車ブランド/車名の自然な日本語訳だけを返してください。余計な説明は不要。: {text}"
        rsp = client.chat.completions.create(
            model=os.getenv("LLM_MODEL", "gpt-4o"),
            messages=[{"role":"user","content":prompt}],
            temperature=0.2,
        )
        out = rsp.choices[0].message.content.strip()
        return out or text
    except Exception:
        return text

# -------------------------
# Cache
# -------------------------
def load_cache(path: str) -> Dict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(path: str, data: Dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# -------------------------
# Main
# -------------------------
def main():
    args = parse_args()
    df = pd.read_csv(args.input)
    cache = load_cache(args.cache)

    # 出力列を準備（既に存在していれば上書きしない）
    for col in (args.brand_ja_col, args.model_ja_col, "model_official_en", "model_official_en_source", "model_official_en_conf"):
        if col not in df.columns:
            df[col] = ""

    # ユニーク化して無駄なCSE呼び出しを減らす
    pairs = df[[args.brand_col, args.model_col]].drop_duplicates().values.tolist()

    # 公式英名の解決
    for brand, model in pairs:
        key = f"{brand}|||{model}"
        if key in cache and all(k in cache[key] for k in ("en","src","conf")):
            en, src, conf = cache[key]["en"], cache[key]["src"], cache[key]["conf"]
        else:
            en, src, conf = resolve_official_en(brand, model)
            cache[key] = {"en": en, "src": src, "conf": conf}
            time.sleep(args.sleep)

        mask = (df[args.brand_col] == brand) & (df[args.model_col] == model)
        if en:
            df.loc[mask, "model_official_en"] = en
        if src:
            df.loc[mask, "model_official_en_source"] = src
        if conf:
            df.loc[mask, "model_official_en_conf"] = conf

    # ja翻訳（従来互換）
    if args.brand_ja_col in df.columns:
        df[args.brand_ja_col] = df[args.brand_ja_col].where(df[args.brand_ja_col].astype(str).str.len()>0,
                                                            df[args.brand_col].astype(str).map(translate_to_ja))
    if args.model_ja_col in df.columns:
        df[args.model_ja_col] = df[args.model_ja_col].where(df[args.model_ja_col].astype(str).str.len()>0,
                                                            df[args.model_col].astype(str).map(translate_to_ja))

    df.to_csv(args.output, index=False, encoding="utf-8")
    save_cache(args.cache, cache)
    print(f"Wrote: {args.output}")

if __name__ == "__main__":
    main()
