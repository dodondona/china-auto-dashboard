#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Official model/series name extractor:
- Input: URL of a candidate official page (brand site, country site OK)
- Output: best-guess official English model/series name

Heuristics:
 1) Structured data (JSON-LD: Product/Vehicle -> name)
 2) <meta property="og:title"> / twitter:title
 3) <h1>, [role=heading] level 1
 4) Breadcrumb last item
 5) <title> (最後の保険)

Then:
 - Normalize: strip brand token, trim separators, collapse spaces
 - Hard filters: months, weekdays, "order/category/config/download/dealer"
                 geo/store/country names, generic words like "electric cars", "SUVs", "sedan"
 - Character/length checks: 2..40 chars, allow [A-Za-z0-9 .+_-]
 - Ranking by source priority & compactness

This avoids returning things like "Jun 7", "BYD Singapore", "Order Yuan Plus", "SUVs", "Category", "File", "mm/kv".
"""

from __future__ import annotations
import re, json, sys, time
from typing import List, Tuple, Optional, Dict
import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36"
TIMEOUT = 20

MONTHS = r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"
WEEKDAYS = r"(mon|tue|wed|thu|fri|sat|sun)"
# 公式名に混入しがちなゴミ語（小文字比較）
STOP_PHRASES = {
    "electric car", "electric cars", "ev", "phev", "dm-i", "dm-p", "dmi", "dmp",
    "category", "categories", "product category", "order", "preorder", "reserve",
    "book now", "config", "configuration", "spec", "specs", "specifications",
    "download", "brochure", "manual", "file", "news", "press", "media",
    "byd singapore", "byd ethiopia", "chery egypt", "geely global",
    "dealer", "dealers", "store", "showroom", "contact", "about",
    "suv", "suvs", "sedan", "mpv", "hatchback", "pickup",
    "kv", "mm"
}
# 国名/地域（小文字比較）
GEO_WORDS = {
    "singapore","ethiopia","egypt","saudi","uae","qatar","oman","kuwait","bahrain",
    "vietnam","indonesia","malaysia","thailand","philippines","japan","korea",
    "europe","global","international","china","usa","canada","mexico","brazil",
    "turkiye","turkey","russia","india","australia","new zealand","uk","germany",
    "france","italy","spain"
}
VALID_CHARS = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 .+\-_/]{1,39}$")
TRAILERS = re.compile(r"(?i)\b(" + "|".join([
    "official site","official website","home","global","byd","geely","wuling",
    "xpeng","xiaomi","volkswagen","toyota","nissan","honda","audi","buick",
    "mercedes-benz","hongqi","haval","changan","chery","galaxy"
]) + r")\b")

def fetch_html(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or r.encoding
    return r.text

def _clean(text: str) -> str:
    t = re.sub(r"[\r\n\t]+", " ", text or "")
    t = re.sub(r"\s+", " ", t).strip()
    # 末尾ブランドや「| Geely Global」等を落とす
    t = re.sub(r"[|•\-–—·•]\s*.*$", "", t)
    t = TRAILERS.sub("", t).strip(" -–—|·•")
    return t

def _looks_like_junk(s: str) -> bool:
    if not s: return True
    low = s.lower().strip()
    if len(low) < 2 or len(low) > 40: return True
    if not VALID_CHARS.match(s): return True
    if re.fullmatch(rf"(?:{MONTHS}|{WEEKDAYS})\.?\s*\d{{1,2}}", low): return True
    if low in GEO_WORDS: return True
    for w in STOP_PHRASES:
        if w in low:
            return True
    # 単なるブランド語だけはNG（例：BMW、BYD だけ）
    if low in {"byd","geely","wuling","xiaomi","xpeng","toyota","nissan","honda","audi","buick","mercedes-benz","hongqi","haval","changan","chery","volkswagen"}:
        return True
    return False

def _strip_brand(name: str, brand_hint: Optional[str]) -> str:
    if not brand_hint: return name
    b = re.escape(brand_hint.strip())
    return re.sub(rf"(?i)\b{b}\b", "", name).strip(" -_/")

def _jsonld_candidates(soup: BeautifulSoup) -> List[str]:
    out = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for it in items:
            typ = (it.get("@type") or "").lower()
            if isinstance(typ, list):
                typ = ",".join([str(t).lower() for t in typ])
            if any(k in typ for k in ["product","vehicle","car","automobile"]):
                name = it.get("name") or it.get("model") or ""
                if name:
                    out.append(str(name))
    return out

def extract_official_name(url: str, brand_hint: Optional[str] = None) -> Tuple[Optional[str], Dict[str, List[str]]]:
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    cands: List[Tuple[str, int, str]] = []  # (name, priority, source)

    # 1) JSON-LD
    for n in _jsonld_candidates(soup):
        cands.append((_clean(n), 1, "jsonld"))

    # 2) meta og/twitter
    for sel, src in [
        ('meta[property="og:title"]', "og:title"),
        ('meta[name="og:title"]', "og:title"),
        ('meta[name="twitter:title"]', "twitter:title"),
    ]:
        m = soup.select_one(sel)
        if m and m.get("content"):
            cands.append((_clean(m["content"]), 2, src))

    # 3) H1
    h1 = soup.find(["h1"])
    if h1 and h1.get_text(strip=True):
        cands.append((_clean(h1.get_text(" ", strip=True)), 3, "h1"))

    # 4) breadcrumb last
    bc = soup.select("nav.breadcrumb li, .breadcrumb li, [itemtype*='Breadcrumb'] a, .crumbs li, .aui-breadcrumb li")
    if bc:
        last = bc[-1].get_text(" ", strip=True)
        cands.append((_clean(last), 4, "breadcrumb"))

    # 5) <title>
    if soup.title and soup.title.get_text(strip=True):
        cands.append((_clean(soup.title.get_text(" ", strip=True)), 5, "title"))

    # normalize + filter
    uniq = []
    seen = set()
    for name, prio, src in cands:
        name = _strip_brand(name, brand_hint)
        name = re.sub(r"\s{2,}", " ", name).strip(" -_/")
        # 先頭の一般語を削る（例: "The New", "All New", "New", "Order")
        name = re.sub(r"(?i)^(the\s+)?(all\s+new|brand\s+new|new|order|category)\s+", "", name).strip()
        if not name or _looks_like_junk(name):
            continue
        key = (name.lower(), prio)
        if key not in seen:
            uniq.append((name, prio, src))
            seen.add(key)

    if not uniq:
        return (None, {"raw": [c[0] for c in cands], "kept": []})

    # スコア：低い priority（=信頼源が強い）ほど加点。短すぎ/長すぎは減点。記号の少なさを加点。
    def score(t: Tuple[str,int,str]) -> float:
        name, pr, _ = t
        s = 100 - pr*10
        if 2 <= len(name) <= 18: s += 8
        if "-" in name or " " in name: s += 0
        if any(x.isdigit() for x in name): s += 2
        # 末尾系語の除去安全度
        if re.search(r"(?i)\b(pro|plus|max|mini|mini ev|ev|dm-i|dm-p|dm|s|se|gt|gti|rs|amg|l|xl|x|ix)\b", name):
            s += 1
        return s

    uniq.sort(key=score, reverse=True)
    best = uniq[0][0]
    return (best, {"raw": [c[0] for c in cands], "kept": [f"{n} <{src} p{p}>" for n,p,src in uniq]})

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--brand-hint", default="")
    args = ap.parse_args()
    name, dbg = extract_official_name(args.url, args.brand_hint or None)
    print(name or "")
