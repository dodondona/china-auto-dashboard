#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, csv, html as html_lib, json, re, sys, time
from pathlib import Path
from typing import Dict, Any, Optional
from urllib.request import Request, urlopen

# より寛容な __NEXT_DATA__ 抽出
NEXT_DATA_RE = re.compile(
    r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>\s*(\{.*?\})\s*</script>',
    re.DOTALL | re.IGNORECASE
)
# og:image
META_OG_IMAGE_RE = re.compile(
    r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
    re.I
)
# <title>…</title>
TITLE_RE = re.compile(r'<title>(.+?)</title>', re.DOTALL | re.IGNORECASE)
# <meta charset=...> または http-equiv
META_CHARSET_RE = re.compile(
    r'<meta[^>]+charset=["\']?([\w-]+)["\']?[^>]*>|<meta[^>]+http-equiv=["\']content-type["\'][^>]+content=["\'][^"\']*charset=([\w-]+)[^"\']*["\']',
    re.I
)

def http_get(url: str, timeout: int = 25) -> str:
    """文字コードをHTTPヘッダ/HTMLから推定して正しくdecode"""
    req = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    })
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        # 1) ヘッダ優先
        ct = resp.headers.get("Content-Type", "")
        m = re.search(r"charset=([\w-]+)", ct, re.I)
        enc = m.group(1) if m else None
        if not enc:
            # 2) HTML中の <meta charset=...> で判定
            head = raw[:4096].decode("utf-8", errors="ignore")
            mm = META_CHARSET_RE.search(head)
            if mm:
                enc = (mm.group(1) or mm.group(2) or "").lower()
        if not enc:
            enc = "utf-8"
        try:
            return raw.decode(enc, errors="replace")
        except LookupError:
            return raw.decode("utf-8", errors="replace")

def extract_next_data(html: str) -> Optional[Dict[str, Any]]:
    m = NEXT_DATA_RE.search(html)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None

def detect_type_hint_from_next(next_data: Dict[str, Any]) -> Optional[str]:
    found = []
    def walk(x: Any):
        if isinstance(x, dict):
            for k, v in x.items():
                lk = str(k).lower()
                if lk in ("energytype","energy","energytypename","energy_type","vehicletype","powertype","power","fueltype","fuel_type"):
                    if isinstance(v, str):
                        found.append(v)
                walk(v)
        elif isinstance(x, list):
            for it in x:
                walk(it)
    walk(next_data)
    if found:
        s = " ".join(found)
        if "纯电" in s or "純電" in s or "纯电动" in s or re.search(r"\bEV\b", s, re.I):
            return "EV"
        if "插电" in s or "插电混动" in s or "插电式混合动力" in s or re.search(r"\bPHEV\b", s, re.I):
            return "PHEV"
        if "混动" in s or re.search(r"\bHEV\b", s, re.I):
            return "HEV"
        if "燃油" in s or "汽油" in s or "柴油" in s:
            return "ICE"
    return None

def detect_type_hint_from_text(html: str) -> Optional[str]:
    s = html_lib.unescape(html)
    if re.search(r"纯电|純電|纯电动|\bEV\b", s, re.I): return "EV"
    if re.search(r"插电|PHEV|插电混动|插电式混合动力", s, re.I): return "PHEV"
    if re.search(r"混动|\bHEV\b", s, re.I): return "HEV"
    if re.search(r"燃油|汽油|柴油", s, re.I): return "ICE"
    return None

def extract_image_url(html: str) -> str:
    m = META_OG_IMAGE_RE.search(html)
    if m:
        u = html_lib.unescape(m.group(1)).strip()
        if u.startswith("//"):  # プロトコル省略を補完
            u = "https:" + u
        return u
    # フォールバック：最初の img の src|data-src
    m2 = re.search(r'<img[^>]+(?:data-src|src)=["\']([^"\']+)["\']', html, re.I)
    if m2:
        u = html_lib.unescape(m2.group(1)).strip()
        if u.startswith("//"):
            u = "https:" + u
        return u
    return ""

def extract_title_raw(html: str) -> str:
    m = TITLE_RE.search(html)
    if m:
        return html_lib.unescape(m.group(1)).strip()
    return ""

def split_brand_model_from_title(title_raw: str) -> (str, str):
    # 例：【Model 3】特斯拉_Model 3报价_…
    brand = ""
    model = ""
    if title_raw.startswith("【") and "】" in title_raw and "_" in title_raw:
        try:
            model_cn = title_raw[1:title_raw.index("】")]
            after = title_raw[title_raw.index("】")+1:]
            brand_guess = after.split("_", 1)[0]
            brand = brand_guess.strip()
            model = model_cn.strip()
        except Exception:
            pass
    return brand, model

def normalize_rank_change(val: str) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    # 既に「↑2」「↓3」「-」などならそのまま
    if re.fullmatch(r"[↑↓]\s*\d+|-", s):
        if s == "-": return "0"
        # ↑2 → +2, ↓3 → -3 に正規化
        return ("+" if "↑" in s else "-") + re.sub(r"\D", "", s)
    # 数値文字列ならそのまま（"0","2","-3" など）
    if re.fullmatch(r"[+-]?\d+", s):
        return s
    return "0"

def enrich_row(row: Dict[str, str]) -> Dict[str, str]:
    url = row.get("series_url", "")
    if not url:
        return row
    page = http_get(url)

    # title
    row["title_raw"] = extract_title_raw(page) or row.get("title_raw","")

    # type hint
    th = None
    next_data = extract_next_data(page)
    if next_data:
        th = detect_type_hint_from_next(next_data)
    if not th:
        th = detect_type_hint_from_text(page)
    row["type_hint"] = th or "Unknown"

    # image
    row["image_url"] = extract_image_url(page)

    # brand/model（titleから素直に）
    b, m = split_brand_model_from_title(row.get("title_raw",""))
    if not row.get("brand"): row["brand"] = b
    if not row.get("model"): row["model"] = m

    # rank_change をきちんと数値文字列に正規化（Stage1由来の値を正規化）
    row["rank_change"] = normalize_rank_change(row.get("rank_change",""))

    return row

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inp", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    inp = Path(args.inp)
    out = Path(args.out)
    with inp.open("r", encoding="utf-8-sig") as f:   # ← BOMありも読める
        reader = csv.DictReader(f)
        rows = list(reader)

    # 出力列（既存＋追記）
    fieldnames = reader.fieldnames or []
    for extra in ["type_hint","image_url","title_raw","brand","model","rank_change"]:
        if extra not in fieldnames:
            fieldnames.append(extra)

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8-sig") as f:  # ← Excel対策
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            try:
                row = enrich_row(row)
            except Exception:
                row.setdefault("type_hint", "Unknown")
                row.setdefault("image_url", "")
            w.writerow(row)
            time.sleep(0.25)

    print(f"[ok] enriched rows={len(rows)} -> {args.out}")

if __name__ == "__main__":
    sys.exit(main() or 0)
