#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import html
import re
import sys
from pathlib import Path
from typing import Dict, Any, Optional
import json
import time

import urllib.request

NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(?P<json>{.+?})</script>',
    re.DOTALL
)

META_OG_IMAGE_RE = re.compile(
    r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\'](?P<u>[^"\']+)["\']', re.I
)

TITLE_RE = re.compile(r'<title>(?P<t>.+?)</title>', re.DOTALL | re.IGNORECASE)

def http_get(url: str, timeout: int = 20) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        }
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
        return data.decode("utf-8", errors="replace")

def extract_next_data(html_str: str) -> Optional[Dict[str, Any]]:
    m = NEXT_DATA_RE.search(html_str)
    if not m:
        return None
    try:
        return json.loads(m.group("json"))
    except json.JSONDecodeError:
        return None

def detect_type_hint_from_next(next_data: Dict[str, Any]) -> Optional[str]:
    """
    車種ページの NEXT_DATA からエネルギー種別を推定。
    例: '纯电', '插电混动', '燃油' 等
    """
    found = []

    def walk(x: Any, path: str = ""):
        if isinstance(x, dict):
            # シリーズ基本情報やスペックに "energy" 的なキーが出ることが多い
            for k, v in x.items():
                lk = k.lower()
                if lk in ("energytype", "energy", "energytypename", "energy_type", "vehicletype", "powertype"):
                    if isinstance(v, str):
                        found.append(v)
                walk(v, path + "/" + k)
        elif isinstance(x, list):
            for i, it in enumerate(x):
                walk(it, path + f"/[{i}]")

    walk(next_data)
    if found:
        s = " ".join(found)
        # 代表値を正規化
        if "纯电" in s or "純電" in s or "纯电动" in s:
            return "EV"
        if "插电" in s or "PHEV" in s or "插电混动" in s:
            return "PHEV"
        if "混动" in s or "HEV" in s:
            return "HEV"
        if "燃油" in s:
            return "ICE"
    return None

def detect_type_hint_from_text(html_str: str) -> Optional[str]:
    # タイトル付近や見出しに「纯电」「插电混动」などのバッジがあるケース
    s = html.unescape(html_str)
    if re.search(r"纯电|純電|纯电动", s):
        return "EV"
    if re.search(r"插电|PHEV|插电混动", s, re.IGNORECASE):
        return "PHEV"
    if re.search(r"混动|HEV", s, re.IGNORECASE):
        return "HEV"
    if re.search(r"燃油", s):
        return "ICE"
    return None

def extract_image_url(html_str: str) -> str:
    m = META_OG_IMAGE_RE.search(html_str)
    if m:
        return html.unescape(m.group("u"))
    return ""

def extract_title_raw(html_str: str) -> str:
    m = TITLE_RE.search(html_str)
    if m:
        return html.unescape(m.group("t")).strip()
    return ""

def enrich_row(row: Dict[str, str]) -> Dict[str, str]:
    url = row.get("series_url", "")
    if not url:
        return row

    try:
        page = http_get(url)
        # title
        row["title_raw"] = extract_title_raw(page) or row.get("title_raw","")

        # type hint
        next_data = extract_next_data(page)
        th = None
        if next_data:
            th = detect_type_hint_from_next(next_data)
        if not th:
            th = detect_type_hint_from_text(page)
        row["type_hint"] = th or "Unknown"

        # image
        row["image_url"] = extract_image_url(page)

        # brand / model を title から素直に分解（フォーマット固定：「【シリーズ】ブランド_シリーズ报价…」）
        # 例：【Model 3】特斯拉_Model 3报价_…
        t = row.get("title_raw","")
        brand = row.get("brand","")
        model = row.get("model","")
        if not (brand and model) and t.startswith("【") and "】" in t:
            # 【シリーズ】ブランド_シリーズ报价…
            series_cn = t[1:t.index("】")]
            # 「】」のあとは「ブランド_シリーズ报价…」の想定
            after = t[t.index("】")+1:]
            # 先頭のブランド候補は '_' まで
            m_brand = after.split("_", 1)[0]
            # モデルはシリーズ名（上記で抜いた series_cn）
            if not brand:
                brand = m_brand.strip()
            if not model:
                model = series_cn.strip()
        row["brand"] = brand
        row["model"] = model

    except Exception:
        # 失敗しても Unknown/空で返す（落とさない）
        row.setdefault("type_hint", "Unknown")
        row.setdefault("image_url", "")
        row.setdefault("title_raw", row.get("title_raw",""))

    return row

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inp", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    inp = Path(args.inp)
    out = Path(args.out)
    rows = []

    # 入力CSVを読み込み
    with inp.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = list(r)

    # 出力カラム（従来の列に追記するだけ。ワークフローは変更不要）
    fieldnames = r.fieldnames + [c for c in ["type_hint","image_url"] if c not in r.fieldnames]

    # 逐次 enrich
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            row = enrich_row(row)
            w.writerow(row)
            # 軽いレート制御（相手サイト負荷配慮）
            time.sleep(0.3)

    print(f"[ok] enriched rows={len(rows)} -> {args.out}")

if __name__ == "__main__":
    sys.exit(main() or 0)
