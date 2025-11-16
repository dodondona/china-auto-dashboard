# -*- coding: utf-8 -*-
# tools/autohome_company_from_html.py
#
# Autohome のランキングページを毎月自動で取得し、
# output/company 以下に CSV + 画像 を保存する。

import os
import re
import csv
import base64
import requests
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


# =============================
# ① 対象月を自動生成 （今日の1ヶ月前/次月など調整可能）
# =============================
# 今回は GitHub Actions 実行日を基準に “先月”
today = datetime.utcnow()
target_month = today.replace(day=1) - timedelta(days=1)     # 1ヶ月前
year = target_month.year
month = target_month.month

# URL 形式に変換
target_str = f"{year}-{month:02d}"
print("▶ Target:", target_str)

# =============================
# ② Autohome のランキング URL
# =============================
BASE_URL = f"https://www.autohome.com.cn/rank/1-3-1072-x/{target_str}.html"


# =============================
# ③ 保存先
# =============================
BASE_DIR = Path("output/company") / target_str
IMG_DIR = BASE_DIR / "images"
CSV_PATH = BASE_DIR / f"autohome_company_ranking_{target_str}.csv"

BASE_DIR.mkdir(parents=True, exist_ok=True)
IMG_DIR.mkdir(parents=True, exist_ok=True)


def sanitize_filename(s: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", (s or "company").strip())
    return s[:80].strip("_") or "company"


def save_base64_image(data_url: str, rank: int, manufacturer: str):
    """data:image/base64 を画像として保存"""
    if not data_url.startswith("data:image"):
        return ""

    try:
        header, b64 = data_url.split(",", 1)
        img_bytes = base64.b64decode(b64)
        fname = f"{rank:03d}_{sanitize_filename(manufacturer)}.png"
        outpath = IMG_DIR / fname
        with open(outpath, "wb") as f:
            f.write(img_bytes)
        return str(outpath)
    except Exception:
        return ""


def parse_delta(card):
    """SVG 色 ＋ 数字から +2 / -1 / → / NEW を判定"""
    svg = card.find("svg")
    if not svg:
        return "NEW"

    svg_html = str(svg)
    fills = {c.lower() for c in re.findall(r'fill="(#?[0-9a-fA-F]{3,6})"', svg_html)}

    text = svg.get_text(strip=True)
    m = re.search(r"\d+", text)
    num = m.group(0) if m else None

    if not num:
        return "→"

    # 上昇：オレンジ
    if any(x in fills for x in {"#f60", "#ff6600"}):
        return f"+{num}"

    # 下降：青緑
    if any(x in fills for x in {"#1ccd99"}):
        return f"-{num}"

    return num


def parse_units(card):
    """カード内のテキストから台数(大きな数字)を抽出"""
    text = card.get_text(" ", strip=True)
    candidates = re.findall(r"\d{4,7}", text)
    if not candidates:
        return None
    return int(candidates[-1])


def extract_one_card(card):
    rank = int(card.get("data-rank-num"))

    # メーカー名
    name_el = card.select_one(".tw-text-lg.tw-font-medium")
    manufacturer = name_el.get_text(strip=True) if name_el else ""

    units = parse_units(card)
    delta = parse_delta(card)

    img_tag = card.find("img")
    img_src = img_tag["src"] if img_tag else ""
    img_path = ""
    if img_src.startswith("data:image"):
        img_path = save_base64_image(img_src, rank, manufacturer)

    return {
        "rank": rank,
        "manufacturer": manufacturer,
        "units": units,
        "delta": delta,
        "image": img_path,
    }


def main():
    print("📥 Downloading:", BASE_URL)
    r = requests.get(BASE_URL, headers={"User-Agent": "Mozilla/5.0"})
    r.encoding = "utf-8"
    html = r.text

    soup = BeautifulSoup(html, "lxml")

    cards = soup.find_all("div", attrs={"data-rank-num": True})

    rows = [extract_one_card(card) for card in cards]
    rows.sort(key=lambda x: x["rank"])

    with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["rank", "manufacturer", "units", "delta", "image"])
        for r in rows:
            w.writerow([
                r["rank"],
                r["manufacturer"],
                r["units"],
                r["delta"],
                r["image"],
            ])

    print(f"✔ CSV saved → {CSV_PATH}")
    print(f"✔ Images → {len(list(IMG_DIR.glob('*.png')))} files")


if __name__ == "__main__":
    main()
