# -*- coding: utf-8 -*-
# .github/scripts/stage1_html_to_csv.py
#
# ./captures/*.htm → ./csv/{name}.csv
# 抽出: rank / name / units / delta(±) / link / price / image

import os
import re
from urllib.parse import urljoin
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup

CAP_DIR = Path("captures")
OUT_DIR = Path("csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

KEEP_FULL_DATA_IMAGE = os.environ.get("KEEP_FULL_DATA_IMAGE", "0") == "1"
BASE = "https://www.autohome.com.cn"

def _txt(el):
    return el.get_text(" ", strip=True) if hasattr(el, "get_text") else ""

def _first_url_from_srcset(val: str | None) -> str | None:
    if not val:
        return None
    part = val.split(",")[0].strip().split(" ")[0]
    return part if part.startswith(("http://","https://")) else None

def _url_from_style(style: str | None) -> str | None:
    if not style:
        return None
    m = re.search(r'url\((["\']?)(.*?)\1\)', style)
    if m and m.group(2).startswith(("http://","https://")):
        return m.group(2)
    return None

def _pick_image_url(div) -> str | None:
    # 1) picture>source[srcset]
    pic = div.find("picture")
    if pic:
        src_el = pic.find("source", attrs={"srcset": True})
        url = _first_url_from_srcset(src_el.get("srcset")) if src_el else None
        if url:
            return url

    # 2) <img> パターン（src / data-* / srcset / style）
    img = div.find("img")
    if img:
        src = img.get("src")
        if src and src.startswith(("http://","https://")):
            return src

        for key in ("data-src","data-original","data-lazy-src","data-url"):
            v = img.get(key)
            if v and v.startswith(("http://","https://")):
                return v

        url = _first_url_from_srcset(img.get("srcset"))
        if url:
            return url

        s = img.get("style") or (img.parent.get("style") if img.parent else None)
        url = _url_from_style(s)
        if url:
            return url

        # data:image は必要なら短縮してCSVへ
        if src and src.startswith("data:image/"):
            return src if KEEP_FULL_DATA_IMAGE else (src[:120] + "...")
        for key in ("data-src","data-original","data-lazy-src","data-url"):
            v = img.get(key)
            if v and v.startswith("data:image/"):
                return v if KEEP_FULL_DATA_IMAGE else (v[:120] + "...")

    return None

def _extract_link(div) -> str | None:
    # 優先：button[data-series-id]
    btn = div.find("button", attrs={"data-series-id": True})
    if btn and btn.get("data-series-id"):
        return f"{BASE}/{btn['data-series-id']}"

    # Fallback: <a href="/12345"> or absolute
    a = div.find("a", href=True)
    if a:
        href = a["href"].strip()
        # /12345 → 絶対化
        if re.fullmatch(r"/\d{3,6}/?", href):
            return urljoin(BASE, href)
        # https://www.autohome.com.cn/12345
        if re.match(r"^https?://www\.autohome\.com\.cn/\d{3,6}/?$", href):
            return href

    # さらに、data-series-id が別要素にあるケース
    any_sid = div.find(attrs={"data-series-id": True})
    if any_sid and any_sid.get("data-series-id"):
        return f"{BASE}/{any_sid['data-series-id']}"

    return None

def parse_card(div):
    rec = {
        "rank": None,
        "name": None,
        "units": None,
        "delta_vs_last_month": None,
        "link": None,
        "price": None,
        "image": None,
    }

    if div.has_attr("data-rank-num"):
        try:
            rec["rank"] = int(div["data-rank-num"])
        except Exception:
            pass

    name_tag = div.select_one(".tw-text-nowrap.tw-text-lg") or div.find(re.compile(r'^h[1-4]$'))
    if name_tag:
        rec["name"] = name_tag.get_text(strip=True)

    # price (7.99-17.49万)
    for d in div.find_all("div"):
        t = _txt(d)
        m = re.search(r"\d+(?:\.\d+)?-\d+(?:\.\d+)?万", t)
        if m:
            rec["price"] = m.group(0)
            break

    rec["link"]  = _extract_link(div)
    rec["image"] = _pick_image_url(div)

    # units
    label = div.find(string=lambda s: isinstance(s, str) and "车系销量" in s)
    if label:
        cur = label.parent
        for node in [cur.next_sibling, cur.parent, cur.parent.next_sibling]:
            if node and hasattr(node, "get_text"):
                m = re.search(r'(\d{1,3}(?:,\d{3})+|\d{4,6})', _txt(node))
                if m:
                    rec["units"] = int(m.group(1).replace(",", ""))
                    break
        if rec["units"] is None:
            nums = re.findall(r'(\d{1,3}(?:,\d{3})+|\d{4,6})', _txt(div))
            if nums:
                rec["units"] = int(nums[-1].replace(",", ""))

    # delta ±
    svg = div.find("svg")
    if svg:
        num = None
        s = svg.find_next(string=True)
        if isinstance(s, str):
            m = re.search(r"\d+", s.strip())
            if m: num = int(m.group(0))
        if num is None:
            m = re.search(r"\d+", _txt(svg.parent))
            if m: num = int(m.group(0))

        colors = set((p.get("fill", "") or "").lower() for p in svg.find_all("path"))
        sign = None
        if "#f60" in colors or "#ff6600" in colors:
            sign = +1
        elif "#1ccd99" in colors or "#00cc99" in colors or "#1ccd9a" in colors:
            sign = -1

        if num is not None:
            rec["delta_vs_last_month"] = f"{'+' if sign==1 else '-' if sign==-1 else ''}{num}"

    return rec

def parse_file(html_path: Path) -> pd.DataFrame:
    soup = BeautifulSoup(html_path.read_text(encoding="utf-8", errors="ignore"), "lxml")
    rows = []
    for div in soup.select("div[data-rank-num]"):
        rec = parse_card(div)
        if rec["name"]:
            rows.append(rec)
    df = pd.DataFrame(rows).sort_values("rank")
    return df

def main():
    htmls = sorted(CAP_DIR.glob("*.htm")) + sorted(CAP_DIR.glob("*.html"))
    if not htmls:
        print("No rendered HTML found under ./captures")
        return
    for html in htmls:
        df = parse_file(html)
        out = OUT_DIR / (html.stem + ".csv")
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"✅ Saved {out} rows={len(df)}")

if __name__ == "__main__":
    main()
