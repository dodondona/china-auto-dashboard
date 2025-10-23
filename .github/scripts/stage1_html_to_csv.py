# -*- coding: utf-8 -*-
# .github/scripts/stage1_html_to_csv.py
#
# ./captures/*.htm を読み、Autohomeのレンダリング後HTMLから
# rank / name / units / delta(±) / link / price / image を抽出して
# ./csv/{name}.csv に出力します。

import os
import re
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup

CAP_DIR = Path("captures")
OUT_DIR = Path("csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

KEEP_FULL_DATA_IMAGE = os.environ.get("KEEP_FULL_DATA_IMAGE", "0") == "1"

def _txt(el):
    return el.get_text(" ", strip=True) if hasattr(el, "get_text") else ""

def _pick_image_url(img_el) -> str | None:
    """imgタグや周辺のlazy属性・styleから最良のURLを返す。無ければ data:image も可。"""
    if not img_el:
        return None

    # 1) まず src が http(s)
    src = img_el.get("src")
    if src and src.startswith(("http://", "https://")):
        return src

    # 2) lazy系属性
    for key in ("data-src", "data-original", "data-lazy-src", "data-url"):
        v = img_el.get(key)
        if v and v.startswith(("http://", "https://")):
            return v

    # 3) srcset から最初のURL
    srcset = img_el.get("srcset")
    if srcset:
        # "url1 1x, url2 2x" → 最初のURL
        first = srcset.split(",")[0].strip().split(" ")[0]
        if first.startswith(("http://", "https://")):
            return first

    # 4) 親要素の style="background-image:url(...)" を拾う
    style = img_el.get("style") or (img_el.parent.get("style") if img_el.parent else None)
    if style:
        m = re.search(r'url\((["\']?)(.*?)\1\)', style)
        if m and m.group(2).startswith(("http://", "https://")):
            return m.group(2)

    # 5) ここまででURLがない場合、data:image を返す（既定では短縮）
    if src and src.startswith("data:image/"):
        return src if KEEP_FULL_DATA_IMAGE else (src[:120] + "...")
    # data-original 等が data:image の場合
    for key in ("data-src", "data-original", "data-lazy-src", "data-url"):
        v = img_el.get(key)
        if v and v.startswith("data:image/"):
            return v if KEEP_FULL_DATA_IMAGE else (v[:120] + "...")

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

    # rank
    if div.has_attr("data-rank-num"):
        try:
            rec["rank"] = int(div["data-rank-num"])
        except Exception:
            pass

    # name
    name_tag = div.select_one(".tw-text-nowrap.tw-text-lg") or div.find(re.compile(r'^h[1-4]$'))
    if name_tag:
        rec["name"] = name_tag.get_text(strip=True)

    # price (例: 7.99-17.49万)
    for d in div.find_all("div"):
        t = _txt(d)
        m = re.search(r"\d+(?:\.\d+)?-\d+(?:\.\d+)?万", t)
        if m:
            rec["price"] = m.group(0)
            break

    # link（series_id -> https://www.autohome.com.cn/<id>）
    btn = div.find("button", attrs={"data-series-id": True})
    if btn:
        sid = btn.get("data-series-id")
        if sid:
            rec["link"] = f"https://www.autohome.com.cn/{sid}"

    # image（lazy対応＋data:image対応）
    img = div.find("img")
    rec["image"] = _pick_image_url(img)

    # units（「车系销量」近傍から 4-6桁 or カンマ区切り数値を拾う）
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
            # フォールバック：カード全体の末尾近くの数字を拾う
            nums = re.findall(r'(\d{1,3}(?:,\d{3})+|\d{4,6})', _txt(div))
            if nums:
                rec["units"] = int(nums[-1].replace(",", ""))

    # delta（SVGの色で方向判定、隣の数字で幅）
    svg = div.find("svg")
    if svg:
        # 幅の数値
        num = None
        s = svg.find_next(string=True)
        if isinstance(s, str):
            m = re.search(r"\d+", s.strip())
            if m: num = int(m.group(0))
        if num is None:
            m = re.search(r"\d+", _txt(svg.parent))
            if m: num = int(m.group(0))

        # fill色で方向推定
        colors = set((p.get("fill", "") or "").lower() for p in svg.find_all("path"))
        sign = None
        if "#f60" in colors or "#ff6600" in colors:
            sign = +1   # オレンジ＝上昇
        elif "#1ccd99" in colors or "#00cc99" in colors or "#1ccd9a" in colors:
            sign = -1   # グリーン＝下降

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
