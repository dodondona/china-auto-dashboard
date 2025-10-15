# ===== rank1_stage0_from_html_nextdata.py =====
# AutohomeランキングHTML（保存済みファイル）から、__NEXT_DATA__ JSONを抽出し、
# 順位順に seriesname / seriesid / linkurl / brand / count などをCSV化する
# ※他の処理・ネットワークアクセス・画像キャプチャなどは一切行わない

import json
import re
import csv
import sys
from pathlib import Path
from bs4 import BeautifulSoup

def extract_nextdata_json(html_path):
    """HTML内の__NEXT_DATA__ JSONを抽出してdict化"""
    html = Path(html_path).read_text(encoding='utf-8', errors='ignore')
    soup = BeautifulSoup(html, 'html.parser')
    script_tag = soup.find('script', id='__NEXT_DATA__')
    if not script_tag:
        raise ValueError("❌ __NEXT_DATA__ script tag not found")
    return json.loads(script_tag.string)

def parse_rank_data(data):
    """NEXT_DATA構造からランキング情報を抽出"""
    try:
        rank_list = data["props"]["pageProps"]["listRes"]["list"]
    except KeyError:
        raise KeyError("❌ listRes.list not found in NEXT_DATA structure")

    parsed = []
    for item in rank_list:
        seriesid = item.get("seriesid")
        brand = item.get("brandname")
        name = item.get("seriesname")
        link = f"https://www.autohome.com.cn/{seriesid}/" if seriesid else ""
        count = item.get("saleNum", 0)
        rank = item.get("rank", 0)

        parsed.append({
            "rank": rank,
            "seriesid": seriesid,
            "seriesname": name,
            "brand": brand,
            "count": count,
            "link": link
        })

    parsed.sort(key=lambda x: x["rank"])
    return parsed

def save_csv(items, out_csv):
    """CSVに保存"""
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=["rank", "brand", "seriesname", "seriesid", "count", "link"])
        writer.writeheader()
        writer.writerows(items)

def main():
    if len(sys.argv) < 3:
        print("Usage: python rank1_stage0_from_html_nextdata.py input.html output.csv")
        sys.exit(1)

    html_path, out_csv = sys.argv[1], sys.argv[2]
    data = extract_nextdata_json(html_path)
    items = parse_rank_data(data)
    save_csv(items, out_csv)
    print(f"✅ Extracted {len(items)} items → {out_csv}")

if __name__ == "__main__":
    main()
