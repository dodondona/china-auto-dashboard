#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Autohome ランキングページをスクレイピングし、
車種・ブランド・販売台数を取得し、さらに LLM で日本語列を補完するスクリプト。

出力CSVには以下を含む:
- rank_seq: ランキング順序
- rank: ランク番号
- model: 車種名（元の中国語）
- brand: ブランド名（中国語, LLM補完）
- count: 販売台数
- brand_jp: ブランド名（日本語表記, LLM補完）
- model_jp: 車種名（日本語または簡体字を日本語に直したもの, LLM補完）
- link: 車種詳細ページへのURL
"""

import os, csv, json
from pathlib import Path
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from openai import OpenAI

# OpenAIクライアント
client = OpenAI()

def llm_enrich(model_name: str, href: str):
    """
    モデル名とリンクを渡して、ブランド名と日本語表記をLLMで補完。
    """
    prompt = f"""
以下の車両データについて、ブランド名と日本語表記を補完してください。

- モデル名: {model_name}
- リンク: {href}

必ず次のJSON形式で返してください:
{{
  "brand": "ブランド名（中国語）",
  "brand_jp": "ブランド名（日本語表記）",
  "model_jp": "モデル名（日本語表記または簡体字を日本語に直したもの）"
}}
"""
    resp = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    txt = resp.choices[0].message.content.strip()
    try:
        data = json.loads(txt)
    except Exception:
        data = {"brand": "未知", "brand_jp": "", "model_jp": model_name}
    return data

def scrape_and_parse(url: str, out_csv: str = "rank_output.csv"):
    """
    Autohomeのランキングページを取得し、CSVに保存。
    """
    # Playwrightでページを取得
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, timeout=60000)
        html = page.content()
        browser.close()

    # BeautifulSoupで解析
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    rank = 1

    # 各車両をパース
    for item in soup.select("div.rank-list li"):
        name_tag = item.select_one("p a")
        if not name_tag:
            continue
        model_name = name_tag.get_text(strip=True)
        href = name_tag.get("href", "")
        if href.startswith("//"):
            link = f"https:{href}"
        elif href.startswith("/"):
            link = f"https://www.autohome.com.cn{href}"
        else:
            link = href

        count_tag = item.select_one("p em")
        count = count_tag.get_text(strip=True) if count_tag else ""

        # LLMでブランド名・日本語を補完
        enriched = llm_enrich(model_name, link)

        rows.append({
            "rank_seq": rank,
            "rank": rank,
            "model": model_name,
            "brand": enriched.get("brand", "未知"),
            "count": count,
            "brand_jp": enriched.get("brand_jp", ""),
            "model_jp": enriched.get("model_jp", model_name),
            "link": link,
        })
        rank += 1

    # CSV出力
    if rows:
        with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"✅ CSV出力完了: {out_csv}")
    else:
        print("⚠️ データが抽出できませんでした。")

if __name__ == "__main__":
    # 実行例: 2025年8月の月次ランキングページ
    url = "https://www.autohome.com.cn/rank/1-3-1071-x/2025-08.html"
    scrape_and_parse(url, "rank_output.csv")
