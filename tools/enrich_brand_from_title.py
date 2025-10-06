#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
enrich_brand_from_title.py
--------------------------
series_url列を持つCSVを入力として、
各ページの<title>を取得し、LLMでブランド・車種名を抽出して追記。

依存: requests, pandas, openai
"""

import os, re, time, json, argparse, pandas as pd, requests
from bs4 import BeautifulSoup
from openai import OpenAI

def fetch_title(url):
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        t = soup.title.string.strip() if soup.title else ""
        if not t:
            meta = soup.find("meta", attrs={"property": "og:title"})
            if meta and meta.get("content"):
                t = meta["content"]
        return t
    except Exception as e:
        print(f"⚠️ {url}: {e}")
        return ""

def llm_extract_brand_model(client, title, model="gpt-4o-mini"):
    try:
        msg = [
            {"role": "system", "content": "解析汽车之家车系标题并输出JSON格式 {\"brand\":\"品牌名\",\"model\":\"车系名\"}。"},
            {"role": "user", "content": title}
        ]
        res = client.chat.completions.create(model=model, messages=msg)
        txt = res.choices[0].message.content.strip()
        m = re.search(r"\{.*\}", txt, re.S)
        data = json.loads(m.group(0)) if m else {}
        return data.get("brand", ""), data.get("model", "")
    except Exception:
        return "", ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--model", default="gpt-4o-mini")
    args = ap.parse_args()

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    df = pd.read_csv(args.input)
    brands, models, titles = [], [], []

    for i, row in df.iterrows():
        url = row.get("series_url", "")
        if not isinstance(url, str) or not url.strip():
            brands.append(""); models.append(""); titles.append(""); continue
        t = fetch_title(url)
        titles.append(t)
        b, m = llm_extract_brand_model(client, t, args.model)
        brands.append(b); models.append(m)
        print(f"[{i+1}] {b} - {m}")
        time.sleep(0.25)

    df["title"] = titles
    df["brand_llm"] = brands
    df["model_llm"] = models
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"✅ 保存: {args.output}")

if __name__ == "__main__":
    main()
