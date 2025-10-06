#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
enrich_brand_from_title.py
- series_url を開いて <title> を取得
- まずルールベース（角括弧優先）で model/brand を抽出
- 取れない/曖昧な場合のみ LLM で補完
- 出力列: brand, model, ... , brand_conf, series_conf, title_raw を維持
"""

import os
import re
import json
import time
import argparse
import requests
import pandas as pd
from bs4 import BeautifulSoup

# --- LLM（フォールバック用） -----------------------------------------------
def llm_extract_brand_model(title: str, model_name: str = "gpt-4o-mini"):
    """title から JSON {"brand": "...", "model": "..."} を返す（失敗時は空）"""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        messages = [
            {"role": "system",
             "content": "你是一个简体中文的解析器。输入是汽车之家车系详情页<title>。"
                        "从中识别品牌名brand和车系名model，并以严格JSON输出："
                        "{\"brand\":\"…\",\"model\":\"…\"}。不要输出其他字符。"},
            {"role": "user", "content": title}
        ]
        res = client.chat.completions.create(model=model_name, messages=messages, temperature=0)
        txt = (res.choices[0].message.content or "").strip()
        m = re.search(r"\{.*\}", txt, re.S)
        if not m:
            return "", ""
        data = json.loads(m.group(0))
        return str(data.get("brand", "")).strip(), str(data.get("model", "")).strip()
    except Exception:
        return "", ""

# --- ルールベース抽出 ---------------------------------------------------------
# 角括弧のパターン（左, 右 の候補）
BRACKETS = [
    ("【", "】"),
    ("[", "]"),
    ("「", "」"),
    ("（", "）"),
    ("(", ")"),
]

SPLIT_AFTER_BRACKET = r"[ _\t\u3000（(【\[]+"

def extract_rule_based(title: str):
    """
    角括弧の中を model、直後のトークンを brand として抽出。
    例: '【星愿】 吉利银河_星愿报价_…' -> model='星愿', brand='吉利银河'（=直後トークン全体）
         '[Model Y] 特斯拉_Model Y…' -> model='Model Y', brand='特斯拉'
    """
    if not title:
        return "", "", 0.0, 0.0

    # 正規化（全角空白→半角空白）
    t = title.replace("\u3000", " ").strip()

    # どれかの括弧でヒットした最初の箇所を使う
    for lp, rp in BRACKETS:
        # 最短一致
        m = re.search(re.escape(lp) + r"\s*(.+?)\s*" + re.escape(rp), t)
        if not m:
            continue
        model = m.group(1).strip()
        # 括弧の直後の残り
        rest = t[m.end():].lstrip()
        # brand候補 = rest の先頭トークン（空白/アンダーバー/記号で区切る）
        # 例: "吉利银河_星愿报价_..." -> "吉利银河"
        b = re.split(SPLIT_AFTER_BRACKET, rest, maxsplit=1)[0].strip()
        # 万一、記号で始まる／空になった場合はさらに文字列から拾う
        if not b:
            # 記号を飛ばして最初の文字列塊
            mm = re.search(r"([A-Za-z0-9\u4e00-\u9fff]+)", rest)
            b = mm.group(1) if mm else ""

        # 正常に両方取れたら conf=1.0
        if model and b:
            return b, model, 1.0, 1.0
        # 片方だけでも取れたら片側のみ conf=1.0
        if model or b:
            return b, model, 1.0 if b else 0.0, 1.0 if model else 0.0

    # どの括弧にもヒットしない
    return "", "", 0.0, 0.0

# --- タイトル取得 -------------------------------------------------------------
def fetch_title(url: str, timeout=20) -> str:
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        if soup.title and soup.title.string:
            return soup.title.string.strip()
        meta = soup.find("meta", attrs={"property": "og:title"})
        if meta and meta.get("content"):
            return meta["content"].strip()
        return ""
    except Exception:
        return ""

# --- メイン -------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="input CSV (must contain series_url)")
    ap.add_argument("--output", required=True, help="output CSV")
    ap.add_argument("--model", default="gpt-4o-mini")
    ap.add_argument("--conf-threshold", type=float, default=0.7)
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    titles, brands, models = [], [], []
    brand_conf_list, series_conf_list = [], []

    for idx, row in df.iterrows():
        url = str(row.get("series_url", "")).strip()
        title = fetch_title(url) if url else ""
        titles.append(title)

        # 1) 角括弧ルールでまず抽出
        b_rule, m_rule, b_conf, m_conf = extract_rule_based(title)

        b, m = b_rule, m_rule
        bc, mc = b_conf, m_conf

        # 2) どちらか欠落/低確度なら LLM で補完
        if not b or not m:
            b_llm, m_llm = llm_extract_brand_model(title, args.model)
            # 無い側だけ埋める（ルール優先）
            if not b and b_llm:
                b, bc = b_llm, max(bc, args.conf_threshold)  # LLM はしきい値扱い
            if not m and m_llm:
                m, mc = m_llm, max(mc, args.conf_threshold)

        brands.append(b)
        models.append(m)
        brand_conf_list.append(round(bc, 2))
        series_conf_list.append(round(mc, 2))

        print(f"[{idx+1}] brand='{b}' ({bc:.2f}) / model='{m}' ({mc:.2f})")

        # 軽いレート制御（相手に優しく）
        time.sleep(0.2)

    out = df.copy()
    # 既存スキーマを維持
    out["brand"] = brands
    out["model"] = models
    out["brand_conf"] = brand_conf_list
    out["series_conf"] = series_conf_list
    out["title_raw"] = titles

    out.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"✅ 保存: {args.output}")

if __name__ == "__main__":
    main()
