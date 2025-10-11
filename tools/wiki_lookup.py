# -*- coding: utf-8 -*-
"""
Wikipedia から英語タイトルを拾う簡易ルックアップ。
入力の (brand_cn, model_cn) をまとめて単語列にして zh → en を優先。
"""

from typing import Optional
import wikipediaapi

UA = "china-auto-dashboard/1.0 (respect Wiki UA policy; contact maintainer)"

def lookup_wikipedia_en_title(brand_cn: str, model_cn: str) -> Optional[str]:
    zh = wikipediaapi.Wikipedia(user_agent=UA, language="zh")
    en = wikipediaapi.Wikipedia(user_agent=UA, language="en")

    query = f"{brand_cn} {model_cn}".strip()
    if not query:
        return None

    # まず中国語ページを取りに行く
    page_zh = zh.page(query)
    if page_zh and page_zh.exists():
        # 言語間リンクに英語があればそれを採用
        if "en" in page_zh.langlinks:
            return page_zh.langlinks["en"].title

    # 英語版へ直接も試す
    page_en = en.page(query)
    if page_en and page_en.exists():
        return page_en.title

    return None
