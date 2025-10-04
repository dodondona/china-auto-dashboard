#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL → フルページスクショ → タイル分割（overlap付き）→ VLMで行抽出 → CSV

ポイント:
- Playwright側でCJKフォントを強制適用 → 豆腐(□)防止
- LLMで brand と model を推定（辞書不要）
- 判定結果を data/brand_cache.json に永続キャッシュ
- 追加: seriesURL から <title> 抽出 → 正規表現で brand/model 抽出
"""

import os, csv, json, base64, argparse, time, re
from pathlib import Path
from typing import List, Dict
from PIL import Image
from playwright.sync_api import sync_playwright
from openai import OpenAI
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote_plus

# ----------------------------- VLM プロンプト -----------------------------
SYSTEM_PROMPT = """あなたは表の読み取りに特化した視覚アシスタントです。
画像は中国の自動車販売ランキングです。UI部品や広告は無視してください。
出力は JSON のみ。構造:
{
  "rows": [
    {"rank": <int|null>, "name": "<string>", "count": <int|null>}
  ]
}
"""
USER_PROMPT = "この画像に見えている全ての行を JSON で返してください。"

# ----------------------------- ブランド分離プロンプト -----------------------------
BRAND_PROMPT = """你是中国车系名称解析助手。...
只输出 JSON：
{"brand":"<string>","model":"<string>"}
"""

# ----------------------------- ヘルパー -----------------------------
HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9"
}

def _fetch(url: str, sleep: float = 1.0):
    try:
        r = requests.get(url, headers=HEADERS_WEB, timeout=25)
        time.sleep(sleep)
        if r.status_code == 200:
            return r.text
    except Exception:
        return None
    return None

def _extract_json_object(text: str) -> str:
    if not text: return ""
    s = text.strip()
    m = re.search(r"\{[\s\S]*\}", s)
    return m.group(0).strip() if m else ""

# ----------------------------- seriesURL → brand/model -----------------------------
def parse_series_title(title: str):
    """
    Autohome の <title> は以下のような形式:
    '秦PLUS_比亚迪秦PLUS价格_图片_报价-汽车之家'
    → brand='比亚迪', model='秦PLUS'
    """
    if not title:
        return None
    title = title.strip()
    m = re.match(r"([一-龥A-Za-z0-9\-\+ ]+)[_－\-](.+?)-?汽车之家", title)
    if m:
        model = m.group(1).strip()
        rest = m.group(2)
        # rest内にブランド名が含まれている場合を探す
        m2 = re.search(r"(比亚迪|上汽通用五菱|上汽大众|一汽大众|广汽丰田|长安汽车|吉利汽车|宝马|奥迪|本田|红旗|奇瑞|小鹏汽车|赛力斯|特斯拉|丰田|日产|奔驰|五菱|别克|长城|哈弗)", rest)
        if m2:
            brand = m2.group(1)
            return {"brand": brand, "model": model}
    return None

def resolve_brand_via_series(client: OpenAI, model: str, series_url: str):
    html = _fetch(series_url)
    if not html:
        return {"brand": "未知", "model": ""}
    soup = BeautifulSoup(html, "html.parser")
    title_txt = soup.title.get_text(" ", strip=True) if soup.title else ""
    obj = parse_series_title(title_txt)
    if obj:
        return obj
    # fallback: LLM
    resp = client.responses.create(
        model=model,
        input=[
            {"role":"system","content":"你是汽车之家页面解析助手。只输出JSON"},
            {"role":"user","content":title_txt}
        ],
        temperature=0
    )
    raw = (resp.output_text or "").strip()
    js = _extract_json_object(raw)
    try:
        return json.loads(js)
    except Exception:
        return {"brand":"未知","model":""}

# ----------------------------- collect seriesURL from ranking -----------------------------
def _norm_text(s: str) -> str:
    return (s or "").strip().replace(" ", "").replace("\u3000","")

def collect_series_links_from_rank(rank_url: str) -> Dict[str, str]:
    html = _fetch(rank_url)
    if not html:
        return {}
    soup = BeautifulSoup(html, "html.parser")
    mapping = {}
    for a in soup.select("a[href]"):
        href = a.get("href","")
        text = _norm_text(a.get_text(strip=True))
        if not text or "javascript:" in href:
            continue
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = "https://www.autohome.com.cn" + href
        if re.search(r"autohome\.com\.cn/\d+/?", href):
            if text not in mapping:
                mapping[text] = href
    return mapping

# ----------------------------- OpenAIVLM -----------------------------
class OpenAIVLM:
    def __init__(self, model: str, api_key: str):
        self.model = model
        self.client = OpenAI(api_key=api_key)

    def infer_table(self, img_path: Path) -> dict:
        b64 = base64.b64encode(img_path.read_bytes()).decode("utf-8")
        resp = self.client.responses.create(
            model=self.model,
            input=[
                {"role":"system","content":SYSTEM_PROMPT},
                {"role":"user","content":[
                    {"type":"input_text","text":USER_PROMPT},
                    {"type":"input_image","image_url":f"data:image/jpeg;base64,{b64}"}
                ]}
            ],
            temperature=0
        )
        raw = (resp.output_text or "").strip()
        js = _extract_json_object(raw)
        try:
            return json.loads(js)
        except:
            return {"rows":[]}

# ----------------------------- BrandResolver -----------------------------
class BrandResolver:
    def __init__(self, vlm: OpenAIVLM, cache_path: Path = Path("data/brand_cache.json"), series_map=None):
        self.vlm = vlm
        self.cache_path = cache_path
        self.cache = {}
        self.series_map = series_map or {}
        if cache_path.exists():
            try:
                self.cache = json.loads(cache_path.read_text(encoding="utf-8"))
            except: pass

    def _save(self):
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_text(json.dumps(self.cache, ensure_ascii=False, indent=2), encoding="utf-8")
        except: pass

    def resolve(self, raw_name: str):
        key = (raw_name or "").strip()
        if not key: return {"brand":"", "model":raw_name}
        if key in self.cache: return self.cache[key]

        # seriesURL優先
        if key in self.series_map:
            obj = resolve_brand_via_series(self.vlm.client, self.vlm.model, self.series_map[key])
            if obj.get("brand") and obj.get("model"):
                self.cache[key] = obj
                self._save()
                return obj

        # fallback: LLM分解
        bm = {"brand":"未知","model":key}
        self.cache[key] = bm
        self._save()
        return bm

# ----------------------------- その他ヘルパー -----------------------------
def normalize_rows(rows: List[dict]) -> List[dict]:
    return [{"rank":r.get("rank"),"name":(r.get("name") or "").strip(),"count":r.get("count")} for r in rows]

def merge_dedupe_sort(list_of_rows: List[List[dict]]) -> List[dict]:
    merged=[]; seen=set()
    for rows in list_of_rows:
        for r in rows:
            nm=(r.get("name") or "").strip()
            if nm and nm not in seen:
                seen.add(nm); merged.append(r)
    merged.sort(key=lambda r: (-(r.get("count") or 0), r.get("name")))
    for i,r in enumerate(merged,1): r["rank_seq"]=i
    return merged

# ----------------------------- main -----------------------------
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--from-url",required=True)
    ap.add_argument("--tile-height",type=int,default=1200)
    ap.add_argument("--tile-overlap",type=int,default=220)
    ap.add_argument("--out",default="result.csv")
    ap.add_argument("--model",default="gpt-4o")
    ap.add_argument("--openai-api-key",default=os.getenv("OPENAI_API_KEY"))
    args=ap.parse_args()

    series_map=collect_series_links_from_rank(args.from_url)

    full_path=grab_fullpage_to(args.from_url, Path("tiles"))
    tile_paths=[full_path]

    vlm=OpenAIVLM(model=args.model, api_key=args.openai_api_key)
    all_rows=[]
    for p in tile_paths:
        data=vlm.infer_table(p); rows=normalize_rows(data.get("rows",[]))
        all_rows.append(rows)

    merged=merge_dedupe_sort(all_rows)
    resolver=BrandResolver(vlm, series_map=series_map)
    for r in merged:
        bm=resolver.resolve(r["name"])
        r["brand"]=bm.get("brand",""); r["model"]=bm.get("model",r["name"]); r.pop("name",None)

    out=Path(args.out); out.parent.mkdir(parents=True,exist_ok=True)
    with open(out,"w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=["rank_seq","rank","brand","model","count"])
        w.writeheader(); [w.writerow(r) for r in merged]

if __name__=="__main__":
    main()
