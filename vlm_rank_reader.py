#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL → フルページスクショ → タイル分割（overlap付き）→ VLMで行抽出 → CSV

変更点（最小）:
- seriesページの「左上パンくず/上部ヘッダー」を画像として切り出し、VLMに読ませて brand/model を抽出。
- パンくずが無い/読めない場合のみ、既存の title→LLM フォールバックに回す。
- 既存の --fullpage-split / CSV などの仕様はそのまま。
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

# ----------------------------- VLM（表読み取り）プロンプト -----------------------------
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

# ----------------------------- ブランド分離プロンプト（名前のみフォールバック用） -----------------------------
BRAND_PROMPT = """你是中国车系名称解析助手。给定一个“车系/车型名称”，只输出JSON：
{"brand":"<string>","model":"<string>"}
"""

# ----------------------------- ヘルパー -----------------------------
HEADERS_WEB = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
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
    m = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", s, re.I)
    if m: return m.group(1).strip()
    m = re.search(r"\{[\s\S]*\}", s)
    return m.group(0).strip() if m else ""

# ----------------------------- seriesURL → title 正規表現（既存フォールバック） -----------------------------
def parse_series_title(title: str):
    """
    例: '秦PLUS_比亚迪秦PLUS价格_图片_报价-汽车之家' → brand='比亚迪', model='秦PLUS'
    """
    if not title:
        return None
    title = title.strip()
    m = re.match(r"([一-龥A-Za-z0-9\-\+ ]+)[_－\-](.+?)-?汽车之家", title)
    if not m:
        return None
    model = m.group(1).strip()
    rest = m.group(2)
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
    # fallback: LLM（タイトル文字列を読むだけ）
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
        obj = json.loads(js)
        if obj.get("brand") and obj.get("model"):
            return obj
    except Exception:
        pass
    return {"brand":"未知","model":""}

# ----------------------------- ランキングから series URL を集める -----------------------------
def _norm_text(s: str) -> str:
    return (s or "").strip().replace(" ", "").replace("\u3000","")

def collect_series_links_from_rank(rank_url: str) -> Dict[str, str]:
    html = _fetch(rank_url)
    if not html:
        return {}
    soup = BeautifulSoup(html, "html.parser")
    mapping: Dict[str, str] = {}
    for a in soup.select("a[href]"):
        href = a.get("href","")
        text = _norm_text(a.get_text(strip=True))
        if not text or "javascript:" in href:
            continue
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = "https://www.autohome.com.cn" + href
        # 例: https://www.autohome.com.cn/5966/
        if re.search(r"autohome\.com\.cn/\d+/?", href):
            if text not in mapping:
                mapping[text] = href
    return mapping

# ----------------------------- seriesページの上部だけスクショ → VLMで brand/model -----------------------------
SERIES_HEADER_PROMPT = """あなたは中国の自動車サイトのページ上部（パンくず・大見出し）から
【品牌/厂商】と【车系名】だけを読み取るアシスタントです。

・画像はページの最上部（左上パンくず〜見出し）だけが写っています。
・広告/ナビ/タブは無視。パンくずや大見出しに出る正式表記のみを根拠にしてください。
・出力は JSON だけ:
{"brand":"<string>","model":"<string>"}
・判断できなければ brand は "未知"、model はできる範囲で返してください。
"""

def grab_series_header_screenshot(url: str, out_dir: Path, cut_height: int = 260) -> Path:
    """
    シリーズページの最上部だけを切り出して保存。
    DOMには依存せず、単に上から cut_height px を撮る。
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "series_header.jpg"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1380, "height": 800},
            device_scale_factor=2,
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            locale="zh-CN",
            extra_http_headers={"Accept-Language": "zh-CN,zh;q=0.9"},
        )
        page = ctx.new_page()
        page.goto(url, timeout=60000)
        page.wait_for_timeout(2500)
        # 全体を一旦撮影
        tmp_full = out_dir / "series_full.jpg"
        page.screenshot(path=str(tmp_full), full_page=True)
        browser.close()
    # 上部だけを切り出し
    im = Image.open(tmp_full).convert("RGB")
    W, H = im.size
    box = (0, 0, W, min(cut_height, H))
    im.crop(box).save(path, "JPEG", quality=90, optimize=True)
    try:
        tmp_full.unlink()
    except Exception:
        pass
    return path

def vlm_read_brand_model_from_image(client: OpenAI, model: str, img_path: Path) -> Dict[str,str]:
    b64 = base64.b64encode(img_path.read_bytes()).decode("utf-8")
    resp = client.responses.create(
        model=model,
        input=[
            {"role":"system","content":SERIES_HEADER_PROMPT},
            {"role":"user","content":[
                {"type":"input_text","text":"上部パンくず/見出しから品牌と车系名だけを抽出。必ずJSONのみ。"},
                {"type":"input_image","image_url":f"data:image/jpeg;base64,{b64}"}
            ]}
        ],
        temperature=0
    )
    raw = (resp.output_text or "").strip()
    js = _extract_json_object(raw)
    try:
        obj = json.loads(js)
        # brand が取れない時は "未知" に寄せる
        brand = (obj.get("brand") or "").strip() or "未知"
        model_str = (obj.get("model") or "").strip()
        return {"brand": brand, "model": model_str}
    except Exception:
        return {"brand":"未知","model":""}

# ----------------------------- VLM（表→JSON） -----------------------------
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

    def split_brand_model_llm(self, name: str) -> dict:
        resp = self.client.responses.create(
            model=self.model,
            input=[
                {"role":"system","content":BRAND_PROMPT},
                {"role":"user","content":name}
            ],
            temperature=0
        )
        raw = (resp.output_text or "").strip()
        js = _extract_json_object(raw)
        try:
            return json.loads(js)
        except Exception:
            return {"brand":"未知","model":name}

# ----------------------------- 画像行の正規化/マージ -----------------------------
def normalize_rows(rows: List[dict]) -> List[dict]:
    out = []
    for r in rows:
        nm = (r.get("name") or "").replace(" ","").replace("\u3000","")
        out.append({
            "rank": r.get("rank"),
            "name": nm,
            "count": r.get("count")
        })
    return out

def merge_dedupe_sort(list_of_rows: List[List[dict]]) -> List[dict]:
    merged: List[dict] = []
    seen = set()
    for rows in list_of_rows:
        for r in rows:
            key = (r.get("name") or "").replace(" ", "").replace("\u3000","")
            if key and key not in seen:
                seen.add(key)
                merged.append(r)
    merged.sort(key=lambda r: (-(r.get("count") or 0), r.get("name")))
    for i, r in enumerate(merged, 1):
        r["rank_seq"] = i
    return merged

# ----------------------------- BrandResolver（先頭で「上部パンくず読み」） -----------------------------
class BrandResolver:
    def __init__(self, vlm: OpenAIVLM, cache_path: Path = Path("data/brand_cache.json"), series_map: Dict[str,str]=None):
        self.vlm = vlm
        self.cache_path = cache_path
        self.cache: Dict[str, Dict[str,str]] = {}
        self.series_map = series_map or {}
        # 既存キャッシュ読み
        try:
            if self.cache_path.exists():
                self.cache = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except Exception:
            self.cache = {}

    def _save(self):
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_text(json.dumps(self.cache, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def resolve(self, raw_name: str) -> Dict[str,str]:
        key = (raw_name or "").strip()
        if not key:
            return {"brand":"", "model":raw_name}

        # キャッシュヒット
        hit = self.cache.get(key)
        if hit and isinstance(hit, dict) and "brand" in hit and "model" in hit:
            return hit

        # 0) series URL が取れていれば：ページ上部だけスクショ→VLMで brand/model 読み取り（新規追加）
        series_url = self.series_map.get(key) or ""
        if series_url:
            try:
                img = grab_series_header_screenshot(series_url, Path("tiles/series_headers"))
                obj = vlm_read_brand_model_from_image(self.vlm.client, self.vlm.model, img)
                if obj.get("brand") and obj.get("model"):
                    out = {"brand": obj["brand"], "model": obj["model"]}
                    self.cache[key] = out
                    self._save()
                    return out
            except Exception:
                pass

            # 0-2) 画像でダメなら既存の title→LLM フォールバック
            try:
                obj2 = resolve_brand_via_series(self.vlm.client, self.vlm.model, series_url)
                if obj2.get("brand") and obj2.get("model"):
                    out = {"brand": obj2["brand"], "model": obj2["model"]}
                    self.cache[key] = out
                    self._save()
                    return out
            except Exception:
                pass

        # 1) 最終フォールバック：名前だけ渡して LLM の推定（既存）
        bm = self.vlm.split_brand_model_llm(key)
        out = {"brand": bm.get("brand","未知") or "未知", "model": bm.get("model", key)}
        self.cache[key] = out
        self._save()
        return out

# ----------------------------- フルページスクショ / 分割 -----------------------------
def grab_fullpage_to(url: str, out_dir: Path, viewport=(1380, 2400)) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    full_path = out_dir / "full.jpg"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": viewport[0], "height": viewport[1]},
            device_scale_factor=2,
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            locale="zh-CN",
            extra_http_headers={"Accept-Language": "zh-CN,zh;q=0.9"},
        )
        page = ctx.new_page()
        page.goto(url, timeout=60000)
        page.wait_for_timeout(4000)
        page.screenshot(path=str(full_path), full_page=True)
        browser.close()
        return full_path

def split_full_image(full_path: Path, out_dir: Path, tile_height: int, overlap: int) -> List[Path]:
    im = Image.open(full_path).convert("RGB")
    W, H = im.size
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []
    y, i = 0, 0
    step = max(1, tile_height - overlap)
    while y < H:
        y2 = min(y + tile_height, H)
        tile = im.crop((0, y, W, y2))
        p = out_dir / f"tile_{i:02d}.jpg"
        tile.save(p, "JPEG", quality=90, optimize=True)
        paths.append(p)
        i += 1
        if y2 >= H:
            break
        y += step
    return paths

# ----------------------------- MAIN -----------------------------
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--from-url",required=True)
    ap.add_argument("--tile-height",type=int,default=1200)
    ap.add_argument("--tile-overlap",type=int,default=220)
    ap.add_argument("--out",default="result.csv")
    ap.add_argument("--model",default="gpt-4o")
    ap.add_argument("--openai-api-key",default=os.getenv("OPENAI_API_KEY"))
    ap.add_argument("--fullpage-split", action="store_true")  # 既存のまま
    args=ap.parse_args()

    # ランキングHTMLから series URL マップ
    series_map=collect_series_links_from_rank(args.from_url)

    # ランキングのフル画像（VLMで行抽出に使う）
    full_path=grab_fullpage_to(args.from_url, Path("tiles"))
    if args.fullpage_split:
        tile_paths=split_full_image(full_path, Path("tiles"), args.tile_height, args.tile_overlap)
    else:
        tile_paths=[full_path]

    vlm=OpenAIVLM(model=args.model, api_key=args.openai_api_key)
    all_rows=[]
    for p in tile_paths:
        data=vlm.infer_table(p)
        rows=normalize_rows(data.get("rows",[]))
        print(f"[INFO] {p.name}: {len(rows)} rows")
        all_rows.append(rows)

    merged=merge_dedupe_sort(all_rows)
    resolver=BrandResolver(vlm, series_map=series_map)
    for r in merged:
        bm=resolver.resolve(r["name"])
        r["brand"]=bm.get("brand","") or "未知"   # brand 未取得時は "未知"
        r["model"]=bm.get("model", r["name"])
        r.pop("name",None)

    out=Path(args.out); out.parent.mkdir(parents=True,exist_ok=True)
    with open(out,"w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=["rank_seq","rank","brand","model","count"])
        w.writeheader()
        for r in merged:
            w.writerow(r)

    print(f"[DONE] rows={len(merged)} -> {out}  (cache: data/brand_cache.json)")

if __name__=="__main__":
    main()
