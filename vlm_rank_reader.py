#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL → フルページスクショ → タイル分割（overlap付き）→ VLMで行抽出 → CSV

ポイント:
- Playwright側でCJKフォントを強制適用 → 豆腐(□)防止
- LLMで brand と model を推定（辞書不要）
- 判定結果を data/brand_cache.json に永続キャッシュ（同じ車名は次回以降無課金・高速）
- 追加: ランキングの車名リンクから series URL を取得し、そのページ HTML を LLM に読ませて brand/model を確定
"""

import os, csv, json, base64, argparse, time, re
from pathlib import Path
from typing import List, Dict
from PIL import Image
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
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

# ----------------------------- ブランド分離プロンプト -----------------------------
BRAND_PROMPT = """你是中国车系名称解析助手。给定一个“车系/车型名称”或图片片段，请输出对应的【品牌/厂商】与【车型名】。

数据来源与验证顺序（务必遵守）：
1) 必须优先在 汽车之家（autohome.com.cn） 查询并核对：先用“site:autohome.com.cn <车型/车系名>”检索；打开最相关的“车系”或“参数配置/图片/报价”页面。
2) 在页面中寻找「厂商」「品牌」或面包屑位置的“<品牌/厂商>-<车系>”字样（示例：比亚迪-秦PLUS / 上汽通用五菱-宏光MINIEV）。
3) 如在汽车之家未找到完全匹配，请：
   a. 回看输入图片/文本，检查是否有误読。
   b. 结合常见别名/缩写再次在汽车之家检索 1 次。
4) 仍无法在汽车之家确认时，才可短暂参考第二来源（如：易车 yiche.com）。

命名规则：
- 不要仅按第一个词硬拆。
- 厂商はできるだけ正式名称。
- 车型名は後継記号を含めて完全形。

只输出 JSON：
{"brand":"<string>","model":"<string>"}
"""

# ----------------------------- AutoHome/Yiche 参照（スクレイプ&検索） -----------------------------
HEADERS_WEB = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept-Language": "zh-CN,zh;q=0.9"
}
_DDG = "https://duckduckgo.com/html/?q={q}"

_AUTOHOME_PATTERNS = [
    r"https?://car\.autohome\.com\.cn/series/\d+/?",
    r"https?://car\.autohome\.com\.cn/\d+/#?[\w=]*",
    r"https?://car\.autohome\.com\.cn/config/series/\d+/?",
    r"https?://car\.autohome\.com\.cn/pic/series/\d+/?",
    r"https?://www\.autohome\.com\.cn/\d+/?",
]
_AUTOHOME_RE = re.compile("|".join(_AUTOHOME_PATTERNS))
_YICHE_RE = re.compile(r"https?://car\.yiche\.com/[^/\s]+/?")

def _ddg_search(query: str, site: str, topk: int = 6, sleep: float = 1.2):
    q = f"site:{site} {query}"
    url = _DDG.format(q=quote_plus(q))
    try:
        rs = requests.get(url, headers=HEADERS_WEB, timeout=20)
        time.sleep(sleep)
        if rs.status_code != 200:
            return []
        soup = BeautifulSoup(rs.text, "lxml")
        links = []
        for a in soup.select("a[href]"):
            href = a.get("href")
            if href and site in href:
                links.append(href)
        uniq = []
        for u in links:
            if u not in uniq:
                uniq.append(u)
        return uniq[:topk]
    except Exception:
        return []

def _fetch(url: str, sleep: float = 1.0):
    try:
        r = requests.get(url, headers=HEADERS_WEB, timeout=25)
        time.sleep(sleep)
        if r.status_code == 200:
            return r.text
    except Exception:
        return None
    return None

def _extract_brand_autohome(html: str):
    if not html: return None
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"厂商[:：]\s*([^\s/|>《》\-—–]+)", text)
    if m: return m.group(1).strip()
    m = re.search(r"品牌[:：]\s*([^\s/|>《》\-—–]+)", text)
    if m: return m.group(1).strip()
    m = re.search(r"([一-龥A-Za-z0-9]+)\s*[-－—]\s*([一-龥A-Za-z0-9\+\s]+)", text)
    if m: return m.group(1).strip()
    return None

def _extract_brand_yiche(html: str):
    if not html: return None
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"厂商[:：]\s*([^\s/|>《》\-—–]+)", text)
    if m: return m.group(1).strip()
    m = re.search(r"品牌[:：]\s*([^\s/|>《》\-—–]+)", text)
    if m: return m.group(1).strip()
    return None

def resolve_brand_via_web(model_name: str):
    # 汽车之家で確認→無ければ易车。見つかれば (brand, url)、無ければ (None, None)
    cand = _ddg_search(model_name, site="autohome.com.cn", topk=8)
    au = [u for u in cand if _AUTOHOME_RE.search(u)]
    for u in au[:5]:
        html = _fetch(u)
        b = _extract_brand_autohome(html)
        if b:
            return b, u
    cand = _ddg_search(model_name, site="yiche.com", topk=6)
    yi = [u for u in cand if _YICHE_RE.search(u)]
    for u in yi[:3]:
        html = _fetch(u)
        b = _extract_brand_yiche(html)
        if b:
            return b, u
    return None, None

# ----------------------------- ランキングから series URL を集める（最小追加） -----------------------------
def _norm_text(s: str) -> str:
    return (s or "").strip().replace(" ", "").replace("\u3000", "")

def collect_series_links_from_rank(rank_url: str) -> Dict[str, str]:
    """
    ランキングHTMLから「車名テキスト -> series URL」を作る。
    セレクタ固定は避け、a[href] の走査 + URLパターンで判断する。
    """
    html = _fetch(rank_url)
    if not html:
        return {}
    soup = BeautifulSoup(html, "lxml")
    mapping: Dict[str, str] = {}
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = _norm_text(a.get_text(strip=True))
        if not text or "javascript:" in href:
            continue
        # 絶対URLに補正
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = "https://www.autohome.com.cn" + href
        # series URL 判定（例: https://www.autohome.com.cn/5966/ など）
        if re.search(r"autohome\.com\.cn/(?:\d+/|diandongche/series-\d+\.html)", href):
            # すでに同名があれば最初を優先（任意）
            if text not in mapping:
                mapping[text] = href
    return mapping

# ============================= ここから JSON抽出ヘルパー（既存+微修正） =============================
def _extract_json_object(text: str) -> str:
    """
    - ```json ... ``` 優先で中身を抽出
    - なければ最初の { ... } を抽出
    """
    if not text:
        return ""
    s = text.strip()

    m = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", s, re.I)
    if m:
        return m.group(1).strip()

    m = re.search(r"```([\s\S]*?)```", s)
    if m:
        inner = m.group(1).strip()
        if inner.startswith("{") and inner.endswith("}"):
            return inner

    m = re.search(r"\{[\s\S]*\}", s)
    if m:
        return m.group(0).strip()

    return ""

# ----------------------------- 各 series ページを LLM に読ませて抽出（最小追加） -----------------------------
SERIES_READ_SYSTEM = """你是“汽车之家 车系页面”解析助手。只基于给定页面文本判断，不要编造。
优先依据：页面标题（<title>）、面包屑/大标题中的“品牌/厂商 + 车系”。
输出 JSON：{"brand":"<string>","model":"<string>","source":"title|breadcrumb|heading|unknown"}"""

def resolve_brand_via_series_llm(client: OpenAI, model: str, series_url: str) -> Dict[str, str]:
    """
    series_url の HTML を取得し、まず <title> だけ、ダメなら本文一部を LLM に渡して抽出。
    """
    html = _fetch(series_url)
    if not html:
        return {"brand":"未知","model":""}

    soup = BeautifulSoup(html, "lxml")
    title_txt = (soup.title.get_text(" ", strip=True) if soup.title else "")

    # 1st try: タイトルだけ
    resp = client.responses.create(
        model=model,
        input=[
            {"role":"system","content":SERIES_READ_SYSTEM},
            {"role":"user","content":[
                {"type":"input_text","text":f"URL: {series_url}\n以下は<title>文字列です。ここから品牌/厂商と车系名だけを抽出してJSONで返してください。"},
                {"type":"input_text","text":title_txt[:800]}
            ]}
        ],
        temperature=0
    )
    raw = (resp.output_text or "").strip()
    js = _extract_json_object(raw)
    if js:
        try:
            obj = json.loads(js)
            if obj.get("brand") and obj.get("model"):
                return {"brand":obj["brand"], "model":obj["model"]}
        except Exception:
            pass

    # 2nd try: 本文テキスト（先頭～一部だけ）を投入
    body_text = soup.get_text("\n", strip=True)
    body_head = body_text[:20000]  # トークン節約
    resp2 = client.responses.create(
        model=model,
        input=[
            {"role":"system","content":SERIES_READ_SYSTEM},
            {"role":"user","content":[
                {"type":"input_text","text":f"URL: {series_url}\n以下は页面文本の抜粋（先頭～）。見出し/面包屑/标题を最優先に、品牌/厂商と车系名をJSONで返してください。"},
                {"type":"input_text","text":body_head}
            ]}
        ],
        temperature=0
    )
    raw2 = (resp2.output_text or "").strip()
    js2 = _extract_json_object(raw2)
    if js2:
        try:
            obj = json.loads(js2)
            if obj.get("brand") and obj.get("model"):
                return {"brand":obj["brand"], "model":obj["model"]}
        except Exception:
            pass

    return {"brand":"未知","model":""}

# ----------------------------- タイル分割 -----------------------------
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

# ----------------------------- スクショ -----------------------------
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

# ----------------------------- VLMクラス -----------------------------
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
        if not js:
            return {"rows":[]}
        try:
            return json.loads(js)
        except Exception:
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
        if not js:
            return {"brand":"未知","model":name}
        try:
            return json.loads(js)
        except Exception:
            return {"brand":"未知","model":name}

# ----------------------------- normalize -----------------------------
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

# ----------------------------- マージ -----------------------------
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

# ----------------------------- BrandResolver（series URL を最優先に LLM 読み） -----------------------------
class BrandResolver:
    def __init__(self, vlm: OpenAIVLM, cache_path: Path = Path("data/brand_cache.json"), series_map: Dict[str,str] = None):
        self.vlm = vlm
        self.cache_path = cache_path
        self.cache: Dict[str, Dict[str,str]] = {}
        self.series_map = series_map or {}
        self._load()

    def _load(self):
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

    def _find_series_url(self, key: str) -> str:
        k = _norm_text(key)
        if k in self.series_map:
            return self.series_map[k]
        # ゆるめ一致（PLUS/空白差などの小揺れ吸収）
        for nm, url in self.series_map.items():
            if k == nm or k.replace("PLUS","Plus") == nm or k.lower() == nm.lower():
                return url
        return ""

    def resolve(self, raw_name: str) -> Dict[str,str]:
        key = (raw_name or "").strip()
        if not key:
            return {"brand":"", "model":raw_name}

        hit = self.cache.get(key)
        if hit and isinstance(hit, dict) and "brand" in hit and "model" in hit:
            return hit

        # 0) ランキング由来の series URL があれば、まず LLM で series ページを読ませる（最小追加）
        series_url = self._find_series_url(key)
        if series_url:
            try:
                obj = resolve_brand_via_series_llm(self.vlm.client, self.vlm.model, series_url)
                if obj.get("brand") and obj.get("model"):
                    out = {"brand": obj["brand"], "model": obj["model"]}
                    self.cache[key] = out
                    self._save()
                    return out
            except Exception:
                pass

        # 1) 既存: Web 参照（汽车之家優先→易车）
        try:
            wb, wurl = resolve_brand_via_web(key)
        except Exception:
            wb, wurl = (None, None)
        if wb:
            out = {"brand": wb, "model": key}
            self.cache[key] = out
            self._save()
            return out

        # 2) 既存: LLMで分解
        bm = self.vlm.split_brand_model_llm(key)
        self.cache[key] = {"brand": bm.get("brand",""), "model": bm.get("model", key)}
        self._save()
        return self.cache[key]

# ----------------------------- MAIN -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-url", required=True)
    ap.add_argument("--tile-height", type=int, default=1200)
    ap.add_argument("--tile-overlap", type=int, default=220)
    ap.add_argument("--out", default="result.csv")
    ap.add_argument("--model", default="gpt-4o")
    ap.add_argument("--openai-api-key", default=os.getenv("OPENAI_API_KEY"))
    ap.add_argument("--fullpage-split", action="store_true")
    args = ap.parse_args()

    # 追加: ランキングHTMLから series URL マップを事前取得
    series_map = collect_series_links_from_rank(args.from_url)

    full_path = grab_fullpage_to(args.from_url, Path("tiles"))
    if args.fullpage_split:
        tile_paths = split_full_image(full_path, Path("tiles"), args.tile_height, args.tile_overlap)
    else:
        tile_paths = [full_path]

    vlm = OpenAIVLM(model=args.model, api_key=args.openai_api_key)
    all_rows: List[List[dict]] = []
    for p in tile_paths:
        data = vlm.infer_table(p)
        rows = normalize_rows(data.get("rows", []))
        print(f"[INFO] {p.name}: {len(rows)} rows")
        all_rows.append(rows)

    merged = merge_dedupe_sort(all_rows)
    resolver = BrandResolver(vlm, series_map=series_map)  # ← 最小追加
    for r in merged:
        bm = resolver.resolve(r["name"])
        r["brand"] = bm.get("brand","")
        r["model"] = bm.get("model", r["name"])
        r.pop("name", None)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["rank_seq","rank","brand","model","count"])
        w.writeheader()
        for r in merged:
            w.writerow(r)

    print(f"[DONE] rows={len(merged)} -> {out}  (cache: data/brand_cache.json)")

if __name__ == "__main__":
    main()
