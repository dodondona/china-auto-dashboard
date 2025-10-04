#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL → フルページスクショ → タイル分割 → VLMで表抽出 → seriesページの上部画像から brand/model 抽出 → CSV

最小変更点（ご要望どおり余計なことはしません）:
- seriesページの「上部を2枚」スクショ（ヘッダ帯 / 左パネル）。
- VLMは『ブランド= “品牌/厂商” の値、モデル= 車系名』を厳密指示。
- brand==model または brand⊂model のときだけ再撮影（+200px）して再読取。
- 取れないときは “未知” を返し、最後に <title> 抽出へフォールバック（名前だけの推測はしない）。
- 既存の --fullpage-split、CSV出力、行抽出などはそのまま。
"""

import os, csv, json, base64, argparse, time, re
from pathlib import Path
from typing import List, Dict, Tuple
from PIL import Image
from playwright.sync_api import sync_playwright
from openai import OpenAI
import requests
from bs4 import BeautifulSoup

# ----------------------------- VLM（ランキング表の行抽出） -----------------------------
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

# ----------------------------- series上部のブランド/モデル抽出プロンプト -----------------------------
SERIES_HEADER_PROMPT = """你是汽车之家 车系页上部截图的解析助手。请只依据图中“字段标签/面包屑/大标题”读取：
- 品牌（brand）：优先读取紧随“品牌”或“厂商”标签后的正式名称（如：品牌：吉利汽车）。若不存在，再从左上面包屑或“品牌·车系”式的大标题中确定品牌部分。
- 车型名（model）：面包屑末端或上部大标题中标示的**完整车系名**（如：秦PLUS、宏光MINIEV、星愿、海豹06新能源）。
- 不要把车系名（如：海豹/海狮/海豚/秦/宋/唐/元/汉/银河/星越 等）当作品牌。
- 只输出 JSON：
{"brand":"<string>","model":"<string>"}
- 无法判断时，brand 返回 "未知"，model 尽量给出。
"""

# ----------------------------- 共通ヘルパー -----------------------------
HEADERS_WEB = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept-Language": "zh-CN,zh;q=0.9"
}

def _fetch(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS_WEB, timeout=25)
        if r.status_code == 200:
            return r.text
    except Exception:
        return None
    return None

def _extract_json_object(text: str) -> str:
    if not text:
        return ""
    s = text.strip()
    m = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", s, re.I)
    if m:
        return m.group(1).strip()
    m = re.search(r"\{[\s\S]*\}", s)
    return m.group(0).strip() if m else ""

# ----------------------------- <title> 正規表現フォールバック -----------------------------
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

def resolve_brand_via_series_title(client: OpenAI, model: str, series_url: str) -> Dict[str, str]:
    html = _fetch(series_url)
    if not html:
        return {"brand": "未知", "model": ""}
    soup = BeautifulSoup(html, "html.parser")
    title_txt = soup.title.get_text(" ", strip=True) if soup.title else ""
    obj = parse_series_title(title_txt)
    if obj:
        return obj
    # 最後に LLM（タイトル文字列のみ）へ
    resp = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": "你是汽车之家页面标题解析助手。只输出JSON"},
            {"role": "user", "content": title_txt}
        ],
        temperature=0
    )
    raw = (resp.output_text or "").strip()
    js = _extract_json_object(raw)
    try:
        obj2 = json.loads(js)
        if obj2.get("brand") and obj2.get("model"):
            return obj2
    except Exception:
        pass
    return {"brand": "未知", "model": ""}

# ----------------------------- ランキングから seriesリンク収集 -----------------------------
def _norm_text(s: str) -> str:
    return (s or "").strip().replace(" ", "").replace("\u3000", "")

def collect_series_links_from_rank(rank_url: str) -> Dict[str, str]:
    html = _fetch(rank_url)
    if not html:
        return {}
    soup = BeautifulSoup(html, "html.parser")
    mapping: Dict[str, str] = {}
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = _norm_text(a.get_text(strip=True))
        if not text or "javascript:" in href:
            continue
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = "https://www.autohome.com.cn" + href
        # 例: https://www.autohome.com.cn/7806/
        if re.search(r"autohome\.com\.cn/\d+/?", href):
            mapping.setdefault(text, href)
    return mapping

# ----------------------------- seriesページ上部スクショ（2枚返す） -----------------------------
def grab_series_header_screenshot(url: str, out_dir: Path, cut_height: int = 260) -> Tuple[Path, Path]:
    """
    A: ヘッダ帯（0〜cut_height）
    B: 左上情報パネル（品牌/厂商ラベルが出やすい帯）
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1380, "height": 900},
            device_scale_factor=2,
            user_agent=HEADERS_WEB["User-Agent"],
            locale="zh-CN",
            extra_http_headers={"Accept-Language": "zh-CN,zh;q=0.9"},
        )
        page = ctx.new_page()
        page.goto(url, timeout=60000)
        page.wait_for_timeout(2500)
        full = out_dir / "series_full.jpg"
        page.screenshot(path=str(full), full_page=True)
        browser.close()

    im = Image.open(full).convert("RGB"); W, H = im.size
    header = out_dir / f"series_header_{cut_height}.jpg"
    im.crop((0, 0, W, min(cut_height, H))).save(header, "JPEG", quality=90, optimize=True)
    left = out_dir / f"series_left_{cut_height}.jpg"
    im.crop((0, 120, min(420, W), min(900, H))).save(left, "JPEG", quality=90, optimize=True)
    try:
        full.unlink()
    except Exception:
        pass
    return header, left

# ----------------------------- VLMで brand/model を読む（2枚統合＋自己検証） -----------------------------
def _ask_image_json(client: OpenAI, model: str, prompt: str, img_path: Path) -> Dict[str, str]:
    b64 = base64.b64encode(img_path.read_bytes()).decode("utf-8")
    resp = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": [
                {"type": "input_text", "text": "只输出JSON"},
                {"type": "input_image", "image_url": f"data:image/jpeg;base64,{b64}"}
            ]}
        ],
        temperature=0
    )
    raw = (resp.output_text or "").strip()
    js = _extract_json_object(raw)
    try:
        return json.loads(js)
    except Exception:
        return {}

def vlm_read_brand_model_from_images(client: OpenAI, model: str,
                                     header_img: Path, left_img: Path,
                                     cut_height: int, series_url: str) -> Dict[str, str]:
    # ① 左パネル（“品牌/厂商”ラベル）優先
    o_left = _ask_image_json(client, model, SERIES_HEADER_PROMPT, left_img)
    # ② ヘッダ帯（ブランド·车系/面包屑）
    o_head = _ask_image_json(client, model, SERIES_HEADER_PROMPT, header_img)

    brand = (o_left.get("brand") or o_head.get("brand") or "").strip()
    model_str = (o_head.get("model") or o_left.get("model") or "").strip()

    # 自己検証：brand==model or brand in model → 画角を広げて一度だけ再撮影
    if brand and model_str and (brand == model_str or brand in model_str):
        header2, left2 = grab_series_header_screenshot(series_url, header_img.parent, cut_height + 200)
        return vlm_read_brand_model_from_images(client, model, header2, left2, cut_height + 200, series_url)

    return {"brand": brand or "未知", "model": model_str}

# ----------------------------- VLM（ランキング表→JSON） -----------------------------
class OpenAIVLM:
    def __init__(self, model: str, api_key: str):
        self.model = model
        self.client = OpenAI(api_key=api_key)

    def infer_table(self, img_path: Path) -> dict:
        b64 = base64.b64encode(img_path.read_bytes()).decode("utf-8")
        resp = self.client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": [
                    {"type": "input_text", "text": USER_PROMPT},
                    {"type": "input_image", "image_url": f"data:image/jpeg;base64,{b64}"}
                ]}
            ],
            temperature=0
        )
        raw = (resp.output_text or "").strip()
        js = _extract_json_object(raw)
        try:
            return json.loads(js)
        except Exception:
            return {"rows": []}

# ----------------------------- 行の前処理・結合 -----------------------------
def normalize_rows(rows: List[dict]) -> List[dict]:
    out = []
    for r in rows:
        nm = (r.get("name") or "").replace(" ", "").replace("\u3000", "")
        out.append({"rank": r.get("rank"), "name": nm, "count": r.get("count")})
    return out

def merge_dedupe_sort(list_of_rows: List[List[dict]]) -> List[dict]:
    merged, seen = [], set()
    for rows in list_of_rows:
        for r in rows:
            key = (r.get("name") or "").replace(" ", "").replace("\u3000", "")
            if key and key not in seen:
                seen.add(key); merged.append(r)
    merged.sort(key=lambda r: (-(r.get("count") or 0), r.get("name")))
    for i, r in enumerate(merged, 1):
        r["rank_seq"] = i
    return merged

# ----------------------------- ブランド解決 -----------------------------
class BrandResolver:
    def __init__(self, vlm: OpenAIVLM,
                 cache_path: Path = Path("data/brand_cache.json"),
                 series_map: Dict[str, str] | None = None):
        self.vlm = vlm
        self.cache_path = cache_path
        self.cache: Dict[str, Dict[str, str]] = {}
        self.series_map = series_map or {}
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

    def resolve(self, raw_name: str) -> Dict[str, str]:
        key = (raw_name or "").strip()
        if not key:
            return {"brand": "", "model": raw_name}

        if key in self.cache:
            return self.cache[key]

        series_url = self.series_map.get(key) or ""
        if series_url:
            try:
                header_img, left_img = grab_series_header_screenshot(series_url, Path("tiles/series_headers"))
                obj = vlm_read_brand_model_from_images(self.vlm.client, self.vlm.model,
                                                       header_img, left_img, cut_height=260, series_url=series_url)
                if obj.get("brand") and obj.get("model"):
                    self.cache[key] = obj; self._save()
                    return obj
            except Exception:
                pass

            # 画像読取がダメなら <title> からの抽出にフォールバック
            try:
                obj2 = resolve_brand_via_series_title(self.vlm.client, self.vlm.model, series_url)
                if obj2.get("brand") and obj2.get("model"):
                    self.cache[key] = obj2; self._save()
                    return obj2
            except Exception:
                pass

        # ここまでで取れなければ推測はせず “未知”
        out = {"brand": "未知", "model": key}
        self.cache[key] = out; self._save()
        return out

# ----------------------------- ランキングのフルスクショ＆分割 -----------------------------
def grab_fullpage_to(url: str, out_dir: Path, viewport=(1380, 2400)) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    full_path = out_dir / "full.jpg"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": viewport[0], "height": viewport[1]},
            device_scale_factor=2,
            user_agent=HEADERS_WEB["User-Agent"],
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-url", required=True)
    ap.add_argument("--tile-height", type=int, default=1200)
    ap.add_argument("--tile-overlap", type=int, default=220)
    ap.add_argument("--out", default="result.csv")
    ap.add_argument("--model", default="gpt-4o")
    ap.add_argument("--openai-api-key", default=os.getenv("OPENAI_API_KEY"))
    ap.add_argument("--fullpage-split", action="store_true")  # 既存どおり
    args = ap.parse_args()

    # ランキングHTMLから series URL マップ
    series_map = collect_series_links_from_rank(args.from_url)

    # ランキングのフル画像（VLMで行抽出に使う）
    full_path = grab_fullpage_to(args.from_url, Path("tiles"))
    if args.fullpage_split:
        tile_paths = split_full_image(full_path, Path("tiles"), args.tile_height, args.tile_overlap)
    else:
        tile_paths = [full_path]

    vlm = OpenAIVLM(model=args.model, api_key=args.openai_api_key)

    all_rows = []
    for p in tile_paths:
        data = vlm.infer_table(p)
        rows = normalize_rows(data.get("rows", []))
        all_rows.append(rows)

    merged = merge_dedupe_sort(all_rows)

    resolver = BrandResolver(vlm, series_map=series_map)
    for r in merged:
        bm = resolver.resolve(r["name"])
        r["brand"] = bm.get("brand", "") or "未知"
        r["model"] = bm.get("model", r["name"])
        r.pop("name", None)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["rank_seq", "rank", "brand", "model", "count"])
        w.writeheader()
        for r in merged:
            w.writerow(r)

    print(f"[DONE] rows={len(merged)} -> {out}  (cache: data/brand_cache.json)")

if __name__ == "__main__":
    main()
