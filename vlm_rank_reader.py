#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL → フルページスクショ → タイル分割（overlap付き）→ VLMで行抽出 → CSV

ポイント:
- Playwright側でCJKフォントを強制適用 → 豆腐(□)防止
- LLMで brand と model を推定（辞書不要）
- 判定結果を data/brand_cache.json に永続キャッシュ（同じ車名は次回以降無課金・高速）
"""

import os, csv, json, base64, argparse, time
from pathlib import Path
from typing import List, Dict
from PIL import Image
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
from openai import OpenAI

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

# ----------------------------- ブランド分離プロンプト（辞書不要・シリーズ壊さない） -----------------------------
BRAND_PROMPT = """你是中国车系名称解析助手。给定一个“车系/车型名称”或图片片段，请输出对应的【品牌/厂商】与【车型名】。

数据来源与验证顺序（务必遵守）：
1) 必须优先在 汽车之家（autohome.com.cn） 查询并核对：先用“site:autohome.com.cn <车型/车系名>”检索；打开最相关的“车系”或“参数配置/图片/报价”页面。
2) 在页面中寻找「厂商」「品牌」或面包屑位置的“<品牌/厂商>-<车系>”字样（示例：比亚迪-秦PLUS / 上汽通用五菱-宏光MINIEV），据此判定品牌/厂商与车型名。
3) 如在汽车之家未找到完全匹配，请：
   a. 回看输入图片/文本，检查是否有误读（相似名、后缀如“Pro”“MAX”“L”“PLUS”等）。
   b. 结合常见别名/缩写再次在汽车之家检索 1 次。
4) 仍无法在汽车之家确认时，才可短暂参考第二来源（如：易车 yiche.com），但若信息与汽车之家冲突，以汽车之家为准；无法确认则返回“brand":"未知"。

命名规则：
- 不要仅按第一个词硬拆。像“宏光MINIEV”“秦PLUS”“宋PLUS”“汉L”等是完整车系名，不能把“宏光”“秦”“宋”“汉”单独当作品牌。
- 尽可能输出厂商（例如“五菱”“比亚迪”“大众”“吉利”“特斯拉”等）；如果确实没有厂商信息，则输出常用品牌名。
- 车型名请保留完整的车系名（含后缀，如 PLUS / Pro / MAX / L）。

只输出 JSON，结构严格如下（不要多余文字）：
{"brand":"<string>","model":"<string>"}

示例（务必模仿）：
- 输入：宏光MINIEV → {"brand":"五菱","model":"宏光MINIEV"}
- 输入：秦PLUS → {"brand":"比亚迪","model":"秦PLUS"}
- 输入：Model Y → {"brand":"特斯拉","model":"Model Y"}
- 输入：朗逸 → {"brand":"大众","model":"朗逸"}
- 输入：博越L → {"brand":"吉利","model":"博越L"}

"""

# ----------------------------- AutoHome/Yiche 参照（スクレイプ&検索） -----------------------------
import time
from urllib.parse import quote_plus

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
        # unique
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
    m = re.search(r"厂商[:：]\s*([^\s/|>《》\\-—–]+)", text)
    if m: return m.group(1).strip()
    m = re.search(r"品牌[:：]\s*([^\s/|>《》\\-—–]+)", text)
    if m: return m.group(1).strip()
    # パンくず「品牌-车系」
    m = re.search(r"([一-龥A-Za-z0-9]+)\s*[-－—]\s*([一-龥A-Za-z0-9\+\s]+)", text)
    if m: return m.group(1).strip()
    return None

def _extract_brand_yiche(html: str):
    if not html: return None
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"厂商[:：]\s*([^\s/|>《》\\-—–]+)", text)
    if m: return m.group(1).strip()
    m = re.search(r"品牌[:：]\s*([^\s/|>《》\\-—–]+)", text)
    if m: return m.group(1).strip()
    return None

def resolve_brand_via_web(model_name: str):
    \"\"\"汽车之家で確認→無ければ易车。見つかれば (brand, url)、無ければ (None, None)\"\"\"
    # 1) Autohome
    cand = _ddg_search(model_name, site="autohome.com.cn", topk=8)
    au = [u for u in cand if _AUTOHOME_RE.search(u)]
    for u in au[:5]:
        html = _fetch(u)
        b = _extract_brand_autohome(html)
        if b:
            return b, u
    # 2) Yiche fallback
    cand = _ddg_search(model_name, site="yiche.com", topk=6)
    yi = [u for u in cand if _YICHE_RE.search(u)]
    for u in yi[:3]:
        html = _fetch(u)
        b = _extract_brand_yiche(html)
        if b:
            return b, u
    return None, None


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

# ----------------------------- スクショ（HTMLは改変しない / 豆腐防止CSS注入） -----------------------------
def grab_fullpage_to(url: str, out_dir: Path, viewport=(1380, 2400)) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    full_path = out_dir / "full.jpg"

    MAX_RETRY = 3
    for attempt in range(1, MAX_RETRY + 1):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                viewport={"width": viewport[0], "height": viewport[1]},
                device_scale_factor=2,
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="zh-CN",
                extra_http_headers={"Accept-Language": "zh-CN,zh;q=0.9"},
            )
            page = ctx.new_page()

            # 豆腐防止：CJKフォントを強制（Workflow で CJK フォント導入済み前提）
            page.add_init_script("""
              try {
                const style = document.createElement('style');
                style.setAttribute('data-screenshot-font-patch','1');
                style.textContent = `
                  * { font-family:
                      "Noto Sans CJK SC","WenQuanYi Zen Hei","Noto Sans CJK JP",
                      "Noto Sans","Microsoft YaHei","PingFang SC",sans-serif !important; }
                `;
                document.documentElement.appendChild(style);
              } catch(e){}
            """)

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=180000)
                # 緩く本文待ち
                for sel in ["table", ".rank-list", ".content", "body"]:
                    try:
                        page.wait_for_selector(sel, timeout=60000)
                        break
                    except PwTimeout:
                        continue
                # Webフォント準備（可能なら）
                try:
                    page.evaluate("return document.fonts && document.fonts.ready.then(()=>true)")
                except Exception:
                    pass
                page.wait_for_timeout(800)
                try:
                    page.wait_for_load_state("networkidle", timeout=15000)
                except PwTimeout:
                    pass
                page.wait_for_timeout(500)

                page.screenshot(path=full_path, full_page=True, type="jpeg", quality=90)
                browser.close()
                return full_path

            except PwTimeout:
                browser.close()
                if attempt == MAX_RETRY:
                    raise
                time.sleep(1.5 * attempt)
                continue

# ----------------------------- OpenAI クライアント -----------------------------
class OpenAIVLM:
    def __init__(self, model: str, api_key: str | None):
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY 未設定")
        self.client = OpenAI(api_key=api_key)
        self.model = model

    def infer_table(self, image_path: Path) -> dict:
        b64 = base64.b64encode(image_path.read_bytes()).decode("ascii")
        resp = self.client.chat.completions.create(
            model=self.model,  # 例: gpt-4o
            temperature=0,
            max_tokens=1200,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": [
                    {"type":"text","text":USER_PROMPT},
                    {"type":"image_url","image_url":{"url":f"data:image/jpeg;base64,{b64}"}}
                ]},
            ],
            response_format={"type": "json_object"},
        )
        try:
            return json.loads(resp.choices[0].message.content)
        except Exception:
            return {"rows":[]}

    def split_brand_model_llm(self, name: str) -> dict:
        prompt = BRAND_PROMPT + f"\n待解析：{name}\n只输出JSON。"
        resp = self.client.chat.completions.create(
            model=self.model,   # 同じモデルでOK（テキストのみ）
            temperature=0,
            max_tokens=200,
            messages=[
                {"role":"system","content":"你是品牌/厂商与车系名识别助手，只输出JSON。"},
                {"role":"user","content":prompt},
            ],
            response_format={"type":"json_object"},
        )
        try:
            data = json.loads(resp.choices[0].message.content)
            brand = (data.get("brand") or "").strip()
            model = (data.get("model") or "").strip() or name
            return {"brand": brand, "model": model}
        except Exception:
            return {"brand":"", "model":name}

# ----------------------------- 正規化 & 重複排除 -----------------------------
def normalize_rows(rows_in: List[dict]) -> List[dict]:
    out = []
    for r in rows_in:
        name = (r.get("name") or "").strip()
        if not name:
            continue
        rank = r.get("rank")
        cnt = r.get("count")
        if isinstance(cnt, str):
            t = cnt.replace(",", "").replace(" ", "")
            cnt = int(t) if t.isdigit() else None
        out.append({"rank": rank, "name": name, "count": cnt})
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

# ----------------------------- ブランド解決（LLM + 永続キャッシュ） -----------------------------
class BrandResolver:
    def __init__(self, vlm: OpenAIVLM, cache_path: Path = Path("data/brand_cache.json")):
        self.vlm = vlm
        self.cache_path = cache_path
        self.cache: Dict[str, Dict[str,str]] = {}
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

    def resolve(self, raw_name: str) -> Dict[str,str]:
        key = (raw_name or "").strip()
        if not key:
            return {"brand":"", "model":raw_name}

        # キャッシュあれば即返す
        hit = self.cache.get(key)
        if hit and isinstance(hit, dict) and "brand" in hit and "model" in hit:
            return hit
        # まずは Web 参照（汽车之家優先→易车）で決定できるか試す
        try:
            wb, wurl = resolve_brand_via_web(key)
        except Exception:
            wb, wurl = (None, None)

        if wb:
            out = {"brand": wb, "model": key}
            self.cache[key] = out
            self._save()
            return out


        # LLMで分解（辞書不要）
        bm = self.vlm.split_brand_model_llm(key)
        # キャッシュ保存
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
    ap.add_argument("--model", default="gpt-4o")  # 既定は gpt-4o。必要なら引数で変更
    ap.add_argument("--openai-api-key", default=os.getenv("OPENAI_API_KEY"))
    ap.add_argument("--fullpage-split", action="store_true")
    args = ap.parse_args()

    # 1) フルページキャプチャ
    full_path = grab_fullpage_to(args.from_url, Path("tiles"))

    # 2) 分割
    if args.fullpage_split:
        tile_paths = split_full_image(full_path, Path("tiles"), args.tile_height, args.tile_overlap)
    else:
        tile_paths = [full_path]

    # 3) VLM読み取り
    vlm = OpenAIVLM(model=args.model, api_key=args.openai_api_key)
    all_rows: List[List[dict]] = []
    for p in tile_paths:
        data = vlm.infer_table(p)
        rows = normalize_rows(data.get("rows", []))
        print(f"[INFO] {p.name}: {len(rows)} rows")
        all_rows.append(rows)

    # 4) マージ & ブランド分離（LLM＋キャッシュ）
    merged = merge_dedupe_sort(all_rows)
    resolver = BrandResolver(vlm)
    for r in merged:
        bm = resolver.resolve(r["name"])
        r["brand"] = bm.get("brand","")
        r["model"] = bm.get("model", r["name"])
        r.pop("name", None)  # CSVの項目に合わせる

    # 5) CSV出力
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

