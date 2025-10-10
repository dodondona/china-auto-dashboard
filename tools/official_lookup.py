# tools/official_lookup.py
# -*- coding: utf-8 -*-
"""
公式サイト(メーカー直営ドメイン)を優先してモデル英字名を取得する最小実装。
- Google Programmable Search Engine (Custom Search JSON API) を利用
- ページから JSON-LD / OpenGraph / H1 / <title> を順に抽出し、スコアリングして最良候補を採用
- 依存: requests, beautifulsoup4, lxml, regex(標準reでもOK・ここではre)

環境変数:
  GOOGLE_API_KEY : Custom Search API の APIキー
  GOOGLE_CSE_ID  : CSE 検索エンジンID (cx)
"""

from __future__ import annotations
import os, re, json, time
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# ------------------------------------------------------------
# 公式ドメイン（CSE側でもホワイトリスト化している前提。ここでも軽く確認）
# 必要に応じて増やせますが、最小限に留めています
# ------------------------------------------------------------
OFFICIAL_DOMAINS = {
    # BYD
    "byd.com", "bydauto.com.cn",
    # Toyota
    "toyota.com.cn", "toyota-global.com",
    # Nissan
    "nissan.com.cn", "nissan-global.com",
    # Volkswagen (一汽/上汽 含む)
    "vw.com.cn", "saicvolkswagen.com.cn", "fawev.com", "faw-vw.com",
    # Geely / Galaxy
    "geely.com", "galaxy.geely.com",
    # Wuling (SGMW)
    "sgmw.com.cn", "wuling-global.com",
    # Chery / Haval / Hongqi / Leapmotor / AITO / Xiaomi / XPeng
    "chery.cn", "haval.com.cn", "hongqi-auto.com", "leapmotor.com",
    "aito.auto", "auto.xiaomi.com", "xpeng.com",
    # BMW / Mercedes / Audi / Buick / Honda
    "bmw.com.cn", "mercedes-benz.com.cn", "audi.com.cn", "buick.com.cn", "honda.com.cn",
}

# ノイズ語（タイトルなどから除去）
NOISE_WORDS = [
    r"Official\s*Site", r"Official", r"官网", r"首页", r"报价", r"参数", r"配置", r"车型",
    r"价格", r"预约", r"预售", r"新闻", r"活动", r"试驾", r"Overview", r"Price", r"Specs",
    r"全新", r"上市", r"焕新", r"发布", r"了解更多", r"立即", r"预约试驾",
]

# 自動車以外の誤爆抑制
BAD_HINTS = [
    "歌词", "楽曲", "歌曲", "专辑", "アルバム", "映画", "ドラマ", "预告", "OST", "周边",
    "disambiguation", "曖昧さ回避",
]

UA = "china-auto-dashboard/1.0 (+https://github.com/dodondona/china-auto-dashboard)"


def _http_get(url: str, timeout: float = 12.0) -> str | None:
    try:
        resp = requests.get(url, timeout=timeout, headers={"User-Agent": UA})
        if resp.status_code == 200 and resp.text:
            return resp.text
    except Exception:
        return None
    return None


def _domain_ok(url: str) -> bool:
    try:
        netloc = urlparse(url).netloc.lower()
        # サブドメインを含めて official ドメインに一致するか
        return any(netloc.endswith(d) for d in OFFICIAL_DOMAINS)
    except Exception:
        return False


def _cleanup_name(name: str) -> str | None:
    if not name:
        return None
    s = re.sub(r"\s+", " ", str(name)).strip()

    # ノイズ語を末尾から削る（過剰削除を防ぐため弱めのルール）
    for w in NOISE_WORDS:
        s = re.sub(rf"\b{w}\b", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s{2,}", " ", s).strip()
    # ニセの括弧書き/曖昧文字を落とす
    if any(bad.lower() in s.lower() for bad in BAD_HINTS):
        return None
    # タイトルに「|」「-」区切りがある場合、先頭側を優先
    if " | " in s:
        s = s.split(" | ")[0].strip()
    if " - " in s and len(s.split(" - ")[0]) >= 3:
        s = s.split(" - ")[0].strip()
    return s or None


def _best_name_from_html(html: str) -> dict:
    """
    HTMLから候補名を抽出。優先度:
    JSON-LD.name > og:title > h1 > <title>
    返り値: {"jsonld_name": [...], "og_title": "...", "h1": "...", "title": "..."}
    """
    out = {"jsonld_name": [], "og_title": None, "h1": None, "title": None}
    if not html:
        return out
    soup = BeautifulSoup(html, "lxml")

    # JSON-LD
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        # @graph or list で来るケースもある
        def _collect(d):
            if not isinstance(d, dict): return
            t = (d.get("@type") or d.get("type") or "")
            # Vehicle/Product/Article など。name を候補に。
            if isinstance(t, list):
                t = " ".join(t)
            nm = d.get("name") or d.get("headline")
            if nm:
                cname = _cleanup_name(nm)
                if cname:
                    out["jsonld_name"].append(cname)
            for k in ("item", "mainEntityOfPage", "brand"):
                if isinstance(d.get(k), dict):
                    _collect(d[k])
            if isinstance(d.get("@graph"), list):
                for x in d["@graph"]:
                    if isinstance(x, dict):
                        _collect(x)
        if isinstance(data, list):
            for it in data:
                if isinstance(it, dict): _collect(it)
        elif isinstance(data, dict):
            _collect(data)

    # OpenGraph
    og = soup.find("meta", attrs={"property": "og:title"}) or soup.find("meta", attrs={"name": "og:title"})
    if og and og.get("content"):
        out["og_title"] = _cleanup_name(og["content"])

    # H1
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        out["h1"] = _cleanup_name(h1.get_text(" ", strip=True))

    # <title>
    if soup.title and soup.title.string:
        out["title"] = _cleanup_name(soup.title.string)

    return out


def _score_candidate(url: str, data: dict, brand_zh: str, model_zh: str, picked: str | None) -> int:
    """
    候補ページの信頼度スコア。
    """
    score = 0
    if _domain_ok(url):
        score += 40
    # JSON-LD が豊富なら加点
    if data.get("jsonld_name"):
        score += 20
    # og/h1/title がそれらしい
    for k in ("og_title", "h1", "title"):
        if data.get(k):
            score += 5

    # URL/タイトルにモデル語を含むと加点
    u = url.lower()
    if str(model_zh):
        mz = str(model_zh).lower()
        if mz in u:
            score += 10
    if picked:
        p = picked.lower()
        if any(w in p for w in [str(model_zh).lower()]):
            score += 5

    # ニュース/リスト/販売店は減点
    if any(seg in u for seg in ["/news", "/dealer", "/list", "/download", "/join", "/jobs", "/about"]):
        score -= 15

    return score


def _pick_name(data: dict) -> str | None:
    """
    抽出した構造化情報から最も良い名前を1つ返す。
    """
    # JSON-LD に良いのがあれば最優先（複数あれば先頭）
    if data.get("jsonld_name"):
        return data["jsonld_name"][0]
    # 以降は順に
    for k in ("og_title", "h1", "title"):
        if data.get(k):
            return data[k]
    return None


def _normalize_known_patterns(brand_zh: str, model_en: str) -> str:
    """
    ブランド別の軽い整形（辞書化はしない、最小限）。
    """
    s = model_en.strip()

    # Tesla: "Model y" -> "Model Y"
    if brand_zh in {"特斯拉", "Tesla", "テスラ"}:
        s = re.sub(r"\bModel\s*([3ysx])\b", lambda m: f"Model {m.group(1).upper()}", s, flags=re.I)

    # BYD 海洋系列: Seagull/Dolphin/Seal/Sealion + number
    s = re.sub(r"\bsealion\s*0*6\b", "Sealion 06", s, flags=re.I)
    s = re.sub(r"\bseal\s*0*6\b", "Seal 06", s, flags=re.I)
    s = re.sub(r"\bseal\s*0*5\b", "Seal 05", s, flags=re.I)

    # VW China 固有の英名をそのまま
    s = re.sub(r"\bsagitar\b", "Sagitar", s, flags=re.I)
    s = re.sub(r"\bmagotan\b", "Magotan", s, flags=re.I)
    s = re.sub(r"\btayron\b", "Tayron", s, flags=re.I)
    s = re.sub(r"\btharu\b", "Tharu", s, flags=re.I)
    s = re.sub(r"\blavida\b", "Lavida", s, flags=re.I)
    s = re.sub(r"\btiguan\s*l\b", "Tiguan L", s, flags=re.I)

    # Toyota China
    s = re.sub(r"\bfrontlander\b", "Frontlander", s, flags=re.I)
    s = re.sub(r"\bcorolla\s*cross\b", "Corolla Cross", s, flags=re.I)
    s = re.sub(r"\bcamry\b", "Camry", s, flags=re.I)
    s = re.sub(r"\brav4\b", "RAV4", s, flags=re.I)

    # Chery / Changan
    s = re.sub(r"\btiggo\s*8\b", "Tiggo 8", s, flags=re.I)
    s = re.sub(r"\barrizo\s*8\b", "Arrizo 8", s, flags=re.I)  # 誤綴り救済
    s = re.sub(r"\beado\b", "Eado", s, flags=re.I)

    return s.strip()


def _build_query(brand_zh: str, model_zh: str) -> str:
    """
    CSEに渡すクエリ文字列（CSE自体は既に公式サイト限定で構成している想定）。
    """
    terms = []
    if model_zh:
        terms.append(f"\"{model_zh}\"")
    # 既知の英字候補も併記（軽い網を張る）
    # 例: 海狮06 → "Sealion 06", 海豹06 → "Seal 06", 速腾 → "Sagitar", 锋兰达 → "Frontlander" 等
    hint_map = {
        "海狮": "Sealion",
        "海豹": "Seal",
        "海豚": "Dolphin",
        "海鸥": "Seagull",
        "速腾": "Sagitar",
        "迈腾": "Magotan",
        "探岳": "Tayron",
        "途岳": "Tharu",
        "朗逸": "Lavida",
        "锋兰达": "Frontlander",
        "卡罗拉锐放": "Corolla Cross",
        "瑞虎": "Tiggo",
        "艾瑞泽": "Arrizo",
        "逸动": "Eado",
        "缤越": "Binyue",
        "星越": "Xingyue",
        "博越": "Boyue",
        "元PLUS": "Yuan PLUS",
    }
    for zh, en in hint_map.items():
        if zh in str(model_zh):
            terms.append(f"\"{en}\"")
    # ブランド名はそのまま（CSE側がサイトを限定）
    if brand_zh:
        terms.append(f"\"{brand_zh}\"")
    return " ".join(terms)


def _cse_search(query: str, num: int = 6) -> list[dict]:
    key = os.getenv("GOOGLE_API_KEY")
    cx  = os.getenv("GOOGLE_CSE_ID")
    if not key or not cx:
        return []
    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": key, "cx": cx, "q": query, "num": max(1, min(num, 10))}
    try:
        r = requests.get(url, params=params, timeout=12, headers={"User-Agent": UA})
        if r.status_code != 200:
            return []
        data = r.json()
        return data.get("items", []) or []
    except Exception:
        return []


def find_official_english(brand_zh: str, model_zh: str, sleep_sec: float = 0.25) -> str | None:
    """
    公開APIとページ解析を使い、公式サイトからモデルの英字名を推定して返す。
    返値が None のときは公式からは確定できなかったことを意味する。
    """
    query = _build_query(brand_zh, model_zh)
    items = _cse_search(query, num=8)
    best = None
    best_score = -10**9

    for it in items:
        link = it.get("link") or ""
        if not link or not _domain_ok(link):
            continue

        html = _http_get(link)
        if not html:
            continue

        data = _best_name_from_html(html)
        picked = _pick_name(data)
        picked = _cleanup_name(picked) if picked else None
        if not picked:
            continue

        picked = _normalize_known_patterns(brand_zh, picked)

        sc = _score_candidate(link, data, brand_zh, model_zh, picked)
        if sc > best_score:
            best_score = sc
            best = picked

        # ページ間の連打回避
        time.sleep(sleep_sec)

    return best
