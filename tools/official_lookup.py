# tools/official_lookup.py
# 公式サイト（Google CSE）を使ってブランド/モデルの英名を推定する最小実装
# - 環境変数: GOOGLE_API_KEY, GOOGLE_CSE_ID を使用
# - CSE 側では公式ドメインのみ登録済みを想定
# - ここではキャッシュは一切使いません（リクエスト→即判定）

from __future__ import annotations
import os
import re
import time
import html
import json
import logging
from typing import List, Dict, Optional
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID", "")

# ====== 公式ドメイン（あなたのCSEに既に登録済み想定） ======
OFFICIAL_SITES = [
    # BYD
    "byd.com", "bydauto.com.cn", "ocean.byd.com",
    # Geely / Geely Galaxy
    "geely.com", "galaxy.geely.com",
    # Wuling / SGMW
    "sgmw.com.cn", "wuling-global.com",
    # Volkswagen (China joint ventures)
    "vw.com.cn", "saicvolkswagen.com.cn", "faw-vw.com",
    # Toyota
    "toyota.com.cn", "toyota-global.com",
    # Nissan
    "nissan.com.cn", "nissan-global.com",
    # Chery
    "chery.cn", "cheryinternational.com",
    # Changan (+ Qiyuan)
    "changan.com.cn", "changan.com.cn/qiyuan",
    # Haval / GWM
    "haval.com.cn", "gwm-global.com",
    # XPeng / Xiaomi / AITO / Honda / Audi / Buick / Mercedes / BMW / Hongqi
    "xpeng.com", "auto.mi.com", "aito.auto", "honda.com.cn", "audi.cn", "buick.com.cn",
    "mercedes-benz.com.cn", "bmw.com.cn", "hongqi-auto.com",
    # Leapmotor
    "leapmotor.com",
]

# ====== ブランドの最小正規化（辞書はモデルではなく“ブランドのみ”） ======
_BRAND_MAP = {
    "比亚迪": "BYD", "比亞迪": "BYD",
    "吉利": "Geely", "吉利汽车": "Geely", "吉利汽車": "Geely",
    "吉利银河": "Geely Galaxy",
    "五菱汽车": "Wuling", "五菱": "Wuling",
    "大众": "Volkswagen", "大眾": "Volkswagen",
    "丰田": "Toyota", "豐田": "Toyota",
    "本田": "Honda",
    "日产": "Nissan", "日産": "Nissan",
    "奔驰": "Mercedes-Benz",
    "宝马": "BMW",
    "奥迪": "Audi",
    "别克": "Buick",
    "红旗": "Hongqi", "紅旗": "Hongqi",
    "奇瑞": "Chery",
    "长安": "Changan", "長安": "Changan",
    "长安启源": "Changan Qiyuan",
    "哈弗": "Haval",
    "零跑汽车": "Leapmotor", "零跑": "Leapmotor",
    "小鹏": "XPeng",
    "小米汽车": "Xiaomi Auto", "小米": "Xiaomi Auto",
    "AITO": "AITO",
    "特斯拉": "Tesla",
}

# ====== よく出るモデルの軽い正規化（辞書依存はせず“ゆれ直し”のみ） ======
_RE_MODEL_FIXUPS = [
    (re.compile(r"(?:海狮|海獅)\s*0?\s*6(?:\s*新\s*能\s*源)?", re.I), "Sealion 06"),
    (re.compile(r"(?:海豹)\s*0?\s*6(?:\s*新\s*能\s*源)?", re.I), "Seal 06"),
    (re.compile(r"(?:海豹)\s*0?\s*5(?:.*DM\-?i)?", re.I), "Seal 05 DM-i"),
    (re.compile(r"(?:星越L|星越\s*L)", re.I), "Xingyue L"),
    (re.compile(r"(?:朗逸)", re.I), "Lavida"),
    (re.compile(r"(?:速腾|速騰)", re.I), "Sagitar"),
    (re.compile(r"(?:迈腾|邁騰)", re.I), "Magotan"),
    (re.compile(r"(?:途观L|途觀L)", re.I), "Tiguan L"),
    (re.compile(r"(?:途岳)", re.I), "Tharu"),
    (re.compile(r"(?:探岳)", re.I), "Tayron"),
    (re.compile(r"(?:瑞虎)\s*8", re.I), "Tiggo 8"),
    (re.compile(r"(?:逸动|逸動)", re.I), "Eado"),
    (re.compile(r"(?:元)\s*PLUS", re.I), "Yuan PLUS"),
    (re.compile(r"(?:元)\s*UP", re.I), "Yuan UP"),
    (re.compile(r"(?:海鸥|海鷗)", re.I), "Seagull"),
    (re.compile(r"(?:宏光)\s*MINI\s*EV|宏光MINIEV", re.I), "Hongguang MINIEV"),
    (re.compile(r"(?:缤果|繽果)", re.I), "Binguo"),
    (re.compile(r"(?:缤越|繽越)", re.I), "Binyue"),
    (re.compile(r"(?:问界)\s*M8", re.I), "Wenjie M8"),
    (re.compile(r"(?:秦)\s*PLUS", re.I), "Qin PLUS"),
    (re.compile(r"(?:宋)\s*PLUS", re.I), "Song PLUS"),
    (re.compile(r"(?:宋)\s*Pro", re.I), "Song Pro"),
    (re.compile(r"(?:博越)\s*L", re.I), "Boyue L"),
]

_ASCII_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 .\-+/&]*$")

def _is_clean_ascii_name(s: str) -> bool:
    if not s: return False
    s = s.strip()
    if len(s) > 40: return False
    # 日本語/中国語が混じるならNG
    if re.search(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff]", s):
        return False
    return bool(_ASCII_NAME.fullmatch(s))

def _loose_model_fixup(s: str) -> str:
    if not s: return s
    s = s.strip()
    for rx, repl in _RE_MODEL_FIXUPS:
        s2 = rx.sub(repl, s)
        if s2 != s:
            s = s2
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def _google_cse(query: str, sleep_sec: float = 0.1) -> List[Dict]:
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        return []
    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": GOOGLE_API_KEY, "cx": GOOGLE_CSE_ID, "q": query}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            logger.warning("CSE non-200: %s %s", r.status_code, r.text[:200])
            return []
        data = r.json()
        items = data.get("items", []) or []
        # 公式サイト以外が混じるCSE設定の場合に備えて最終フィルタ
        filtered = []
        for it in items:
            link = it.get("link", "")
            if any(d in link for d in OFFICIAL_SITES):
                filtered.append(it)
        time.sleep(sleep_sec)
        return filtered or items  # 公式が0なら元のitemsを返す（保険）
    except Exception as e:
        logger.exception("CSE error: %s", e)
        return []

def _pick_title_name(title: str) -> Optional[str]:
    """
    HTMLタイトルから製品名らしい部分を抽出（A-Z始まりの語＋数字パターンを優先）
    例: "BYD Sealion 06 – Specifications | BYD" -> "Sealion 06"
    """
    if not title:
        return None
    t = html.unescape(title)
    # ハイフンや縦棒で前半が商品名、後半がサイト/説明の想定
    t = re.split(r"\s+[|\-–—]\s+", t)[0]
    # 2語または1語＋数字の固有名を優先
    m = re.search(r"\b([A-Z][A-Za-z]+(?:\s+[A-Z0-9][A-Za-z0-9\-]+)*)\b", t)
    if m:
        return m.group(1).strip()
    return None

def official_brand_name_if_needed(brand_raw: str, current: str) -> str:
    """
    ブランド英名：すでに綺麗な英名ならcurrentを返す。
    そうでなければ _BRAND_MAP で最小正規化し、
    まだ不明ならCSEでトップのサイトタイトルから英名風トークンを抽出。
    """
    cur = (current or "").strip()
    if _is_clean_ascii_name(cur):
        return cur

    # まずは最小辞書（ブランドのみ）
    br = (brand_raw or "").strip()
    if br in _BRAND_MAP:
        return _BRAND_MAP[br]

    # CSEに当てる（ブランド単体）
    q = f"{brand_raw}"
    items = _google_cse(q)
    for it in items:
        t = it.get("title", "") or ""
        name = _pick_title_name(t)
        if name and _is_clean_ascii_name(name):
            return name
    # ダメなら current を英名っぽくトリムして返す
    # （中国語が混じる場合はそのまま brand_raw で返す）
    head = re.split(r"[、，。,(（,]\s*|\s{2,}", cur or br, maxsplit=1)[0].strip()
    return head

def official_model_name_if_needed(brand_en: str, model_raw: str, current: str) -> str:
    """
    モデル英名：currentをまず軽く正規化→公式CSEから上書き候補があれば採用。
    """
    cur = _loose_model_fixup((current or "").strip())
    if _is_clean_ascii_name(cur):
        # すでに十分きれいならそのまま
        candidate = cur
    else:
        candidate = cur

    # CSE 検索：ブランド＋中国語モデル名（原語が強い）
    q = f"{brand_en} {model_raw}"
    items = _google_cse(q)
    for it in items:
        title = it.get("title", "") or ""
        name = _pick_title_name(title)
        name = _loose_model_fixup(name or "")
        if _is_clean_ascii_name(name):
            # 変に長い・説明臭いタイトルは避ける
            if len(name) <= max(10, len(candidate) + 6):
                return name

    # それでもダメなら候補（直したcurrent）を返す
    return candidate
