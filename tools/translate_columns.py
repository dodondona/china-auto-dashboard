from __future__ import annotations
import os, json, time, re
from pathlib import Path
import pandas as pd
from openai import OpenAI

# ====== 入出力 ======
SERIES_ID = os.environ.get("SERIES_ID", "").strip()

def resolve_src_dst():
    csv_in  = os.environ.get("CSV_IN", "").strip()
    csv_out = os.environ.get("CSV_OUT", "").strip()

    def guess_paths_from_series(sid: str):
        if not sid:
            return None, None
        base = f"output/autohome/{sid}/config_{sid}"
        return Path(f"{base}.csv"), Path(f"{base}.ja.csv")

    default_in  = Path("output/autohome/7578/config_7578.csv")
    default_out = Path("output/autohome/7578/config_7578.ja.csv")

    src = Path(csv_in)  if csv_in  else None
    dst = Path(csv_out) if csv_out else None

    if src is None or dst is None:
        s2, d2 = guess_paths_from_series(SERIES_ID)
        src = src or s2
        dst = dst or d2

    src = src or default_in
    dst = dst or default_out
    return src, dst

SRC, DST_PRIMARY = resolve_src_dst()

def make_secondary(dst: Path) -> Path:
    s = dst.name
    if s.endswith(".ja.csv"):
        s2 = s.replace(".ja.csv", "_ja.csv")
    elif s.endswith("_ja.csv"):
        s2 = s.replace("_ja.csv", ".ja.csv")
    else:
        s2 = dst.stem + ".ja.csv"
    return dst.parent / s2

DST_SECONDARY = make_secondary(DST_PRIMARY)

# ====== 設定 ======
MODEL   = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
API_KEY = os.environ.get("OPENAI_API_KEY")

TRANSLATE_VALUES   = os.environ.get("TRANSLATE_VALUES", "true").lower() == "true"
TRANSLATE_COLNAMES = os.environ.get("TRANSLATE_COLNAMES", "true").lower() == "true"
STRIP_GRADE_PREFIX = os.environ.get("STRIP_GRADE_PREFIX", "true").lower() == "true"
SERIES_PREFIX_RE   = os.environ.get("SERIES_PREFIX", "").strip()
EXRATE_CNY_TO_JPY  = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))

BATCH_SIZE, RETRIES, SLEEP_BASE = 60, 3, 1.2

# ====== クリーニング・辞書など ======
NOISE_ANY = ["对比","参数","图片","配置","详情"]
NOISE_PRICE_TAIL = ["询价","计算器","询底价","报价","价格询问","起","起售"]

def clean_any_noise(s:str)->str:
    s=str(s) if s is not None else ""
    for w in NOISE_ANY+NOISE_PRICE_TAIL:
        s=s.replace(w,"")
    return re.sub(r"\s+"," ",s).strip(" 　-—–")

def clean_price_cell(s:str)->str:
    t=clean_any_noise(s)
    for w in NOISE_PRICE_TAIL:
        t=re.sub(rf"(?:\s*{re.escape(w)}\s*)+$","",t)
    return t.strip()

RE_PAREN_ANY_YEN=re.compile(r"（[^）]*(?:日本円|JPY|[¥￥]|円)[^）]*）")
RE_ANY_YEN_TOKEN=re.compile(r"(日本円|JPY|[¥￥]|円)")

def strip_any_yen_tokens(s:str)->str:
    t=str(s)
    t=RE_PAREN_ANY_YEN.sub("",t)
    t=RE_ANY_YEN_TOKEN.sub("",t)
    return re.sub(r"\s+"," ",t).strip()

BRAND_MAP={"BYD":"BYD","比亚迪":"BYD"}

FIX_JA_ITEMS={
    "厂商指导价":"メーカー希望小売価格",
    "经销商参考价":"ディーラー販売価格（元）",
    "经销商报价":"ディーラー販売価格（元）",
    "经销商":"ディーラー販売価格（元）",
    "被动安全":"衝突安全",
}
FIX_JA_SECTIONS={"被动安全":"衝突安全"}

PRICE_ITEM_MSRP_CN={"厂商指导价"}
PRICE_ITEM_DEALER_CN={"经销商参考价","经销商报价","经销商"}

# ====== 金額整形 ======
RE_WAN=re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*万")
RE_YUAN=re.compile(r"(?P<num>[\d,]+)\s*元")

def parse_cny(text:str):
    t=str(text)
    m1=RE_WAN.search(t)
    if m1:return float(m1.group("num"))*10000.0
    m2=RE_YUAN.search(t)
    if m2:return float(m2.group("num").replace(",",""))
    return None

def msrp_to_yuan_and_jpy(cell:str,rate:float)->str:
    t=strip_any_yen_tokens(clean_price_cell(cell))
    if not t or t in {"-","–","—"}:return t
    cny=parse_cny(t)
    if cny is None:
        if("元"not in t)and RE_WAN.search(t):t=f"{t}元"
        return t
    m1=RE_WAN.search(t)
    yuan_disp=f"{m1.group('num')}万元" if m1 else (t if"元"in t else f"{t}元")
    jpy=int(round(cny*rate))
    return f"{yuan_disp}（日本円{jpy:,}円）"

def dealer_to_yuan_only(cell:str)->str:
    t=strip_any_yen_tokens(clean_price_cell(cell))
    if not t or t in {"-","–","—"}:return t
    if("元"not in t)and RE_WAN.search(t):t=f"{t}元"
    return t

# ====== 共通関数 ======
def uniq(seq):
    s, out = set(), []
    for x in seq:
        if x not in s:
            s.add(x)
            out.append(x)
    return out

def chunked(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def parse_json_relaxed(content: str, terms: list[str]) -> dict[str, str]:
    try:
        d = json.loads(content)
        if isinstance(d, dict) and "translations" in d:
            return {t["cn"]: t["ja"] or t["cn"] for t in d["translations"] if t.get("cn")}
    except Exception:
        pass
    return {t: t for t in terms}

# ====== Translator（辞書対応＋API修正） ======
def _load_dict(path: str | None) -> dict[str, str]:
    if not path: return {}
    p = Path(path)
    if not p.exists(): return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_dict(path: str | None, data: dict[str, str]):
    if not path: return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    old = _load_dict(path)
    old.update(data)
    p.write_text(json.dumps(old, ensure_ascii=False, indent=2), encoding="utf-8")

class Translator:
    def __init__(self, model, api_key, dict_sec=None, dict_item=None, cache_sec=None, cache_item=None):
        self.model = model
        self.api_key = api_key
        # ✅ 修正：空文字キーでも認識
        self.client = OpenAI(api_key=api_key) if api_key and api_key.strip() else None
        self.dict_sec = dict_sec or {}
        self.dict_item = dict_item or {}
        self.cache_sec = _load_dict(cache_sec)
        self.cache_item = _load_dict(cache_item)
        self.cache_sec_path = cache_sec
        self.cache_item_path = cache_item
        self.system = "あなたは自動車仕様表の専門翻訳者です。入力は中国語のセクション名または項目名です。自然な日本語に翻訳してください。"

    def _dict_hit(self, terms, kind):
        base = self.dict_sec if kind == "sec" else self.dict_item
        cache = self.cache_sec if kind == "sec" else self.cache_item
        hit, miss = {}, []
        for t in terms:
            if t in base:
                hit[t] = base[t]
            elif t in cache:
                hit[t] = cache[t]
            else:
                miss.append(t)
        return hit, miss

    def _api_batch(self, terms):
        if not self.client:
            print("⚠️ API client not initialized, skipping translation.")
            return {t: t for t in terms}
        msgs = [
            {"role": "system", "content": self.system},
            {"role": "user", "content": json.dumps({"terms": terms}, ensure_ascii=False)},
        ]
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=msgs,
            temperature=0,
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content or ""
        return parse_json_relaxed(content, terms)

    def translate_terms(self, terms, kind):
        hit, miss = self._dict_hit(terms, kind)
        out = dict(hit)
        api_gained = {}
        for chunk in chunked(miss, BATCH_SIZE):
            for attempt in range(1, RETRIES + 1):
                try:
                    got = self._api_batch(chunk)
                    api_gained.update(got)
                    break
                except Exception:
                    if attempt == RETRIES:
                        for t in chunk:
                            api_gained[t] = t
                    time.sleep(SLEEP_BASE * attempt)
        out.update(api_gained)
        if kind == "sec" and api_gained:
            _save_dict(self.cache_sec_path, api_gained)
        if kind == "item" and api_gained:
            _save_dict(self.cache_item_path, api_gained)
        return out

# ====== main ======
def main():
    print(f"🔎 SRC: {SRC}")
    print(f"📝 DST(primary): {DST_PRIMARY}")
    print(f"📝 DST(secondary): {DST_SECONDARY}")

    df = pd.read_csv(SRC, encoding="utf-8-sig").map(clean_any_noise)
    df.columns = [BRAND_MAP.get(c, c) for c in df.columns]

    uniq_sec = uniq(df["セクション"].dropna().astype(str))
    uniq_item = uniq(df["項目"].dropna().astype(str))

    dict_sec = _load_dict(os.environ.get("DICT_SECTIONS", ""))
    dict_item = _load_dict(os.environ.get("DICT_ITEMS", ""))
    tr = Translator(
        MODEL, API_KEY,
        dict_sec=dict_sec, dict_item=dict_item,
        cache_sec=os.environ.get("CACHE_SECTIONS", "cache/sections.ja.json"),
        cache_item=os.environ.get("CACHE_ITEMS", "cache/items.ja.json")
    )

    sec_map = tr.translate_terms(uniq_sec, "sec")
    item_map = tr.translate_terms(uniq_item, "item")
    sec_map.update(FIX_JA_SECTIONS)
    item_map.update(FIX_JA_ITEMS)

    out = df.copy()
    out.insert(1, "セクション_ja", out["セクション"].map(lambda s: sec_map.get(str(s).strip(), str(s).strip())))
    out.insert(3, "項目_ja", out["項目"].map(lambda s: item_map.get(str(s).strip(), str(s).strip())))

    for col in out.columns[4:]:
        if any(out["項目"].isin(PRICE_ITEM_MSRP_CN)):
            out[col] = out[col].map(lambda s: msrp_to_yuan_and_jpy(s, EXRATE_CNY_TO_JPY))
        if any(out["項目"].isin(PRICE_ITEM_DEALER_CN)):
            out[col] = out[col].map(dealer_to_yuan_only)

    DST_PRIMARY.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST_PRIMARY, index=False, encoding="utf-8-sig")
    out.to_csv(DST_SECONDARY, index=False, encoding="utf-8-sig")
    print(f"✅ Saved {DST_PRIMARY.resolve()}")

if __name__ == "__main__":
    main()
