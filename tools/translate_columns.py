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
CACHE_REPO_DIR     = os.environ.get("CACHE_REPO_DIR", "cache").strip()

BATCH_SIZE, RETRIES, SLEEP_BASE = 60, 3, 1.2

# ====== クリーニング・固定訳 ======
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
PRICE_ITEM_MSRP_JA={"メーカー希望小売価格"}
PRICE_ITEM_DEALER_CN={"经销商参考价","经销商报价","经销商"}
PRICE_ITEM_DEALER_JA={"ディーラー販売価格（元）"}

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

# ====== ユーティリティ ======
def uniq(seq):
    s, out = set(), []
    for x in seq:
        if x not in s:
            s.add(x); out.append(x)
    return out

def chunked(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def parse_json_relaxed(content:str,terms:list[str])->dict[str,str]:
    try:
        d=json.loads(content)
        if isinstance(d,dict)and"translations"in d:
            return {str(t["cn"]).strip():str(t["ja"]).strip() or t["cn"] for t in d["translations"] if t.get("cn")}
    except Exception:
        pass
    return {t:t for t in terms}

# ====== LLM ======
class Translator:
    def __init__(self, model: str, api_key: str):
        if not (api_key and api_key.strip()):
            raise RuntimeError("OPENAI_API_KEY is not set")
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.system = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セクション名/項目名/モデル名/セル値』の配列です。"
            "自然で簡潔な日本語へ翻訳してください。数値・年式・排量・AT/MT等の記号は保持。"
            "出力は JSON（{'translations':[{'cn':'原文','ja':'訳文'}]}）のみ。"
        )
        print(f"🟢 Translator ready: model={self.model}")

    def translate_batch(self, terms: list[str]) -> dict[str,str]:
        if not terms:
            return {}
        print(f"🛰️  API call: {len(terms)} terms (sample={terms[:3]})")
        msgs=[
            {"role":"system","content":self.system},
            {"role":"user","content":json.dumps({"terms":terms},ensure_ascii=False)},
        ]
        try:
            resp=self.client.chat.completions.create(
                model=self.model,messages=msgs,temperature=0,
                response_format={"type":"json_object"},
            )
            content=resp.choices[0].message.content or ""
            return parse_json_relaxed(content, terms)
        except Exception as e:
            print("❌ OpenAI error:", repr(e))
            return {t: t for t in terms}

    def translate_unique(self, unique_terms: list[str]) -> dict[str,str]:
        out={}
        for chunk in chunked(unique_terms, BATCH_SIZE):
            for attempt in range(1, RETRIES+1):
                try:
                    out.update(self.translate_batch(chunk))
                    break
                except Exception as e:
                    print(f"❌ translate_unique error attempt={attempt}:", repr(e))
                    if attempt==RETRIES:
                        for t in chunk: out.setdefault(t, t)
                    time.sleep(SLEEP_BASE*attempt)
        return out

# ====== キャッシュ関連 ======
def repo_cache_paths(series_id: str) -> tuple[Path, Path]:
    base = Path(CACHE_REPO_DIR) / str(series_id or "unknown")
    return (base / "cn.csv", base / "ja.csv")

def same_shape_and_headers(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    return (df1.shape == df2.shape) and (list(df1.columns) == list(df2.columns))

def norm_cn_cell(s: str) -> str:
    return clean_any_noise(str(s)).strip()

# ====== main ======
def main():
    print(f"CSV_IN check: {SRC}")
    print(f"Pre-run cache listing:")
    os.system(f"ls -l {CACHE_REPO_DIR}/{SERIES_ID} || echo '(no cache dir)'")

    print(f"🔎 SRC: {SRC}")
    print(f"📝 DST(primary): {DST_PRIMARY}")
    print(f"📝 DST(secondary): {DST_SECONDARY}")

    if not Path(SRC).exists():
        raise FileNotFoundError(f"入力CSVが見つかりません: {SRC}")

    df = pd.read_csv(SRC, encoding="utf-8-sig").map(clean_any_noise)
    df.columns = [BRAND_MAP.get(c, c) for c in df.columns]

    cn_snap_path, ja_prev_path = repo_cache_paths(SERIES_ID)
    cn_exists, ja_exists = cn_snap_path.exists(), ja_prev_path.exists()

    prev_cn_df = pd.read_csv(cn_snap_path, encoding="utf-8-sig").map(clean_any_noise) if cn_exists else None
    prev_ja_df = pd.read_csv(ja_prev_path, encoding="utf-8-sig") if ja_exists else None

    enable_reuse = (prev_cn_df is not None and prev_ja_df is not None and same_shape_and_headers(df, prev_cn_df))
    print(f"♻️ reuse={enable_reuse}")

    tr = Translator(MODEL, API_KEY)
    out = df.copy()

    # 差分比較
    if enable_reuse:
        diff_mask = (df != prev_cn_df)
    else:
        diff_mask = pd.DataFrame(True, index=df.index, columns=df.columns)

    values_to_translate, coords_to_update = [], []
    numeric_like = re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")

    for i in range(len(df)):
        for j in range(4, len(df.columns)):
            cur = str(df.iat[i, j]).strip()
            if cur in {"", "●", "○", "–", "-", "—"} or numeric_like.fullmatch(cur):
                continue
            if not diff_mask.iat[i, j]:
                if enable_reuse:
                    out.iat[i, j] = prev_ja_df.iat[i, j]
            else:
                values_to_translate.append(cur)
                coords_to_update.append((i, j))

    uniq_vals = uniq(values_to_translate)
    print(f"🌐 to_translate={len(uniq_vals)}")
    val_map = tr.translate_unique(uniq_vals) if uniq_vals else {}

    for (i, j) in coords_to_update:
        s = str(df.iat[i, j]).strip()
        if not s:
            continue
        out.iat[i, j] = val_map.get(s, s)

    DST_PRIMARY.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST_PRIMARY, index=False, encoding="utf-8-sig")
    out.to_csv(DST_SECONDARY, index=False, encoding="utf-8-sig")

    cn_snap_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(cn_snap_path, index=False, encoding="utf-8-sig")
    out.to_csv(ja_prev_path, index=False, encoding="utf-8-sig")

    print(f"✅ Saved: {DST_PRIMARY}")
    print(f"📦 Repo cache CN: {cn_snap_path}")
    print(f"📦 Repo cache JA: {ja_prev_path}")

if __name__ == "__main__":
    main()
