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

# ====== クリーニング・辞書など（元のまま） ======
NOISE_ANY = ["对比","参数","图片","配置","详情"]
NOISE_PRICE_TAIL = ["询价","计算器","询底价","报价","价格询问","起","起售"]
def clean_any_noise(s:str)->str:
    s=str(s) if s is not None else ""
    for w in NOISE_ANY+NOISE_PRICE_TAIL: s=s.replace(w,"")
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

# ====== 金額整形（元のまま） ======
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
def uniq(seq):s,out=set(),[];[out.append(x) for x in seq if not(x in s or s.add(x))];return out
def chunked(xs,n):return [xs[i:i+n] for i in range(0,len(xs),n)]

def parse_json_relaxed(content:str,terms:list[str])->dict[str,str]:
    try:
        d=json.loads(content)
        if isinstance(d,dict)and"translations"in d:
            return {str(t["cn"]).strip():str(t["ja"]).strip()or t["cn"] for t in d["translations"] if t.get("cn")}
    except:pass
    mjson=re.search(r"\{[\s\S]*\}",content)
    if mjson:
        try:
            d=json.loads(mjson.group(0))
            if isinstance(d,dict)and"translations"in d:
                return {str(t["cn"]).strip():str(t["ja"]).strip()or t["cn"] for t in d["translations"] if t.get("cn")}
        except:pass
    m={}
    for l in content.splitlines():
        if"\t"in l:
            cn,ja=l.split("\t",1)
            m[cn.strip()]=ja.strip()or cn.strip()
    for t in terms:m.setdefault(t,t)
    return m

# ====== 新: Translator（セクション/項目専用） ======
def _load_dict_json(path:str|None)->dict[str,str]:
    if not path:return {}
    p=Path(path)
    if not p.exists():return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except: return {}

def _save_dict_json(path:str|None,data:dict[str,str]):
    if not path:return
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    old=_load_dict_json(path)
    old.update(data)
    p.write_text(json.dumps(old,ensure_ascii=False,indent=2),encoding="utf-8")

class Translator:
    def __init__(self,model:str,api_key:str,dict_sec=None,dict_item=None,cache_sec=None,cache_item=None):
        self.model=model
        self.api_key=api_key
        self.client=None if not api_key else OpenAI(api_key=api_key)
        self.dict_sec=dict_sec or {}
        self.dict_item=dict_item or {}
        self.cache_sec=_load_dict_json(cache_sec)
        self.cache_item=_load_dict_json(cache_item)
        self.cache_sec_path=cache_sec
        self.cache_item_path=cache_item
        self.system=("あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セクション名/項目名』です。自然で簡潔な日本語へ翻訳してください。"
            "出力は JSON（{'translations':[{'cn':'原文','ja':'訳文'}]}）のみ。")

    def _dict_hit(self,terms:list[str],kind:str)->tuple[dict[str,str],list[str]]:
        base=self.dict_sec if kind=="sec" else self.dict_item
        cache=self.cache_sec if kind=="sec" else self.cache_item
        hit,miss={},[]
        for t in terms:
            if t in base:hit[t]=base[t]
            elif t in cache:hit[t]=cache[t]
            else:miss.append(t)
        return hit,miss

    def _api_batch(self,terms:list[str])->dict[str,str]:
        if not self.client:return {}
        msgs=[{"role":"system","content":self.system},{"role":"user","content":json.dumps({"terms":terms},ensure_ascii=False)}]
        r=self.client.chat.completions.create(model=self.model,messages=msgs,temperature=0,response_format={"type":"json_object"})
        return parse_json_relaxed(r.choices[0].message.content or "",terms)

    def translate_terms(self,terms:list[str],kind:str)->dict[str,str]:
        hit,miss=self._dict_hit(terms,kind)
        out=dict(hit)
        api_gained={}
        for chunk in chunked(miss,BATCH_SIZE):
            for a in range(RETRIES):
                try:
                    got=self._api_batch(chunk)
                    api_gained.update(got);break
                except:
                    time.sleep(SLEEP_BASE*(a+1))
        out.update(api_gained)
        if kind=="sec"and api_gained:_save_dict_json(self.cache_sec_path,api_gained)
        if kind=="item"and api_gained:_save_dict_json(self.cache_item_path,api_gained)
        return out

# ====== グレード系（元のまま） ======
YEAR_TOKEN_RE=re.compile(r"(?:20\d{2}|19\d{2})|(?:\d{2}款|[上中下]市|改款|年款)")
LEADING_TOKEN_RE=re.compile(r"^[\u4e00-\u9fffA-Za-z][\u4e00-\u9fffA-Za-z0-9\- ]{1,40}")
def cut_before_year_or_kuan(s:str)->str|None:
    s=s.strip()
    m=YEAR_TOKEN_RE.search(s)
    if m:return s[:m.start()].strip()
    kuan=re.search(r"款",s)
    if kuan:return s[:kuan.start()].strip()
    m2=LEADING_TOKEN_RE.match(s)
    return m2.group(0).strip() if m2 else None

def detect_common_series_prefix(cols:list[str])->str|None:
    cand=[cut_before_year_or_kuan(str(c)) for c in cols if cut_before_year_or_kuan(str(c))]
    if not cand:return None
    from collections import Counter
    top,ct=Counter(cand).most_common(1)[0]
    return re.escape(top) if ct>=max(1,int(0.6*len(cols))) else None

def strip_series_prefix_from_grades(grade_cols:list[str])->list[str]:
    if not grade_cols or not STRIP_GRADE_PREFIX:return grade_cols
    pattern=SERIES_PREFIX_RE or detect_common_series_prefix(grade_cols)
    if not pattern:return grade_cols
    regex=re.compile(rf"^\s*(?:{pattern})\s*[-:：/ ]*\s*",re.IGNORECASE)
    return [regex.sub("",str(c)).strip() or c for c in grade_cols]

# ====== main ======
def main():
    print(f"🔎 SRC: {SRC}")
    print(f"📝 DST(primary): {DST_PRIMARY}")
    print(f"📝 DST(secondary): {DST_SECONDARY}")

    df=pd.read_csv(SRC,encoding="utf-8-sig").map(clean_any_noise)
    df.columns=[BRAND_MAP.get(c,c) for c in df.columns]

    uniq_sec=uniq([str(x).strip() for x in df["セクション"].fillna("") if str(x).strip()])
    uniq_item=uniq([str(x).strip() for x in df["項目"].fillna("") if str(x).strip()])

    dict_sec=_load_dict_json(os.environ.get("DICT_SECTIONS",""))
    dict_item=_load_dict_json(os.environ.get("DICT_ITEMS",""))
    tr=Translator(MODEL,API_KEY,dict_sec=dict_sec,dict_item=dict_item,
                  cache_sec=os.environ.get("CACHE_SECTIONS","cache/sections.ja.json"),
                  cache_item=os.environ.get("CACHE_ITEMS","cache/items.ja.json"))

    sec_map=tr.translate_terms(uniq_sec,"sec")
    item_map=tr.translate_terms(uniq_item,"item")
    sec_map.update(FIX_JA_SECTIONS);item_map.update(FIX_JA_ITEMS)

    out=df.copy()
    out.insert(1,"セクション_ja",out["セクション"].map(lambda s:sec_map.get(str(s).strip(),str(s).strip())))
    out.insert(3,"項目_ja",out["項目"].map(lambda s:item_map.get(str(s).strip(),str(s).strip())))

    # --- 以下は元のまま（キャッシュ非使用） ---
    PAREN_CURR_RE=re.compile(r"（\s*(?:円|元|人民元|CNY|RMB|JPY)[^）]*）")
    out["項目_ja"]=out["項目_ja"].astype(str).str.replace(PAREN_CURR_RE,"",regex=True).str.strip()
    out.loc[out["項目_ja"].str.match(r"^メーカー希望小売価格.*$",na=False),"項目_ja"]="メーカー希望小売価格"
    out.loc[out["項目_ja"].str.contains(r"ディーラー販売価格",na=False),"項目_ja"]="ディーラー販売価格（元）"

    # 列ヘッダ翻訳
    if TRANSLATE_COLNAMES:
        orig_cols=list(out.columns);fixed=orig_cols[:4];grades=orig_cols[4:]
        grades_norm=[BRAND_MAP.get(c,c) for c in grades]
        grades_stripped=strip_series_prefix_from_grades(grades_norm)
        uniq_grades=uniq([str(c).strip() for c in grades_stripped])
        tr2=Translator(MODEL,API_KEY)  # 通常API版
        grade_map=tr2._api_batch(uniq_grades)
        translated=[grade_map.get(g,g) for g in grades_stripped]
        out.columns=fixed+translated
    else:
        if STRIP_GRADE_PREFIX:
            orig_cols=list(out.columns);fixed=orig_cols[:4];grades=orig_cols[4:]
            out.columns=fixed+strip_series_prefix_from_grades(grades)

    # 価格整形（元のまま）
    MSRP_JA_RE=re.compile(r"^メーカー希望小売価格$")
    DEALER_JA_RE=re.compile(r"^ディーラー販売価格（元）$")
    is_msrp=out["項目"].isin(PRICE_ITEM_MSRP_CN)|out["項目_ja"].str.match(MSRP_JA_RE,na=False)
    is_dealer=out["項目"].isin(PRICE_ITEM_DEALER_CN)|out["項目_ja"].str.match(DEALER_JA_RE,na=False)
    for col in out.columns[4:]:
        out.loc[is_msrp,col]=out.loc[is_msrp,col].map(lambda s:msrp_to_yuan_and_jpy(s,EXRATE_CNY_TO_JPY))
        out.loc[is_dealer,col]=out.loc[is_dealer,col].map(lambda s:dealer_to_yuan_only(s))

    if TRANSLATE_VALUES:
        values=[];num_like=re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")
        mask=~(is_msrp|is_dealer)
        for col in out.columns[4:]:
            for v in out.loc[mask,col].astype(str):
                vv=v.strip()
                if not vv or vv in {"●","○","–","-","—"}:continue
                if num_like.fullmatch(vv):continue
                values.append(vv)
        uniq_vals=uniq(values)
        trv=Translator(MODEL,API_KEY)
        val_map=trv._api_batch(uniq_vals)
        for col in out.columns[4:]:
            out.loc[mask,col]=out.loc[mask,col].map(lambda s:val_map.get(str(s).strip(),str(s).strip()))

    DST_PRIMARY.parent.mkdir(parents=True,
