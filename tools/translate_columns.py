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

# ====== 固定訳・正規化 ======
NOISE_ANY = ["对比","参数","图片","配置","详情"]
NOISE_PRICE_TAIL = ["询价","计算器","询底价","报价","价格询问","起","起售","到店","经销商"]

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

BRAND_MAP={
    "BYD":"BYD","比亚迪":"BYD",
    "奔驰":"メルセデス・ベンツ","梅赛德斯-奔驰":"メルセデス・ベンツ",
}

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

# ====== LLM 翻訳ユーティリティ ======
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
            return {str(t["cn"]).strip():str(t.get("ja",t["cn"])).strip()
                    for t in d["translations"] if t.get("cn")}
    except Exception:
        pass
    pairs=re.findall(r'"cn"\s*:\s*"([^"]+)"\s*,\s*"ja"\s*:\s*"([^"]*)"', content)
    if pairs:
        return {cn.strip():ja.strip() for cn,ja in pairs}
    return {t:t for t in terms}

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

    def translate_batch(self, terms: list[str]) -> dict[str,str]:
        if not terms:
            return {}
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

# ====== モデル名（グレード列）用：接頭辞カット＋ルール置換 ======
YEAR_TOKEN_RE = re.compile(r"(?:20\d{2}|19\d{2})|(?:\d{2}款|[上中下]市|改款|年款)")
LEADING_TOKEN_RE = re.compile(r"^[\u4e00-\u9fffA-Za-z][\u4e00-\u9fffA-Za-z0-9\- ]{1,40}")

def cut_before_year_or_kuan(s: str) -> str | None:
    s = s.strip()
    m = YEAR_TOKEN_RE.search(s)
    if m: return s[:m.start()].strip()
    kuan = re.search(r"款", s)
    if kuan: return s[:kuan.start()].strip()
    m2 = LEADING_TOKEN_RE.match(s)
    return m2.group(0).strip() if m2 else None

def detect_common_series_prefix(cols: list[str]) -> str | None:
    cand=[]
    for c in cols:
        p = cut_before_year_or_kuan(str(c))
        if p and len(p) >= 2: cand.append(p)
    if not cand: return None
    from collections import Counter
    top, ct = Counter(cand).most_common(1)[0]
    return re.escape(top) if ct >= max(1, int(0.6*len(cols))) else None

def strip_series_prefix_from_grades(grade_cols: list[str]) -> list[str]:
    if not grade_cols or not STRIP_GRADE_PREFIX: return grade_cols
    pattern = SERIES_PREFIX_RE or detect_common_series_prefix(grade_cols)
    if not pattern: return grade_cols
    regex = re.compile(rf"^\s*(?:{pattern})\s*[-:：/ ]*\s*", re.IGNORECASE)
    return [regex.sub("", str(c)).strip() or c for c in grade_cols]

# 「2025款」→「2025年モデル」, 「改款」→「改良版」, 「运动型」→「スポーツタイプ」など
def grade_rule_ja(s: str) -> str:
    t = str(s).strip()
    t = re.sub(r"(\d{4})\s*款", r"\1年モデル", t)
    # よくある語
    repl = {
        "改款": "改良版",
        "运动型": "スポーツタイプ",
        "运动": "スポーツ",
        "四驱": "4WD",
        "两驱": "2WD",
        "全驱": "AWD",
    }
    for cn, ja in repl.items():
        t = t.replace(cn, ja)
    # 余計な接続記号の整形
    t = re.sub(r"\s*[-:：/]\s*", " ", t).strip()
    return t

# ====== main ======
def main():
    print(f"CSV_IN: {SRC}")
    if not Path(SRC).exists():
        raise FileNotFoundError(f"入力CSVが見つかりません: {SRC}")

    # 原文ロード＆軽ノイズ除去
    df = pd.read_csv(SRC, encoding="utf-8-sig").map(clean_any_noise)
    # ヘッダのブランド正規化（列名をまず補正）
    df.columns = [BRAND_MAP.get(c, c) for c in df.columns]

    # ====== セクション／項目：辞書優先→なければLLM ======
    uniq_sec  = uniq([str(x).strip() for x in df["セクション"].fillna("") if str(x).strip()])
    uniq_item = uniq([str(x).strip() for x in df["項目"].fillna("")    if str(x).strip()])

    tr = Translator(MODEL, API_KEY)

    sec_dict = {**FIX_JA_SECTIONS}
    item_dict = {**FIX_JA_ITEMS}

    sec_missing  = [s for s in uniq_sec  if s not in sec_dict]
    item_missing = [s for s in uniq_item if s not in item_dict]

    if sec_missing:
        sec_dict.update(tr.translate_unique(sec_missing))
    if item_missing:
        item_dict.update(tr.translate_unique(item_missing))

    # JA列を挿入
    df.insert(1, "セクション_ja", df["セクション"].map(lambda s: sec_dict.get(str(s).strip(), str(s).strip())))
    df.insert(3, "項目_ja",     df["項目"].map(lambda s: item_dict.get(str(s).strip(),   str(s).strip())))

    # ====== モデル名（グレード列）: 接頭辞カット＋ルール適用＋（必要時LLM） ======
    if TRANSLATE_COLNAMES:
        orig_cols = list(df.columns)
        fixed = orig_cols[:4]                 # セクション, セクション_ja, 項目, 項目_ja
        grades = orig_cols[4:]                # 以降がモデル（グレード）列

        # 接頭辞（ブランド＋シリーズ名）を一括カット
        grades_stripped = strip_series_prefix_from_grades(grades)

        # まずルール変換
        grades_rule_ja = [grade_rule_ja(g) for g in grades_stripped]

        # LLMが必要なケース（上のルールで未カバーな純中文語が残る場合）だけ抽出
        # 例：完全に中文語が残っている等を簡易検出（漢字＋空白のみ等）
        need_llm = []
        for g in grades_rule_ja:
            # 数字・英字・既知記号のみになっていれば LLM 不要
            if re.fullmatch(r"[0-9A-Za-z\s\-\+\./°]+", g):
                continue
            # 典型的に訳したい中国語語彙が残る場合
            if re.search(r"[\u4e00-\u9fff]", g):
                need_llm.append(g)

        if need_llm:
            uniq_need = uniq(need_llm)
            llm_map = tr.translate_unique(uniq_need)
        else:
            llm_map = {}

        final_grades = [llm_map.get(g, g) for g in grades_rule_ja]
        df.columns = fixed + final_grades

    # ====== 価格行の整形（値セルの翻訳対象からも除外） ======
    MSRP_JA_RE=re.compile(r"^メーカー希望小売価格$")
    DEALER_JA_RE=re.compile(r"^ディーラー販売価格（元）$")
    is_msrp   = df["項目"].isin(PRICE_ITEM_MSRP_CN)  | df["項目_ja"].str.match(MSRP_JA_RE,na=False)
    is_dealer = df["項目"].isin(PRICE_ITEM_DEALER_CN)| df["項目_ja"].str.match(DEALER_JA_RE,na=False)

    for col in df.columns[4:]:
        df.loc[is_msrp,  col]=df.loc[is_msrp,  col].map(lambda s:msrp_to_yuan_and_jpy(s,EXRATE_CNY_TO_JPY))
        df.loc[is_dealer,col]=df.loc[is_dealer,col].map(lambda s:dealer_to_yuan_only(s))

    # ====== 値セル翻訳（価格行除外・数値/記号類除外） ======
    if TRANSLATE_VALUES:
        numeric_like=re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")
        tr_values=[]; coords=[]
        for i in range(len(df)):
            if is_msrp.iloc[i] or is_dealer.iloc[i]:
                continue
            for j in range(4,len(df.columns)):
                v=str(df.iat[i,j]).strip()
                if v in {"","●","○","–","-","—"}: continue
                if numeric_like.fullmatch(v): continue
                tr_values.append(v); coords.append((i,j))
        uniq_vals=uniq(tr_values)
        val_map=Translator(MODEL, API_KEY).translate_unique(uniq_vals) if uniq_vals else {}
        for (i,j) in coords:
            s=str(df.iat[i,j]).strip()
            df.iat[i,j]=val_map.get(s,s)

    # ====== 出力 ======
    DST_PRIMARY.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(DST_PRIMARY,   index=False, encoding="utf-8-sig")
    df.to_csv(DST_SECONDARY, index=False, encoding="utf-8-sig")
    print(f"✅ Saved: {DST_PRIMARY}")

if __name__ == "__main__":
    main()
