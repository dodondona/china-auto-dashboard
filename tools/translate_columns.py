from __future__ import annotations
import os, json, time, re
from pathlib import Path
import pandas as pd
from openai import OpenAI

# ====== 入出力（YAML変更なしで動くよう互換重視） ======
SERIES_ID = os.environ.get("SERIES_ID", "").strip()

def resolve_src_dst():
    csv_in  = os.environ.get("CSV_IN", "").strip()
    csv_out = os.environ.get("CSV_OUT", "").strip()

    def guess_paths_from_series(sid: str):
        if not sid:
            return None, None
        base = f"output/autohome/{sid}/config_{sid}"
        return Path(f"{base}.csv"), Path(f"{base}.ja.csv")  # 既定は .ja.csv

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

# ====== OpenAI ======
MODEL   = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
API_KEY = os.environ.get("OPENAI_API_KEY")

# スイッチ
TRANSLATE_VALUES   = os.environ.get("TRANSLATE_VALUES", "true").lower() == "true"
TRANSLATE_COLNAMES = os.environ.get("TRANSLATE_COLNAMES", "true").lower() == "true"

# 先頭車名を削る（既定ON）。明示パターンは SERIES_PREFIX（例: "駆逐艦05|驱逐舰05"）
STRIP_GRADE_PREFIX = os.environ.get("STRIP_GRADE_PREFIX", "true").lower() == "true"
SERIES_PREFIX_RE   = os.environ.get("SERIES_PREFIX", "").strip()

# CNY→JPY
EXRATE_CNY_TO_JPY  = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))

BATCH_SIZE  = 60
RETRIES     = 3
SLEEP_BASE  = 1.2

# ====== クリーニング・辞書 ======
NOISE_ANY = ["对比", "参数", "图片", "配置", "详情"]
NOISE_PRICE_TAIL = ["询价", "计算器", "询底价", "报价", "价格询问", "価格問い合わせ", "起", "起售"]

def clean_any_noise(s: str) -> str:
    s = str(s) if s is not None else ""
    for w in NOISE_ANY + NOISE_PRICE_TAIL:
        s = s.replace(w, "")
    s = re.sub(r"\s+", " ", s).strip(" 　-—–")
    return s

def clean_price_cell(s: str) -> str:
    t = clean_any_noise(s)
    for w in NOISE_PRICE_TAIL:
        t = re.sub(rf"(?:\s*{re.escape(w)}\s*)+$", "", t)
    return t.strip()

# ブランド正規化（BYDは翻訳しない）
BRAND_MAP = {"BYD": "BYD", "比亚迪": "BYD"}

# 固定訳
# ・メーカー希望小売価格：通貨表記なし
# ・ディーラー価格：見出しに（元）を明記
FIX_JA_ITEMS = {
    "厂商指导价":   "メーカー希望小売価格",      # ★（円）/（元）など付けない
    "经销商参考价": "ディーラー販売価格（元）",
    "经销商报价":   "ディーラー販売価格（元）",
    "经销商":       "ディーラー販売価格（元）",
    "被动安全":     "衝突安全",
}
FIX_JA_SECTIONS = {"被动安全": "衝突安全"}

PRICE_ITEM_MSRP_CN = {"厂商指导价"}
PRICE_ITEM_MSRP_JA = {"メーカー希望小売価格"}
PRICE_ITEM_DEALER_CN = {"经销商参考价", "经销商报价", "经销商"}
PRICE_ITEM_DEALER_JA = {"ディーラー販売価格（元）"}

# ====== 価格整形 ======
RE_WAN       = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*万")
RE_YUAN      = re.compile(r"(?P<num>[\d,]+)\s*元")
RE_JPY_PAREN = re.compile(r"（日本円[0-9,]+円）|（約¥[0-9,]+）")

def parse_cny(text: str):
    """文字列から CNY 金額（元）を抽出。万→元 に換算。失敗時 None。"""
    m1 = RE_WAN.search(text)
    if m1:
        return float(m1.group("num")) * 10000.0
    m2 = RE_YUAN.search(text)
    if m2:
        return float(m2.group("num").replace(",", ""))
    return None

def msrp_to_yuan_and_jpy(cell: str, rate: float) -> str:
    """
    メーカー希望小売価格のセルを「xx万元（日本円YYY円）」に統一。
    ・既存の円表記は一旦除去して再生成
    ・「11.98万」→「11.98万元（日本円251,580円）」のように出力
    ・ダッシュ等はそのまま
    """
    t = str(cell).strip()
    if not t or t in {"-", "–", "—"}:
        return t
    t = RE_JPY_PAREN.sub("", t).strip()

    cny = parse_cny(t)
    if cny is None:
        # 末尾に「元」を付与だけ（情報が無い場合は触らない）
        if ("元" not in t) and RE_WAN.search(t):
            t = f"{t}元"
        return t

    # 表示用：元は「万」表記を尊重（"11.98万" が残っていればそれをベースに）
    m1 = RE_WAN.search(t)
    if m1:
        yuan_disp = f"{m1.group('num')}万元"
    else:
        # 129,800元 → 「129,800元」
        if "元" not in t:
            t = f"{t}元"
        yuan_disp = t

    jpy = int(round(cny * rate))
    jpy_fmt = f"{jpy:,}"
    return f"{yuan_disp}（日本円{jpy_fmt}円）"

def dealer_to_yuan_only(cell: str) -> str:
    """
    ディーラー価格は「…元」だけ（円は付けない）。
    ・既存の円表記は除去
    ・「11.98万」には「元」を明記して「11.98万元」
    """
    t = str(cell).strip()
    if not t or t in {"-", "–", "—"}:
        return t
    t = RE_JPY_PAREN.sub("", t).strip()
    if ("元" not in t) and RE_WAN.search(t):
        t = f"{t}元"
    return t

# ====== LLM ======
def uniq(seq):
    s, out = set(), []
    for x in seq:
        if x not in s:
            s.add(x); out.append(x)
    return out

def chunked(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def parse_json_relaxed(content: str, terms: list[str]) -> dict[str, str]:
    try:
        data = json.loads(content)
        if isinstance(data, dict) and "translations" in data:
            m = {}
            for d in data["translations"]:
                cn = str(d.get("cn", "")).strip()
                ja = str(d.get("ja", "")).strip()
                if cn:
                    m[cn] = ja or cn
            if m:
                return m
    except Exception:
        pass
    mjson = re.search(r"\{[\s\S]*\}", content)
    if mjson:
        try:
            data = json.loads(mjson.group(0))
            if isinstance(data, dict) and "translations" in data:
                m = {}
                for d in data["translations"]:
                    cn = str(d.get("cn", "")).strip()
                    ja = str(d.get("ja", "")).strip()
                    if cn:
                        m[cn] = ja or cn
                if m:
                    return m
        except Exception:
            pass
    m = {}
    for line in content.splitlines():
        if "\t" in line:
            cn, ja = line.split("\t", 1)
            cn = cn.strip(); ja = ja.strip()
            if cn:
                m[cn] = ja or cn
    for t in terms:
        m.setdefault(t, t)
    return m

class Translator:
    def __init__(self, model: str, api_key: str):
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set")
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.system = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セクション名/項目名/モデル名/セル値』の配列です。"
            "自然で簡潔な日本語へ翻訳してください。数値・年式・排量・AT/MT等の記号は保持。"
            "出力は JSON（{'translations':[{'cn':'原文','ja':'訳文'}]}）のみ。"
        )

    def translate_batch(self, terms: list[str]) -> dict[str, str]:
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

    def translate_unique(self, unique_terms: list[str]) -> dict[str, str]:
        out = {}
        for chunk in chunked(unique_terms, BATCH_SIZE):
            for attempt in range(1, RETRIES+1):
                try:
                    out.update(self.translate_batch(chunk))
                    break
                except Exception:
                    if attempt == RETRIES:
                        for t in chunk:
                            out.setdefault(t, t)
                    time.sleep(SLEEP_BASE * attempt)
        return out

# ====== 先頭車名のルールベース削除（汎用化） ======
# ヒューリスティック：
#  1) 年式/「款」などの直前までをプレフィックス候補として抽出
#  2) 列全体で頻出する先頭語を共通接頭辞とみなし削除
#  3) 明示指定（SERIES_PREFIX）があればそれを優先
YEAR_TOKEN_RE = re.compile(r"(?:20\d{2}|19\d{2})|(?:\d{2}款|[上中下]市|改款|年款)")
LEADING_TOKEN_RE = re.compile(r"^[\u4e00-\u9fffA-Za-z][\u4e00-\u9fffA-Za-z0-9\- ]{1,40}")

def cut_before_year_or_kuan(s: str) -> str | None:
    s = s.strip()
    m = YEAR_TOKEN_RE.search(s)
    if m:
        return s[:m.start()].strip()
    kuan = re.search(r"款", s)
    if kuan:
        return s[:kuan.start()].strip()
    m2 = LEADING_TOKEN_RE.match(s)
    return m2.group(0).strip() if m2 else None

def detect_common_series_prefix(cols: list[str]) -> str | None:
    cand = []
    for c in cols:
        p = cut_before_year_or_kuan(str(c))
        if p and len(p) >= 2:
            cand.append(p)
    if not cand:
        return None
    from collections import Counter
    top, ct = Counter(cand).most_common(1)[0]
    if ct >= max(1, int(0.6 * len(cols))):
        return re.escape(top)
    return None

def strip_series_prefix_from_grades(grade_cols: list[str]) -> list[str]:
    if not grade_cols or not STRIP_GRADE_PREFIX:
        return grade_cols
    pattern = SERIES_PREFIX_RE if SERIES_PREFIX_RE else detect_common_series_prefix(grade_cols)
    if not pattern:
        return grade_cols
    regex = re.compile(rf"^\s*(?:{pattern})\s*[-:：/ ]*\s*", re.IGNORECASE)
    cleaned = [regex.sub("", str(c)).strip() or c for c in grade_cols]
    return cleaned

# ====== main ======
def main():
    print(f"🔎 SRC: {SRC}")
    print(f"📝 DST(primary): {DST_PRIMARY}")
    print(f"📝 DST(secondary): {DST_SECONDARY}")

    if not Path(SRC).exists():
        print("⚠ 入力CSVが見つかりません。近傍のCSVを探索します…")
        for p in Path("output").glob("**/config_*.csv"):
            print("  -", p)
        raise FileNotFoundError(f"入力CSVが見つかりません: {SRC}")

    df = pd.read_csv(SRC, encoding="utf-8-sig")
    df = df.map(clean_any_noise)

    # 列ヘッダのブランド正規化
    df.columns = [BRAND_MAP.get(c, c) for c in df.columns]

    # セクション/項目 翻訳
    uniq_sec  = uniq([str(x).strip() for x in df["セクション"].fillna("").tolist() if str(x).strip()])
    uniq_item = uniq([str(x).strip() for x in df["項目"].fillna("").tolist() if str(x).strip()])

    tr = Translator(MODEL, API_KEY)
    sec_map  = tr.translate_unique(uniq_sec)
    item_map = tr.translate_unique(uniq_item)
    sec_map.update(FIX_JA_SECTIONS)
    item_map.update(FIX_JA_ITEMS)

    out = df.copy()
    out.insert(1, "セクション_ja", out["セクション"].map(lambda s: sec_map.get(str(s).strip(), str(s).strip())))
    out.insert(3, "項目_ja",     out["項目"].map(lambda s: item_map.get(str(s).strip(), str(s).strip())))

    # 列ヘッダ（グレード）翻訳＆先頭車名削除（汎用化）
    if TRANSLATE_COLNAMES:
        orig_cols   = list(out.columns)
        fixed_cols  = orig_cols[:4]
        grade_cols  = orig_cols[4:]
        grade_cols_norm     = [BRAND_MAP.get(c, c) for c in grade_cols]
        grade_cols_stripped = strip_series_prefix_from_grades(grade_cols_norm)
        uniq_grades = uniq([str(c).strip() for c in grade_cols_stripped])
        grade_map   = tr.translate_unique(uniq_grades)
        translated  = [grade_map.get(g, g) or g for g in grade_cols_stripped]
        out.columns = fixed_cols + translated
    else:
        if STRIP_GRADE_PREFIX:
            orig_cols   = list(out.columns)
            fixed_cols  = orig_cols[:4]
            grade_cols  = orig_cols[4:]
            out.columns = fixed_cols + strip_series_prefix_from_grades(grade_cols)

    # ===== 価格セル整形 =====
    is_msrp_row   = out["項目"].isin(list(PRICE_ITEM_MSRP_CN)) | out["項目_ja"].isin(list(PRICE_ITEM_MSRP_JA))
    is_dealer_row = out["項目"].isin(list(PRICE_ITEM_DEALER_CN)) | out["項目_ja"].isin(list(PRICE_ITEM_DEALER_JA))

    # MSRP: 「xx万元（日本円YYY円）」、Dealer: 「…元」のみ
    for col in out.columns[4:]:
        out.loc[is_msrp_row, col] = out.loc[is_msrp_row, col].map(
            lambda s: msrp_to_yuan_and_jpy(clean_price_cell(s), EXRATE_CNY_TO_JPY)
        )
        out.loc[is_dealer_row, col] = out.loc[is_dealer_row, col].map(
            lambda s: dealer_to_yuan_only(clean_price_cell(s))
        )

    # 値セルの翻訳（価格行は対象外）
    if TRANSLATE_VALUES:
        values = []
        numeric_like = re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")
        non_price_mask = ~(is_msrp_row | is_dealer_row)
        for col in out.columns[4:]:
            for v in out.loc[non_price_mask, col].astype(str).tolist():
                vv = v.strip()
                if vv in {"", "●", "○", "–", "-", "—"}:
                    continue
                if numeric_like.fullmatch(vv):
                    continue
                values.append(vv)
        uniq_vals = uniq(values)
        val_map = tr.translate_unique(uniq_vals)
        for col in out.columns[4:]:
            out.loc[non_price_mask, col] = out.loc[non_price_mask, col].map(
                lambda s: val_map.get(str(s).strip(), str(s).strip())
            )

    # 出力（Artifacts 揺れ対策で二重書き）
    DST_PRIMARY.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST_PRIMARY, index=False, encoding="utf-8-sig")
    out.to_csv(DST_SECONDARY, index=False, encoding="utf-8-sig")

    print(f"✅ Saved: {DST_PRIMARY.resolve()}")
    print(f"✅ Saved: {DST_SECONDARY.resolve()}")
    print(f"📦 Exists (primary)? {DST_PRIMARY.exists()}")
    print(f"📦 Exists (secondary)? {DST_SECONDARY.exists()}")

if __name__ == "__main__":
    main()
