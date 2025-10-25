from __future__ import annotations
import os, json, time, re
from pathlib import Path
import pandas as pd
from openai import OpenAI

# ====== 入出力の決定（YAML変更なしで動くよう互換重視） ======
SERIES_ID = os.environ.get("SERIES_ID", "").strip()

def resolve_src_dst():
    # 1) ユーザ明示
    csv_in  = os.environ.get("CSV_IN", "").strip()
    csv_out = os.environ.get("CSV_OUT", "").strip()

    # 2) SERIES_ID から推定
    def guess_paths_from_series(sid: str):
        if not sid:
            return None, None
        base = f"output/autohome/{sid}/config_{sid}"
        return Path(f"{base}.csv"), Path(f"{base}.ja.csv")  # 既定は .ja.csv

    # 3) 従来の既定
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

# 互換目的：Artifacts が _ja.csv を期待しても拾えるよう、**二重出力**する
# 例) config_6337.ja.csv と config_6337_ja.csv の両方を書き出し
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

# OpenAI
MODEL   = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
API_KEY = os.environ.get("OPENAI_API_KEY")

# スイッチ
TRANSLATE_VALUES   = os.environ.get("TRANSLATE_VALUES", "true").lower() == "true"
TRANSLATE_COLNAMES = os.environ.get("TRANSLATE_COLNAMES", "true").lower() == "true"

# 先頭車名を削る（既定ON）。明示パターンは SERIES_PREFIX（例: "駆逐艦05|驱逐舰05"）
STRIP_GRADE_PREFIX = os.environ.get("STRIP_GRADE_PREFIX", "true").lower() == "true"
SERIES_PREFIX_RE   = os.environ.get("SERIES_PREFIX", "").strip()

# 為替
EXRATE_CNY_TO_JPY  = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))

BATCH_SIZE  = 60
RETRIES     = 3
SLEEP_BASE  = 1.2

# ====== クリーニング・辞書 ======
NOISE_ANY = ["对比", "参数", "图片", "配置", "详情"]
NOISE_PRICE_TAIL = ["询价", "计算器", "询底价", "报价", "价格询问", "価格問い合わせ"]

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

BRAND_MAP = {"BYD": "BYD", "比亚迪": "BYD"}

FIX_JA_ITEMS = {
    "厂商指导价":   "メーカー希望小売価格（元）",
    "经销商参考价": "ディーラー販売価格（元）",
    "经销商报价":   "ディーラー販売価格（元）",
    "被动安全":     "衝突安全",
}
FIX_JA_SECTIONS = {"被动安全": "衝突安全"}

PRICE_ITEM_CN = {"厂商指导价", "经销商参考价", "经销商报价"}
PRICE_ITEM_JA = {"メーカー希望小売価格（元）", "ディーラー販売価格（元）"}

RE_WAN  = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*万")
RE_YUAN = re.compile(r"(?P<num>[\d,]+)\s*元")

def append_jpy_with_yuan_label(s: str, rate: float) -> str:
    t = str(s).strip()
    if not t or t in {"-", "–", "—"}:
        return t
    m1 = RE_WAN.search(t)
    m2 = RE_YUAN.search(t)
    cny = None
    if m1:
        cny = float(m1.group("num")) * 10000.0
    elif m2:
        cny = float(m2.group("num").replace(",", ""))
    if cny is not None and "元" not in t:
        t = f"{t}元"
    if cny is None:
        return t
    jpy = int(round(cny * rate))
    jpy_fmt = f"{jpy:,}"
    if "（約¥" in t or "(約¥" in t:
        return t
    return f"{t}（約¥{jpy_fmt}）"

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

# ====== グレード先頭の車名削除 ======
RE_SERIES_TOKEN = re.compile(r"^[\u4e00-\u9fffA-Za-z][\u4e00-\u9fffA-Za-z0-9\- ]{1,20}")

def detect_common_prefix(candidates: list[str]) -> str | None:
    tokens = []
    for c in candidates:
        m = RE_SERIES_TOKEN.match(c.strip())
        if m:
            tokens.append(m.group(0).strip())
    if not tokens:
        return None
    from collections import Counter
    top, n = Counter(tokens).most_common(1)[0]
    if n >= max(1, int(0.8 * len(candidates))) and len(top) >= 2:
        return re.escape(top)
    return None

def strip_series_prefix_from_grades(grade_cols: list[str]) -> list[str]:
    if not grade_cols or not STRIP_GRADE_PREFIX:
        return grade_cols
    pattern = SERIES_PREFIX_RE if SERIES_PREFIX_RE else detect_common_prefix(grade_cols)
    if not pattern:
        return grade_cols
    regex = re.compile(rf"^\s*(?:{pattern})\s*[-:：/ ]*\s*", re.IGNORECASE)
    cleaned = [regex.sub("", c).strip() or c for c in grade_cols]
    return cleaned

# ====== main ======
def main():
    print(f"🔎 SRC: {SRC}")
    print(f"📝 DST(primary): {DST_PRIMARY}")
    print(f"📝 DST(secondary): {DST_SECONDARY}")

    if not Path(SRC).exists():
        # よくある取り違い対策：_ja.csv を入力にしていないか等をヒント表示
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

    # 列ヘッダ（グレード）翻訳＆先頭車名削除
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

    # 価格セル：「元」明記 + 円併記
    is_price_row = out["項目"].isin(list(PRICE_ITEM_CN)) | out["項目_ja"].isin(list(PRICE_ITEM_JA))
    for col in out.columns[4:]:
        out.loc[is_price_row, col] = out.loc[is_price_row, col].map(
            lambda s: append_jpy_with_yuan_label(clean_price_cell(s), EXRATE_CNY_TO_JPY)
        )

    # 値セルの翻訳（価格行は対象外）
    if TRANSLATE_VALUES:
        values = []
        numeric_like = re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")
        for col in out.columns[4:]:
            for v in out[col].astype(str).tolist():
                vv = v.strip()
                if vv in {"", "●", "○", "–", "-", "—"}:
                    continue
                if numeric_like.fullmatch(vv):
                    continue
                values.append(vv)
        uniq_vals = uniq(values)
        val_map = tr.translate_unique(uniq_vals)
        non_price_mask = ~is_price_row
        for col in out.columns[4:]:
            out.loc[non_price_mask, col] = out[non_price_mask][col].map(
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
