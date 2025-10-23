# tools/translate_columns.py
from __future__ import annotations
import os, json, time, re
from pathlib import Path
import pandas as pd
from openai import OpenAI

SRC = Path(os.environ.get("CSV_IN",  "output/autohome/7578/config_7578.csv"))
DST = Path(os.environ.get("CSV_OUT", "output/autohome/7578/config_7578_ja.csv"))
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
API_KEY = os.environ.get("OPENAI_API_KEY")
TRANSLATE_VALUES = os.environ.get("TRANSLATE_VALUES", "true").lower() == "true"

# 為替レート（CNY→JPY）: デフォルト 21.0
EXRATE_CNY_TO_JPY = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))

BATCH_SIZE = 60
RETRIES = 3

# -------------------------------------------------------
# ノイズ除去と固定訳
# -------------------------------------------------------
NOISE_PRICE_TAIL = ["询价", "计算器", "报价"]
NOISE_ANY = ["对比", "参数", "图片", "配置", "详情"]

def clean_text(s: str) -> str:
    s = str(s).strip()
    for w in NOISE_ANY + NOISE_PRICE_TAIL:
        s = s.replace(w, "")
    s = re.sub(r"\s+", " ", s)
    return s.strip(" -　")

# -------------------------------------------------------
# 固定訳辞書
# -------------------------------------------------------
BRAND_MAP = {
    "BYD": "BYD",  # ← 翻訳しない
    "比亚迪": "BYD",
    "NIO": "蔚来",
    "XPeng": "小鵬",
    "Geely": "吉利",
    "Changan": "長安",
    "Chery": "奇瑞",
    "Li Auto": "理想",
    "AITO": "問界",
}

FIX_JA_ITEMS = {
    "厂商指导价": "メーカー希望小売価格（元）",
    "经销商报价": "ディーラー販売価格（元）",
    "经销商参考价": "ディーラー販売価格（元）",
    "被动安全": "衝突安全",
}

FIX_JA_SECTIONS = {"被动安全": "衝突安全"}

PRICE_ITEM_CN = {"厂商指导价", "经销商报价", "经销商参考价"}
PRICE_ITEM_JA = {"メーカー希望小売価格（元）", "ディーラー販売価格（元）"}

# -------------------------------------------------------
# 円併記処理 ("11.98万中国元（約¥251,580）")
# -------------------------------------------------------
RE_WAN = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*万")
RE_YUAN = re.compile(r"(?P<num>[\d,]+)\s*元")

def append_jpy(s: str, rate: float) -> str:
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
    if cny is None:
        return t
    jpy = int(round(cny * rate))
    jpy_fmt = f"{jpy:,}"
    # 「中国元」がない場合は補う
    if "元" not in t:
        t = f"{t}中国元"
    if "¥" in t:
        return t
    return f"{t}（約¥{jpy_fmt}）"

# -------------------------------------------------------
# OpenAI翻訳まわり
# -------------------------------------------------------
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
            return {d["cn"]: d["ja"] for d in data["translations"] if "cn" in d and "ja" in d}
    except Exception:
        pass
    for line in content.splitlines():
        if "\t" in line:
            cn, ja = line.split("\t", 1)
            yield (cn.strip(), ja.strip())

class Translator:
    def __init__(self, model: str, api_key: str):
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.system = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セクション名/項目名/モデル名/セル値』の配列です。"
            "自然で簡潔な日本語へ翻訳してください。"
            "数値・単位は保持。JSONで {'translations':[{'cn':'原文','ja':'訳文'}]} で返すこと。"
        )

    def translate_batch(self, terms):
        msg = [
            {"role": "system", "content": self.system},
            {"role": "user", "content": json.dumps({"terms": terms}, ensure_ascii=False)},
        ]
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=msg,
            temperature=0,
            response_format={"type": "json_object"},
        )
        c = resp.choices[0].message.content or ""
        try:
            data = json.loads(c)
            if "translations" in data:
                return {d["cn"]: d["ja"] for d in data["translations"]}
        except Exception:
            pass
        return {t: t for t in terms}

    def translate_unique(self, terms):
        out = {}
        for chunk in chunked(terms, BATCH_SIZE):
            out.update(self.translate_batch(chunk))
        return out

# -------------------------------------------------------
# main
# -------------------------------------------------------
def main():
    df = pd.read_csv(SRC, encoding="utf-8-sig")
    df = df.map(clean_text)
    df.columns = [BRAND_MAP.get(c, c) for c in df.columns]

    uniq_sec  = uniq(df["セクション"].dropna().astype(str))
    uniq_item = uniq(df["項目"].dropna().astype(str))
    tr = Translator(MODEL, API_KEY)

    sec_map  = tr.translate_unique(uniq_sec)
    item_map = tr.translate_unique(uniq_item)
    sec_map.update(FIX_JA_SECTIONS)
    item_map.update(FIX_JA_ITEMS)

    out = df.copy()
    out.insert(1, "セクション_ja", out["セクション"].map(lambda s: sec_map.get(s, s)))
    out.insert(3, "項目_ja", out["項目"].map(lambda s: item_map.get(s, s)))

    # ---- 価格欄を日本円併記に変換 ----
    is_price_row = out["項目"].isin(list(PRICE_ITEM_CN)) | out["項目_ja"].isin(list(PRICE_ITEM_JA))
    for col in out.columns[4:]:
        out.loc[is_price_row, col] = out.loc[is_price_row, col].map(lambda s: append_jpy(clean_text(s), EXRATE_CNY_TO_JPY))

    DST.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST, index=False, encoding="utf-8-sig")
    print(f"✅ Saved translated file: {DST}")

if __name__ == "__main__":
    main()
