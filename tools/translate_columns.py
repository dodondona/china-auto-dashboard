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

# 為替レート（CNY→JPY）を環境変数で指定。未設定は 21.0 とする（例）
EXRATE_CNY_TO_JPY = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))

BATCH_SIZE = 60
RETRIES = 3
SLEEP_BASE = 1.2

# -----------------------
# ノイズ／用語設定
# -----------------------
# Autohome固有のUIノイズ。先に削除してから翻訳に回す。
NOISE_WORDS_ANY = [
    "计算器", "询价", "询底价", "对比", "图片", "配置", "参数", "详情", "报价",
    "价格询问", "价格問合せ", "価格問い合わせ"
]

# 価格セル専用で削る語（値の末尾にぶら下がる「询价」「计算器」など）
NOISE_PRICE_TAIL = [
    "询价", "计算器", "询底价", "价格询问", "报价"
]

# ブランド・メーカーの固定訳
BRAND_MAP = {
    "BYD": "比亜迪", "比亚迪": "比亜迪",
    "NIO": "蔚来",
    "XPeng": "小鵬", "Xpeng": "小鵬",
    "Geely": "吉利",
    "Changan": "長安",
    "Chery": "奇瑞",
    "Li Auto": "理想",
    "AITO": "問界",
    "Wuling": "五菱",
    "Ora": "欧拉",
    "Zeekr": "極氪",
    "Lynk & Co": "領克",
}

# 項目・セクションの日本語正規化（最優先）
FIX_JA_ITEMS = {
    # 価格まわり（日本語の括弧は全角）
    "厂商指导价": "メーカー希望小売価格（元）",
    "经销商参考价": "ディーラー参考価格（元）",
    "经销商报价": "ディーラー参考価格（元）",
    # 用語系
    "被动安全": "衝突安全",   # 受動安全→衝突安全 に訂正
    "语音助手唤醒词": "音声アシスタント起動ワード",
    # よくある項目名
    "后排出风口": "後席送風口",
    "后座出风口": "後席送風口",
}

FIX_JA_SECTIONS = {
    "被动安全": "衝突安全",
}

# 価格項目判定（CN/JA両対応）
PRICE_ITEM_CN = {"厂商指导价", "经销商参考价", "经销商报价"}
PRICE_ITEM_JA = {"メーカー希望小売価格（元）", "ディーラー参考価格（元）"}

# -----------------------
# クリーニング関数
# -----------------------
def clean_any_noise(s: str) -> str:
    s = str(s) if s is not None else ""
    for w in NOISE_WORDS_ANY:
        s = s.replace(w, "")
    s = re.sub(r"\s+", " ", s).strip(" 　-—–")
    return s

def clean_price_cell(s: str) -> str:
    s = clean_any_noise(s)
    # 価格セル末尾のノイズ語を削る（"11.98万 询价" → "11.98万"）
    for w in NOISE_PRICE_TAIL:
        s = re.sub(rf"(?:\s*{re.escape(w)}\s*)+$", "", s)
    return s.strip()

# -----------------------
# 価格 → 円併記
# -----------------------
RE_WAN = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*万")
RE_YUAN = re.compile(r"(?P<num>[\d,]+)\s*元")

def append_jpy(s: str, rate: float) -> str:
    """
    '11.98万' → '11.98万（約¥xxx）'
    '119,800元' → '119,800元（約¥xxx）'
    """
    t = str(s)
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
    # カンマ区切り
    jpy_fmt = f"{jpy:,}"
    # すでに（約¥…）があれば重複防止
    if "（約¥" in t or "(約¥" in t:
        return t
    return f"{t}（約¥{jpy_fmt}）"

# -----------------------
# 目覚めワードの整形（語をダブルクォートで囲む）
# -----------------------
def format_wake_words(s: str) -> str:
    # 例: 小迪, 小迪同学 → "小迪", "小迪同学"
    raw = clean_any_noise(s)
    # 区切りは逗点/読点/空白/スラッシュ等を包括
    tokens = re.split(r"[，,、/|｜\s]+", raw)
    tokens = [t for t in tokens if t]
    if not tokens:
        return raw
    quoted = ['"{}"'.format(t) for t in tokens]
    return ", ".join(quoted)

# -----------------------
# LLM 翻訳
# -----------------------
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
    mapp = {}
    # 想定: {"translations":[{"cn":"…","ja":"…"}, ...]}
    try:
        data = json.loads(content)
        if isinstance(data, dict) and "translations" in data:
            for d in data["translations"]:
                cn = str(d.get("cn", "")).strip()
                ja = str(d.get("ja", "")).strip()
                if cn:
                    mapp[cn] = ja or cn
            if mapp: return mapp
    except Exception:
        pass
    m = re.search(r"\{[\s\S]*\}", content)
    if m:
        try:
            data = json.loads(m.group(0))
            if isinstance(data, dict) and "translations" in data:
                for d in data["translations"]:
                    cn = str(d.get("cn", "")).strip()
                    ja = str(d.get("ja", "")).strip()
                    if cn:
                        mapp[cn] = ja or cn
                if mapp: return mapp
        except Exception:
            pass
    # タブ区切りフォールバック
    for line in content.splitlines():
        if "\t" in line:
            cn, ja = line.split("\t", 1)
            mapp[cn.strip()] = ja.strip()
    for t in terms:
        mapp.setdefault(t, t)
    return mapp

class Translator:
    def __init__(self, model: str, api_key: str):
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set")
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.system = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セクション名/項目名/モデル名/セル値』の配列です。"
            "自然で簡潔な日本語へ翻訳してください。"
            "数値・単位は保持。JSONで {'translations':[{'cn':'原文','ja':'訳文'}]} の形式のみで返してください。"
        )
        self.jargon = (
            "用語指針: 车身→車体, 外观→外観, 灯光→照明, 方向盘→ステアリング, 后视镜→ミラー, "
            "座椅→シート, 底盘→シャシー, 转向→ステアリング, 制动→ブレーキ, 多媒体→マルチメディア, "
            "电机/电动机→電動機, 电池→バッテリー, 充电→充電, 发动机→エンジン, 智能→スマート, "
            "主动安全→予防安全, 被动安全→衝突安全。句読点は不要。"
        )

    def translate_batch(self, terms: list[str]) -> dict[str, str]:
        payload = {"terms": terms}
        messages = [
            {"role": "system", "content": self.system},
            {"role": "user", "content": self.jargon},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}
        ]
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0,
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content or ""
        mapp = parse_json_relaxed(content, terms)
        if sum(1 for t in terms if mapp.get(t, "") != t) == 0:
            print("⚠️ zero translation; raw head:", content[:400])
        return mapp

    def translate_unique(self, unique_terms: list[str]) -> dict[str, str]:
        out = {}
        for chunk in chunked(unique_terms, BATCH_SIZE):
            for attempt in range(1, RETRIES+1):
                try:
                    part = self.translate_batch(chunk)
                    out.update(part)
                    done = sum(1 for t in unique_terms if t in out)
                    print(f"✅ translated chunk {len(chunk)} (acc={done}/{len(unique_terms)})")
                    break
                except Exception as e:
                    print(f"⚠️ attempt {attempt} failed: {e}")
                    if attempt == RETRIES:
                        for t in chunk:
                            out.setdefault(t, t)
                    time.sleep(SLEEP_BASE * attempt)
        return out

# -----------------------
# メイン
# -----------------------
def main():
    df = pd.read_csv(SRC, encoding="utf-8-sig")

    # まず全体ノイズ除去（日本語化前）
    df = df.map(clean_any_noise)

    # ブランド辞書を列ヘッダに適用（先に翻訳の土台）
    df.columns = [BRAND_MAP.get(c, c) for c in df.columns]

    # セクション/項目のユニーク
    uniq_sec  = uniq([str(x).strip() for x in df["セクション"].fillna("").tolist() if str(x).strip()])
    uniq_item = uniq([str(x).strip() for x in df["項目"].fillna("").tolist() if str(x).strip()])

    # LLM翻訳（縦列）
    tr = Translator(MODEL, API_KEY)
    sec_map  = tr.translate_unique(uniq_sec)
    item_map = tr.translate_unique(uniq_item)

    # 最優先の手直しマップを適用（LLM訳より強い）
    for k, v in FIX_JA_SECTIONS.items():
        sec_map[k] = v
    for k, v in FIX_JA_ITEMS.items():
        item_map[k] = v

    out = df.copy()
    out.insert(1, "セクション_ja", out["セクション"].map(lambda s: sec_map.get(str(s).strip(), str(s).strip())))
    out.insert(3, "項目_ja",     out["項目"].map(lambda s: item_map.get(str(s).strip(), str(s).strip())))

    # モデル名（列ヘッダ）も翻訳対象に
    model_headers = out.columns[4:].tolist()
    model_map = tr.translate_unique(model_headers)
    # ブランドマップを上書き優先
    for k, v in BRAND_MAP.items():
        model_map[k] = v
    out.columns = list(out.columns[:4]) + [model_map.get(c, c) for c in model_headers]

    # ---- セル本文の翻訳（＋価格整形）----
    if TRANSLATE_VALUES:
        # 1) 翻訳対象セルのユニーク値
        values = []
        for col in out.columns[4:]:
            col_vals = [str(v).strip() for v in out[col].tolist()]
            # 記号・空白・純数値のみは除外（翻訳不要）
            for v in col_vals:
                if v in {"", "●", "○", "–", "-", "—"}:
                    continue
                if re.fullmatch(r"[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+", v):
                    continue
                values.append(v)
        uniq_vals = uniq(values)

        val_map = tr.translate_unique(uniq_vals)

        # 特別扱い：语音助手唤醒词 の行は、値を引用符で整形（翻訳は最小限）
        is_wake = out["項目"].isin(["语音助手唤醒词"])
        for col in out.columns[4:]:
            out.loc[is_wake, col] = out.loc[is_wake, col].map(format_wake_words)

        # 価格セルの整形（CNY表示＋日本円併記）とノイズ削除
        is_price_row = out["項目"].isin(list(PRICE_ITEM_CN)) | out["項目_ja"].isin(list(PRICE_ITEM_JA))
        for col in out.columns[4:]:
            # 先にノイズ除去
            out.loc[is_price_row, col] = out.loc[is_price_row, col].map(clean_price_cell)
            # 円併記
            out.loc[is_price_row, col] = out.loc[is_price_row, col].map(lambda s: append_jpy(s, EXRATE_CNY_TO_JPY))

        # 通常セル：訳語を適用
        non_price = ~is_price_row & ~is_wake
        for col in out.columns[4:]:
            out.loc[non_price, col] = out.loc[non_price, col].map(lambda s: val_map.get(str(s).strip(), str(s).strip()))

    DST.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST, index=False, encoding="utf-8-sig")
    print(f"✅ Saved translated file: {DST}")

if __name__ == "__main__":
    main()
