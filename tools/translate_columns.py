# tools/translate_columns.py
from __future__ import annotations
import os, json, time, re
from pathlib import Path
import pandas as pd
from openai import OpenAI

SRC = Path(os.environ.get("CSV_IN",  "output/autohome/7578/config_7578.csv"))
DST = Path(os.environ.get("CSV_OUT", "output/autohome/7578/config_7578_ja.csv"))
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")  # 品質寄りは gpt-4.1 推奨
API_KEY = os.environ.get("OPENAI_API_KEY")

# true/false でセル本文の翻訳を有効化
TRANSLATE_VALUES = os.environ.get("TRANSLATE_VALUES", "true").lower() == "true"

BATCH_SIZE = 60
RETRIES = 3
SLEEP_BASE = 1.2

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
    # 期待: {"translations":[{"cn":"…","ja":"…"}, ...]}
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
    for line in content.splitlines():
        if "\t" in line:
            cn, ja = line.split("\t", 1)
            cn = cn.strip(); ja = ja.strip()
            if cn:
                mapp[cn] = ja or cn
    for t in terms:
        mapp.setdefault(t, t)
    return mapp

class Translator:
    def __init__(self, model: str, api_key: str, do_not_translate: list[str] = None):
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set")
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.dnt = do_not_translate or []
        self.system = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セクション名/項目名/セル値』の配列です。"
            "自然で簡潔な日本語へ翻訳してください。"
            "車名・グレード名・列ヘッダは訳さない（原文維持）。"
            "数値や単位は保持（例: 140kW, 1,920mm）。"
            "出力は JSON で、{'translations': [{'cn':'原文','ja':'訳文'}, ...]} の形式のみで返してください。"
        )
        self.jargon = (
            "用語指針: 车身→車体, 外观→外観, 灯光→照明, 方向盘→ステアリング, 后视镜→ミラー, "
            "座椅→シート, 底盘→シャシー, 转向→ステアリング, 制动→ブレーキ, 多媒体→マルチメディア, "
            "电机/电动机→電動機, 电池→バッテリー, 充电→充電, 发动机→エンジン, 智能→スマート, "
            "主动安全→予防安全, 被动安全→受動安全。句読点は不要。"
        )

    def translate_batch(self, terms: list[str]) -> dict[str, str]:
        # Do-Not-Translate提示・正規表現ガード
        payload = {
            "do_not_translate": self.dnt[:200],  # プロンプト肥大防止
            "terms": terms
        }
        messages = [
            {"role": "system", "content": self.system},
            {"role": "user", "content": self.jargon},
            {"role": "user", "content":
                "次の語を翻訳してください。"
                "do_not_translate に含まれる語やその完全一致は原文維持。"
                "数字と単位は保持。JSONのみで返すこと。\n" +
                json.dumps(payload, ensure_ascii=False)}
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

def is_symbol_or_empty(s: str) -> bool:
    s = str(s).strip()
    return (s == "" or s in {"●","○","–","-","—"})

NUMERIC_LIKE = re.compile(r"^[\s\d\.,%:/xX\-＋\+\(\)~～mmcMkKwWhHVVAhL丨·—–]+$")

def should_translate_cell(s: str) -> bool:
    if is_symbol_or_empty(s):
        return False
    if NUMERIC_LIKE.fullmatch(str(s)):  # 純数値/記号のみ
        return False
    return True

def main():
    df = pd.read_csv(SRC, encoding="utf-8-sig")

    # 列ヘッダ（モデル名）は DNT に入れる
    model_headers = [c for c in df.columns[2:]]
    tr = Translator(MODEL, API_KEY, do_not_translate=model_headers)

    # 1) セクション/項目の辞書
    uniq_sec  = uniq([str(x).strip() for x in df["セクション"].fillna("").tolist() if str(x).strip()])
    uniq_item = uniq([str(x).strip() for x in df["項目"].fillna("").tolist() if str(x).strip()])

    sec_map  = tr.translate_unique(uniq_sec)
    item_map = tr.translate_unique(uniq_item)

    out = df.copy()
    out.insert(1, "セクション_ja", out["セクション"].map(lambda s: sec_map.get(str(s).strip(), str(s).strip())))
    out.insert(3, "項目_ja",     out["項目"].map(lambda s: item_map.get(str(s).strip(), str(s).strip())))

    # 2) セル本文の翻訳（任意）
    if TRANSLATE_VALUES:
        # 翻訳対象のユニーク値を集約（●/○/–や純数値は除外）
        values = []
        for col in df.columns[2:]:
            col_vals = [str(v).strip() for v in df[col].tolist()]
            values += [v for v in col_vals if should_translate_cell(v)]
        uniq_vals = uniq(values)
        print(f"Translating cell values: {len(uniq_vals)} unique terms")

        val_map = tr.translate_unique(uniq_vals)

        # 置換適用（数値や記号はそのまま）
        for col in out.columns[4:]:
            out[col] = out[col].map(lambda s: val_map.get(str(s).strip(), str(s).strip()))

    DST.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST, index=False, encoding="utf-8-sig")
    print(f"✅ Saved translated file: {DST}")

if __name__ == "__main__":
    main()
