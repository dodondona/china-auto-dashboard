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
    """
    想定: {"translations":[{"cn":"…","ja":"…"}, ...]}
    多少崩れても最大限拾う。
    """
    mapp = {}
    # 1) 期待どおりのJSON
    try:
        data = json.loads(content)
        if isinstance(data, dict) and "translations" in data:
            for d in data["translations"]:
                cn = str(d.get("cn", "")).strip()
                ja = str(d.get("ja", "")).strip()
                if cn:
                    mapp[cn] = ja or cn
            if mapp:
                return mapp
    except Exception:
        pass
    # 2) コードブロックから抽出
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
                if mapp:
                    return mapp
        except Exception:
            pass
    # 3) タブ区切りフォールバック: "cn\tja"
    for line in content.splitlines():
        if "\t" in line:
            cn, ja = line.split("\t", 1)
            cn = cn.strip(); ja = ja.strip()
            if cn:
                mapp[cn] = ja or cn
    # 穴埋め
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
            "入力は中国語の『セクション名/項目名』の配列です。"
            "自然で簡潔な日本語へ翻訳してください。固有名詞・グレード名・車名は極力原文維持。"
            "出力は JSON で、{'translations': [{'cn':'原文','ja':'訳文'}, ...]} の形式のみで返してください。"
        )
        self.jargon = (
            "用語指針: 车身→車体, 外观→外観, 灯光→照明, 方向盘→ステアリング, 后视镜→ミラー, "
            "座椅→シート, 底盘→シャシー, 转向→ステアリング, 制动→ブレーキ, 多媒体→マルチメディア, "
            "电机/电动机→電動機, 电池→バッテリー, 充电→充電, 发动机→エンジン, 智能→スマート, "
            "主动安全→予防安全, 被动安全→受動安全。句読点は不要。"
        )

    def translate_batch(self, terms: list[str]) -> dict[str, str]:
        # Chat Completions + JSON object 指定（スキーマ強制は不可だが実用的）
        messages = [
            {"role": "system", "content": self.system},
            {"role": "user", "content": self.jargon},
            {"role": "user", "content": json.dumps({"terms": terms}, ensure_ascii=False)}
        ]
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0,
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content or ""
        mapp = parse_json_relaxed(content, terms)
        # デバッグ：0件ならレスポンスを少し出す
        if sum(1 for t in terms if mapp.get(t, "") != t) == 0:
            print("⚠️ zero translation; raw response head:", content[:400])
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
                        # フォールバック：恒等写像
                        for t in chunk:
                            out.setdefault(t, t)
                    time.sleep(SLEEP_BASE * attempt)
        return out

def main():
    df = pd.read_csv(SRC, encoding="utf-8-sig")
    uniq_sec = uniq([str(x).strip() for x in df["セクション"].fillna("").tolist() if str(x).strip()])
    uniq_item = uniq([str(x).strip() for x in df["項目"].fillna("").tolist() if str(x).strip()])

    print(f"Translating {len(uniq_sec)} sections + {len(uniq_item)} items using {MODEL}...")

    tr = Translator(MODEL, API_KEY)
    sec_map = tr.translate_unique(uniq_sec)
    item_map = tr.translate_unique(uniq_item)

    out = df.copy()
    out.insert(1, "セクション_ja", out["セクション"].map(lambda s: sec_map.get(str(s).strip(), str(s).strip())))
    out.insert(3, "項目_ja",     out["項目"].map(lambda s: item_map.get(str(s).strip(), str(s).strip())))

    DST.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(DST, index=False, encoding="utf-8-sig")
    print(f"✅ Saved translated file: {DST}")

if __name__ == "__main__":
    main()
