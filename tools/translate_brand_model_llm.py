#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, json, os, time
from pathlib import Path
import pandas as pd
from pandas.errors import EmptyDataError, ParserError

# ===== Anthropic =====
try:
    from anthropic import Anthropic
except Exception:
    Anthropic = None

CACHE_DIR = Path("tools/.llm_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHE_DIR / "anthropic_translations.json"


def _safe_read_csv(path: str) -> pd.DataFrame:
    """
    できるだけ壊れにくく CSV を読む。
    - 文字コード: utf-8 → utf-8-sig フォールバック
    - 区切り: 明示 sep=","、engine を python にもフォールバック
    - ヘッダが読めない場合は列名を指定して救済
    - すべて文字列・欠損は空文字に統一
    """
    candidates = [
        dict(sep=",", header=0, dtype=str, encoding="utf-8", keep_default_na=False),
        dict(sep=",", header=0, dtype=str, encoding="utf-8-sig", keep_default_na=False),
        dict(sep=",", header=None,
             names=["rank_seq","rank","brand","model","count",
                    "series_url","brand_conf","series_conf","title_raw"],
             dtype=str, encoding="utf-8", keep_default_na=False,
             engine="python", on_bad_lines="skip"),
    ]
    last_err = None
    for kw in candidates:
        try:
            df = pd.read_csv(path, **kw)
            # 期待列が無いケースに備え、存在する列だけを使う
            for col in ["brand","model","title_raw","series_url","rank","count"]:
                if col in df.columns:
                    df[col] = df[col].astype(str).fillna("")
            return df
        except (EmptyDataError, ParserError) as e:
            last_err = e
            time.sleep(0.2)
            continue
    raise SystemExit(f"[FATAL] Failed to read CSV: {path} ({last_err})")


def load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_cache(cache: dict) -> None:
    try:
        CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def build_prompt(brand_cn: str, model_cn: str, title_raw: str) -> str:
    return (
        "あなたは中国自動車モデル名の対訳アシスタントです。\n"
        "入力の中国語ブランド名・車種名（必要ならtitle_rawも参照）から、\n"
        "日本語の読み（ブランドは日本で一般的な呼称、車種はカタカナ/英字併記可）を返してください。\n"
        "出力はJSONで: {\"brand_ja\":\"...\",\"model_ja\":\"...\"} のみ。\n\n"
        f"brand_cn: {brand_cn}\n"
        f"model_cn: {model_cn}\n"
        f"title_raw: {title_raw}\n"
    )


def translate_rows_with_claude(df: pd.DataFrame, model: str) -> pd.DataFrame:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise SystemExit("[FATAL] ANTHROPIC_API_KEY が未設定です。")

    if Anthropic is None:
        raise SystemExit("[FATAL] anthropic ライブラリが見つかりません。requirements を確認してください。")

    client = Anthropic(api_key=api_key)
    cache = load_cache()

    out_brand = []
    out_model = []

    for _, row in df.iterrows():
        b = (row.get("brand") or "").strip()
        m = (row.get("model") or "").strip()
        t = (row.get("title_raw") or "").strip()
        key = f"{b}|{m}"

        # 既存キャッシュ
        if key in cache:
            out_brand.append(cache[key]["brand_ja"])
            out_model.append(cache[key]["model_ja"])
            continue

        prompt = build_prompt(b, m, t)
        try:
            resp = client.messages.create(
                model=model,
                system="Return only valid JSON. No explanation.",
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}],
            )
            text = resp.content[0].text if resp and resp.content else ""
            # ざっくりJSON抽出
            start = text.find("{")
            end = text.rfind("}")
            brand_ja, model_ja = "", ""
            if start != -1 and end != -1 and end > start:
                body = text[start : end + 1]
                try:
                    js = json.loads(body)
                    brand_ja = (js.get("brand_ja") or "").strip()
                    model_ja = (js.get("model_ja") or "").strip()
                except Exception:
                    pass

            # フォールバック（空はそのまま）
            brand_ja = brand_ja or b
            model_ja = model_ja or m

            cache[key] = {"brand_ja": brand_ja, "model_ja": model_ja}
            out_brand.append(brand_ja)
            out_model.append(model_ja)

            # 軽いレート制御
            time.sleep(0.4)

        except Exception as e:
            # 失敗時はそのまま通す（後工程で修正可能にする）
            out_brand.append(b)
            out_model.append(m)

    save_cache(cache)
    df["brand_ja"] = out_brand
    df["model_ja"] = out_model
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--provider", default="anthropic")
    ap.add_argument("--model", default="claude-3-5-sonnet-20241022")
    args = ap.parse_args()

    df = _safe_read_csv(args.input)

    # 最低限の列を保証
    for col in ["brand", "model", "title_raw"]:
        if col not in df.columns:
            df[col] = ""

    if args.provider != "anthropic":
        raise SystemExit("[FATAL] 現在は --provider anthropic のみ対応です。")

    df = translate_rows_with_claude(df, args.model)

    # 出力（既存列は可能な限り維持）
    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"Saved: {args.output}")


if __name__ == "__main__":
    main()
