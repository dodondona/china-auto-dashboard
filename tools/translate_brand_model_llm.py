#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, json, os, time
import pandas as pd
from pandas.errors import EmptyDataError, ParserError

# ==== Anthropic (翻訳専用) ====
from anthropic import Anthropic
CLAUDE_MODEL_DEFAULT = "claude-3-5-sonnet-20241022"

def _read_csv_loose(path:str) -> pd.DataFrame:
    tries = [
        dict(sep=",", header=0, dtype=str, encoding="utf-8", keep_default_na=False),
        dict(sep=",", header=0, dtype=str, encoding="utf-8-sig", keep_default_na=False),
        dict(sep=",", header=None,
             names=["rank_seq","rank","brand","model","count","series_url",
                    "brand_conf","series_conf","title_raw"],
             dtype=str, encoding="utf-8", keep_default_na=False, engine="python",
             on_bad_lines="skip"),
    ]
    last = None
    for kw in tries:
        try:
            df = pd.read_csv(path, **kw)
            for c in ["brand","model","title_raw"]:
                if c in df.columns:
                    df[c] = df[c].astype(str).fillna("")
            return df
        except (EmptyDataError, ParserError) as e:
            last = e; time.sleep(0.2)
    raise SystemExit(f"[FATAL] CSV read failed: {path} ({last})")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--model", default=CLAUDE_MODEL_DEFAULT)
    args = ap.parse_args()

    df = _read_csv_loose(args.input)

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise SystemExit("[FATAL] ANTHROPIC_API_KEY not set.")
    client = Anthropic(api_key=api_key)

    out_b, out_m = [], []
    sys = "Return only JSON like {\"brand_ja\":\"…\",\"model_ja\":\"…\"}."

    for _, row in df.iterrows():
        b = (row.get("brand") or "").strip()
        m = (row.get("model") or "").strip()
        t = (row.get("title_raw") or "").strip()
        if not (b or m):
            out_b.append(""); out_m.append(""); continue

        user = (
          "中国ブランド・車名を日本語表記にしてください。ブランドは日本で一般的な表記、"
          "車名はカタカナ/英字の適切な併記可。追加説明なしでJSONのみ返答。\n"
          f"brand_cn: {b}\nmodel_cn: {m}\n(title_raw: {t})"
        )
        try:
            resp = client.messages.create(
                model=args.model,
                system=sys,
                max_tokens=200,
                messages=[{"role":"user","content":user}],
            )
            txt = resp.content[0].text if resp and resp.content else ""
            s = txt.find("{"); e = txt.rfind("}")
            bj, mj = b, m
            if s!=-1 and e!=-1 and e>s:
                obj = json.loads(txt[s:e+1])
                bj = (obj.get("brand_ja") or bj).strip()
                mj = (obj.get("model_ja") or m).strip()
            out_b.append(bj); out_m.append(mj)
        except Exception:
            out_b.append(b); out_m.append(m)

        time.sleep(0.3)

    df["brand_ja"] = out_b
    df["model_ja"] = out_m
    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"Saved: {args.output}")

if __name__ == "__main__":
    main()
