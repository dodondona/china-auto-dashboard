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
        try:
            sid = str(int(sid))
        except Exception:
            return None, None
        src = Path(f"output/autohome/{sid}/config_{sid}.csv")
        dst = Path(f"output/autohome/{sid}/config_{sid}.ja.csv")
        return src, dst

    # 既定値（ローカル試験用）
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
    else:
        s2 = s.replace(".csv", "_ja.csv")
    return dst.parent / s2

DST_SECONDARY = make_secondary(DST_PRIMARY)

# ====== OpenAI ======
def get_openai() -> OpenAI | None:
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not key:
        return None
    try:
        client = OpenAI(api_key=key)
        return client
    except Exception:
        return None

CLIENT = get_openai()
MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"

def chat_translate_batch(items: list[str]) -> list[str]:
    """LLMで簡易翻訳（項目ラベル向け）。失敗時は原文返し。"""
    if CLIENT is None or not items:
        return items
    sys = "表の見出し・短いラベルを簡潔に日本語化してください。数値と単位は維持。1行1訳で、順番を保って返答。"
    user = "\n".join(f"- {x}" for x in items)
    try:
        r = CLIENT.chat.completions.create(
            model=MODEL,
            messages=[{"role":"system","content":sys},{"role":"user","content":user}],
            temperature=0.2,
        )
        txt = r.choices[0].message.content.strip()
        lines = [ln.strip(" -•\t") for ln in txt.splitlines() if ln.strip()]
        if len(lines) != len(items):
            # 行数ずれは安全優先で原文埋め
            while len(lines) < len(items):
                lines.append(items[len(lines)])
            lines = lines[:len(items)]
        return lines
    except Exception:
        return items

# ====== CSV I/O ======
def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"入力CSVが見つかりません: {path}")
    return pd.read_csv(path, encoding="utf-8-sig")

def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")

# ====== 前処理 ======
def ensure_required_columns(df: pd.DataFrame) -> None:
    need = ["セクション", "項目"]
    for col in need:
        if col not in df.columns:
            raise SystemExit(f"必須列が不足: {need}  取得列: {list(df.columns)}")

def is_price_row_label(s: str) -> bool:
    x = str(s or "").strip()
    return x in {"厂商指导价", "经销商报价", "メーカー希望小売価格", "ディーラー販売価格"}

def is_trivial(s: str) -> bool:
    if s is None:
        return True
    x = str(s).strip()
    if not x:
        return True
    if re.fullmatch(r"[-—–·\.\/\s]+", x):
        return True
    if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?(?:\s*[万千kK])?(?:\s*[元円¥])?", x):
        return True
    return False

# ====== 価格整形 ======
CNY_TO_JPY = float(os.environ.get("CNY_TO_JPY", "20.0"))

def parse_cny_amount(raw: str) -> float | None:
    if not raw:
        return None
    x = str(raw).replace(",", "").strip()
    m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*万", x)
    if m:
        return float(m.group(1)) * 10000.0
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*元?", x)
    if m:
        return float(m.group(1))
    return None

def fmt_jpy(n: float) -> str:
    return "¥{:,}".format(int(round(n)))

def format_price_cell(cell: str) -> str:
    x = (cell or "").strip()
    if not x:
        return x
    amt = parse_cny_amount(x)
    if amt is None:
        return x
    if "万" in x:
        # 原文の先頭数値（小数あり）を維持
        m = re.match(r"([0-9]+(?:\.[0-9]+)?)", x.replace(" ", ""))
        base = m.group(1) if m else "{:.2f}".format(amt / 10000.0)
        jpy = fmt_jpy(amt * CNY_TO_JPY)
        return f"{base}万元（日本円{jpy}）"
    else:
        jpy = fmt_jpy(amt * CNY_TO_JPY)
        return f"{int(amt)}元（日本円{jpy}）"

# ====== 本体 ======
def main():
    print(f"SRC={SRC}")
    print(f"DST_PRIMARY={DST_PRIMARY}")
    df = read_csv(SRC)
    ensure_required_columns(df)

    # 出力骨格
    out = pd.DataFrame(index=df.index)
    out["セクション"] = df["セクション"]
    out["項目"] = df["項目"]
    out["セクション_ja"] = ""
    out["項目_ja"] = ""
    grade_cols = [c for c in df.columns if c not in ["セクション", "項目"]]
    for c in grade_cols:
        out[c] = df[c]  # ヘッダは翻訳しない

    # ---- セクション/項目の翻訳 ----
    sec_src = df["セクション"].astype(str).tolist()
    item_src = df["項目"].astype(str).tolist()

    # LLMバッチ（空/数値は除外）
    need_sec = [s for s in sec_src if s and not is_trivial(s)]
    need_item = [s for s in item_src if s and not is_trivial(s)]
    trans_sec = chat_translate_batch(need_sec)
    trans_item = chat_translate_batch(need_item)
    map_sec = {s:j for s,j in zip(need_sec, trans_sec)}
    map_item = {s:j for s,j in zip(need_item, trans_item)}

    out["セクション_ja"] = [map_sec.get(s, s) for s in sec_src]
    out["項目_ja"]       = [map_item.get(s, s) for s in item_src]

    # ---- グレード列の値 ----
    is_price = df["項目"].map(is_price_row_label)

    # 価格以外をバッチ翻訳
    non_price_values: list[str] = []
    coords: list[tuple[int,int]] = []
    for i in range(len(df)):
        if is_price.iloc[i]:
            continue
        for j, col in enumerate(grade_cols):
            val = str(df.iat[i, 2+j]) if (2+j) < df.shape[1] else ""
            v = val.strip()
            if not v or is_trivial(v):
                continue
            non_price_values.append(v)
            coords.append((i, 4+j))

    trans_vals = chat_translate_batch(non_price_values)
    for (i, out_j), v in zip(coords, trans_vals):
        out.iat[i, out_j] = v

    # 価格行はルール整形
    for i in range(len(df)):
        if not is_price.iloc[i]:
            continue
        for j, col in enumerate(grade_cols):
            raw = str(df.iat[i, 2+j]) if (2+j) < df.shape[1] else ""
            out.iat[i, 4+j] = format_price_cell(raw)

    # 最終出力（CN列は出さない）
    final_out = out[["セクション_ja", "項目_ja"] + grade_cols].copy()

    write_csv(final_out, DST_PRIMARY)
    if DST_SECONDARY:
        write_csv(final_out, DST_SECONDARY)

    print(f"✅ Wrote: {DST_PRIMARY}")
    if DST_SECONDARY:
        print(f"✅ Wrote: {DST_SECONDARY}")

if __name__ == "__main__":
    main()
