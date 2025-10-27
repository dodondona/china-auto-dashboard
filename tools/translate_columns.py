#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
tools/translate_columns.py  — full replacement

要件（このスレッドの合意をすべて反映）:
  - キャッシュは一切使わない/作らない（再利用・保存なし）
  - 入力: 「セクション」「項目」+ グレード列（CN）
  - 出力: 「セクション_ja」「項目_ja」+ グレード列（= 列ヘッダ）
      * グレード列ヘッダは『年(20xx/20xx款)より前』を捨て、残り(尾部)のみを翻訳して表示
  - セクション/項目は辞書優先、残りだけ重複排除してバッチ翻訳
  - 値セル:
      * 価格行は LLM を通さず「xx.x万元（日本円¥…）」整形（表記ゆらぎを吸収）
      * その他は重複排除してバッチ翻訳
  - 数値/記号/ダッシュ等は翻訳スキップ
  - LLM のゴミ（「計算機」「計算機と計算機」など）は安全に除去
  - YAML/ワークフローは環境変数 CSV_IN / CSV_OUT or DST_PRIMARY を想定
"""

from __future__ import annotations
import os, re
from pathlib import Path
from typing import Dict, List
import pandas as pd

# =========================
# 入出力解決
# =========================
SERIES_ID = os.environ.get("SERIES_ID", "").strip()

def resolve_src_dst():
    csv_in  = os.environ.get("CSV_IN", "").strip()
    csv_out = os.environ.get("CSV_OUT", "").strip()
    dst_primary = os.environ.get("DST_PRIMARY", "").strip()

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

    default_in  = Path("output/autohome/7578/config_7578.csv")
    default_out = Path("output/autohome/7578/config_7578.ja.csv")

    src = Path(csv_in)  if csv_in  else None
    dst = Path(csv_out) if csv_out else None
    if dst is None and dst_primary:
        dst = Path(dst_primary)

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

# =========================
# OpenAI（バッチ翻訳）
# =========================
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = (os.environ.get("OPENAI_MODEL", "") or "gpt-4o-mini").strip()
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "80"))
TIMEOUT_SEC = int(os.environ.get("TIMEOUT_SEC", "25"))

def chat_translate_batch(items: List[str]) -> List[str]:
    """短いラベル/値の日本語化。失敗時は原文返し（順序維持）。"""
    items = list(items or [])
    if not OPENAI_API_KEY or not items:
        return items
    out: List[str] = []
    import requests
    sys = (
        "自動車仕様の短い見出し/値を日本語に簡潔に訳してください。"
        "数値や単位は維持し、余計な文は書かず、入力順で1行1訳で返答。"
    )
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    for i in range(0, len(items), BATCH_SIZE):
        chunk = items[i:i+BATCH_SIZE]
        user = "\n".join(f"- {x}" for x in chunk)
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json={
                    "model": OPENAI_MODEL,
                    "messages": [{"role":"system","content":sys},{"role":"user","content":user}],
                    "temperature": 0.2,
                },
                timeout=TIMEOUT_SEC,
            )
            r.raise_for_status()
            txt = r.json()["choices"][0]["message"]["content"].strip()
            lines = [ln.strip(" -•\t") for ln in txt.splitlines() if ln.strip()]
            while len(lines) < len(chunk):
                lines.append(chunk[len(lines)])
            lines = lines[:len(chunk)]
            out.extend(lines)
        except Exception:
            out.extend(chunk)
    return out

# =========================
# CSV I/O
# =========================
def read_csv(path: Path) -> pd.DataFrame:
    if not Path(path).exists():
        raise SystemExit(f"入力CSVが見つかりません: {path}")
    return pd.read_csv(path, encoding="utf-8-sig")

def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")

# =========================
# 判定/整形ユーティリティ
# =========================
def ensure_required_columns(df: pd.DataFrame) -> None:
    need = ["セクション", "項目"]
    for col in need:
        if col not in df.columns:
            raise SystemExit(f"必須列が不足: {need}  取得列: {list(df.columns)}")

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

def clean_llm_artifacts(s: str) -> str:
    if not s:
        return s
    x = str(s)
    # “XとX” の重複を除去
    x = re.sub(r"^([\u3040-\u30FF\u4E00-\u9FFFA-Za-z0-9]{1,15})と\1$", r"\1", x)
    # 同語の単純重複
    x = re.sub(r"^(.{1,12})\1$", r"\1", x)
    x = re.sub(r"^(.{1,12})\s+\1$", r"\1", x)
    # 記号/空白整形
    x = re.sub(r"[、，,]{2,}", "、", x)
    x = re.sub(r"\s{2,}", " ", x)
    # 価格ノイズ（日本語の「計算機」も除去）
    x = re.sub(r"(計算機|计算器|询底价|询价)\b", "", x).strip()
    return x

# =========================
# 固定辞書（セクション/項目）
# =========================
SECTION_DICT: Dict[str, str] = {
    "基本参数": "基本情報", "车身参数": "車体寸法", "外部配置": "外装装備",
    "内部配置": "内装装備", "座椅配置": "シート", "安全配置": "安全装備",
    "主/被动安全": "主/受動安全", "操控配置": "走行/操縦", "智驾辅助": "運転支援",
    "驾驶辅助": "運転支援", "灯光配置": "ライト", "多媒体配置": "マルチメディア",
    "空调/冰箱": "空調/冷蔵", "动力系统": "パワートレイン", "发动机": "エンジン",
    "电机": "モーター", "变速箱": "トランスミッション", "底盘转向": "シャシー/ステアリング",
    "车轮制动": "ホイール/ブレーキ", "保修政策": "保証", "整车质保": "車両保証",
}
ITEM_DICT: Dict[str, str] = {
    "厂商指导价": "メーカー希望小売価格", "经销商报价": "ディーラー販売価格",
    "排量(L)": "排気量(L)", "最大功率(kW)": "最大出力(kW)", "最大扭矩(N·m)": "最大トルク(N·m)",
    "变速箱类型": "変速機形式", "前悬架类型": "フロントサスペンション", "后悬架类型": "リアサスペンション",
    "驱动方式": "駆動方式", "车身结构": "ボディ構造", "长*宽*高(mm)": "全長×全幅×全高(mm)",
    "轴距(mm)": "ホイールベース(mm)", "主/副驾驶安全气囊": "運転席/助手席エアバッグ",
    "前/后排头部气囊(气帘)": "前/後席カーテンエアバッグ", "车道保持辅助系统": "レーンキープアシスト",
    "自适应巡航": "アダプティブクルーズ", "并线辅助": "ブラインドスポットモニター",
    "自动驻车": "オートホールド", "电动天窗": "電動サンルーフ", "全景天窗": "パノラマサンルーフ",
    "LED日间行车灯": "LEDデイタイムランニングライト", "大灯自动开闭": "オートライト",
    "车内中控锁": "集中ドアロック", "无钥匙进入系统": "キーレスエントリー", "无钥匙启动系统": "プッシュスタート",
    "多功能方向盘": "マルチファンクションステアリング", "定速巡航": "クルーズコントロール",
    "座椅材质": "シート素材", "主驾驶座椅调节": "運転席シート調整", "副驾驶座椅调节": "助手席シート調整",
    "前/后排座椅加热": "前/後席シートヒーター", "自动空调": "オートエアコン", "后座出风口": "後席エアアウトレット",
}

# =========================
# 価格（LLM非依存）
# =========================
CNY_TO_JPY = float(os.environ.get("CNY_TO_JPY", "20.0"))

# 価格ラベル（表記ゆらぎ対応）
PRICE_RE_CN = re.compile(r"^(厂商指导价|经销商报价|经销商参考价|经销商)$")
PRICE_RE_JA = re.compile(r"^(メーカー(?:希|推)望小売価格|メーカー推奨価格|ディーラー販売価格)(?:（?\(元\))?)$")

def is_price_row_label_any(s: str) -> bool:
    if not s:
        return False
    x = str(s).strip()
    x = x.replace(" ", "").replace("　", "")
    return bool(PRICE_RE_CN.match(x) or PRICE_RE_JA.match(x))

def parse_cny_amount(raw: str) -> float | None:
    if not raw:
        return None
    x = str(raw)
    # よく混ざるノイズを先に除去
    x = re.sub(r"(計算機|计算器|询底价|询价|报价|价格询问|到店|起售|起)\b", "", x)
    x = x.replace(",", "").strip()
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
        return clean_llm_artifacts(x)  # 解析できないときはノイズだけ落として返す
    if "万" in x:
        m = re.match(r"([0-9]+(?:\.[0-9]+)?)", x.replace(" ", ""))
        base = m.group(1) if m else "{:.2f}".format(amt / 10000.0)
        jpy = fmt_jpy(amt * CNY_TO_JPY)
        return f"{base}万元（日本円{jpy}）"
    else:
        jpy = fmt_jpy(amt * CNY_TO_JPY)
        return f"{int(amt)}元（日本円{jpy}）"

# =========================
# 「年より前カット」— ヘッダ処理
# =========================
YEAR_TOKEN = re.compile(r"(20\d{2})(?:\s*款)?")  # 例: 2004款, 2025款

def split_prefix_by_year(s: str) -> tuple[str, str]:
    """
    年(20xx/20xx款)の直前で (prefix, tail) に分割。
    prefix: 年より前（シリーズ名など） / tail: 年以降
    年が無ければ (s, "")。
    """
    if not s:
        return "", ""
    x = str(s).strip()
    m = YEAR_TOKEN.search(x)
    if m:
        return x[:m.start()].rstrip(), x[m.start():].lstrip()
    return x, ""

# =========================
# 本体
# =========================
def main():
    df = read_csv(SRC)
    ensure_required_columns(df)

    # 出力骨格
    out = pd.DataFrame(index=df.index)
    out["セクション"] = df["セクション"]
    out["項目"] = df["項目"]
    out["セクション_ja"] = ""
    out["項目_ja"] = ""

    # ---- グレード列ヘッダ（列名）: 年より前カット → 年以降だけ翻訳
    all_cols = list(df.columns)
    grade_cols_src = [c for c in all_cols if c not in ["セクション", "項目"]]

    prefixes, tails = [], []
    for g in grade_cols_src:
        p, t = split_prefix_by_year(g)
        prefixes.append(p)
        tails.append(t)

    uniq_tails = [t for t in dict.fromkeys([t for t in tails if t])]
    tail_map: Dict[str, str] = {}
    if uniq_tails:
        trans_tails = chat_translate_batch(uniq_tails)
        # ノイズ掃除もかけておく
        trans_tails = [clean_llm_artifacts(z) for z in trans_tails]
        tail_map = {src: dst for src, dst in zip(uniq_tails, trans_tails)}

    # 最終ヘッダは「tail の訳のみ」（シリーズ名は含めない）
    grade_cols = []
    for p, t in zip(prefixes, tails):
        if t:
            grade_cols.append(tail_map.get(t, t))
        else:
            grade_cols.append(p or "")

    # out に元値をコピー（列数揃え）
    for c in grade_cols_src:
        out[c] = df[c]

    # ---- セクション/項目：辞書優先 → 残りだけ翻訳
    sec_src = df["セクション"].astype(str).tolist()
    item_src = df["項目"].astype(str).tolist()

    sec_ja = [SECTION_DICT.get(s) for s in sec_src]
    item_ja = [ITEM_DICT.get(s) for s in item_src]

    need: List[str] = []
    idx_sec, idx_item = [], []
    seen = set()
    for i, s in enumerate(sec_src):
        if (not sec_ja[i]) and s and (not is_trivial(s)) and s not in seen:
            need.append(s); idx_sec.append(i); seen.add(s)
    for i, s in enumerate(item_src):
        if (not item_ja[i]) and s and (not is_trivial(s)) and s not in seen:
            need.append(s); idx_item.append(i); seen.add(s)

    if need:
        trans = chat_translate_batch(need)
        trans = [clean_llm_artifacts(z) for z in trans]
        mapping = {k:v for k, v in zip(need, trans)}
        for i in idx_sec:
            sec_ja[i] = mapping.get(sec_src[i], sec_src[i])
        for i in idx_item:
            item_ja[i] = mapping.get(item_src[i], item_src[i])

    out["セクション_ja"] = sec_ja
    out["項目_ja"]       = item_ja

    # ---- 値セル：価格はルール整形 / その他はバッチ翻訳
    # 価格判定（CN/JAのゆらぎ両対応）
    is_price = out["項目"].map(is_price_row_label_any)
    if "項目_ja" in out.columns:
        is_price = is_price | out["項目_ja"].map(is_price_row_label_any)

    # 非価格セルの翻訳候補収集（重複まとめ）
    non_price_values: List[str] = []
    coords: List[tuple[int,int]] = []
    for i in range(len(out)):
        if is_price.iloc[i]:
            continue
        for j, col in enumerate(grade_cols_src):
            raw = str(df.iat[i, 2+j]) if (2+j) < df.shape[1] else ""
            val = raw.strip()
            if not val or is_trivial(val):
                continue
            non_price_values.append(val)
            coords.append((i, 4+j))  # out 内のセル座標

    # 重複ユニーク化 → バッチ翻訳
    uniq_vals = []
    pos = []
    index_map = {}
    for s in non_price_values:
        if s in index_map:
            pos.append(index_map[s])
        else:
            index_map[s] = len(uniq_vals)
            uniq_vals.append(s)
            pos.append(index_map[s])

    trans_map: Dict[str, str] = {}
    if uniq_vals:
        translated = chat_translate_batch(uniq_vals)
        translated = [clean_llm_artifacts(z) for z in translated]
        for src, ja in zip(uniq_vals, translated):
            trans_map[src] = ja

    # 反映（非価格行）
    for (i, out_j), idx in zip(coords, pos):
        src = uniq_vals[idx]
        ja  = trans_map.get(src, src)
        out.iat[i, out_j] = ja

    # 価格行（必ず日本円併記）
    for i in range(len(out)):
        if not is_price.iloc[i]:
            continue
        for j, col in enumerate(grade_cols_src):
            raw = str(df.iat[i, 2+j]) if (2+j) < df.shape[1] else ""
            out.iat[i, 4+j] = format_price_cell(raw)

    # ---- 最終出力：ヘッダ（= グレード列名）を置換
    final_out = out[["セクション_ja", "項目_ja"] + grade_cols_src].copy()
    final_out.columns = ["セクション_ja", "項目_ja"] + grade_cols

    # 保存
    DST_PRIMARY.parent.mkdir(parents=True, exist_ok=True)
    final_out.to_csv(DST_PRIMARY,   index=False, encoding="utf-8-sig")
    if DST_SECONDARY:
        final_out.to_csv(DST_SECONDARY, index=False, encoding="utf-8-sig")

    print(f"✅ Wrote: {DST_PRIMARY}")
    if DST_SECONDARY:
        print(f"✅ Wrote: {DST_SECONDARY}")

if __name__ == "__main__":
    SRC, DST_PRIMARY = resolve_src_dst()
    main()
