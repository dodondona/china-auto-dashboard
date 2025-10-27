#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
translate_columns.py  (cacheless + dict-first + batch + price formatting + brand normalization)

目的:
  - Autohome 設定CSVを日本語化して .ja.csv を出力
  - 既存の仕様を壊さない:
      * 出力列は ["セクション_ja","項目_ja"] + 既存グレード列（ヘッダは翻訳しない）
      * ヒア構文なし / YAML変更不要
  - 不具合対策:
      * モデル名など中文が残る → 強制翻訳補助（辞書&プロンプト）
      * 価格行: 「xx.x万」等を「xx.x万元（日本円△△円）」にルール整形（LLMに任せない）
      * LLMの謎重複・ゴミ語句 → 正規化で除去
  - コスト節約:
      * セクション/項目: 辞書優先
      * in-memory短期キャッシュ（ファイルなし）
      * バッチ翻訳（既定80項目/回）
      * 数値/記号は翻訳スキップ

環境変数:
  - CSV_IN (必須)
  - DST_PRIMARY or CSV_OUT（どちらか必須）
  - DST_SECONDARY（任意）
  - OPENAI_API_KEY（任意）
  - OPENAI_MODEL（任意、既定: gpt-4o-mini）
  - BATCH_SIZE（任意、既定: 80）
  - TIMEOUT_SEC（任意、既定: 25）
  - CNY_TO_JPY（任意、既定: 20.0）  # 円換算レート
"""

from __future__ import annotations
import os, re
from pathlib import Path
from typing import Dict, List
import pandas as pd

# ====== 環境変数 ======
SRC = os.environ.get("CSV_IN", "").strip()
DST_PRIMARY = os.environ.get("DST_PRIMARY", "").strip()
CSV_OUT = os.environ.get("CSV_OUT", "").strip()
DST_SECONDARY = os.environ.get("DST_SECONDARY", "").strip()

if not SRC:
    raise SystemExit("CSV_IN が未設定です。")
if not DST_PRIMARY:
    if CSV_OUT:
        DST_PRIMARY = CSV_OUT
    else:
        raise SystemExit("DST_PRIMARY か CSV_OUT のいずれかを設定してください。")

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "80"))
TIMEOUT_SEC = int(os.environ.get("TIMEOUT_SEC", "25"))
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = (os.environ.get("OPENAI_MODEL", "") or "gpt-4o-mini").strip()
CNY_TO_JPY = float(os.environ.get("CNY_TO_JPY", "20.0"))

# ====== I/O ======
def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8-sig")

def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")

def ensure_required_columns(df: pd.DataFrame) -> None:
    need = ["セクション", "項目"]
    for col in need:
        if col not in df.columns:
            raise ValueError(f"入力CSVに必須列 {col} が見当たりません。列名: {list(df.columns)}")

# ====== 正規化/判定 ======
def is_trivial_no_translate(s: str) -> bool:
    if s is None:
        return True
    x = str(s).strip()
    if not x:
        return True
    if re.fullmatch(r"[-—–·\.\/\s]+", x):
        return True
    if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?(?:\s*[万千kK])?(?:\s*[元圆円¥])?", x):
        return True
    return False

def clean_llm_artifacts(s: str) -> str:
    """LLMの重複/ゴミ語を抑制（安全な除去のみ）"""
    if not s:
        return s
    x = str(s)
    # 「計算機と計算機」等の “XとX” 重複を削る（短語のみ）
    x = re.sub(r"^([\u3040-\u30FF\u4E00-\u9FFFA-Za-z0-9]{1,10})と\1$", r"\1", x)
    # 二重ダッシュやスペースの圧縮
    x = re.sub(r"\s{2,}", " ", x).strip()
    return x

# ====== 固定辞書 ======
SECTION_DICT: Dict[str, str] = {
    "基本参数": "基本情報", "车身": "車体", "车身参数": "車体寸法",
    "外部配置": "外装装備", "内部配置": "内装装備", "座椅配置": "シート",
    "安全配置": "安全装備", "主/被动安全": "主/受動安全",
    "操控配置": "走行/操縦", "智驾辅助": "運転支援", "驾驶辅助": "運転支援",
    "灯光配置": "ライト", "多媒体配置": "マルチメディア",
    "空调/冰箱": "空調/冷蔵", "动力系统": "パワートレイン",
    "发动机": "エンジン", "电机": "モーター", "变速箱": "トランスミッション",
    "底盘转向": "シャシー/ステアリング", "车轮制动": "ホイール/ブレーキ",
    "保修政策": "保証", "整车质保": "車両保証",
}
ITEM_DICT: Dict[str, str] = {
    "厂商指导价": "メーカー希望小売価格",
    "经销商报价": "ディーラー販売価格",
    "排量(L)": "排気量(L)",
    "最大功率(kW)": "最大出力(kW)",
    "最大扭矩(N·m)": "最大トルク(N·m)",
    "变速箱类型": "変速機形式",
    "前悬架类型": "フロントサスペンション",
    "后悬架类型": "リアサスペンション",
    "驱动方式": "駆動方式",
    "车身结构": "ボディ構造",
    "长*宽*高(mm)": "全長×全幅×全高(mm)",
    "轴距(mm)": "ホイールベース(mm)",
    "整车质保": "車両保証",
    "主/副驾驶安全气囊": "運転席/助手席エアバッグ",
    "前/后排头部气囊(气帘)": "前/後席カーテンエアバッグ",
    "车道保持辅助系统": "レーンキープアシスト",
    "自适应巡航": "アダプティブクルーズ",
    "并线辅助": "ブラインドスポットモニター",
    "自动驻车": "オートホールド",
    "电动天窗": "電動サンルーフ",
    "全景天窗": "パノラマサンルーフ",
    "LED日间行车灯": "LEDデイタイムランニングライト",
    "大灯自动开闭": "オートライト",
    "车内中控锁": "集中ドアロック",
    "无钥匙进入系统": "キーレスエントリー",
    "无钥匙启动系统": "プッシュスタート",
    "多功能方向盘": "マルチファンクションステアリング",
    "定速巡航": "クルーズコントロール",
    "座椅材质": "シート素材",
    "主驾驶座椅调节": "運転席シート調整",
    "副驾驶座椅调节": "助手席シート調整",
    "前/后排座椅加热": "前/後席シートヒーター",
    "自动空调": "オートエアコン",
    "后座出风口": "後席エアアウトレット",
}

# ブランド/車系の最小辞書（“中文が残る”対策）
BRAND_MAP = {
    "比亚迪": "BYD",
    "吉利": "ジーリー",
    "长安": "長安",
    "五菱": "五菱",
    "丰田": "トヨタ",
    "本田": "ホンダ",
    "日产": "日産",
    "大众": "フォルクスワーゲン",
    "宝马": "BMW",
    "奥迪": "アウディ",
    "奔驰": "メルセデス・ベンツ",
}
SERIES_PATTERNS = [
    (re.compile(r"([A-Z])级"), r"\1クラス"),   # E级 → Eクラス
]

def normalize_brand_series(text: str) -> str:
    if not text:
        return text
    s = str(text)
    # 先頭ブランド置換（安全に）
    for cn, ja in BRAND_MAP.items():
        if s.startswith(cn):
            s = ja + s[len(cn):]
            break
    # “E级/ S级” → “Eクラス” 置換
    for pat, repl in SERIES_PATTERNS:
        s = pat.sub(repl, s)
    return s

# ====== 価格整形（LLMに任せない） ======
PRICE_ITEM_KEYS = {"厂商指导价", "经销商报价"}
def parse_cny_amount(raw: str) -> float | None:
    """'45.18万' '11.98万' '299800元' → CNY額（元）"""
    if not raw:
        return None
    x = str(raw).strip()
    x = x.replace(",", "")
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
    """「xx.x万」→「xx.x万元（日本円△△円）」に統一。未解釈は原文."""
    x = (cell or "").strip()
    if not x:
        return x
    amt = parse_cny_amount(x)
    if amt is None:
        return x
    # 「万」表記は維持する（原文に万があればそれを優先）
    if "万" in x:
        # 小数1〜2桁に整える（原文尊重: そのままに近い）
        m = re.match(r"([0-9]+(?:\.[0-9]+)?)", x.replace(" ", ""))
        base = m.group(1) if m else "{:.2f}".format(amt / 10000.0)
        jpy = fmt_jpy(amt * CNY_TO_JPY)
        return f"{base}万元（日本円{jpy}）"
    else:
        jpy = fmt_jpy(amt * CNY_TO_JPY)
        return f"{int(amt)}元（日本円{jpy}）"

# ====== OpenAI: 重複排除＋バッチ翻訳 ======
def batch_translate_unique(values: List[str]) -> Dict[str, str]:
    uniq: List[str] = []
    for v in values:
        x = (v or "").strip()
        if not x or is_trivial_no_translate(x):
            continue
        if x not in uniq:
            uniq.append(x)

    if not uniq or not OPENAI_API_KEY:
        return {u: normalize_brand_series(u) for u in uniq}  # ブランド置換だけでも前進

    import requests
    out: Dict[str, str] = {}
    for i in range(0, len(uniq), BATCH_SIZE):
        chunk = uniq[i:i+BATCH_SIZE]
        sys_prompt = (
            "車両仕様表の短い要素を日本語に正確かつ簡潔に訳してください。"
            "数値や単位は維持。ブランドは以下の方針: "
            "『比亚迪→BYD』『奔驰→メルセデス・ベンツ』『宝马→BMW』『奥迪→アウディ』『丰田→トヨタ』『本田→ホンダ』『日产→日産』『大众→フォルクスワーゲン』。"
            "英字+級（例:E级）は『Eクラス』としてください。"
            "出力は入力順で1行1訳、余計な語は出さない。"
        )
        user_payload = "\n".join(f"- {s}" for s in chunk)
        try:
            r = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": OPENAI_MODEL,
                    "messages": [
                        {"role": "system", "content": sys_prompt},
                        {"role": "user", "content": user_payload},
                    ],
                    "temperature": 0.2,
                },
                timeout=TIMEOUT_SEC,
            )
            r.raise_for_status()
            text = r.json()["choices"][0]["message"]["content"].strip()
            lines = [ln.strip(" -•\t") for ln in text.splitlines() if ln.strip()]
            if len(lines) != len(chunk):
                while len(lines) < len(chunk):
                    # 失敗分はブランド置換だけ適用した原文
                    lines.append(normalize_brand_series(chunk[len(lines)]))
                if len(lines) > len(chunk):
                    lines = lines[:len(chunk)]
        except Exception:
            lines = [normalize_brand_series(s) for s in chunk]

        for src, ja in zip(chunk, lines):
            out[src] = clean_llm_artifacts(ja or normalize_brand_series(src))
    return out

# ====== メイン ======
def main():
    df = read_csv(Path(SRC))
    ensure_required_columns(df)

    out = pd.DataFrame(index=df.index)
    out["セクション"] = df["セクション"]
    out["項目"] = df["項目"]
    out["セクション_ja"] = ""
    out["項目_ja"] = ""

    grade_cols = [c for c in df.columns if c not in ["セクション", "項目"]]
    for c in grade_cols:
        out[c] = df[c]  # ヘッダは翻訳しない（既存仕様）

    # --- 1) セクション/項目: 辞書→バッチ ---
    sec_src = df["セクション"].astype(str).tolist()
    item_src = df["項目"].astype(str).tolist()

    sec_ja = [SECTION_DICT.get(s) for s in sec_src]
    item_ja = [ITEM_DICT.get(s) for s in item_src]

    need_sec = [s for s, v in zip(sec_src, sec_ja) if not v and not is_trivial_no_translate(s)]
    need_item = [s for s, v in zip(item_src, item_ja) if not v and not is_trivial_no_translate(s)]

    trans_map = batch_translate_unique(need_sec + need_item)

    out["セクション_ja"] = [
        (SECTION_DICT.get(s) or trans_map.get(s) or s) for s in sec_src
    ]
    out["項目_ja"] = [
        (ITEM_DICT.get(s) or trans_map.get(s) or s) for s in item_src
    ]

    # --- 2) グレード列セル: 価格はルール整形 / それ以外はバッチ翻訳 ---
    # 価格行の判定
    is_price_row = df["項目"].isin(list(PRICE_ITEM_KEYS))

    # まず翻訳候補を集める（価格行は除外してLLM節約）
    grade_values: List[str] = []
    coords: List[tuple[int, int]] = []  # (row_index, col_index_in_out)
    for i in range(len(df)):
        if is_price_row.iloc[i]:
            continue  # 価格は後でルール整形
        for j, col in enumerate(grade_cols):
            val = str(df.iat[i, 2 + j]) if (2 + j) < df.shape[1] else ""
            val = val.strip()
            if not val or is_trivial_no_translate(val):
                continue
            grade_values.append(val)
            coords.append((i, 4 + j))  # out の該当セル位置

    gmap = batch_translate_unique(grade_values)

    # 反映（非価格行）
    k = 0
    for (i, out_j) in coords:
        src = grade_values[k]
        k += 1
        out.iat[i, out_j] = gmap.get(src, normalize_brand_series(src))

    # 価格行の反映（ルール整形、（）日本円併記を必ず付ける）
    for i in range(len(df)):
        if not is_price_row.iloc[i]:
            continue
        for j, col in enumerate(grade_cols):
            raw = str(df.iat[i, 2 + j]) if (2 + j) < df.shape[1] else ""
            out.iat[i, 4 + j] = format_price_cell(raw)

    # --- 3) 最終出力（CN列は出さない）
    final_out = out[["セクション_ja", "項目_ja"] + grade_cols].copy()

    # --- 4) 保存 ---
    write_csv(final_out, Path(DST_PRIMARY))
    if DST_SECONDARY:
        write_csv(final_out, Path(DST_SECONDARY))

    print(f"✅ Wrote: {DST_PRIMARY}")
    if DST_SECONDARY:
        print(f"✅ Wrote: {DST_SECONDARY}")

if __name__ == "__main__":
    main()
