#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Autohome 設定CSV 翻訳・成形スクリプト（最小修正・堅牢化版）

要件:
- セクション_ja/項目_ja は辞書で確定（LLMは上書きしない）
- モデル列の年を厳密検出 + 前方補完 (2025款/2025.03/2025 すべて拾う)
- 2025年以降のみを残す（同年ブロックは省略列も含めて落とさない救済あり）
- “中文が含まれるなら訳す”を価格スキップより優先（最後の2行抜け対策）
- OpenAI は任意。無ければ辞書パートのみで安全動作（エラーにしない）

入出力:
- 環境変数 CSV_IN / CSV_OUT（なければ SERIES_ID から既定推定）
- 併せて DST(secondary) に _ja.csv も保存可能（CSV_ALT_OUT）
"""

from __future__ import annotations
import os
import re
import sys
import csv
import json
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd

# -----------------------------
# 入出力パス解決
# -----------------------------
SERIES_ID = os.environ.get("SERIES_ID", "").strip()

def resolve_src_dst() -> tuple[Path, Path, Optional[Path]]:
    csv_in  = os.environ.get("CSV_IN", "").strip()
    csv_out = os.environ.get("CSV_OUT", "").strip()
    csv_alt = os.environ.get("CSV_ALT_OUT", "").strip()

    def guess_from_series(sid: str) -> tuple[Path, Path, Path]:
        if not sid:
            raise SystemExit("SERIES_ID が未設定で CSV_IN も未指定です。")
        base = Path("output") / "autohome" / sid
        base.mkdir(parents=True, exist_ok=True)
        return (
            base / f"config_{sid}.csv",
            base / f"config_{sid}.ja.csv",
            base / f"config_{sid}_ja.csv",
        )

    if csv_in and csv_out:
        src = Path(csv_in)
        dst = Path(csv_out)
        alt = Path(csv_alt) if csv_alt else None
    else:
        src, dst, alt_guess = guess_from_series(SERIES_ID)
        alt = Path(csv_alt) if csv_alt else alt_guess

    if not src.exists():
        raise SystemExit(f"入力CSVが見つかりません: {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    if alt:
        alt.parent.mkdir(parents=True, exist_ok=True)
    return src, dst, alt

# -----------------------------
# 固定辞書（セクション/項目）
# -----------------------------
SECTION_MAP = {
    "基本参数": "基本",
    "车身": "ボディ",
    "发动机": "エンジン",
    "变速箱": "トランスミッション",
    "底盘转向": "シャシー／ステアリング",
    "车轮制动": "ホイール／ブレーキ",
    "被动安全": "受動安全装置",
    "主动安全": "能動安全装置",
    "驾驶操控": "ドライビング／操縦",
    "驾驶硬件": "運転支援ハードウェア",
    "驾驶功能": "運転支援機能",
    "外观/防盗": "外装／防盗",
    "车外灯光": "車外照明",
    "天窗/玻璃": "サンルーフ／ウインドウ",
    "外后视镜": "ドアミラー",
    "屏幕/系统": "ディスプレイ／車載システム",
    "智能化配置": "インテリジェント化",
    "车内充电": "車内充電",
    "方向盘/内后视镜": "ステアリング／ルームミラー",
    "座椅配置": "シート",
    "音响/车内灯光": "オーディオ／室内照明",
    "空调/冰箱": "空調／冷蔵",
    "颜色": "カラー",
    "选装包": "オプションパッケージ",
}

ITEM_MAP = {
    "厂商指导价(元)": "メーカー希望小売価格",
    "经销商报价": "ディーラー販売価格（元）",
    "厂商": "メーカー",
    "级别": "車格",
    "能源类型": "燃料種別",
    "环保标准": "排出ガス基準",
    "上市时间": "発売時期",
    "最大功率(kW)": "最大出力（kW）",
    "最大扭矩(N·m)": "最大トルク（N·m）",
    "变速箱": "トランスミッション",
    "车身结构": "ボディ構造",
    "发动机": "エンジン",
    "长*宽*高(mm)": "全長×全幅×全高（mm）",
    "官方0-100km/h加速(s)": "0-100km/h加速（公式）（s）",
    "最高车速(km/h)": "最高速度（km/h）",
    "WLTC综合油耗(L/100km)": "WLTC総合燃費（L/100km）",
    "整车质保": "車両保証",
    "整备质量(kg)": "車両重量（kg）",
    "最大满载质量(kg)": "最大総重量（kg）",
    "长度(mm)": "全長（mm）",
    "宽度(mm)": "全幅（mm）",
    "高度(mm)": "全高（mm）",
    "轴距(mm)": "ホイールベース（mm）",
    "前轮距(mm)": "フロントトレッド（mm）",
    "后轮距(mm)": "リアトレッド（mm）",
    "接近角(°)": "アプローチアングル（°）",
    "离去角(°)": "デパーチャーアングル（°）",
    "车门开启方式": "ドア開閉方式",
    "车门数(个)": "ドア数（枚）",
    "座位数(个)": "乗車定員（名）",
    "油箱容积(L)": "燃料タンク容量（L）",
    "后备厢容积(L)": "ラゲッジ容量（L）",
    "风阻系数(Cd)": "空気抵抗係数（Cd）",
    "发动机型号": "エンジン型式",
    "排量(mL)": "総排気量（mL）",
    "排量(L)": "総排気量（L）",
    "进气形式": "過給方式",
    "发动机布局": "エンジン配置",
    "气缸排列形式": "シリンダー配列",
    "气缸数(个)": "シリンダー数（個）",
    "每缸气门数(个)": "1気筒当たりバルブ数（個）",
    "配气机构": "バルブ機構",
    "最大马力(Ps)": "最高出力（Ps）",
    "最大功率转速(rpm)": "最大出力回転数（rpm）",
    "最大扭矩转速(rpm)": "最大トルク回転数（rpm）",
    "最大净功率(kW)": "最大正味出力（kW）",
    "燃油标号": "推奨燃料オクタン価",
    "供油方式": "燃料供給方式",
    "缸盖材料": "シリンダーヘッド材質",
    "缸体材料": "シリンダーブロック材質",
    "简称": "略称",
    "挡位个数": "段数",
    "变速箱类型": "トランスミッション形式",
    "驱动方式": "駆動方式",
    "四驱形式": "四輪駆動方式",
    "中央差速器结构": "センターデフ構造",
    "前悬架类型": "フロントサスペンション形式",
    "后悬架类型": "リアサスペンション形式",
    "助力类型": "ステアリングアシスト方式",
    "车体结构": "フレーム構造",
    "前制动器类型": "フロントブレーキ形式",
    "后制动器类型": "リアブレーキ形式",
    "驻车制动类型": "パーキングブレーキ形式",
    "前轮胎规格": "フロントタイヤサイズ",
    "后轮胎规格": "リアタイヤサイズ",
    "备胎规格": "スペアタイヤ仕様",
    "主/副驾驶座安全气囊": "運転席／助手席エアバッグ",
    "前/后排侧气囊": "前席／後席サイドエアバッグ",
    "前/后排头部气囊(气帘)": "前後席カーテンエアバッグ",
    "膝部气囊": "ニーエアバッグ",
    "前排中间气囊": "前席センターエアバッグ",
    "被动行人保护": "歩行者保護（受動）",
    "ABS防抱死": "ABS（アンチロックブレーキ）",
    "制动力分配(EBD/CBC等)": "制動力配分（EBD/CBC等）",
    "刹车辅助(EBA/BAS/BA等)": "ブレーキアシスト（EBA/BAS/BA等）",
    "牵引力控制(ASR/TCS/TRC等)": "トラクションコントロール（ASR/TCS/TRC等）",
    "车身稳定控制(ESC/ESP/DSC等)": "車両安定制御（ESC/ESP/DSC等）",
    "胎压监测功能": "タイヤ空気圧監視",
    "安全带未系提醒": "シートベルト非装着警報",
    "ISOFIX儿童座椅接口": "ISOFIXチャイルドシート固定具",
    "车道偏离预警系统": "車線逸脱警報",
    "主动刹车/主动安全系统": "自動緊急ブレーキ（AEB）",
    "疲劳驾驶提示": "ドライバー疲労警報",
    "前方碰撞预警": "前方衝突警報",
    "内置行车记录仪": "ドライブレコーダー内蔵",
    "道路救援呼叫": "ロードアシストコール",
    "驾驶模式切换": "ドライビングモード切替",
    "发动机启停技术": "アイドリングストップ",
    "自动驻车": "オートホールド",
    "上坡辅助": "ヒルスタートアシスト",
    "可变悬架功能": "可変サスペンション機能",
    "可变转向比": "可変ステアリング比",
    "前/后驻车雷达": "前後パーキングセンサー",
    "驾驶辅助影像": "周囲監視カメラ",
    "前方感知摄像头": "前方検知カメラ",
    "摄像头数量": "カメラ数",
    "车内摄像头数量": "車内カメラ数",
    "超声波雷达数量": "超音波センサー数",
    "巡航系统": "クルーズ制御",
    "辅助驾驶等级": "運転支援レベル",
    "卫星导航系统": "ナビゲーションシステム",
    "导航路况信息显示": "交通情報表示",
    "地图品牌": "地図ブランド",
    "AR实景导航": "AR実写ナビ",
    "并线辅助": "車線変更支援",
    "车道保持辅助系统": "車線維持支援",
    "车道居中保持": "車線中央維持",
    "道路交通标识识别": "交通標識認識",
    "辅助泊车入位": "駐車支援システム",
    "辅助变道": "自動車線変更支援",
    "辅助匝道自动驶出(入)": "インターチェンジ出入支援",
    "辅助驾驶路段": "支援対応路種",
    "外观套件": "エクステリアパッケージ",
    "轮圈材质": "ホイール材質",
    "电动后备厢": "電動テールゲート",
    "感应后备厢": "ハンズフリーテールゲート",
    "电动后备厢位置记忆": "テールゲート開度記憶",
    "发动机电子防盗": "エンジンイモビライザー",
    "车内中控锁": "集中ドアロック",
    "钥匙类型": "キータイプ",
    "无钥匙启动系统": "キーレス始動システム",
    "无钥匙进入功能": "キーレスエントリー",
    "隐藏电动门把手": "格納式ドアハンドル",
    "远程启动功能": "リモートスタート",
    "近光灯光源": "ロービーム光源",
    "远光灯光源": "ハイビーム光源",
    "灯光特色功能": "ライト特別機能",
    "LED日间行车灯": "LEDデイタイムランニングライト",
    "自适应远近光": "アダプティブハイビーム",
    "自动头灯": "オートライト",
    "转向头灯": "コーナリングライト",
    "车前雾灯": "フロントフォグランプ",
    "大灯高度可调": "ヘッドライトレベライザー",
    "大灯延时关闭": "ライトオフディレイ",
    "天窗类型": "サンルーフ形式",
    "前/后电动车窗": "前後パワーウインドウ",
    "车窗一键升降功能": "ワンタッチウインドウ",
    "车窗防夹手功能": "挟み込み防止機構",
    "侧窗多层隔音玻璃": "多層遮音ガラス（サイド）",
    "后风挡遮阳帘": "リアウインドウサンシェード",
    "后排侧窗遮阳帘": "後席サイドサンシェード",
    "车内化妆镜": "バニティミラー",
    "后雨刷": "リアワイパー",
    "感应雨刷功能": "レインセンサー",
    "外后视镜功能": "ドアミラー機能",
    "中控彩色屏幕": "センターディスプレイ",
    "中控屏幕尺寸": "センターディスプレイサイズ",
    "副驾娱乐屏尺寸": "助手席ディスプレイサイズ",
    "蓝牙/车载电话": "Bluetooth／車載電話",
    "手机互联/映射": "スマートフォン連携／ミラーリング",
    "语音识别控制系统": "音声認識コントロール",
    "语音助手唤醒词": "音声アシスタント起動語",
    "语音免唤醒词": "ウェイクワードレス音声操作",
    "语音分区域唤醒识别": "エリア別音声起動認識",
    "语音连续识别": "連続音声認識",
    "可见即可说": "視覚連動音声操作",
    "手势控制": "ジェスチャーコントロール",
    "应用商店": "アプリストア",
    "车载智能系统": "車載OS／インフォテインメント",
    "车机智能芯片": "車載SoC",
    "车联网": "車載通信（コネクテッド）",
    "4G/5G网络": "4G/5G通信",
    "OTA升级": "OTAアップデート",
    "V2X通讯": "V2X通信",
    "手机APP远程功能": "スマホアプリ遠隔機能",
    "方向盘材质": "ステアリング材質",
    "方向盘位置调节": "ステアリング位置調整",
    "换挡形式": "シフト形式",
    "多功能方向盘": "マルチファンクションステアリング",
    "方向盘换挡拨片": "パドルシフト",
    "方向盘加热": "ステアリングヒーター",
    "方向盘记忆": "ステアリングメモリー",
    "行车电脑显示屏幕": "ドライブコンピュータ表示",
    "全液晶仪表盘": "フル液晶メーターパネル",
    "液晶仪表尺寸": "メーター液晶サイズ",
    "HUD抬头数字显示": "ヘッドアップディスプレイ（HUD）",
    "内后视镜功能": "ルームミラー機能",
    "ETC装置": "ETC装置",
    "多媒体/充电接口": "マルチメディア／充電インターフェース",
    "USB/Type-C接口数量": "USB/Type-Cポート数",
    "手机无线充电功能": "スマートフォンワイヤレス充電",
    "座椅材质": "シート材質",
    "主座椅调节方式": "運転席調整方式",
    "副座椅调节方式": "助手席調整方式",
    "主/副驾驶座电动调节": "運転席／助手席電動調整",
    "前排座椅功能": "前席シート機能",
    "电动座椅记忆功能": "電動シートメモリー",
    "副驾驶位后排可调节按钮": "助手席後席調整ボタン",
    "第二排座椅调节": "後席調整機能",
    "第二排座椅电动调节": "後席電動調整",
    "第二排座椅功能": "後席シート機能",
    "后排座椅放倒形式": "後席可倒方式",
    "前/后中央扶手": "前後センターアームレスト",
    "后排杯架": "後席カップホルダー",
    "扬声器品牌名称": "スピーカーブランド",
    "扬声器数量": "スピーカー数",
    "杜比全景声(Dolby Atmos)": "Dolby Atmos",
    "车内环境氛围灯": "アンビエントライト",
    "主动式环境氛围灯": "アクティブアンビエントライト",
    "空调温度控制方式": "空調温度制御方式",
    "后排独立空调": "後席独立空調",
    "后座出风口": "後席エアアウトレット",
    "温度分区控制": "温度独立調整（ゾーン）",
    "车载空气净化器": "車載空気清浄機",
    "车内PM2.5过滤装置": "車内PM2.5フィルター",
    "空气质量监测": "空気質モニタリング",
    "外观颜色": "外装色",
    "内饰颜色": "内装色",
    "智享套装2": "スマートコンフォートパッケージ2",
}

# -----------------------------
# 文字種別判定
# -----------------------------
_re_zh = re.compile(r"[\u4e00-\u9fff]")

def contains_zh(s: str) -> bool:
    return bool(_re_zh.search(s or ""))

_price_like = re.compile(r"\d+(?:\.\d+)?\s*万?元")

def is_price_like(s: str) -> bool:
    return bool(_price_like.search(s or ""))

# -----------------------------
# 年検出（厳密化）＋ 前方補完
# -----------------------------
def extract_year(col_name: str) -> Optional[int]:
    """
    “2025款” “2025.03” “2025” すべて対応。
    単語境界に依存しない：\b ではなく lookahead（款 or 非数字 or 終端）
    """
    m = re.search(r"(20\d{2})(?=款|\D|$)", col_name or "")
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None

def forward_fill_years(model_cols: List[str]) -> List[Optional[int]]:
    filled: List[Optional[int]] = []
    last: Optional[int] = None
    for c in model_cols:
        y = extract_year(c)
        if y is not None:
            last = y
            filled.append(y)
        else:
            filled.append(last)
    return filled

# -----------------------------
# OpenAI（任意）
# -----------------------------
def maybe_init_openai():
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None, False
    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=api_key)
        return client, True
    except Exception:
        # ランタイムに openai 未インストールでも落とさない
        return None, False

def llm_translate_batch(client, texts: List[str]) -> List[str]:
    """
    必要最低限の多段防御: 失敗しても原文返し。
    モデル名は環境に合わせて。既定は 'gpt-4o-mini-transcribe' 等にせず、無指定で最小化。
    """
    out: List[str] = []
    if client is None:
        return texts[:]  # LLM無効時は原文返し
    try:
        # まとめてsystem指示 + 逐次userメッセージで簡易に
        system = (
            "你是专业的中日翻译助手。保留数值/单位/括号内の金額表記はそのまま。"
            "固有名詞は原則維持。意味の通る自然な日本語に。"
        )
        for t in texts:
            t2 = t or ""
            if not contains_zh(t2):
                out.append(t2)
                continue
            # 単発呼び出し（堅牢性優先）
            resp = client.chat.completions.create(
                model=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": f"以下を日本語に翻訳：\n{t2}"},
                ],
                temperature=0.2,
            )
            cand = resp.choices[0].message.content or t2
            out.append(cand.strip())
        return out
    except Exception:
        # 失敗時は原文返し（パイプラインを止めない）
        return texts[:]

# -----------------------------
# メイン処理
# -----------------------------
def main():
    src, dst, alt = resolve_src_dst()

    print(f"SRC: {src}")
    print(f"DST (primary): {dst}")
    if alt:
        print(f"DST (secondary): {alt}")

    df = pd.read_csv(src, dtype=str).fillna("")
    # 期待ヘッダ: セクション, セクション_ja, 項目, 項目_ja, <モデル列...>
    if df.shape[1] < 5:
        print("⚠ ヘッダ列数が想定より少ないようです（少なくとも 5 列想定）。続行します。")

    # === 1) セクション_ja / 項目_ja を辞書で確定（LLMは一切上書きしない） ===
    # 既存値が空なら辞書で埋める。既に値がある場合は尊重（=元の挙動維持）。
    for col_pair in [("セクション", "セクション_ja"), ("項目", "項目_ja")]:
        src_col, ja_col = col_pair
        if src_col not in df.columns:
            print(f"⚠ 列が見つかりません: {src_col}（続行）")
            df[src_col] = ""
        if ja_col not in df.columns:
            df[ja_col] = ""

    def map_if_empty(orig: str, mapped: Optional[str], current: str) -> str:
        if current.strip():
            return current  # 既存値尊重（＝元の挙動）
        return (mapped or "").strip()

    df["セクション_ja"] = [
        map_if_empty(cn, SECTION_MAP.get(cn, ""), ja)
        for cn, ja in zip(df["セクション"].tolist(), df["項目"].tolist() if "項目" in df.columns else [""]*len(df))
        for _ in [0]
    ][:len(df)]  # 上の書き方の都合で長さ調整

    # 正しくセクション_jaを埋め直す（上のワークアラウンドを補正）
    df["セクション_ja"] = [
        map_if_empty(sec_cn, SECTION_MAP.get(sec_cn, ""), sec_ja)
        for sec_cn, sec_ja in zip(df["セクション"].tolist(), df["セクション_ja"].tolist())
    ]

    df["項目_ja"] = [
        map_if_empty(item_cn, ITEM_MAP.get(item_cn, ""), item_ja)
        for item_cn, item_ja in zip(df["項目"].tolist(), df["項目_ja"].tolist())
    ]

    # === 2) モデル列（4列目以降）の年検出＋前方補完＋ 2025 以降フィルタ ===
    all_cols = list(df.columns)
    fixed_cols = all_cols[:4]  # ["セクション","セクション_ja","項目","項目_ja"] を想定
    model_cols = all_cols[4:]

    filled_years = forward_fill_years(model_cols)

    # 2025 以降を基本採用
    kept_model_cols = [c for c, y in zip(model_cols, filled_years) if (y is not None and y >= 2025)]

    # 万一ゼロになった場合の救済：最初に現れた年ブロック（年 or 省略列）を落とさない
    if not kept_model_cols:
        base_year = next((y for y in filled_years if y is not None), None)
        if base_year is not None:
            kept_model_cols = [
                c for c, y in zip(model_cols, filled_years)
                if (y is None or y == base_year)
            ]
        else:
            # 年がどこにも無ければ「元の挙動」を優先し全列維持
            kept_model_cols = model_cols[:]

    # 列を再構成
    out_cols = fixed_cols + kept_model_cols
    df = df[out_cols]

    # === 3) LLM翻訳の対象を厳密選別（“中文なら訳す”を価格判定より優先） ===
    # セクション_ja / 項目_ja は LLM 対象から除外（=上書き禁止）
    translate_target_cols = [c for c in df.columns if c not in ("セクション_ja", "項目_ja")]

    client, llm_enabled = maybe_init_openai()
    if llm_enabled:
        # バッチ抽出
        texts: List[str] = []
        indices: List[tuple[int, str]] = []  # (row_idx, col_name)
        for idx, row in df.iterrows():
            for col in translate_target_cols:
                val = str(row[col] if col in row else "")
                if not val:
                    continue
                # “中文が含まれるなら訳す”を最優先
                if contains_zh(val):
                    texts.append(val)
                    indices.append((idx, col))
                else:
                    # 中文を含まない場合は現状維持（元の挙動を壊さない）
                    pass

        if texts:
            trans = llm_translate_batch(client, texts)
            # 逐次反映
            for (r, c), v in zip(indices, trans):
                df.at[r, c] = v

    # === 4) 保存（primary / secondary） ===
    # 元のパイプラインに合わせ、index=False, utf-8-sig で保存
    df.to_csv(dst, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    print(f"✅ Saved: {dst}")
    if alt:
        df.to_csv(alt, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
        print(f"✅ Saved (alt): {alt}")

if __name__ == "__main__":
    main()
