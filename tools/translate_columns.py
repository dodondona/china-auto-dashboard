# tools/translate_columns.py
# 方針:
#  - セクション_ja・項目_jaは辞書のみ
#  - 値セルはLLM翻訳（価格行はルール整形のみ）
#  - モデル名ヘッダー（5列目以降）も中国語ならLLM翻訳
#  - YEAR_MINでモデル列フィルタ（既定: 2025、厳格度はYEAR_FILTER_STRICTで調整）
import os, re, json, time, pathlib, csv
import pandas as pd
from typing import List, Dict

# ========= 入出力 =========
CSV_IN = os.environ.get("CSV_IN", "").strip()
if not CSV_IN:
    raise RuntimeError("CSV_IN が未設定です")
src_path = pathlib.Path(CSV_IN)
series_id = re.search(r"(\d+)", src_path.stem or src_path.name)
series_id = series_id.group(1) if series_id else "unknown"

OUT_DIR = src_path.parent
DST_PRIMARY   = OUT_DIR / f"{src_path.stem}.ja.csv"
DST_SECONDARY = OUT_DIR / f"{src_path.stem}_ja.csv"

# ========= 設定 =========
MODEL   = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
API_KEY = os.environ.get("OPENAI_API_KEY", "")

TRANSLATE_VALUES    = os.environ.get("TRANSLATE_VALUES", "true").lower() == "true"
TRANSLATE_COLNAMES  = os.environ.get("TRANSLATE_COLNAMES", "true").lower() == "true"
# 既定: ヘッダーのプレフィクス除去は行わない（=フル表記維持）
STRIP_GRADE_PREFIX  = os.environ.get("STRIP_GRADE_PREFIX", "false").lower() == "true"

# モデル列の“年式フィルタ”
YEAR_MIN            = int(os.environ.get("YEAR_MIN", "2025"))
YEAR_FILTER_STRICT  = os.environ.get("YEAR_FILTER_STRICT", "true").lower() == "true"  # true=年が無い列も落とす, false=年不明は残す

EXRATE_CNY_TO_JPY   = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))

CACHE_REPO_DIR = pathlib.Path(os.environ.get("CACHE_REPO_DIR", "cache")).joinpath(series_id)
CACHE_REPO_DIR.mkdir(parents=True, exist_ok=True)

BATCH_SIZE, RETRIES, SLEEP_BASE = 60, 3, 1.2

# ========= クリーニング・固定訳 =========
NOISE_ANY = ["对比","参数","图片","配置","详情"]
NOISE_PRICE_TAIL = ["询价","计算器","询底价","报价","价格询问","起","起售","到店","经销商"]

def clean_any_noise(s: str) -> str:
    s = str(s) if s is not None else ""
    for w in NOISE_ANY + NOISE_PRICE_TAIL:
        s = s.replace(w, "")
    return re.sub(r"\s+", " ", s).strip(" 　-—–")

def clean_price_cell(s: str) -> str:
    t = clean_any_noise(s)
    for w in NOISE_PRICE_TAIL:
        t = re.sub(rf"(?:\s*{re.escape(w)}\s*)+$", "", t)
    return t.strip()

RE_PAREN_ANY_YEN = re.compile(r"（[^）]*(?:日本円|JPY|[¥￥]|円)[^）]*）")
RE_ANY_YEN_TOKEN = re.compile(r"(日本円|JPY|[¥￥]|円)")
def strip_any_yen_tokens(s: str) -> str:
    t = str(s)
    t = RE_PAREN_ANY_YEN.sub("", t)
    t = RE_ANY_YEN_TOKEN.sub("", t)
    return re.sub(r"\s+", " ", t).strip()

def uniq(seq):
    s, out = set(), []
    for x in seq:
        if x not in s:
            s.add(x); out.append(x)
    return out

def chunked(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def parse_json_relaxed(content: str, terms: List[str]) -> Dict[str, str]:
    try:
        d = json.loads(content)
        if isinstance(d, dict) and "translations" in d:
            return {
                str(t["cn"]).strip(): str(t.get("ja", t["cn"])).strip()
                for t in d["translations"] if t.get("cn")
            }
    except Exception:
        pass
    pairs = re.findall(r'"cn"\s*:\s*"([^"]+)"\s*,\s*"ja"\s*:\s*"([^"]*)"', content)
    if pairs:
        return {cn.strip(): ja.strip() for cn, ja in pairs}
    return {t: t for t in terms}

# ========= LLM =========
class Translator:
    def __init__(self, model: str, api_key: str):
        if not (api_key and api_key.strip()):
            raise RuntimeError("OPENAI_API_KEY is not set")
        from openai import OpenAI
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.system_values = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セル値』配列。自然で簡潔な日本語へ。数値やAT/MT等の記号は保持。"
            "出力は JSON（{'translations':[{'cn':'原文','ja':'訳文'}]}）のみ。"
        )
        self.system_headers = (
            "あなたは自動車グレード名の専門翻訳者です。"
            "入力は中国語の『グレード/モデル名』配列。年式や排気量、駆動記号（4MATIC 等）や記号は保持し、"
            "自然な日本語へ変換（例：运动型→スポーツ、豪华型→ラグジュアリー）。"
            "出力は JSON（{'translations':[{'cn':'原文','ja':'訳文'}]}）のみ。"
        )

    def _translate(self, terms: List[str], use_header_prompt: bool) -> Dict[str, str]:
        if not terms:
            return {}
        msgs = [
            {"role": "system", "content": self.system_headers if use_header_prompt else self.system_values},
            {"role": "user", "content": json.dumps({"terms": terms}, ensure_ascii=False)},
        ]
        resp = self.client.chat.completions.create(
            model=self.model, messages=msgs, temperature=0,
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content or ""
        return parse_json_relaxed(content, terms)

    def translate_values(self, unique_terms: List[str]) -> Dict[str, str]:
        out = {}
        for chunk in chunked(unique_terms, BATCH_SIZE):
            for attempt in range(1, RETRIES + 1):
                try:
                    out.update(self._translate(chunk, use_header_prompt=False))
                    break
                except Exception:
                    if attempt == RETRIES:
                        for t in chunk:
                            out.setdefault(t, t)
                    time.sleep(SLEEP_BASE * attempt)
        return out

    def translate_headers(self, unique_terms: List[str]) -> Dict[str, str]:
        out = {}
        for chunk in chunked(unique_terms, BATCH_SIZE):
            for attempt in range(1, RETRIES + 1):
                try:
                    out.update(self._translate(chunk, use_header_prompt=True))
                    break
                except Exception:
                    if attempt == RETRIES:
                        for t in chunk:
                            out.setdefault(t, t)
                    time.sleep(SLEEP_BASE * attempt)
        return out

# ========= 固定訳（セクション/項目は辞書のみ） =========
FIX_JA_SECTIONS = {
    "該当なし": "該当なし",
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
    "方向盘/内后视镜": "ステアリング／ルームミラー",
    "车内充电": "車内充電",
    "座椅配置": "シート",
    "音响/车内灯光": "オーディオ／室内照明",
    "空调/冰箱": "空調／冷蔵",
    "颜色": "カラー",
    "选装包": "オプションパッケージ",
}

FIX_JA_ITEMS = {
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

PRICE_ITEM_MSRP_CN   = {"厂商指导价(元)","厂商指导价","厂商建议零售价"}
PRICE_ITEM_DEALER_CN = {"经销商报价","经销商参考价","经销商"}

def norm_cn_cell(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).strip()

# ========= 価格整形 =========
RE_WAN = re.compile(r"(\d+(?:\.\d+)?)\s*万")
RE_NUM = re.compile(r"(\d+(?:\.\d+)?)")
def _parse_cny_amount(cell: str) -> float | None:
    t = clean_price_cell(cell)
    m = RE_WAN.search(t)
    if m:
        return float(m.group(1)) * 10000.0
    m = RE_NUM.search(t)
    if m:
        return float(m.group(1))
    return None

def msrp_to_yuan_and_jpy(cell: str, rate: float) -> str:
    t = strip_any_yen_tokens(clean_price_cell(cell))
    amt = _parse_cny_amount(t)
    if amt is None:
        return t
    jpy = int(round(amt * rate))
    if "万" in t and "元" not in t:
        t = f"{t}元"
    return f"{t}（日本円{jpy:,}円）"

def dealer_to_yuan_only(cell: str) -> str:
    t = strip_any_yen_tokens(clean_price_cell(cell))
    if not t or t in {"-","–","—"}:
        return t
    if ("元" not in t) and RE_WAN.search(t):
        t = f"{t}元"
    return t

# ========= グレード列 前置語除去（必要時のみ） =========
def strip_grade_prefix(name: str) -> str:
    s = str(name)
    if not STRIP_GRADE_PREFIX:
        return s
    # 具体ルールが必要な場合のみ適用（既定は何もしない）
    s = re.sub(r"^[^,，\s]{1,40}\s*\d{4}款\s*改款\s*", "", s).strip()
    return s

def extract_year(name: str) -> int | None:
    m = re.search(r"\b(20\d{2})\b", str(name))
    return int(m.group(1)) if m else None

# ========= 実処理 =========
df = pd.read_csv(src_path, dtype=str).fillna("")
prev_cn_path = CACHE_REPO_DIR / "config_cn_snapshot.csv"
prev_ja_path = CACHE_REPO_DIR / "config_ja_prev.csv"
prev_cn_df = pd.read_csv(prev_cn_path, dtype=str).fillna("") if prev_cn_path.exists() else None
prev_ja_df = pd.read_csv(prev_ja_path, dtype=str).fillna("") if prev_ja_path.exists() else None
enable_reuse = (prev_cn_df is not None) and (prev_ja_df is not None)

# ---- モデル列（5列目以降）: 年式フィルタ & ヘッダー翻訳 ----
columns = list(df.columns)
fixed_cols = columns[:4]
model_cols = columns[4:]

# 年式フィルタ
def keep_col(colname: str) -> bool:
    y = extract_year(colname)
    if y is None:
        return not YEAR_FILTER_STRICT  # 厳格なら落とす / 非厳格なら残す
    return y >= YEAR_MIN

kept_model_cols = [c for c in model_cols if keep_col(c)]
df = df[fixed_cols + kept_model_cols]

# ヘッダー整形（stripは既定OFF）
if TRANSLATE_COLNAMES and kept_model_cols:
    zh_char = re.compile(r"[\u4e00-\u9fff]")
    # LLM翻訳対象（中国語を含むヘッダーのみ）
    headers_to_tr = [c for c in kept_model_cols if zh_char.search(c)]
    header_map = {}
    if headers_to_tr and API_KEY.strip():
        tr = Translator(MODEL, API_KEY)
        header_map = tr.translate_headers(uniq(headers_to_tr))
    # 置換（LLMで返らなければ元のまま）
    new_model_cols = []
    for c in kept_model_cols:
        cc = strip_grade_prefix(c)
        if c in header_map:
            cc = header_map[c] or cc
        new_model_cols.append(cc)
    df.columns = fixed_cols + new_model_cols

# ---- セクション/項目：辞書のみ（LLM不使用） ----
sec_map_old, item_map_old = {}, {}
if enable_reuse:
    if "セクション_ja" in prev_ja_df.columns:
        for cur, old_cn, old_ja in zip(df["セクション"].astype(str),
                                       prev_cn_df["セクション"].astype(str),
                                       prev_ja_df["セクション_ja"].astype(str)):
            if norm_cn_cell(cur) == norm_cn_cell(old_cn):
                sec_map_old[str(cur).strip()] = str(old_ja).strip() or str(cur).strip()
    if "項目_ja" in prev_ja_df.columns:
        for cur, old_cn, old_ja in zip(df["項目"].astype(str),
                                       prev_cn_df["項目"].astype(str),
                                       prev_ja_df["項目_ja"].astype(str)):
            if norm_cn_cell(cur) == norm_cn_cell(old_cn):
                item_map_old[str(cur).strip()] = str(old_ja).strip() or str(cur).strip()

sec_map  = dict(sec_map_old);  sec_map.update(FIX_JA_SECTIONS)
item_map = dict(item_map_old); item_map.update(FIX_JA_ITEMS)

out_full = df.copy()
out_full.insert(1, "セクション_ja", out_full["セクション"].map(lambda s: sec_map.get(str(s).strip(), str(s).strip())))
out_full.insert(3, "項目_ja",     out_full["項目"].map(lambda s: item_map.get(str(s).strip(), str(s).strip())))

# 見出し（価格名）のゆらぎ補正
PAREN_CURR_RE = re.compile(r"（\s*(?:円|元|人民元|CNY|RMB|JPY)[^）]*）")
out_full["項目_ja"] = out_full["項目_ja"].astype(str).str.replace(PAREN_CURR_RE, "", regex=True).str.strip()
out_full.loc[out_full["項目_ja"].str.match(r"^メーカー希望小売価格.*$", na=False), "項目_ja"] = "メーカー希望小売価格"
out_full.loc[out_full["項目_ja"].str.contains(r"ディーラー販売価格", na=False), "項目_ja"] = "ディーラー販売価格（元）"

# 価格セル整形（翻訳しない）
MSRP_JA_RE   = re.compile(r"^メーカー希望小売価格$")
DEALER_JA_RE = re.compile(r"^ディーラー販売価格（元）$")
is_msrp   = out_full["項目"].isin(PRICE_ITEM_MSRP_CN)   | out_full["項目_ja"].str.match(MSRP_JA_RE,   na=False)
is_dealer = out_full["項目"].isin(PRICE_ITEM_DEALER_CN) | out_full["項目_ja"].str.match(DEALER_JA_RE, na=False)

for col in out_full.columns[4:]:
    out_full.loc[is_msrp,  col] = out_full.loc[is_msrp,  col].map(lambda s: msrp_to_yuan_and_jpy(s, EXRATE_CNY_TO_JPY))
    out_full.loc[is_dealer, col] = out_full.loc[is_dealer, col].map(lambda s: dealer_to_yuan_only(s))

# ===== 値セル翻訳（価格行除外） =====
if TRANSLATE_VALUES:
    numeric_like = re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")
    zh_char = re.compile(r"[\u4e00-\u9fff]")
    non_price_mask = ~(is_msrp | is_dealer)

    values_to_translate: List[str] = []
    coords_to_update: List[tuple] = []

    if enable_reuse and prev_cn_df.shape == df.shape and list(prev_cn_df.columns) == list(df.columns):
        diff_mask = (df != prev_cn_df)
        for i in range(len(df)):
            if not non_price_mask.iloc[i]:
                continue
            for j in range(4, len(df.columns)):
                cur = str(df.iat[i, j]).strip()
                if cur in {"", "●", "○", "–", "-", "—"}:
                    continue
                if numeric_like.fullmatch(cur):
                    continue

                need = diff_mask.iat[i, j]
                prev_cn = str(prev_cn_df.iat[i, j]).strip()
                prev_ja = str(prev_ja_df.iat[i, j]).strip()

                # 差分が無くても、前回JA=CN/空/中国語含み → 翻訳対象
                if not need and (prev_ja == "" or prev_ja == prev_cn or zh_char.search(prev_ja)):
                    need = True

                # 今回セル自体が中国語含み → 強制翻訳
                if zh_char.search(cur):
                    need = True

                if need:
                    values_to_translate.append(cur)
                    coords_to_update.append((i, j))
                else:
                    out_full.iat[i, j] = prev_ja_df.iat[i, j]
    else:
        for i in range(len(df)):
            if not non_price_mask.iloc[i]:
                continue
            for j in range(4, len(df.columns)):
                v = str(df.iat[i, j]).strip()
                if v in {"", "●", "○", "–", "-", "—"}:
                    continue
                if numeric_like.fullmatch(v):
                    continue
                if re.search(r"[\u4e00-\u9fff]", v):
                    values_to_translate.append(v)
                    coords_to_update.append((i, j))

    if values_to_translate:
        if not API_KEY.strip():
            print("⚠ OPENAI_API_KEY が未設定のため、値セル翻訳はスキップしました（価格整形は適用済み）。")
        else:
            tr = Translator(MODEL, API_KEY)
            uniq_vals = uniq(values_to_translate)
            val_map = tr.translate_values(uniq_vals)
            for (i, j), cn in zip(coords_to_update, values_to_translate):
                out_full.iat[i, j] = val_map.get(cn, cn)

# ===== 保存（CSVの欠落対策：クォート＆BOM付き） =====
out_full.to_csv(DST_PRIMARY, index=False,
                quoting=csv.QUOTE_MINIMAL, lineterminator="\n", encoding="utf-8-sig")
out_full.to_csv(DST_SECONDARY, index=False,
                quoting=csv.QUOTE_MINIMAL, lineterminator="\n", encoding="utf-8-sig")

# スナップショット保存（再利用用）
df.to_csv(prev_cn_path, index=False,
          quoting=csv.QUOTE_MINIMAL, lineterminator="\n", encoding="utf-8-sig")
out_full.to_csv(prev_ja_path, index=False,
                quoting=csv.QUOTE_MINIMAL, lineterminator="\n", encoding="utf-8-sig")

print(f"✅ Saved: {DST_PRIMARY}")
print(f"✅ Saved (alt): {DST_SECONDARY}")
print(f"📦 Repo cache CN: {prev_cn_path}")
print(f"📦 Repo cache JA: {prev_ja_path}")
