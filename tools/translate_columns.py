from __future__ import annotations
import os, json, time, re, urllib.request
from pathlib import Path
import pandas as pd
from openai import OpenAI

# =============================
# 入出力解決
# =============================
SERIES_ID = os.environ.get("SERIES_ID", "").strip()

def resolve_src_dst():
    csv_in  = os.environ.get("CSV_IN", "").strip()
    csv_out = os.environ.get("CSV_OUT", "").strip()

    def guess_paths_from_series(sid: str):
        if not sid:
            return None, None
        base = f"output/autohome/{sid}/config_{sid}"
        return Path(f"{base}.csv"), Path(f"{base}.ja.csv")

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
    elif s.endswith("_ja.csv"):
        s2 = s.replace("_ja.csv", ".ja.csv")
    else:
        s2 = dst.stem + ".ja.csv"
    return dst.parent / s2

DST_SECONDARY = make_secondary(DST_PRIMARY)

def detect_series_id_from_path(p: Path) -> str:
    # output/autohome/<sid>/config_<sid>.csv の <sid> を推定
    try:
        name = p.stem  # config_8042
        m = re.search(r"config_(\d+)", name)
        if m:
            return m.group(1)
    except Exception:
        pass
    try:
        parent = p.parent.name  # 8042
        if parent.isdigit():
            return parent
    except Exception:
        pass
    return SERIES_ID or "misc"

SERIES_FOR_CACHE = detect_series_id_from_path(SRC)

# =============================
# 設定
# =============================
MODEL   = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
API_KEY = os.environ.get("OPENAI_API_KEY")
TRANSLATE_VALUES   = os.environ.get("TRANSLATE_VALUES", "true").lower() == "true"
TRANSLATE_COLNAMES = os.environ.get("TRANSLATE_COLNAMES", "true").lower() == "true"
STRIP_GRADE_PREFIX = os.environ.get("STRIP_GRADE_PREFIX", "true").lower() == "true"
SERIES_PREFIX_RE   = os.environ.get("SERIES_PREFIX", "").strip()
EXRATE_CNY_TO_JPY  = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))
CURRENCYFREAKS_KEY = os.environ.get("CURRENCY", "").strip()

BATCH_SIZE, RETRIES, SLEEP_BASE = 60, 3, 1.2

# =============================
# 為替（CurrencyFreaks優先 / 失敗時はフォールバック）
# =============================
def get_cny_jpy_rate_fallback(default_rate: float) -> float:
    if not CURRENCYFREAKS_KEY:
        print(f"⚠️ No API key set (CURRENCY). Using fallback rate {default_rate}")
        return default_rate
    try:
        url = f"https://api.currencyfreaks.com/latest?apikey={CURRENCYFREAKS_KEY}&symbols=JPY,CNY"
        with urllib.request.urlopen(url, timeout=8) as r:
            data = json.loads(r.read().decode("utf-8"))
        jpy = float(data["rates"]["JPY"])
        cny = float(data["rates"]["CNY"])
        rate = jpy / cny  # 1CNY あたりの JPY
        if rate < 1:
            rate = 1 / rate
        print(f"💱 Rate from CurrencyFreaks: 1CNY = {rate:.2f}JPY")
        return rate
    except Exception as e:
        print(f"⚠️ CurrencyFreaks fetch failed ({e}). Using fallback rate {default_rate}")
        return default_rate

EXRATE_CNY_TO_JPY = get_cny_jpy_rate_fallback(EXRATE_CNY_TO_JPY)

# =============================
# 固定訳・正規化
# =============================
NOISE_ANY = ["对比", "参数", "图片", "配置", "详情"]
NOISE_PRICE_TAIL = ["询价", "计算器", "询底价", "报价", "价格询问", "起", "起售", "到店", "经销商"]

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

BRAND_MAP = {
    "BYD": "BYD",
    "比亚迪": "BYD",
    "奔驰": "メルセデス・ベンツ",
    "梅赛德斯-奔驰": "メルセデス・ベンツ",
}

# ====== セクション/項目の辞書（CN→JA） ======
FIX_JA_SECTIONS = {
    "基本参数": "基本仕様",
    "车身": "車体",
    "发动机": "エンジン",
    "变速箱": "トランスミッション",
    "底盘转向": "シャシー・ステアリング",
    "车轮制动": "ホイール・ブレーキ",
    "被动安全": "衝突安全",
    "主动安全": "アクティブセーフティ",
    "驾驶操控": "ドライビングコントロール",
    "驾驶硬件": "運転ハードウェア",
    "驾驶功能": "運転機能",
    "外观/防盗": "エクステリア・盗難防止",
    "车外灯光": "外部照明",
    "天窗/玻璃": "サンルーフ・ガラス",
    "外后视镜": "外側ドアミラー",
    "屏幕/系统": "ディスプレイ・システム",
    "智能化配置": "スマート機能装備",
    "方向盘/内后视镜": "ステアリング・内側ミラー",
    "车内充电": "車内充電設備",
    "座椅配置": "シート装備",
    "音响/车内灯光": "オーディオ・室内照明",
    "空调/冰箱": "エアコン・冷蔵庫",
    "颜色": "カラー",
    "选装包": "オプションパッケージ",
}

FIX_JA_ITEMS = {
    "厂商指导价(元)": "メーカー希望小売価格(元)",
    "经销商报价": "ディーラー販売価格（元）",
    "厂商": "メーカー",
    "级别": "クラス",
    "能源类型": "エネルギータイプ",
    "环保标准": "環境基準",
    "上市时间": "発売時期",
    "最大功率(kW)": "最大出力(kW)",
    "最大扭矩(N·m)": "最大トルク(N·m)",
    "变速箱": "トランスミッション",
    "车身结构": "ボディ構造",
    "发动机": "エンジン",
    "长*宽*高(mm)": "全長×全幅×全高(mm)",
    "官方0-100km/h加速(s)": "公式0-100km/h加速(s)",
    "最高车速(km/h)": "最高速度(km/h)",
    "WLTC综合油耗(L/100km)": "WLTC総合燃費(L/100km)",
    "整车质保": "車両保証",
    "整备质量(kg)": "車両重量(kg)",
    "最大满载质量(kg)": "最大積載質量(kg)",
    "长度(mm)": "全長(mm)",
    "宽度(mm)": "全幅(mm)",
    "高度(mm)": "全高(mm)",
    "轴距(mm)": "ホイールベース(mm)",
    "前轮距(mm)": "前トレッド(mm)",
    "后轮距(mm)": "後トレッド(mm)",
    "最小离地间隙(mm)": "最低地上高(mm)",
    "车门开启方式": "ドア開閉方式",
    "车门数(个)": "ドア数(枚)",
    "座位数(个)": "乗車定員(名)",
    "油箱容积(L)": "燃料タンク容量(L)",
    "行李厢容积(L)": "ラゲッジ容量(L)",
    "工信部纯电续航里程(km)": "公称EV航続距離(km)",
    "电动机(Peak功率kW)": "モーター(ピーク出力kW)",
    "电动机(Peak扭矩N·m)": "モーター(ピークトルクN·m)",
    "电动机(额定功率kW)": "モーター(定格出力kW)",
    "电动机(额定扭矩N·m)": "モーター(定格トルクN·m)",
    "系统综合功率(kW)": "システム総合出力(kW)",
    "系统综合扭矩(N·m)": "システム総合トルク(N·m)",
    "发动机型号": "エンジン型式",
    "排量(mL)": "排気量(mL)",
    "进气形式": "過給形式",
    "气缸排列形式": "シリンダー配列",
    "气缸数(个)": "シリンダー数(個)",
    "每缸气门数(个)": "1気筒あたりバルブ数(個)",
    "压缩比": "圧縮比",
    "配气机构": "バルブ機構",
    "缸径(mm)": "ボア(mm)",
    "行程(mm)": "ストローク(mm)",
    "最大马力(Ps)": "最高出力(Ps)",
    "最大扭矩转速(rpm)": "最大トルク発生回転数(rpm)",
    "最大功率转速(rpm)": "最大出力発生回転数(rpm)",
    "燃料形式": "燃料種類",
    "燃油标号": "燃料オクタン価",
    "供油方式": "燃料供給方式",
    "缸体材质": "シリンダーブロック材質",
    "缸盖材质": "シリンダーヘッド材質",
    "电机类型": "モーター種類",
    "电动机总功率(kW)": "モーター総出力(kW)",
    "电动机总扭矩(N·m)": "モーター総トルク(N·m)",
    "电动机数量": "モーター数",
    "电池容量(kWh)": "バッテリー容量(kWh)",
    "电池类型": "バッテリー種類",
    "电池能量密度(Wh/kg)": "バッテリーエネルギー密度(Wh/kg)",
    "电池温度管理": "バッテリー温度管理",
    "CLTC纯电续航里程(km)": "CLTC航続距離(km)",
    "WLTC纯电续航里程(km)": "WLTC航続距離(km)",
    "快充功率(kW)": "急速充電出力(kW)",
    "快充时间(小时)": "急速充電時間(時間)",
    "慢充功率(kW)": "普通充電出力(kW)",
    "慢充时间(小时)": "普通充電時間(時間)",
    "百公里加速时间(s)": "0-100km/h加速(s)",
    "百公里制动距离(m)": "100-0km/h制動距離(m)",
    "NEDC续航里程(km)": "NEDC航続距離(km)",
    "充电口位置": "充電ポート位置",
    "充电接口": "充電コネクタ",
    "电池保修": "バッテリー保証",
    "电芯品牌": "電池セルブランド",
    "变速箱描述": "トランスミッション詳細",
    "挡位个数": "段数",
    "变速箱类型": "トランスミッション形式",
    "驱动方式": "駆動方式",
    "四驱形式": "4WD方式",
    "中央差速器结构": "センターデフ形式",
    "前悬架类型": "フロントサスペンション形式",
    "后悬架类型": "リヤサスペンション形式",
    "助力类型": "パワーステアリング形式",
    "车体结构": "車体構造",
    "前制动器类型": "フロントブレーキ形式",
    "后制动器类型": "リヤブレーキ形式",
    "驻车制动类型": "パーキングブレーキ形式",
    "前轮胎规格": "フロントタイヤサイズ",
    "后轮胎规格": "リヤタイヤサイズ",
    "备胎规格": "スペアタイヤ仕様",
    "轮圈材质": "ホイール材質",
    "轮胎品牌": "タイヤブランド",
    "车内中控锁": "集中ドアロック",
    "无钥匙启动系统": "キーレススタート",
    "远程启动功能": "リモートエンジンスタート",
    "固定速度巡航": "クルーズコントロール",
    "自动驻车": "オートホールド",
    "上坡辅助": "ヒルスタートアシスト",
    "陡坡缓降": "ヒルディセントコントロール",
    "可变转向比": "可変ステアレシオ",
    "运动驾驶模式": "スポーツモード",
    "经济驾驶模式": "エコモード",
    "雪地/沙地等驾驶模式": "スノー/サンド等ドライブモード",
    "悬架软硬调节": "サスペンション減衰調整",
    "空气悬架": "エアサスペンション",
    "可变悬架": "可変サスペンション",
    "主动闭合进气格栅": "アクティブグリルシャッター",
    "制动能量回收系统": "回生ブレーキ",
    "自动变道辅助": "自動レーンチェンジ支援",
    "并线辅助": "ブラインドスポットアシスト",
    "车道保持辅助系统": "レーンキープアシスト",
    "车道偏离预警系统": "レーンディパーチャー警報",
    "道路交通标识识别": "交通標識認識",
    "疲劳驾驶提示": "ドライバー疲労警報",
    "前方碰撞预警": "前方衝突警告",
    "主动刹车/主动安全系统": "自動緊急ブレーキ(AEB)",
    "夜视系统": "ナイトビジョン",
    "DOW开门预警": "ドアオープン警報(DOW)",
    "前/后驻车雷达": "前/後パーキングセンサー",
    "倒车车侧预警系统": "後退時車両接近警報",
    "透明底盘": "フロア透過表示",
    "全速自适应巡航": "全車速ACC",
    "自动泊车入位": "自動駐車",
    "遥控泊车": "リモートパーキング",
    "驾驶辅助级别": "運転支援レベル",
    "驾驶辅助影像": "運転支援用カメラ表示",
    "倒车影像": "バックカメラ",
    "360°全景影像": "360°全周囲カメラ",
    "倒车车侧预警": "後退車両検知警報",
    "前/后方交叉来车预警": "前/後方クロストラフィック警報",
    "道路救援呼叫": "緊急通報(eCall)",
    "主/副驾驶安全气囊": "運転席/助手席エアバッグ",
    "前/后排侧气囊": "前/後席サイドエアバッグ",
    "前/后排头部气囊(气帘)": "前/後席カーテンエアバッグ",
    "膝部气囊": "ニーエアバッグ",
    "前排中间气囊": "フロントセンターエアバッグ",
    "安全带未系提醒": "シートベルト非着用警告",
    "胎压监测功能": "タイヤ空気圧監視",
    "ABS防抱死": "ABS",
    "制动力分配(EBD/CBC等)": "EBD/CBC",
    "刹车辅助(EBA/BAS/BA等)": "ブレーキアシスト(EBA/BAS/BA)",
    "牵引力控制(ASR/TCS/TRC等)": "トラクションコントロール",
    "车身稳定控制(ESC/ESP/DSC等)": "横滑り防止装置",
    "并线辅助(BSD/CTA等)": "BSD/CTA",
    "儿童座椅接口(ISOFIX)": "ISOFIXチャイルドシート固定",
    "被动行人保护": "歩行者保護(受動)",
    "前排安全带调节": "フロントシートベルト調整",
    "后排安全带调节": "リヤシートベルト調整",
    "车内灭火器": "車載消火器",
    "副驾驶安全气囊关闭": "助手席エアバッグオフ",
    "中控屏幕尺寸": "センターディスプレイサイズ",
    "中控彩色屏幕": "センターカラーディスプレイ",
    "车载互联": "車載コネクティビティ",
    "导航系统": "ナビゲーション",
    "语音识别控制系统": "音声認識操作",
    "手势控制": "ジェスチャー操作",
    "手机互联/映射": "スマホ連携/ミラーリング",
    "OTA升级": "OTAアップデート",
    "V2X通讯": "V2X通信",
    "HUD抬头数字显示": "ヘッドアップディスプレイ",
    "液晶仪表样式": "液晶メーター表示",
    "仪表屏幕尺寸": "メーター画面サイズ",
    "副驾屏幕尺寸": "助手席画面サイズ",
    "流媒体后视镜": "デジタルインナーミラー",
    "AR实景导航": "ARナビゲーション",
    "音响品牌": "オーディオブランド",
    "扬声器数量(个)": "スピーカー数(個)",
    "后排多媒体控制": "後席マルチメディア操作",
    "车内氛围灯": "アンビエントライト",
    "车内阅读灯": "読書灯",
    "感应后备厢": "ハンズフリーテールゲート",
    "电动后备厢": "パワーテールゲート",
    "车顶行李架": "ルーフレール",
    "运动外观套件": "スポーツ外観キット",
    "隐藏电动门把手": "自動格納式ドアハンドル",
    "外后视镜功能": "ドアミラー機能",
    "外后视镜加热": "ドアミラー加熱",
    "外后视镜电动调节": "ドアミラー電動調整",
    "外后视镜自动防眩目": "ドアミラー自動防眩",
    "外后视镜电动折叠": "ドアミラー電動格納",
    "外后视镜锁车自动折叠": "施錠時ミラー自動格納",
    "外后视镜记忆": "ドアミラー位置メモリー",
    "外后视镜倒车自动下翻": "後退時ミラー自動下向き",
    "外后视镜盲区影像系统": "ミラーブラインドビュー",
    "前/后电动车窗": "前/後パワーウィンドウ",
    "多层隔音玻璃": "遮音ガラス",
    "隐私玻璃": "プライバシーガラス",
    "感应雨刷": "オートワイパー",
    "后雨刷": "リヤワイパー",
    "矩阵式大灯": "マトリクスLEDヘッドライト",
    "自适应远近光": "アダプティブハイビーム",
    "自动大灯": "オートライト",
    "大灯高度可调": "ヘッドライトレベライザー",
    "大灯清洗装置": "ヘッドライトウォッシャー",
    "LED日间行车灯": "LEDデイタイムランニングライト",
    "前雾灯": "フロントフォグランプ",
    "转向辅助灯": "コーナリングランプ",
    "雨雾模式": "レイン/フォグモード",
    "前大灯光源": "ヘッドライト光源",
    "近光灯光源": "ロービーム光源",
    "远光灯光源": "ハイビーム光源",
    "前大灯自动开闭": "ヘッドライト自動点灯/消灯",
    "自适应远近光(ADB)": "ADB自動配光",
    "大灯随动转向": "AFS(可変配光)",
    "车内遮阳板化妆镜": "バイザー・バニティミラー",
    "内后视镜功能": "ルームミラー機能",
    "内后视镜自动防眩目": "自動防眩ルームミラー",
    "方向盘材质": "ステアリング素材",
    "方向盘调节": "ステアリング調整",
    "方向盘电动调节": "電動ステアリング調整",
    "方向盘记忆": "ステアリング位置メモリー",
    "方向盘加热": "ステアリングヒーター",
    "方向盘换挡": "パドルシフト",
    "方向盘样式": "ステアリングデザイン",
    "内饰颜色": "内装色",
    "座椅材质": "シート素材",
    "运动风格座椅": "スポーツシート",
    "座椅电动调节": "電動シート調整",
    "主/副驾驶座电动调节": "運転席/助手席電動調整",
    "主/副驾驶座电动座椅记忆": "運転席/助手席シートメモリー",
    "主/副驾驶座电动腿托": "運転席/助手席電動レッグサポート",
    "主/副驾驶座腰部支撑调节": "運転席/助手席ランバーサポート",
    "副驾驶座后排可调节按钮": "助手席後席調整スイッチ",
    "副驾驶座老板键": "助手席ボスキー",
    "第二排座椅调节方式": "2列目シート調整方式",
    "第二排靠背调节": "2列目リクライニング",
    "第二排座椅电动调节": "2列目電動調整",
    "第二排座椅电动腿托": "2列目電動レッグサポート",
    "第二排座椅加热": "2列目シートヒーター",
    "第二排座椅通风": "2列目シートベンチレーション",
    "第二排座椅按摩": "2列目シートマッサージ",
    "后排座椅放倒": "後席シート可倒",
    "后排座椅比例放倒": "後席6:4/4:2:4分割",
    "后排座椅电动放倒": "後席電動可倒",
    "后排中央头枕": "後席中央ヘッドレスト",
    "后排杯架": "後席カップホルダー",
    "前/后中央扶手": "フロント/リヤセンターアームレスト",
    "主驾驶座椅功能": "運転席シート機能",
    "副驾驶座椅功能": "助手席シート機能",
    "前排座椅功能": "フロントシート機能",
    "后排座椅功能": "リヤシート機能",
    "座椅布局": "シートレイアウト",
    "座椅通风": "シートベンチレーション",
    "座椅加热": "シートヒーター",
    "座椅按摩": "シートマッサージ",
    "天窗类型": "サンルーフ種類",
    "天窗尺寸": "サンルーフサイズ",
    "可开启全景天窗": "開閉式パノラマルーフ",
    "车顶控制面板": "ルーフコントロールパネル",
    "车厢冷藏功能": "キャビン冷蔵機能",
    "空调控制方式": "エアコン操作方式",
    "空调温区控制": "エアコン温度ゾーン",
    "后排独立空调": "後席独立エアコン",
    "车载PM2.5过滤装置": "PM2.5フィルター",
    "车内香氛系统": "車内フレグランス",
    "负离子发生器": "マイナスイオン発生器",
    "座舱清洁/自洁": "キャビン清浄/セルフクリーニング",
    "车内紫外线杀菌灯": "UV殺菌ライト",
    "空气质量监测": "空気質センサー",
    "手机无线充电": "スマホワイヤレス充電",
    "USB/Type-C接口数量": "USB/Type-Cポート数",
    "车内电源电压": "車内電源電圧",
    "12V电源接口": "12V電源ソケット",
    "220V/230V电源": "AC電源(220/230V)",
    "车载冰箱": "車載冷蔵庫",
    "车内充电接口": "車内充電ポート",
    "车内充电功率": "車内充電出力",
    "钥匙类型": "キー種類",
    "遥控钥匙": "リモコンキー",
    "智能遥控钥匙": "スマートキー",
    "蓝牙钥匙": "Bluetoothキー",
    "数字钥匙": "デジタルキー",
    "隐藏式门把手": "格納式ドアハンドル",
    "中控锁": "集中ドアロック",
    "一键升降": "ワンタッチ上下",
    "玻璃防夹功能": "挟み込み防止",
    "车窗一键防夹": "ワンタッチ挟み込み防止",
    "后排隐私玻璃": "後席プライバシーガラス",
    "电吸门": "ソフトクローズドア",
    "电动侧滑门": "電動スライドドア",
    "电动尾门位置记忆": "テールゲート開度メモリー",
    "后备厢12V电源": "ラゲッジ12V電源",
    "感应雨刮": "オートワイパー",
    "雨量感应": "レインセンサー",
    "车外温度显示": "外気温表示",
    "风阻系数(Cd)": "空気抵抗係数(Cd)",
    "转向助力类型": "パワステ形式",
    "转向比": "ステアリング比",
    "最小转弯半径(m)": "最小回転半径(m)",
    "行车记录仪": "ドライブレコーダー",
    "内置行车记录仪": "内蔵ドライブレコーダー",
    "雷达探测器": "レーダー探知機",
    "驾驶模式切换": "ドライブモード切替",
    "驾驶辅助影像": "運転支援映像",
    "手机APP远程功能": "スマホアプリ遠隔操作",
    "远程启动功能": "リモートスタート機能",
    "ETC装置": "ETC装置",
    "V2X": "V2X"
}

# =============================
# 金額整形（万元→元→円、「日本円 約…円」）
# =============================
RE_WAN = re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*万")
RE_YUAN = re.compile(r"(?P<num>[\d,]+)\s*元")

def parse_cny(text: str):
    t = str(text)
    m1 = RE_WAN.search(t)
    if m1:
        return float(m1.group("num")) * 10000.0
    m2 = RE_YUAN.search(t)
    if m2:
        return float(m2.group("num").replace(",", ""))
    return None

def _format_yuan_and_jpy(cell: str, rate: float) -> str:
    t = strip_any_yen_tokens(clean_price_cell(cell))
    if not t or t in {"-", "–", "—"}:
        return t
    cny = parse_cny(t)
    if cny is None:
        if ("元" not in t) and RE_WAN.search(t):
            t = f"{t}元"
        return t
    m1 = RE_WAN.search(t)
    yuan_disp = f"{m1.group('num')}万元" if m1 else (t if "元" in t else f"{t}元")
    jpy = int(round(cny * EXRATE_CNY_TO_JPY))
    return f"{yuan_disp}（日本円 約{jpy:,}円）"

def msrp_to_yuan_and_jpy(cell: str, rate: float) -> str:
    return _format_yuan_and_jpy(cell, rate)

def dealer_to_yuan_and_jpy(cell: str, rate: float) -> str:
    return _format_yuan_and_jpy(cell, rate)

# =============================
# 便利関数
# =============================
def uniq(seq):
    s, out = set(), []
    for x in seq:
        if x not in s:
            s.add(x)
            out.append(x)
    return out

def chunked(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def parse_json_relaxed(content: str, terms: list[str]) -> dict[str, str]:
    try:
        d = json.loads(content)
        if isinstance(d, dict) and "translations" in d:
            return {
                str(t["cn"]).strip(): str(t.get("ja", t["cn"])).strip()
                for t in d["translations"]
                if t.get("cn")
            }
    except Exception:
        pass
    pairs = re.findall(r'"cn"\s*:\s*"([^"]+)"\s*,\s*"ja"\s*:\s*"([^"]*)"', content)
    if pairs:
        return {cn.strip(): ja.strip() for cn, ja in pairs}
    return {t: t for t in terms}

# =============================
# 翻訳クラス
# =============================
class Translator:
    def __init__(self, model: str, api_key: str):
        if not (api_key and api_key.strip()):
            raise RuntimeError("OPENAI_API_KEY is not set")
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.system = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セクション名/項目名/モデル名/セル値』の配列です。"
            "セクション名/項目名も鑑みつつ、自然で簡潔な日本語へ翻訳してください。数値・年式・排量・AT/MT等の記号は保持。"
            "出力は JSON（{'translations':[{'cn':'原文','ja':'訳文'}]}）のみ。"
        )

    def translate_batch(self, terms: list[str]) -> dict[str, str]:
        if not terms:
            return {}
        msgs = [
            {"role": "system", "content": self.system},
            {"role": "user", "content": json.dumps({"terms": terms}, ensure_ascii=False)},
        ]
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=msgs,
                temperature=0,
                response_format={"type": "json_object"},
            )
            content = resp.choices[0].message.content or ""
            return parse_json_relaxed(content, terms)
        except Exception as e:
            print("❌ OpenAI error:", repr(e))
            return {t: t for t in terms}

    def translate_unique(self, unique_terms: list[str]) -> dict[str, str]:
        out = {}
        for chunk in chunked(unique_terms, BATCH_SIZE):
            for attempt in range(1, RETRIES + 1):
                try:
                    out.update(self.translate_batch(chunk))
                    break
                except Exception as e:
                    print(f"❌ translate_unique error attempt={attempt}:", repr(e))
                    if attempt == RETRIES:
                        for t in chunk:
                            out.setdefault(t, t)
                    time.sleep(SLEEP_BASE * attempt)
        return out

# =============================
# キャッシュ（固定辞書 → シリーズキャッシュ(JSON) → メモリ）
# =============================
def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

CACHE_DIR = ensure_dir(Path("cache") / SERIES_FOR_CACHE)
CACHE_FILES = {
    "section": CACHE_DIR / "sections.json",
    "item":    CACHE_DIR / "items.json",
    "value":   CACHE_DIR / "values.json",
    "col":     CACHE_DIR / "columns.json",
}

def load_json(p: Path) -> dict[str, str]:
    try:
        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"⚠️ cache load failed {p}: {e}")
    return {}

def dump_json_safe(p: Path, data: dict[str, str]):
    try:
        ensure_dir(p.parent)
        tmp = p.with_suffix(p.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(p)
    except Exception as e:
        print(f"⚠️ cache save failed {p}: {e}")

# メモリキャッシュ（実行中のみ）
MEM_CACHE = {
    "section": {},
    "item": {},
    "value": {},
    "col": {},
}

SERIES_CACHE = {
    "section": load_json(CACHE_FILES["section"]),
    "item":    load_json(CACHE_FILES["item"]),
    "value":   load_json(CACHE_FILES["value"]),
    "col":     load_json(CACHE_FILES["col"]),
}

def translate_with_caches(kind: str, terms: list[str], fixed_map: dict[str, str], tr: Translator) -> dict[str, str]:
    """
    優先順: 固定辞書 > シリーズキャッシュ(JSON) > メモリキャッシュ > LLM
    """
    out: dict[str, str] = {}

    # 1) 固定辞書
    for t in terms:
        if t in fixed_map:
            out[t] = fixed_map[t]

    # 2) シリーズキャッシュ(JSON)
    for t in terms:
        if t not in out and t in SERIES_CACHE[kind]:
            out[t] = SERIES_CACHE[kind][t]

    # 3) メモリキャッシュ
    for t in terms:
        if t not in out and t in MEM_CACHE[kind]:
            out[t] = MEM_CACHE[kind][t]

    # 4) LLM
    need = [t for t in terms if t not in out]
    if need:
        llm_map = tr.translate_unique(uniq(need))
        out.update(llm_map)
        # メモリ・シリーズキャッシュに反映
        for k, v in llm_map.items():
            MEM_CACHE[kind][k] = v
            SERIES_CACHE[kind][k] = v

    return out

# =============================
# モデル名・グレード整形
# =============================
YEAR_TOKEN_RE = re.compile(r"(?:20\d{2}|19\d{2})|(?:\d{2}款|[上中下]市|改款|年款)")
LEADING_TOKEN_RE = re.compile(r"^[\u4e00-\u9fffA-Za-z][\u4e00-\u9fffA-Za-z0-9\- ]{1,40}")

def cut_before_year_or_kuan(s: str) -> str | None:
    s = s.strip()
    m = YEAR_TOKEN_RE.search(s)
    if m:
        return s[:m.start()].strip()
    kuan = re.search(r"款", s)
    if kuan:
        return s[:kuan.start()].strip()
    m2 = LEADING_TOKEN_RE.match(s)
    return m2.group(0).strip() if m2 else None

def detect_common_series_prefix(cols: list[str]) -> str | None:
    cand = []
    for c in cols:
        p = cut_before_year_or_kuan(str(c))
        if p and len(p) >= 2:
            cand.append(p)
    if not cand:
        return None
    from collections import Counter
    top, ct = Counter(cand).most_common(1)[0]
    return re.escape(top) if ct >= max(1, int(0.6 * len(cols))) else None

def strip_series_prefix_from_grades(grade_cols: list[str]) -> list[str]:
    if not grade_cols or not STRIP_GRADE_PREFIX:
        return grade_cols
    pattern = SERIES_PREFIX_RE or detect_common_series_prefix(grade_cols)
    if not pattern:
        return grade_cols
    regex = re.compile(rf"^\s*(?:{pattern})\s*[-:：/ ]*\s*", re.IGNORECASE)
    return [regex.sub("", str(c)).strip() or c for c in grade_cols]

def grade_rule_ja(s: str) -> str:
    t = str(s).strip()
    t = re.sub(r"(\d{4})\s*款", r"\1年モデル", t)
    repl = {
        "改款": "改良版",
        "运动型": "スポーツタイプ",
        "运动": "スポーツ",
        "四驱": "4WD",
        "两驱": "2WD",
        "全驱": "AWD",
    }
    for cn, ja in repl.items():
        t = t.replace(cn, ja)
    t = re.sub(r"\s*[-:：/]\s*", " ", t).strip()
    return t

# =============================
# main
# =============================
def main():
    print(f"CSV_IN: {SRC}")
    if not Path(SRC).exists():
        raise FileNotFoundError(f"入力CSVが見つかりません: {SRC}")

    df = pd.read_csv(SRC, encoding="utf-8-sig")
    df.columns = [BRAND_MAP.get(c, c) for c in df.columns]

    tr = Translator(MODEL, API_KEY)

    # セクション/項目：辞書を先に適用、無いものはキャッシュ優先で補完
    uniq_sec  = uniq([str(x).strip() for x in df["セクション"].fillna("") if str(x).strip()])
    uniq_item = uniq([str(x).strip() for x in df["項目"].fillna("")    if str(x).strip()])

    sec_map = translate_with_caches("section", uniq_sec, FIX_JA_SECTIONS, tr)
    item_map = translate_with_caches("item", uniq_item, FIX_JA_ITEMS, tr)

    df.insert(1, "セクション_ja", df["セクション"].map(lambda s: sec_map.get(str(s).strip(), str(s).strip())))
    df.insert(3, "項目_ja",       df["項目"].map(lambda s: item_map.get(str(s).strip(),   str(s).strip())))

    # モデル列（ヘッダ）
    if TRANSLATE_COLNAMES:
        orig_cols = list(df.columns)
        fixed = orig_cols[:4]
        grades = orig_cols[4:]
        grades_stripped = strip_series_prefix_from_grades(grades)
        grades_rule_ja = [grade_rule_ja(g) for g in grades_stripped]
        need_llm = [g for g in grades_rule_ja if re.search(r"[\u4e00-\u9fff]", g)]
        col_map = translate_with_caches("col", uniq(need_llm), {}, tr) if need_llm else {}
        final_grades = [col_map.get(g, g) for g in grades_rule_ja]
        df.columns = fixed + final_grades

    # 価格行検出
    def norm_key(s: str) -> str:
        s = str(s)
        s = re.sub(r"[ \t\u3000\u00A0\u200b\ufeff]+", "", s)
        s = re.sub(r"[（(].*?[）)]", "", s)
        return s

    key_cn_norm = df["項目"].map(norm_key)
    key_ja_norm = df["項目_ja"].map(norm_key)

    is_msrp = (
        key_cn_norm.str.contains("厂商指导", na=False) |
        key_ja_norm.str.contains("メーカー希望小売", na=False)
    )
    is_dealer = (
        key_cn_norm.str.contains("经销商", na=False) |
        key_ja_norm.str.contains("ディーラー販売価格", na=False)
    )

    msrp_count = int(is_msrp.sum())
    dealer_count = int(is_dealer.sum())
    print(f"🔎 price rows: msrp={msrp_count}, dealer={dealer_count}")
    if msrp_count:
        i0 = is_msrp.idxmax()
        print(f"  sample MSRP key: CN='{df.at[i0,'項目']}', JA='{df.at[i0,'項目_ja']}'")
    if dealer_count:
        j0 = is_dealer.idxmax()
        print(f"  sample Dealer key: CN='{df.at[j0,'項目']}', JA='{df.at[j0,'項目_ja']}'")

    # 価格セル変換（列番号で処理）＋ロック
    converted_cells: dict[tuple[int, int], str] = {}
    for col_idx in range(4, len(df.columns)):
        for row_idx in df.index[is_msrp]:
            oldv = df.iloc[row_idx, col_idx]
            newv = msrp_to_yuan_and_jpy(oldv, EXRATE_CNY_TO_JPY)
            converted_cells[(row_idx, col_idx)] = newv
        for row_idx in df.index[is_dealer]:
            oldv = df.iloc[row_idx, col_idx]
            newv = dealer_to_yuan_and_jpy(oldv, EXRATE_CNY_TO_JPY)
            converted_cells[(row_idx, col_idx)] = newv

    # 値セルクリーン（価格行は除外）
    df_vals = df.copy()
    for row_idx in range(len(df_vals)):
        for col_idx in range(4, len(df_vals.columns)):
            if is_msrp.iloc[row_idx] or is_dealer.iloc[row_idx]:
                continue
            df_vals.iat[row_idx, col_idx] = clean_any_noise(df_vals.iat[row_idx, col_idx])

    # 値セル翻訳（固定→シリーズ→メモリ→LLM）
    if TRANSLATE_VALUES:
        numeric_like = re.compile(r"^[\d\.\,\%\:/xX\+\-\(\)~～\smmkKwWhHVVAhL丨·—–]+$")
        tr_values_terms = []
        coords = []
        for row_idx in range(len(df_vals)):
            if is_msrp.iloc[row_idx] or is_dealer.iloc[row_idx]:
                continue
            for col_idx in range(4, len(df_vals.columns)):
                v = str(df_vals.iat[row_idx, col_idx]).strip()
                if v in {"", "●", "○", "–", "-", "—"}:
                    continue
                if numeric_like.fullmatch(v):
                    continue
                tr_values_terms.append(v)
                coords.append((row_idx, col_idx))
        uniq_vals = uniq(tr_values_terms)
        # 値の固定辞書は今は無し({})。キャッシュ優先。
        val_map = translate_with_caches("value", uniq_vals, {}, tr) if uniq_vals else {}
        for (row_idx, col_idx) in coords:
            s = str(df_vals.iat[row_idx, col_idx]).strip()
            df.iat[row_idx, col_idx] = val_map.get(s, s)

    # 価格ロック再適用
    for (row_idx, col_idx), val in converted_cells.items():
        df.iat[row_idx, col_idx] = val

    # キャッシュ保存
    dump_json_safe(CACHE_FILES["section"], SERIES_CACHE["section"])
    dump_json_safe(CACHE_FILES["item"],    SERIES_CACHE["item"])
    dump_json_safe(CACHE_FILES["value"],   SERIES_CACHE["value"])
    dump_json_safe(CACHE_FILES["col"],     SERIES_CACHE["col"])

    # 出力
    DST_PRIMARY.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(DST_PRIMARY,   index=False, encoding="utf-8-sig")
    df.to_csv(DST_SECONDARY, index=False, encoding="utf-8-sig")
    print(f"✅ Saved: {DST_PRIMARY}")

if __name__ == "__main__":
    main()
