from __future__ import annotations
import os, json, time, re
from pathlib import Path
import pandas as pd

# ====== 入出力 ======
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

# ====== 設定 ======
MODEL   = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
API_KEY = os.environ.get("OPENAI_API_KEY")  # 未知語が無い場合は未設定でもOK

TRANSLATE_VALUES   = os.environ.get("TRANSLATE_VALUES", "true").lower() == "true"
TRANSLATE_COLNAMES = os.environ.get("TRANSLATE_COLNAMES", "true").lower() == "true"
STRIP_GRADE_PREFIX = os.environ.get("STRIP_GRADE_PREFIX", "true").lower() == "true"
SERIES_PREFIX_RE   = os.environ.get("SERIES_PREFIX", "").strip()
EXRATE_CNY_TO_JPY  = float(os.environ.get("EXRATE_CNY_TO_JPY", "21.0"))

BATCH_SIZE, RETRIES, SLEEP_BASE = 60, 3, 1.2

# ====== クリーニング・固定訳 ======
NOISE_ANY = ["对比","参数","图片","配置","详情"]
NOISE_PRICE_TAIL = ["询价","计算器","询底价","报价","价格询问","起","起售","到店","经销商"]

def clean_any_noise(s:str)->str:
    s=str(s) if s is not None else ""
    for w in NOISE_ANY+NOISE_PRICE_TAIL:
        s=s.replace(w,"")
    return re.sub(r"\s+"," ",s).strip(" 　-—–")

def clean_price_cell(s:str)->str:
    t=clean_any_noise(s)
    for w in NOISE_PRICE_TAIL:
        t=re.sub(rf"(?:\s*{re.escape(w)}\s*)+$","",t)
    return t.strip()

RE_PAREN_ANY_YEN=re.compile(r"（[^）]*(?:日本円|JPY|[¥￥]|円)[^）]*）")
RE_ANY_YEN_TOKEN=re.compile(r"(日本円|JPY|[¥￥]|円)")
def strip_any_yen_tokens(s:str)->str:
    t=str(s)
    t=RE_PAREN_ANY_YEN.sub("",t)
    t=RE_ANY_YEN_TOKEN.sub("",t)
    return re.sub(r"\s+"," ",t).strip()

BRAND_MAP={
    "BYD":"BYD","比亚迪":"BYD",
    "奔驰":"メルセデス・ベンツ","梅赛德斯-奔驰":"メルセデス・ベンツ",
}

# ====== 固定辞書（技術文書的な日本語・CSVサンプルに基づく） ======
FIX_JA_SECTIONS = {
    "基本参数": "基本仕様",
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
    "智能化配置": "コネクテッド／電子装備",
    "方向盘/内后视镜": "ステアリング／ルームミラー",
    "车内充电": "車内充電",
    "座椅配置": "シート",
    "音响/车内灯光": "オーディオ／室内照明",
    "空调/冰箱": "空調／冷蔵",
    "颜色": "カラー",
    "选装包": "オプションパッケージ",
}

FIX_JA_ITEMS = {
    # 価格・販売
    "厂商指导价": "メーカー希望小売価格",
    "厂商指导价(元)": "メーカー希望小売価格",
    "经销商报价": "ディーラー販売価格（元）",
    "经销商参考价": "ディーラー販売価格（元）",
    "经销商": "ディーラー販売価格（元）",

    # 基本仕様
    "厂商": "メーカー",
    "级别": "車格",
    "能源类型": "燃料種別",
    "环保标准": "排出ガス基準",
    "上市时间": "発売時期",
    "整车质保": "車両保証",
    "整备质量(kg)": "車両重量（kg）",
    "最大满载质量(kg)": "最大総重量（kg）",

    # 寸法
    "长*宽*高(mm)": "全長×全幅×全高（mm）",
    "长度(mm)": "全長（mm）",
    "宽度(mm)": "全幅（mm）",
    "高度(mm)": "全高（mm）",
    "轴距(mm)": "ホイールベース（mm）",
    "前轮距(mm)": "フロントトレッド（mm）",
    "后轮距(mm)": "リアトレッド（mm）",
    "接近角(°)": "アプローチアングル（°）",
    "离去角(°)": "デパーチャーアングル（°）",
    "车身结构": "ボディ構造",
    "车门开启方式": "ドア開閉方式",
    "车门数(个)": "ドア数（枚）",
    "座位数(个)": "乗車定員（名）",
    "油箱容积(L)": "燃料タンク容量（L）",
    "后备厢容积(L)": "ラゲッジ容量（L）",
    "风阻系数(Cd)": "空気抵抗係数（Cd）",

    # エンジン
    "发动机": "エンジン",
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
    "最大功率(kW)": "最大出力（kW）",
    "最大功率转速(rpm)": "最大出力回転数（rpm）",
    "最大扭矩(N·m)": "最大トルク（N·m）",
    "最大扭矩转速(rpm)": "最大トルク回転数（rpm）",
    "最大净功率(kW)": "最大正味出力（kW）",
    "燃油标号": "推奨燃料オクタン価",
    "供油方式": "燃料供給方式",
    "缸盖材料": "シリンダーヘッド材質",
    "缸体材料": "シリンダーブロック材質",

    # 走行性能・燃費
    "官方0-100km/h加速(s)": "0-100km/h加速（公式）（s）",
    "最高车速(km/h)": "最高速度（km/h）",
    "WLTC综合油耗(L/100km)": "WLTC総合燃費（L/100km）",

    # 変速機
    "简称": "略称",
    "挡位个数": "段数",
    "变速箱": "トランスミッション",
    "变速箱类型": "トランスミッション形式",

    # 駆動／シャシー
    "驱动方式": "駆動方式",
    "四驱形式": "四輪駆動方式",
    "中央差速器结构": "センターデフ構造",
    "前悬架类型": "フロントサスペンション形式",
    "后悬架类型": "リアサスペンション形式",
    "助力类型": "ステアリングアシスト方式",
    "车体结构": "フレーム構造",

    # ブレーキ・タイヤ
    "前制动器类型": "フロントブレーキ形式",
    "后制动器类型": "リアブレーキ形式",
    "驻车制动类型": "パーキングブレーキ形式",
    "前轮胎规格": "フロントタイヤサイズ",
    "后轮胎规格": "リアタイヤサイズ",
    "备胎规格": "スペアタイヤ仕様",

    # 受動安全
    "主/副驾驶座安全气囊": "運転席／助手席エアバッグ",
    "前/后排侧气囊": "前席／後席サイドエアバッグ",
    "前/后排头部气囊(气帘)": "前後席カーテンエアバッグ",
    "膝部气囊": "ニーエアバッグ",
    "前排中间气囊": "前席センターエアバッグ",
    "被动行人保护": "歩行者保護（受動）",

    # 能動安全
    "ABS防抱死": "ABS（アンチロックブレーキ）",
    "制动力分配(EBD/CBC等)": "制動力配分（EBD/CBC等）",
    "刹车辅助(EBA/BAS/BA等)": "ブレーキアシスト（EBA/BAS/BA等）",
    "牵引力控制(ASR/TCS/TRC等)": "トラクションコントロール（ASR/TCS/TRC等）",
    "车身稳定控制(ESC/ESP/DSC等)": "車両安定制御（ESC/ESP/DSC等）",
    "胎压监测功能": "タイヤ空気圧監視",
    "安全带未系提醒": "シートベルト非装着警報",
    "ISOFIX儿童座椅接口": "ISOFIXチャイルドシート固定具",

    # 運転支援ハード
    "前/后驻车雷达": "前後パーキングセンサー",
    "驾驶辅助影像": "周囲監視カメラ",
    "前方感知摄像头": "前方検知カメラ",
    "摄像头数量": "カメラ数",
    "车内摄像头数量": "車内カメラ数",
    "超声波雷达数量": "超音波センサー数",

    # 運転支援機能
    "巡航系统": "クルーズ制御",
    "辅助驾驶等级": "運転支援レベル",
    "卫星导航系统": "ナビゲーションシステム",
    "导航路况信息显示": "交通情報表示",
    "地图品牌": "地図ブランド",
    "AR实景导航": "AR実写ナビ",
    "并线辅助": "車線変更支援",
    "车道保持辅助系统": "車線維持支援",
    "车道偏离预警系统": "車線逸脱警報",
    "车道居中保持": "車線中央維持",
    "道路交通标识识别": "交通標識認識",
    "主动刹车/主动安全系统": "自動緊急ブレーキ（AEB）",
    "疲劳驾驶提示": "ドライバー疲労警報",
    "前方碰撞预警": "前方衝突警報",
    "内置行车记录仪": "ドライブレコーダー内蔵",
    "道路救援呼叫": "ロードアシストコール",
    "自动泊车入位": "自動駐車支援",
    "辅助变道": "自動車線変更支援",
    "辅助匝道自动驶出(入)": "インターチェンジ出入支援",
    "辅助驾驶路段": "支援対応路種",
    "驾驶模式切换": "ドライビングモード切替",
    "发动机启停技术": "アイドリングストップ",
    "自动驻车": "オートホールド",
    "上坡辅助": "ヒルスタートアシスト",
    "可变悬架功能": "可変サスペンション機能",
    "可变转向比": "可変ステアリング比",

    # 外装／防盗
    "外观套件": "エクステリアパッケージ",
    "运动风格": "スポーツスタイル",
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

    # 車外照明
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

    # サンルーフ／ウインドウ
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

    # ドアミラー
    "外后视镜功能": "ドアミラー機能",

    # ディスプレイ／システム
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

    # コネクテッド
    "车联网": "車載通信（コネクテッド）",
    "4G/5G网络": "4G/5G通信",
    "OTA升级": "OTAアップデート",
    "V2X通讯": "V2X通信",
    "手机APP远程功能": "スマホアプリ遠隔機能",

    # ステアリング／ミラー（内装）
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

    # 充電・インターフェース
    "多媒体/充电接口": "マルチメディア／充電インターフェース",
    "USB/Type-C接口数量": "USB/Type-Cポート数",
    "手机无线充电功能": "スマートフォンワイヤレス充電",

    # シート
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

    # オーディオ／室内照明
    "扬声器品牌名称": "スピーカーブランド",
    "扬声器数量": "スピーカー数",
    "杜比全景声(Dolby Atmos)": "Dolby Atmos",
    "车内环境氛围灯": "アンビエントライト",
    "主动式环境氛围灯": "アクティブアンビエントライト",

    # 空調／冷蔵
    "空调温度控制方式": "空調温度制御方式",
    "后排独立空调": "後席独立空調",
    "后座出风口": "後席エアアウトレット",
    "温度分区控制": "温度独立調整（ゾーン）",
    "车载空气净化器": "車載空気清浄機",
    "车内PM2.5过滤装置": "車内PM2.5フィルター",
    "空气质量监测": "空気質モニタリング",

    # カラー／オプション
    "外观颜色": "外装色",
    "内饰颜色": "内装色",
    "智享套装2": "スマートコンフォートパッケージ2",
    "智能领航辅助Max": "インテリジェントナビゲーションアシストMax",
    "智乐套装": "スマートエンターテインメントパッケージ",
}

# ====== 金額整形（必要なら利用可：本スクリプトでは辞書優先でLLM前提の変更は無し） ======
RE_WAN=re.compile(r"(?P<num>\d+(?:\.\d+)?)\s*万")
RE_YUAN=re.compile(r"(?P<num>[\d,]+)\s*元")

def parse_cny(text:str):
    t=str(text)
    m1=RE_WAN.search(t)
    if m1:return float(m1.group("num"))*10000.0
    m2=RE_YUAN.search(t)
    if m2:return float(m2.group("num").replace(",",""))
    return None

def msrp_to_yuan_and_jpy(cell:str,rate:float)->str:
    t=strip_any_yen_tokens(clean_price_cell(cell))
    if not t or t in {"-","–","—"}:return t
    cny=parse_cny(t)
    if cny is None:
        if("元"not in t)and RE_WAN.search(t):t=f"{t}元"
        return t
    m1=RE_WAN.search(t)
    yuan_disp=f"{m1.group('num')}万元" if m1 else (t if"元"in t else f"{t}元")
    jpy=int(round(cny*rate))
    return f"{yuan_disp}（日本円{jpy:,}円）"

def dealer_to_yuan_only(cell:str)->str:
    t=strip_any_yen_tokens(clean_price_cell(cell))
    if not t or t in {"-","–","—"}:return t
    if("元"not in t)and RE_WAN.search(t):t=f"{t}元"
    return t

# ====== ユーティリティ ======
def uniq(seq):
    s, out = set(), []
    for x in seq:
        if x not in s:
            s.add(x); out.append(x)
    return out

def chunked(xs, n):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def parse_json_relaxed(content:str,terms:list[str])->dict[str,str]:
    try:
        d=json.loads(content)
        if isinstance(d,dict)and"translations"in d:
            return {str(t["cn"]).strip():str(t.get("ja",t["cn"])).strip()
                    for t in d["translations"] if t.get("cn")}
    except Exception:
        pass
    pairs=re.findall(r'"cn"\s*:\s*"([^"]+)"\s*,\s*"ja"\s*:\s*"([^"]*)"', content)
    if pairs:
        return {cn.strip():ja.strip() for cn,ja in pairs}
    return {t:t for t in terms}

# ====== LLM（未知語がある時のみ使用） ======
class Translator:
    def __init__(self, model: str, api_key: str):
        from openai import OpenAI  # 未使用時はimportしない
        if not (api_key and api_key.strip()):
            raise RuntimeError("OPENAI_API_KEY is not set")
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.system = (
            "あなたは自動車仕様表の専門翻訳者です。"
            "入力は中国語の『セクション名/項目名/モデル名/セル値』の配列です。"
            "技術文書的な日本語に翻訳してください。単位・記号は保持。"
            "出力は JSON（{'translations':[{'cn':'原文','ja':'訳文'}]}）のみ。"
        )
        print(f"🟢 Translator ready: model={self.model}")

    def translate_batch(self, terms: list[str]) -> dict[str,str]:
        if not terms:
            return {}
        msgs=[
            {"role":"system","content":self.system},
            {"role":"user","content":json.dumps({"terms":terms},ensure_ascii=False)},
        ]
        try:
            resp=self.client.chat.completions.create(
                model=self.model,messages=msgs,temperature=0,
                response_format={"type":"json_object"},
            )
            content=resp.choices[0].message.content or ""
            return parse_json_relaxed(content, terms)
        except Exception as e:
            print("❌ OpenAI error:", repr(e))
            return {t: t for t in terms}

    def translate_unique(self, unique_terms: list[str]) -> dict[str,str]:
        out={}
        for chunk in chunked(unique_terms, BATCH_SIZE):
            for attempt in range(1, RETRIES+1):
                try:
                    out.update(self.translate_batch(chunk))
                    break
                except Exception as e:
                    print(f"❌ translate_unique error attempt={attempt}:", repr(e))
                    if attempt==RETRIES:
                        for t in chunk: out.setdefault(t, t)
                    time.sleep(SLEEP_BASE*attempt)
        return out

# ====== main ======
def main():
    print(f"CSV_IN check: {SRC}")
    print(f"🔎 SRC: {SRC}")
    print(f"📝 DST(primary): {DST_PRIMARY}")
    print(f"📝 DST(secondary): {DST_SECONDARY}")

    if not Path(SRC).exists():
        raise FileNotFoundError(f"入力CSVが見つかりません: {SRC}")

    # 原文（CN）読込・ノイズ掃除
    df = pd.read_csv(SRC, encoding="utf-8-sig").map(clean_any_noise)
    # 列ヘッダのブランド正規化
    df.columns = [BRAND_MAP.get(c, c) for c in df.columns]

    # ------- セクション／項目（辞書優先でキュー除外） -------
    uniq_sec  = uniq([str(x).strip() for x in df["セクション"].fillna("") if str(x).strip()])
    uniq_item = uniq([str(x).strip() for x in df["項目"].fillna("")    if str(x).strip()])
    print(f"🔢 uniq_sec={len(uniq_sec)}, uniq_item={len(uniq_item)}")

    known_sec  = set(FIX_JA_SECTIONS.keys())
    known_item = set(FIX_JA_ITEMS.keys())

    # 既知は最初から確定。未知のみ翻訳候補へ（通常は0件を想定）
    sec_to_translate  = [x for x in uniq_sec  if x not in known_sec]
    item_to_translate = [x for x in uniq_item if x not in known_item]
    print(f"🌐 to_translate: sec={len(sec_to_translate)} item={len(item_to_translate)}")

    # 未知語がある場合だけ翻訳器を初期化
    if sec_to_translate or item_to_translate:
        tr = Translator(MODEL, API_KEY)
        sec_map_new  = tr.translate_unique(sec_to_translate)  if sec_to_translate  else {}
        item_map_new = tr.translate_unique(item_to_translate) if item_to_translate else {}
    else:
        sec_map_new, item_map_new = {}, {}

    # 最終マップ（辞書を優先、未知語分のみ追加）
    sec_map  = {**FIX_JA_SECTIONS, **sec_map_new}
    item_map = {**FIX_JA_ITEMS,    **item_map_new}

    out_full = df.copy()
    out_full.insert(1, "セクション_ja", out_full["セクション"].map(lambda s: sec_map.get(str(s).strip(), str(s).strip())))
    out_full.insert(3, "項目_ja",     out_full["項目"].map(lambda s: item_map.get(str(s).strip(), str(s).strip())))

    # 列ヘッダ（グレード列）の前処理（ブランド記号やシリーズ接頭辞の除去のみ。翻訳はそのまま）
    if TRANSLATE_COLNAMES:
        orig_cols=list(out_full.columns); fixed=orig_cols[:4]; grades=orig_cols[4:]
        grades_norm=[BRAND_MAP.get(c,c) for c in grades]
        def detect_common_series_prefix(cols:list[str])->str|None:
            if not cols: return None
            s=os.path.commonprefix([str(c) for c in cols]).strip()
            return s if s and len(s)>=2 else None
        def strip_series_prefix_from_grades(grade_cols:list[str])->list[str]:
            if not grade_cols or not STRIP_GRADE_PREFIX:return grade_cols
            pattern=SERIES_PREFIX_RE or detect_common_series_prefix(grade_cols)
            if not pattern:return grade_cols
            regex=re.compile(rf"^\s*(?:{re.escape(pattern)})\s*[-:：/ ]*\s*",re.IGNORECASE)
            return [regex.sub("",str(c)).strip() or c for c in grade_cols]
        grades_stripped=strip_series_prefix_from_grades(grades_norm)
        out_full.columns=fixed+grades_stripped

    # 出力（CN列は残さない）
    def _norm(s: str) -> str:
        s = str(s)
        s = re.sub(r"[\u200b\ufeff\u00A0\u3000 \t]+", "", s)
        return s

    cols = list(out_full.columns)
    cols_norm = [_norm(c) for c in cols]

    try:
        idx_sec_ja  = cols_norm.index("セクション_ja")
        idx_item_ja = cols_norm.index("項目_ja")
    except ValueError:
        idx_sec_ja  = next(i for i,c in enumerate(cols) if str(c).strip()=="セクション_ja")
        idx_item_ja = next(i for i,c in enumerate(cols) if str(c).strip()=="項目_ja")

    CN_KEYS_NORM = {"セクション", "項目"}
    take_grade_idxs = []
    for i, n in enumerate(cols_norm):
        if i in (idx_sec_ja, idx_item_ja): continue
        if n in CN_KEYS_NORM:              continue
        take_grade_idxs.append(i)

    final_out = pd.concat(
        [out_full.iloc[:, [idx_sec_ja, idx_item_ja]],
         out_full.iloc[:, take_grade_idxs]],
        axis=1
    )

    final_out = final_out.drop(columns=["セクション", "項目"], errors="ignore")

    # 保存
    DST_PRIMARY.parent.mkdir(parents=True, exist_ok=True)
    final_out.to_csv(DST_PRIMARY,   index=False, encoding="utf-8-sig")
    final_out.to_csv(DST_SECONDARY, index=False, encoding="utf-8-sig")
    print(f"✅ Saved: {DST_PRIMARY}")

if __name__ == "__main__":
    main()
