# -*- coding: utf-8 -*-
# tools/stage_translate_maker_to_ja.py
#
# 目的:
#   - 'manufacturer'列を日本語化して'manufacturer_ja'列を追加
#   - 'name'列の隣に'global_name'列を追加
#   - global_nameは辞書優先、なければピンイン補助、最終的に元のnameを使用
#   - 既存動作・出力構造は変更しない
#
# 使い方:
#   python tools/stage_translate_maker_to_ja.py <csv>

import os, sys, re, json
from pathlib import Path
import pandas as pd

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

# ==== メーカー翻訳辞書 ====
DICT_ZH_TO_JA = {
    # ✅ 自主ブランド
    "比亚迪": "BYD",
    "吉利": "吉利（Geely）",
    "吉利银河": "吉利銀河（Geely Galaxy）",
    "奇瑞": "奇瑞（Chery）",
    "奇瑞风云": "奇瑞風雲（Chery Fengyun）",
    "长安": "長安（Changan）",
    "长安启源": "長安啓源（Changan Qiyuan）",
    "哈弗": "哈弗（Haval）",
    "魏牌": "魏牌（WEY）",
    "红旗": "紅旗（Hongqi）",
    "名爵": "名爵（MG）",
    "荣威": "栄威（Roewe）",
    "零跑汽车": "零跑（Leapmotor）",
    "理想汽车": "理想（Li Auto）",
    "小鹏": "小鵬（Xpeng）",
    "极狐": "極狐（ARCFOX）",
    "深蓝汽车": "深藍（Deepal）",
    "领克": "リンク・アンド・コー（Lynk & Co）",
    "乐道": "楽道（Le Dao）",
    "方程豹": "方程豹（Fang Cheng Bao）",
    "iCAR": "iCAR（奇瑞iCAR）",
    "腾势": "騰勢（DENZA）",
    "ARCFOX": "極狐（ARCFOX）",

    # ✅ 上汽グループ系
    "上汽": "上海汽車（SAIC）",
    "上汽集团": "上海汽車（SAIC）",
    "上汽通用": "上汽通用（SAIC-GM）",
    "上汽通用五菱": "上汽通用五菱（SGMW／五菱）",
    "五菱汽车": "五菱（Wuling）",
    "宝骏": "宝駿（Baojun）",

    # ✅ 外資系合弁
    "大众": "フォルクスワーゲン（Volkswagen）",
    "奥迪": "アウディ（Audi）",
    "宝马": "BMW",
    "奔驰": "メルセデス・ベンツ（Mercedes-Benz）",
    "丰田": "トヨタ（Toyota）",
    "本田": "ホンダ（Honda）",
    "日产": "日産（Nissan）",
    "马自达": "マツダ（Mazda）",
    "三菱": "三菱（Mitsubishi）",
    "铃木": "スズキ（Suzuki）",
    "斯巴鲁": "スバル（Subaru）",
    "雷克萨斯": "レクサス（Lexus）",
    "别克": "ビュイック（Buick）",
    "雪佛兰": "シボレー（Chevrolet）",
    "捷途": "捷途（Jetour）",
    "奔腾": "奔騰（Bestune）",

    # ✅ 新興および外資独資
    "特斯拉": "テスラ（Tesla）",
    "小米汽车": "小米（Xiaomi Auto）",
    "AITO 问界": "AITO（問界）",
    "ARCFOX极狐": "極狐（ARCFOX）",
    "方程豹汽车": "方程豹（Fang Cheng Bao）",
    "哈弗猛龙新能源": "哈弗（Haval）",
    "深蓝": "深藍（Deepal）",
    "银河": "銀河（Geely Galaxy）",
    "启源": "啓源（Changan Qiyuan）",
}

DICT_KEYS_SORTED = sorted(DICT_ZH_TO_JA.keys(), key=len, reverse=True)

# ==== グローバル名辞書 ====
# ==== グローバル名辞書 ====
DICT_GLOBAL_NAME = {
    # 前10位
    "宏光MINIEV": "宏光MINIEV（Hongguang MINI EV）",
    "Model Y": "モデルY",
    "星愿": "星願（Xingyuan）",
    "秦PLUS": "秦PLUS",
    "轩逸": "シルフィ",
    "海狮06新能源": "海狮06新能源（Haishi 06 EV）",
    "博越L": "博越L（Boyue L）",
    "海豹06新能源": "海豹06新能源（Haibao 06 EV）",
    "秦L": "秦L",
    "元UP": "元UP",

    # 11–20
    "海鸥": "シーガル",
    "速腾": "サギター（Sagitar）",
    "长安Lumin": "ルミン（Lumin）",
    "小米YU7": "YU7",
    "朗逸": "ラヴィーダ",
    "海豚": "ドルフィン",
    "问界M8": "問界M8（AITO M8）",
    "凯美瑞": "カムリ",
    "Model 3": "モデル3",
    "RAV4荣放": "RAV4",

    # 21–40
    "小米SU7": "SU7",
    "途观L": "ティグアンL",
    "帕萨特": "パサート",
    "逸动": "逸動（Yidong）",
    "星越L": "星越L（Xingyue L）",
    "迈腾": "マゴタン",
    "哈弗大狗": "ビッグドッグ（Big Dog）",
    "奥迪A6L": "A6L",
    "探岳": "タイロン（Tayron）",
    "卡罗拉锐放": "カローラクロス",

    # 41–60
    "瑞虎8": "ティゴ8（Tiggo 8）",
    "小鹏MONA M03": "MONA M03",
    "本田CR-V": "CR-V",
    "红旗H5": "H5",
    "缤越": "クールレイ（Coolray）",
    "锋兰达": "フロントランダー",
    "艾瑞泽8": "アリゾ8（Arrizo 8）",
    "宋Pro新能源": "宋Pro新能源（Song Pro EV）",
    "雅阁": "アコード",
    "深蓝S05": "深藍S05（Deepal S05）",
    "奔驰E级": "Eクラス",
    "熊猫": "パンダ",
    "银河A7": "銀河A7",
    "昂科威Plus": "エンビジョンPlus（Envision Plus）",
    "零跑C10": "C10",
    "元PLUS": "アット3（Atto 3）",
    "海豹05 DM-i": "シール05 DM-i（Seal 05 DM-i）",
    "零跑B01": "B01",
    "宝马3系": "3シリーズ",
    "途岳": "途岳（Tharu）",

    # 61–80
    "奔腾小马": "ポニー（Pony）",
    "理想L6": "L6",
    "奥迪Q5L": "Q5L",
    "威兰达": "ウィランダー",
    "海狮05 EV": "海狮05 EV（Haishi 05 EV）",
    "长安CS75PLUS": "CS75プラス",
    "MG4": "MG4",
    "亚洲龙": "アバロン",
    "奔驰GLC": "GLC",
    "哈弗猛龙新能源": "ラプター（Haval Raptor）",
    "宋PLUS新能源": "宋PLUS新能源（Song PLUS EV）",
    "乐道L90": "L90（Le Dao L90）",
    "零跑C11": "C11",
    "问界M9": "問界M9（AITO M9）",
    "奔驰C级": "Cクラス",
    "长安启源Q07": "啓源Q07（Qiyuan Q07）",
    "捷途X70": "X70（Jetour X70）",
    "银河E5": "銀河E5",
    "宋L DM-i": "宋L DM-i",
    "极狐T1": "極狐T1（ARCFOX T1）",

    # 81–100
    "银河星耀8": "銀河星耀8",
    "风云A9L": "風雲A9L",
    "皓影": "ブリーズ",
    "五菱缤果": "ビンゴ（Bingo）",
    "零跑B10": "B10",
    "长安X5 PLUS": "X5プラス",
    "零跑C16": "C16",
    "宝马5系": "5シリーズ",
    "铂智3X": "ポルチ3X（bZ3X）",
    "荣威i5": "i5",
    "银河星舰7": "銀河星艦7",
    "赛那SIENNA": "シエナ（Sienna）",
    "钛7": "レパード7（Leopard 7）",
    "小鹏P7": "P7",
    "宝马X3": "X3",
    "长安UNI-Z新能源": "UNI-Z新能源",
    "魏牌 高山": "高山（Wey Gaoshan）",
    "iCAR 超级V23": "iCAR V23",
    "奥迪A4L": "A4L",
    "红旗HS5": "HS5",
    "逍客": "キャシュカイ",
    "领克900": "リンク・アンド・コー 09（Lynk & Co 09）",
    "星瑞": "プレフェイス（Preface）",
    "腾势D9": "デンツァD9（Denza D9）",
    "驱逐舰05": "デストロイヤー05（Destroyer 05）",
    "卡罗拉": "カローラ",
    "别克GL8新能源": "GL8",
    "宝来": "ボーラ（Bora）",
    "传祺GS3": "GS3（Trumpchi GS3）",
}
}

# ==== ピンイン補助 ====
try:
    from pypinyin import lazy_pinyin
    _PINYIN_OK = True
except Exception:
    _PINYIN_OK = False

_HAN = r"\u4e00-\u9fff"

def add_block_pinyin_inline(name: str, global_name: str) -> str:
    if re.search(r"[A-Za-zａ-ｚＡ-Ｚァ-ヴー]", global_name or ""):
        return global_name
    if global_name or not re.search(fr"[{_HAN}]", name or ""):
        return global_name or name
    if not _PINYIN_OK:
        return name
    s = str(name)
    out = []
    i = 0
    while i < len(s):
        if re.match(fr"[{_HAN}]", s[i]):
            j = i
            while j < len(s) and re.match(fr"[{_HAN}]", s[j]):
                j += 1
            block = s[i:j]
            py = " ".join(lazy_pinyin(block))
            out.append(f"{block}({py})")
            i = j
        else:
            out.append(s[i])
            i += 1
    return "".join(out)

# ==== メイン ====
def process_csv(csv_path: Path) -> Path | None:
    print(f"\n=== Processing {csv_path} ===")
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"⚠️ cannot read CSV: {e}")
        return None
    if "manufacturer" not in df.columns or "name" not in df.columns:
        print("ℹ️ skip (no 'manufacturer' or 'name')")
        return None

    # manufacturer_ja
    ja_list = []
    cache = {}
    for val in df["manufacturer"].astype(str):
        if val in cache:
            ja_list.append(cache[val])
            continue
        ja = next((DICT_ZH_TO_JA[k] for k in DICT_KEYS_SORTED if k in val), val)
        cache[val] = ja
        ja_list.append(ja)
    df["manufacturer_ja"] = ja_list

    # global_name
    globals_ = []
    for n in df["name"].astype(str):
        g = DICT_GLOBAL_NAME.get(n, "")
        g = add_block_pinyin_inline(n, g)
        globals_.append(g)
    insert_at = df.columns.get_loc("name") + 1
    df.insert(insert_at, "global_name", globals_)

    # ✅ ファイル名修正：末尾の _with_maker を1回だけ除去
    base = re.sub(r"_with_maker$", "", csv_path.stem)
    out = csv_path.with_name(base + "_with_maker_with_maker_ja.csv")

    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"✅ saved: {out}  rows={len(df)}")
    return out

def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/stage_translate_maker_to_ja.py <csv>")
        sys.exit(1)
    for arg in sys.argv[1:]:
        p = Path(arg)
        if p.exists() and p.suffix.lower() == ".csv":
            process_csv(p)

if __name__ == "__main__":
    main()
