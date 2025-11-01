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
    "比亚迪": "BYD",
    "上汽": "上海汽車（SAIC）",
    "上汽集团": "上海汽車（SAIC）",
    "一汽": "第一汽車（FAW）",
    "东风": "東風（Dongfeng）",
    "广汽": "広州汽車（GAC）",
    "北汽": "北京汽車（BAIC）",
    "长安": "長安（Changan）",
    "长城": "長城（Great Wall）",
    "吉利": "吉利（Geely）",
    "奇瑞": "奇瑞（Chery）",
    "红旗": "紅旗（Hongqi）",
    "蔚来": "蔚来（NIO）",
    "小鹏": "小鵬（Xpeng）",
    "理想": "理想（Li Auto）",
    "上汽通用": "上汽通用（SAIC-GM）",
    "上汽通用五菱": "上汽通用五菱（SGMW／五菱）",
    "五菱": "五菱（Wuling）",
    "特斯拉": "テスラ",
    "大众": "フォルクスワーゲン",
    "奥迪": "アウディ",
    "宝马": "BMW",
    "奔驰": "メルセデス・ベンツ",
    "丰田": "トヨタ",
    "本田": "ホンダ",
    "日产": "日産",
    "马自达": "マツダ",
    "三菱": "三菱",
    "铃木": "スズキ",
    "斯巴鲁": "スバル",
    "雷克萨ス": "レクサス",
}

DICT_KEYS_SORTED = sorted(DICT_ZH_TO_JA.keys(), key=len, reverse=True)

# ==== グローバル名辞書 ====
DICT_GLOBAL_NAME = {
  "宏光MINIEV": "宏光(hong guang)MINIEV",
  "Model Y": "モデルY",
  "星愿": "星願(xing yuan)",
  "秦PLUS": "秦(qin)PLUS",
  "轩逸": "シルフィ",
  "海狮06新能源": "海獅(hai shi)06新能源(xin neng yuan)",
  "博越L": "博越(bo yue)L",
  "海豹06新能源": "海豹(hai bao)06新能源(xin neng yuan)",
  "秦L": "秦(qin)L",
  "元UP": "元(yuan)UP",
  "海鸥": "シーガル",
  "速腾": "速騰(su teng)",
  "长安Lumin": "ルミン",
  "小米YU7": "YU7",
  "朗逸": "ラヴィーダ",
  "海豚": "ドルフィン",
  "问界M8": "M8",
  "凯美瑞": "カムリ",
  "Model 3": "モデル3",
  "RAV4荣放": "RAV4",
  "小米SU7": "SU7",
  "途观L": "ティグアンL",
  "帕萨特": "パサート",
  "逸动": "逸動(yi dong)",
  "星越L": "星越(xing yue)L",
  "迈腾": "マゴタン",
  "哈弗大狗": "ビッグドッグ",
  "奥迪A6L": "A6L",
  "探岳": "タイロン",
  "卡罗拉锐放": "カローラクロス",
  "瑞虎8": "ティゴ8",
  "小鹏MONA M03": "MONA M03",
  "本田CR-V": "CR-V",
  "红旗H5": "H5",
  "缤越": "クールレイ",
  "锋兰达": "フロントランダー",
  "艾瑞泽8": "アリゾ8",
  "宋Pro新能源": "宋(song)Pro新能源(xin neng yuan)",
  "雅阁": "アコード",
  "深蓝S05": "深藍(shen lan)S05",
  "奔驰E级": "Eクラス",
  "熊猫": "パンダ",
  "银河A7": "銀河(yin he)A7",
  "昂科威Plus": "昂科威(ang ke wei)Plus",
  "零跑C10": "C10",
  "元PLUS": "アット3",
  "海豹05 DM-i": "シール05 DM-i",
  "零跑B01": "B01",
  "宝马3系": "3シリーズ",
  "途岳": "途岳(tu yue)",
  "奔腾小马": "ポニー",
  "理想L6": "L6",
  "奥迪Q5L": "Q5L",
  "威兰达": "ウィランダー",
  "海狮05 EV": "海獅(hai shi)05 EV",
  "长安CS75PLUS": "CS75プラス",
  "MG4": "MG4",
  "亚洲龙": "アバロン",
  "奔驰GLC": "GLC",
  "哈弗猛龙新能源": "ラプター",
  "宋PLUS新能源": "宋(song)PLUS新能源(xin neng yuan)",
  "乐道L90": "L90",
  "零跑C11": "C11",
  "问界M9": "M9",
  "奔驰C级": "Cクラス",
  "长安启源Q07": "啓源(qi yuan)Q07",
  "捷途X70": "X70",
  "银河E5": "銀河(yin he)E5",
  "宋L DM-i": "宋(song)L DM-i",
  "极狐T1": "T1",
  "银河星耀8": "銀河(yin he)星耀(xing yao)8",
  "风云A9L": "風雲(feng yun)A9L",
  "皓影": "ブリーズ",
  "五菱缤果": "ビンゴ",
  "零跑B10": "B10",
  "长安X5 PLUS": "X5プラス",
  "零跑C16": "C16",
  "宝马5系": "5シリーズ",
  "铂智3X": "鉑智(bo zhi)3X",
  "荣威i5": "i5",
  "银河星舰7": "銀河(yin he)星艦(xing jian)7",
  "赛那SIENNA": "シエナ",
  "钛7": "レパード7",
  "小鹏P7": "P7",
  "宝马X3": "X3",
  "长安UNI-Z新能源": "UNI-Z",
  "魏牌 高山": "高山(gao shan)",
  "iCAR 超级V23": "超级(chao ji)V23",
  "奥迪A4L": "A4L",
  "红旗HS5": "HS5",
  "逍客": "キャシュカイ",
  "领克900": "09",
  "星瑞": "プレフェイス",
  "腾势D9": "D9",
  "驱逐舰05": "デストロイヤー05",
  "卡罗拉": "カローラ",
  "别克GL8新能源": "GL8",
  "宝来": "ボーラ",
  "传祺GS3": "GS3",
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
