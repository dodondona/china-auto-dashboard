# -*- coding: utf-8 -*-
# tools/stage_translate_maker_to_ja.py
#
# 目的:
#   - 'manufacturer'列を日本語化して'manufacturer_ja'列を追加
#   - 'name'列の隣に'global_name'列を追加
#   - global_nameは辞書優先、なければキャッシュ、最後にLLM翻訳
#   - 既存動作・出力構造は変更しない
#
# 使い方:
#   python tools/stage_translate_maker_to_ja.py <csv>

import os, sys, re, json, time
from pathlib import Path
import pandas as pd
from openai import OpenAI

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

# ==== OpenAI Translator ====
class Translator:
    def __init__(self, model: str, api_key: str | None):
        self.model = model
        self.client = OpenAI(api_key=api_key) if api_key else None
        self.batch_size = 60
        self.retries = 3
        self.sleep_base = 1.2

    def translate_unique(self, terms: list[str]) -> dict[str, str]:
        if not self.client:
            print("⚠️ No OpenAI API key; skipping LLM translation")
            return {t: t for t in terms}
        
        result = {}
        for i in range(0, len(terms), self.batch_size):
            batch = terms[i:i + self.batch_size]
            for attempt in range(self.retries):
                try:
                    prompt = self._build_prompt(batch)
                    resp = self.client.chat.completions.create(
                        model=self.model,
                        messages=[{"role": "user", "content": prompt}],
                        temperature=0.3,
                    )
                    content = resp.choices[0].message.content or ""
                    parsed = self._parse_response(content, batch)
                    result.update(parsed)
                    break
                except Exception as e:
                    print(f"⚠️ LLM translation attempt {attempt+1}/{self.retries} failed: {e}")
                    if attempt < self.retries - 1:
                        time.sleep(self.sleep_base ** (attempt + 1))
                    else:
                        for t in batch:
                            result[t] = t
        return result

    def _build_prompt(self, terms: list[str]) -> str:
        lines = "\n".join(f"{i+1}. {t}" for i, t in enumerate(terms))
        return f"""以下の中国語の自動車メーカー名または車名を日本語に翻訳してください。
可能であれば日本語名と英語表記を併記してください（例: トヨタ（Toyota））。
元の中国語が既に英語やローマ字の場合はそのまま返してください。

入力:
{lines}

出力形式（番号: 翻訳結果）:
1. 翻訳結果1
2. 翻訳結果2
..."""

    def _parse_response(self, content: str, batch: list[str]) -> dict[str, str]:
        result = {}
        lines = [ln.strip() for ln in content.split("\n") if ln.strip()]
        for i, line in enumerate(lines):
            m = re.match(r"^\d+[\.\)]\s*(.+)$", line)
            if m and i < len(batch):
                result[batch[i]] = m.group(1).strip()
        # Fill missing translations
        for t in batch:
            if t not in result:
                result[t] = t
        return result

# ==== キャッシュ管理 ====
CACHE_DIR = Path("cache/translations")

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

CACHE_FILES = {
    "manufacturer": CACHE_DIR / "manufacturer_ja.json",
    "vehicle_name": CACHE_DIR / "vehicle_name_ja.json",
}

for cf in CACHE_FILES.values():
    ensure_dir(cf.parent)

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

# メモリキャッシュ
MEM_CACHE = {
    "manufacturer": {},
    "vehicle_name": {},
}

# JSONキャッシュ読み込み
JSON_CACHE = {
    "manufacturer": load_json(CACHE_FILES["manufacturer"]),
    "vehicle_name": load_json(CACHE_FILES["vehicle_name"]),
}

def translate_with_caches(kind: str, terms: list[str], fixed_map: dict[str, str], tr: Translator) -> dict[str, str]:
    """
    優先順: 固定辞書 > JSONキャッシュ > メモリキャッシュ > LLM
    """
    out: dict[str, str] = {}

    # 1) 固定辞書
    for t in terms:
        if t in fixed_map:
            out[t] = fixed_map[t]

    # 2) JSONキャッシュ
    for t in terms:
        if t not in out and t in JSON_CACHE[kind]:
            out[t] = JSON_CACHE[kind][t]

    # 3) メモリキャッシュ
    for t in terms:
        if t not in out and t in MEM_CACHE[kind]:
            out[t] = MEM_CACHE[kind][t]

    # 4) LLM
    need = [t for t in terms if t not in out]
    if need:
        print(f"🤖 Translating {len(need)} {kind}(s) with LLM...")
        llm_map = tr.translate_unique(need)
        out.update(llm_map)
        # メモリ・JSONキャッシュに反映
        for k, v in llm_map.items():
            MEM_CACHE[kind][k] = v
            JSON_CACHE[kind][k] = v

    return out

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

    # OpenAI設定
    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    api_key = os.environ.get("OPENAI_API_KEY")
    tr = Translator(model, api_key)

    # manufacturer_ja - 辞書優先、なければLLM
    print("\n📋 Translating manufacturers...")
    uniq_makers = list(set(df["manufacturer"].dropna().astype(str).unique()))
    
    # まず辞書でマッチング（部分一致）
    maker_ja_map = {}
    for val in uniq_makers:
        matched = next((DICT_ZH_TO_JA[k] for k in DICT_KEYS_SORTED if k in val), None)
        if matched:
            maker_ja_map[val] = matched
    
    # 辞書にないものをLLMで翻訳
    need_llm_makers = [m for m in uniq_makers if m not in maker_ja_map]
    if need_llm_makers:
        llm_maker_map = translate_with_caches("manufacturer", need_llm_makers, {}, tr)
        maker_ja_map.update(llm_maker_map)
    
    # データフレームに適用
    df["manufacturer_ja"] = df["manufacturer"].astype(str).map(lambda x: maker_ja_map.get(x, x))

    # global_name - 辞書優先、なければLLM、最後にピンイン
    print("\n📋 Translating vehicle names...")
    uniq_names = list(set(df["name"].dropna().astype(str).unique()))
    
    # 固定辞書からマッチング
    name_map = {}
    for n in uniq_names:
        if n in DICT_GLOBAL_NAME:
            name_map[n] = DICT_GLOBAL_NAME[n]
    
    # 辞書にないものをLLMで翻訳
    need_llm_names = [n for n in uniq_names if n not in name_map]
    if need_llm_names:
        llm_name_map = translate_with_caches("vehicle_name", need_llm_names, DICT_GLOBAL_NAME, tr)
        name_map.update(llm_name_map)
    
    # ピンインフォールバック（LLMで翻訳できなかった、または中国語のみの場合）
    globals_ = []
    for n in df["name"].astype(str):
        g = name_map.get(n, "")
        # 中国語のみの場合はピンインを追加
        if not g or (g == n and re.search(r"[\u4e00-\u9fff]", g) and not re.search(r"[A-Za-z]", g)):
            g = add_block_pinyin_inline(n, g)
        globals_.append(g)
    
    insert_at = df.columns.get_loc("name") + 1
    df.insert(insert_at, "global_name", globals_)

    # キャッシュ保存
    dump_json_safe(CACHE_FILES["manufacturer"], JSON_CACHE["manufacturer"])
    dump_json_safe(CACHE_FILES["vehicle_name"], JSON_CACHE["vehicle_name"])

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
