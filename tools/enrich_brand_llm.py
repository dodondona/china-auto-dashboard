#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
enrich_brand_llm.py
一次CSV（rank_seq,rank,name,count）から brand 列を付与した二次CSVを生成。
既存の VLM パイプライン（vlm_rank_reader.py）は変更しません。

判定手順:
 1) ルール: 「モデル（ブランド）」「ブランド+モデル」「ブランド·モデル」「ブランド-モデル」
 2) 正規化辞書: 合弁社名・会社名・グループ名などを販売実ブランドへ正規化
 3) LLMフォールバック（任意）: ルールで決まらない行だけ候補集合から1語選択（要 OPENAI_API_KEY）
    - --no-llm を付ければ LLM 不使用（未知は「未知」のまま残す）

出力: rank_seq,rank,model,brand,count
"""

import csv, re, json, os, argparse, time
from pathlib import Path

# --- 正規名（販売実ブランド粒度）の候補（厚め初期投入） ---
CANONICAL_BRANDS = [
    # 中国（グループ/サブブランド含む・販売実ブランド寄せ）
    "比亚迪","方程豹","腾势",
    "吉利","吉利银河","领克","极氪",
    "长安","深蓝","阿维塔",
    "上汽","荣威","名爵","五菱","宝骏",
    "广汽","埃安",
    "一汽","红旗",
    "东风","启辰",
    "奇瑞","捷途","星途",
    "蔚来","小鹏","理想","哪吒","零跑","岚图","问界","极狐","极越",
    "长城","哈弗","魏牌","坦克","欧拉",
    "北汽","北京","极石","小米汽车","赛力斯",

    # 合資・外資（販売実ブランド）
    "大众","斯柯达","奥迪","保时捷","宾利","兰博基尼","布加迪",
    "丰田","雷克萨斯","日野",
    "本田","讴歌",
    "日产","英菲尼迪",
    "马自达","斯巴鲁","三菱",
    "宝马","劳斯莱斯",
    "奔驰","迈巴赫","斯玛特",
    "沃尔沃","极星",
    "捷豹","路虎",
    "标致","雪铁龙","DS",
    "别克","雪佛兰","凯迪拉克",
    "福特","林肯",
    "现代","起亚",
    "特斯拉","Rivian"
]

# --- エイリアス→正規名（ゆれ吸収・会社名/合弁名→販売ブランド） ---
ALIAS_TO_CANON = {
    # 上汽系
    "上汽集团":"上汽","上汽":"上汽","上汽乘用车":"上汽",
    "上汽大众":"大众","大众汽车":"大众","一汽-大众（上汽侧）":"大众",
    "上汽通用":"别克","上汽通用五菱":"五菱",
    "荣威":"荣威","名爵":"名爵","宝骏":"宝骏",

    # 广汽系
    "广汽集团":"广汽","广汽乘用车":"广汽","广汽":"广汽",
    "广汽丰田":"丰田","广汽本田":"本田","广汽埃安":"埃安","埃安":"埃安",

    # 一汽系
    "一汽集团":"一汽","一汽":"一汽","红旗":"红旗",
    "一汽丰田":"丰田","一汽大众":"大众","一汽奥迪":"奥迪","一汽-大众奥迪":"奥迪",

    # 东风系
    "东风":"东风","东风日产":"日产","东风本田":"本田","东风启辰":"启辰",
    "东风标致":"标致","东风雪铁龙":"雪铁龙","启辰":"启辰",

    # 长安系
    "长安汽车":"长安","长安":"长安","深蓝汽车":"深蓝","阿维塔科技":"阿维塔","阿维塔":"阿维塔",

    # 吉利系
    "吉利汽车":"吉利","吉利":"吉利","吉利银河":"吉利银河","领克":"领克","极氪":"极氪",

    # 奇瑞系
    "奇瑞汽车":"奇瑞","奇瑞":"奇瑞","捷途":"捷途","星途":"星途",

    # 长城系
    "长城汽车":"长城","长城":"长城","哈弗":"哈弗","WEY":"魏牌","魏牌":"魏牌","坦克":"坦克","欧拉":"欧拉",

    # 北汽系
    "北京汽车":"北京","北汽":"北京","极狐汽车":"极狐","极狐":"极狐","极石汽车":"极石","极石":"极石",

    # SAIC-GM/VW 分岐
    "别克":"别克","雪佛兰":"雪佛兰","凯迪拉克":"凯迪拉克",
    "大众":"大众","上汽大众斯柯达":"斯柯达","斯柯达":"斯柯达","奥迪":"奥迪","保时捷":"保时捷",

    # トヨタ/ホンダ/日産
    "丰田":"丰田","雷克萨斯":"雷克萨斯","日野":"日野",
    "本田":"本田","讴歌":"讴歌",
    "日产":"日产","英菲尼迪":"英菲尼迪",

    # 独/瑞
    "宝马":"宝马","劳斯莱斯":"劳斯莱斯",
    "奔驰":"奔驰","迈巴赫":"迈巴赫","斯玛特":"斯玛特",
    "沃尔沃":"沃尔沃","极星":"极星",

    # 英/仏/米/韓
    "捷豹":"捷豹","路虎":"路虎",
    "标致":"标致","雪铁龙":"雪铁龙","DS":"DS",
    "福特":"福特","林肯":"林肯",
    "现代":"现代","起亚":"起亚",

    # 新勢力/EV/その他
    "比亚迪":"比亚迪","方程豹":"方程豹","腾势":"腾势",
    "蔚来":"蔚来","小鹏":"小鹏","理想":"理想","哪吒":"哪吒","零跑":"零跑","岚图":"岚图","问界":"问界","极越":"极越",
    "特斯拉":"特斯拉","Rivian":"Rivian",
    "赛力斯汽车":"赛力斯","赛力斯":"赛力斯","小米":"小米汽车","小米汽车":"小米汽车"
}

# 逆引き用セット
CANON_SET = set(CANONICAL_BRANDS)
ALIAS_KEYS = set(ALIAS_TO_CANON.keys())
ALL_BRAND_TOKENS = CANON_SET.union(ALIAS_KEYS)

# --- 1) ルール判定 ---
def split_by_rules(name: str):
    t = (name or "").strip()
    if not t:
        return "", ""

    # 1) モデル（ブランド）
    m = re.match(r'^(.+?)\s*[（(]\s*([^（）()]+?)\s*[)）]\s*$', t)
    if m:
        model = m.group(1).strip()
        brand_raw = m.group(2).strip()
        brand = normalize_brand(brand_raw)
        if brand:
            return model, brand

    # 2) 既知ブランドの接頭辞
    for b in sorted(ALL_BRAND_TOKENS, key=len, reverse=True):
        if t.startswith(b):
            rest = t[len(b):].lstrip(" ·・-—–")
            if rest:
                nb = normalize_brand(b)
                if nb:
                    return rest.strip(), nb

    # 3) ブランド·モデル / ブランド-モデル
    m = re.match(r'^([^·・\-—–]+)[·・\-—–]\s*(.+)$', t)
    if m:
        braw = m.group(1).strip()
        nb = normalize_brand(braw)
        if nb:
            return m.group(2).strip(), nb

    return "", ""  # 未確定

# --- 正規化（会社名/合弁名→販売ブランドへ寄せる） ---
def normalize_brand(b: str) -> str:
    b = (b or "").strip()
    if not b:
        return ""
    # 直接一致（正規名）
    if b in CANON_SET:
        return b
    # エイリアス → 正規名
    if b in ALIAS_TO_CANON:
        return ALIAS_TO_CANON[b]
    # 末尾の“汽车/集团/公司/乘用车/科技”等を落として再試行
    b2 = re.sub(r'(汽车|集团|公司|乘用车|科技)$', '', b)
    if b2 and b2 != b:
        if b2 in CANON_SET:
            return b2
        if b2 in ALIAS_TO_CANON:
            return ALIAS_TO_CANON[b2]
    return ""

# --- 3) LLM フォールバック（必要な時だけ） ---
def ask_llm_for_brand(model_name: str, brand_choices):
    """
    モデル名からブランドを1語で返す。分からなければ '未知'。
    OPENAI_API_KEY 環境変数が必要。使わない場合は --no-llm を指定。
    """
    import openai  # pip install openai
    client = openai.OpenAI()

    sys_prompt = (
        "你是汽车品牌判别助手。"
        "给定一个车型名称（中文/英文/混合），请从候选品牌列表中选择其所属品牌（中文）。"
        "若无法确定，请输出“未知”。"
    )
    user_prompt = (
        "候选品牌（只可从中选择）：\n"
        + "、".join(brand_choices)
        + "\n\n车型名："
        + model_name
        + "\n\n输出要求：只输出一个词（品牌名）或“未知”。不要输出任何其他内容。"
    )

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"system","content":sys_prompt},{"role":"user","content":user_prompt}],
        temperature=0,
        max_tokens=8,
    )
    out = (resp.choices[0].message.content or "").strip()
    out = re.sub(r"\s", "", out)
    nb = normalize_brand(out)  # 合弁名なども正規化
    if nb:
        return nb
    return "未知"

# --- キャッシュIO ---
def load_cache(p: Path):
    if p.is_file():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cache(p: Path, d: dict):
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)

# --- メイン ---
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in",  dest="input_csv",  required=True, help="一次CSV (rank_seq,rank,name,count)")
    ap.add_argument("--out", dest="output_csv", required=True, help="二次CSV (model,brand 付与)")
    ap.add_argument("--cache", default=".brand_cache.json", help="LLM結果キャッシュ（JSON）")
    ap.add_argument("--no-llm", dest="no_llm", action="store_true", help="LLMを使わない（未知で残す）")
    args = ap.parse_args()

    cache_path = Path(args.cache)
    cache = load_cache(cache_path)

    with open(args.input_csv, newline="", encoding="utf-8-sig") as f:
        src_rows = list(csv.DictReader(f))

    out_rows = []
    for r in src_rows:
        name = (r.get("name") or "").strip()
        model, brand = split_by_rules(name)

        # ルールで決まらなければ LLM（ただし --no-llm 時はスキップ）
        if not brand and not args.no_llm:
            key = name
            brand = cache.get(key, "")
            if not brand:
                brand = ask_llm_for_brand(name, CANONICAL_BRANDS)
                cache[key] = brand
                time.sleep(0.2)  # 軽くスロットリング

        if not brand:
            brand = "未知"
        if not model:
            model = name

        out_rows.append({
            "rank_seq": r.get("rank_seq",""),
            "rank":     r.get("rank",""),
            "model":    model,
            "brand":    brand,
            "count":    r.get("count",""),
        })

    with open(args.output_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["rank_seq","rank","model","brand","count"])
        w.writeheader()
        w.writerows(out_rows)

    save_cache(cache_path, cache)
    print(f"[OK] {len(out_rows)} rows -> {args.output_csv}")
    print(f"[CACHE] {len(cache)} keys -> {cache_path}")

if __name__ == "__main__":
    main()
