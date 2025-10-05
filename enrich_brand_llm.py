#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
enrich_brand_llm.py
一次CSV（rank_seq,rank,name,count）から brand 列を付与した二次CSVを生成。
"""

import csv, re, json, argparse, time
from pathlib import Path

# --- ブランド辞書（厚め） ---
CANONICAL_BRANDS = [
    "比亚迪","方程豹","腾势","吉利","吉利银河","领克","极氪","长安","深蓝","阿维塔",
    "上汽","荣威","名爵","五菱","宝骏","广汽","埃安","一汽","红旗","东风","启辰",
    "奇瑞","捷途","星途","蔚来","小鹏","理想","哪吒","零跑","岚图","问界","极狐","极越",
    "长城","哈弗","魏牌","坦克","欧拉","北汽","北京","极石","小米汽车","赛力斯",
    "大众","斯柯达","奥迪","保时捷","丰田","雷克萨斯","本田","讴歌","日产","英菲尼迪",
    "马自达","斯巴鲁","三菱","宝马","奔驰","沃尔沃","极星","捷豹","路虎","标致","雪铁龙",
    "别克","雪佛兰","凯迪拉克","福特","林肯","现代","起亚","特斯拉"
]

ALIAS_TO_CANON = {
    "上汽集团":"上汽","上汽大众":"大众","上汽通用":"别克","上汽通用五菱":"五菱",
    "广汽集团":"广汽","广汽丰田":"丰田","广汽本田":"本田","广汽埃安":"埃安",
    "一汽集团":"一汽","一汽大众":"大众","一汽丰田":"丰田","红旗":"红旗",
    "东风日产":"日产","东风本田":"本田","东风启辰":"启辰",
    "长安汽车":"长安","深蓝汽车":"深蓝","阿维塔科技":"阿维塔",
    "吉利汽车":"吉利","极氪汽车":"极氪","奇瑞汽车":"奇瑞",
    "长城汽车":"长城","WEY":"魏牌",
    "北京汽车":"北京","极狐汽车":"极狐","极石汽车":"极石",
    "大众汽车":"大众","斯柯达汽车":"斯柯达","奥迪汽车":"奥迪",
    "丰田汽车":"丰田","雷克萨斯汽车":"雷克萨斯",
    "本田汽车":"本田","讴歌汽车":"讴歌","日产汽车":"日产",
    "宝马汽车":"宝马","奔驰汽车":"奔驰","沃尔沃汽车":"沃尔沃","极星汽车":"极星",
    "标致汽车":"标致","雪铁龙汽车":"雪铁龙","别克汽车":"别克","雪佛兰汽车":"雪佛兰","凯迪拉克汽车":"凯迪拉克",
    "福特汽车":"福特","林肯汽车":"林肯","现代汽车":"现代","起亚汽车":"起亚","特斯拉汽车":"特斯拉"
}

def normalize_brand(b):
    b = (b or "").strip()
    if not b:
        return ""
    if b in CANONICAL_BRANDS:
        return b
    if b in ALIAS_TO_CANON:
        return ALIAS_TO_CANON[b]
    return ""

def split_by_rules(name: str):
    t = (name or "").strip()
    if not t:
        return "", ""

    m = re.match(r'^(.+?)\s*[（(]\s*([^（）()]+?)\s*[)）]\s*$', t)
    if m:
        return m.group(1).strip(), normalize_brand(m.group(2).strip())

    for b in sorted(CANONICAL_BRANDS+list(ALIAS_TO_CANON.keys()), key=len, reverse=True):
        if t.startswith(b):
            rest = t[len(b):].lstrip(" ·・-—–")
            if rest:
                return rest.strip(), normalize_brand(b)

    m = re.match(r'^([^·・\-—–]+)[·・\-—–]\s*(.+)$', t)
    if m:
        return m.group(2).strip(), normalize_brand(m.group(1).strip())

    return "", ""

def ask_llm_for_brand(model_name: str, brand_choices):
    import openai
    client = openai.OpenAI()
    sys_prompt = "你是汽车品牌判别助手。给定车型名，请从候选中选择品牌（中文）。无法确定时输出“未知”。"
    user_prompt = "候选品牌：\n" + "、".join(brand_choices) + "\n\n车型名：" + model_name
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"system","content":sys_prompt},{"role":"user","content":user_prompt}],
        temperature=0,
        max_tokens=8,
    )
    out = (resp.choices[0].message.content or "").strip()
    nb = normalize_brand(out)
    return nb if nb else "未知"

def load_cache(p: Path):
    if p.is_file():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}

def save_cache(p: Path, d: dict):
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in",  dest="input_csv",  required=True)
    ap.add_argument("--out", dest="output_csv", required=True)
    ap.add_argument("--cache", default=".brand_cache.json")
    ap.add_argument("--no-llm", dest="no_llm", action="store_true", help="LLMを使わない")
    args = ap.parse_args()

    cache_path = Path(args.cache)
    cache = load_cache(cache_path)

    with open(args.input_csv, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    out_rows = []
    for r in rows:
        name = (r.get("name") or "").strip()
        model, brand = split_by_rules(name)

        if not brand and not args.no_llm:
            key = name
            brand = cache.get(key, "")
            if not brand:
                brand = ask_llm_for_brand(name, CANONICAL_BRANDS)
                cache[key] = brand
                time.sleep(0.2)

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

if __name__ == "__main__":
    main()
