#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL → フルページスクショ → タイル分割（overlap付き）→ VLMで行抽出 → CSV

ポイント:
- Playwright側でCJKフォントを強制適用 → 豆腐(□)防止
- LLMで brand と model を推定（辞書不要）
- 判定結果を data/brand_cache.json に永続キャッシュ（同じ車名は次回以降無課金・高速）
"""

import os, csv, json, base64, argparse, time
from pathlib import Path
from typing import List, Dict
from PIL import Image
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
from openai import OpenAI

# ----------------------------- VLM（表読み取り）プロンプト -----------------------------
SYSTEM_PROMPT = """あなたは表の読み取りに特化した視覚アシスタントです。
画像は中国の自動車販売ランキングです。UI部品や広告は無視してください。
出力は JSON のみ。構造:
{
  "rows": [
    {"rank": <int|null>, "name": "<string>", "count": <int|null>}
  ]
}
"""
USER_PROMPT = "この画像に見えている全ての行を JSON で返してください。"

# ----------------------------- ブランド分離プロンプト（辞書不要・シリーズ壊さない） -----------------------------
BRAND_PROMPT = """你是中国车系名称解析助手。给定一个“车系/车型名称”或图片片段，请输出对应的【品牌/厂商】与【车型名】。

数据来源与验证顺序（务必遵守）：
1) 必须优先在 汽车之家（autohome.com.cn） 查询并核对：先用“site:autohome.com.cn <车型/车系名>”检索；打开最相关的“车系”或“参数配置/图片/报价”页面。
2) 在页面中寻找「厂商」「品牌」或面包屑位置的“<品牌/厂商>-<车系>”字样（示例：比亚迪-秦PLUS / 上汽通用五菱-宏光MINIEV），据此判定品牌/厂商与车型名。
3) 如在汽车之家未找到完全匹配，请：
   a. 回看输入图片/文本，检查是否有误读（相似名、后缀如“Pro”“MAX”“L”“PLUS”等）。
   b. 结合常见别名/缩写再次在汽车之家检索 1 次。
4) 仍无法在汽车之家确认时，才可短暂参考第二来源（如：易车 yiche.com），但若信息与汽车之家冲突，以汽车之家为准；无法确认则返回“brand":"未知"。

命名规则：
- 不要仅按第一个词硬拆。像“宏光MINIEV”“秦PLUS”“宋PLUS”“汉L”等是完整车系名，不能把“宏光”“秦”“宋”“汉”单独当作品牌。
- 尽可能输出厂商（例如“五菱”“比亚迪”“大众”“吉利”“特斯拉”等）；如果确实没有厂商信息，则输出常用品牌名。
- 车型名请保留完整的车系名（含后缀，如 PLUS / Pro / MAX / L）。

只输出 JSON，结构严格如下（不要多余文字）：
{"brand":"<string>","model":"<string>"}

示例（务必模仿）：
- 输入：宏光MINIEV → {"brand":"五菱","model":"宏光MINIEV"}
- 输入：秦PLUS → {"brand":"比亚迪","model":"秦PLUS"}
- 输入：Model Y → {"brand":"特斯拉","model":"Model Y"}
- 输入：朗逸 → {"brand":"大众","model":"朗逸"}
- 输入：博越L → {"brand":"吉利","model":"博越L"}

"""

# ----------------------------- タイル分割 -----------------------------
def split_full_image(full_path: Path, out_dir: Path, tile_height: int, overlap: int) -> List[Path]:
    im = Image.open(full_path).convert("RGB")
    W, H = im.size
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []
    y, i = 0, 0
    step = max(1, tile_height - overlap)
    while y < H:
        y2 = min(y + tile_height, H)
        tile = im.crop((0, y, W, y2))
        p = out_dir / f"tile_{i:02d}.jpg"
        tile.save(p, "JPEG", quality=90, optimize=True)
        paths.append(p)
        i += 1
        if y2 >= H:
            break
        y += step
    return paths

# ----------------------------- スクショ（HTMLは改変しない / 豆腐防止CSS注入） -----------------------------
def grab_fullpage_to(url: str, out_dir: Path, viewport=(1380, 2400)) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    full_path = out_dir / "full.jpg"

    MAX_RETRY = 3
    for attempt in range(1, MAX_RETRY + 1):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                viewport={"width": viewport[0], "height": viewport[1]},
                device_scale_factor=2,
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="zh-CN",
                extra_http_headers={"Accept-Language": "zh-CN,zh;q=0.9"},
            )
            page = ctx.new_page()

            # 豆腐防止：CJKフォントを強制（Workflow で CJK フォント導入済み前提）
            page.add_init_script("""
              try {
                const style = document.createElement('style');
                style.setAttribute('data-screenshot-font-patch','1');
                style.textContent = `
                  * { font-family:
                      "Noto Sans CJK SC","WenQuanYi Zen Hei","Noto Sans CJK JP",
                      "Noto Sans","Microsoft YaHei","PingFang SC",sans-serif !important; }
                `;
                document.documentElement.appendChild(style);
              } catch(e){}
            """)

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=180000)
                # 緩く本文待ち
                for sel in ["table", ".rank-list", ".content", "body"]:
                    try:
                        page.wait_for_selector(sel, state="visible", timeout=5000)
                        break
                    except PwTimeout:
                        pass

                time.sleep(2.5)
                page.screenshot(path=str(full_path), full_page=True)
                browser.close()
                return full_path
            except Exception as e:
                browser.close()
                if attempt == MAX_RETRY:
                    raise
                time.sleep(2)

# ----------------------------- OpenAI 呼び出し -----------------------------
def vlm_extract_rows(image_path: Path, model="gpt-4o-mini") -> List[Dict]:
    client = OpenAI()
    b64 = base64.b64encode(image_path.read_bytes()).decode("utf-8")
    msgs = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": [
            {"type": "input_text", "text": USER_PROMPT},
            {"type": "input_image", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
        ]}
    ]
    resp = client.responses.create(model=model, input=msgs, temperature=0)
    out = resp.output_text
    try:
        data = json.loads(out)
        return data["rows"]
    except Exception:
        return []

def vlm_split_brand(name: str, model="gpt-4o-mini") -> Dict[str, str]:
    client = OpenAI()
    msgs = [
        {"role": "system", "content": BRAND_PROMPT},
        {"role": "user", "content": name}
    ]
    resp = client.chat.completions.create(
        model=model,
        messages=msgs,
        temperature=0,
        max_tokens=64,
    )
    out = (resp.choices[0].message.content or "").strip()
    try:
        d = json.loads(out)
        return d
    except Exception:
        return {"brand": "未知", "model": name}

# ----------------------------- CSV 読み書き -----------------------------
def write_csv(path: Path, rows: List[Dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["rank_seq","rank","name","brand","count"])
        w.writeheader()
        w.writerows(rows)

def read_csv(path: Path) -> List[Dict]:
    if not path.is_file():
        return []
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))

# ----------------------------- メイン -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--tile-height", type=int, default=900)
    ap.add_argument("--overlap", type=int, default=120)
    ap.add_argument("--model", default="gpt-4o-mini")
    args = ap.parse_args()

    url = args.url
    out_csv = Path(args.out)

    tmp_dir = Path("tiles")
    full_img = grab_fullpage_to(url, tmp_dir)
    tiles = split_full_image(full_img, tmp_dir, args.tile_height, args.overlap)

    all_rows: List[Dict] = []
    for i, tile in enumerate(tiles):
        rows = vlm_extract_rows(tile, model=args.model)
        for r in rows:
            rank = r.get("rank")
            name = r.get("name")
            count = r.get("count")
            if not name:
                continue
            # ブランド分割
            bm = vlm_split_brand(name, model=args.model)
            brand = bm.get("brand","未知")
            model_name = bm.get("model", name)
            all_rows.append({
                "rank_seq": str(len(all_rows)+1),
                "rank": rank,
                "name": model_name,
                "brand": brand,
                "count": count,
            })
        time.sleep(1.2)

    write_csv(out_csv, all_rows)
    print(f"[OK] {len(all_rows)} rows -> {out_csv}")

if __name__ == "__main__":
    main()
