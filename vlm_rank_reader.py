#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
vlm_rank_reader.py
URL からのフルページスクショ取得（Playwright）→ VLM（AI目視）で「順位/車名/台数」を抽出 → CSV に出力。
"""

import os, io, re, glob, csv, time, json, base64, argparse
from typing import List, Dict, Tuple, Optional
from PIL import Image

# ========== Playwright スクショ ==========
def grab_fullpage_to(out_dir: str, url: str, viewport_w: int, viewport_h: int,
                     device_scale_factor: float, split: bool, tile_height: int) -> List[str]:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    os.makedirs(out_dir, exist_ok=True)
    full_path = os.path.join(out_dir, "_page_full.png")

    def smooth_scroll(page):
        page.evaluate("""
        () => new Promise(resolve => {
          let y = 0;
          const step = 800;
          const timer = setInterval(() => {
            window.scrollBy(0, step);
            y += step;
            if (y + window.innerHeight >= document.body.scrollHeight) {
              clearInterval(timer);
              setTimeout(resolve, 800);
            }
          }, 200);
        })
        """)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled","--no-sandbox"])
        context = browser.new_context(
            viewport={"width": viewport_w, "height": viewport_h},
            device_scale_factor=device_scale_factor,
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"),
            locale="zh-CN",
            extra_http_headers={"Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8"},
        )
        page = context.new_page()
        page.set_default_navigation_timeout(90_000)
        page.set_default_timeout(90_000)

        last_err=None
        for attempt in range(3):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except PWTimeout:
                    pass
                smooth_scroll(page)
                page.screenshot(path=full_path, full_page=True)
                break
            except Exception as e:
                last_err=e
                if attempt==2:
                    browser.close()
                    raise
                time.sleep(2.0+attempt)
        browser.close()

    return split_full_image(full_path, out_dir, tile_height) if split else [full_path]

def split_full_image(full_path: str, out_dir: str, tile_height: int) -> List[str]:
    im = Image.open(full_path).convert("RGB")
    W,H = im.size
    paths=[]
    idx=0
    for y0 in range(0,H,tile_height):
        y1=min(y0+tile_height,H)
        tile=im.crop((0,y0,W,y1))
        p=os.path.join(out_dir,f"tile_{idx:02d}.png")
        tile.save(p)
        paths.append(p)
        idx+=1
    return paths

# ========== 画像処理＆VLM呼び出し ==========
def load_and_downscale_for_vlm(path: str, max_side=2200, jpeg_quality=85) -> Tuple[str, Tuple[int,int]]:
    im = Image.open(path).convert("RGB")
    w,h = im.size
    if max(w,h) > max_side:
        scale = max_side/float(max(w,h))
        im = im.resize((int(w*scale),int(h*scale)), Image.LANCZOS)
    buf=io.BytesIO()
    im.save(buf,format="JPEG",quality=jpeg_quality,optimize=True)
    b64=base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/jpeg;base64,{b64}", im.size

def strict_json_from_text(txt: str) -> dict:
    m=re.search(r"```json\s*(\{.*?\})\s*```",txt,flags=re.S)
    if m: return json.loads(m.group(1))
    m=re.search(r"(\{[\s\S]*\})",txt)
    if m: return json.loads(m.group(1))
    m=re.search(r"(\[[\s\S]*\])",txt)
    if m: return {"rows":json.loads(m.group(1))}
    return json.loads(txt)

def rows_from_payload(payload) -> List[dict]:
    if isinstance(payload,list):
        rows=payload
    elif isinstance(payload,dict):
        rows=payload.get("rows") or payload.get("data") or payload.get("items") or []
    else:
        rows=[]
    norm=[]
    for r in rows:
        rank=r.get("rank")
        if isinstance(rank,float): rank=int(rank)
        if not isinstance(rank,int): rank=None
        name=(r.get("name") or r.get("brand") or r.get("model") or "").strip()
        cnt=r.get("count") or r.get("sales") or r.get("units")
        if isinstance(cnt,str):
            t=cnt.replace(",","").replace(" ","")
            cnt=int(t) if t.isdigit() else None
        if isinstance(cnt,float): cnt=int(cnt)
        norm.append({"rank":rank,"name":name,"count":cnt})
    return norm

def merge_and_reindex(all_rows: List[dict]) -> List[dict]:
    with_rank=[r for r in all_rows if isinstance(r.get("rank"),int)]
    no_rank=[r for r in all_rows if not isinstance(r.get("rank"),int)]
    out=sorted(with_rank,key=lambda x:x["rank"])+no_rank
    dedup,seen=[],set()
    for r in out:
        key=(r.get("rank"),r.get("name"),r.get("count"))
        if r.get("name") and r.get("count") and key not in seen:
            seen.add(key); dedup.append(r)
    for i,r in enumerate(dedup,start=1):
        r["rank_seq"]=i
    return dedup

SYSTEM_PROMPT="""あなたは表の読み取りに特化した視覚アシスタントです。
画像は中国の自動車販売ランキング（車系の月販台数）です。
UIの飾り・ボタン・注釈は無視してください。
出力は JSON のみ。"""

def make_user_prompt(): return "画像内のランキング表から行を抽出し、JSONだけを返してください。"

# ========== OpenAI クライアント ==========
class OpenAIClient:
    def __init__(self, model, api_key=None, base_url=None):
        from openai import OpenAI
        if api_key: os.environ["OPENAI_API_KEY"]=api_key
        if base_url: os.environ["OPENAI_BASE_URL"]=base_url
        self.client=OpenAI(); self.model=model
    def infer(self,data_urls,system_prompt,user_prompt,max_retries=3)->str:
        messages=[{"role":"system","content":system_prompt},
                  {"role":"user","content":[{"type":"text","text":user_prompt},
                                            *[{"type":"image_url","image_url":{"url":u}} for u in data_urls]]}]
        err=None
        for k in range(max_retries):
            try:
                resp=self.client.chat.completions.create(
                    model=self.model,messages=messages,temperature=0,
                    response_format={"type":"json_object"})
                return resp.choices[0].message.content
            except Exception as e:
                err=e; time.sleep(1.2*(k+1))
        raise err

# ========== メイン ==========
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--from-url")
    ap.add_argument("--input")
    ap.add_argument("--out-dir",default="tiles")
    ap.add_argument("--fullpage-split",action="store_true")
    ap.add_argument("--tile-height",type=int,default=1200)
    ap.add_argument("--viewport-w",type=int,default=1680)
    ap.add_argument("--viewport-h",type=int,default=2600)
    ap.add_argument("--device-scale-factor",type=float,default=3.0)
    ap.add_argument("--provider",choices=["openai"],default="openai")
    ap.add_argument("--model",default="gpt-4o")
    ap.add_argument("--openai-api-key",default=os.getenv("OPENAI_API_KEY"))
    ap.add_argument("--csv",default="result.csv")
    args=ap.parse_args()

    if args.from_url:
        image_paths=grab_fullpage_to(args.out_dir,args.from_url,args.viewport_w,args.viewport_h,
                                     args.device_scale_factor,args.fullpage_split,args.tile_height)
    else:
        image_paths=sorted(glob.glob(args.input or ""))

    client=OpenAIClient(model=args.model,api_key=args.openai_api_key)
    user_prompt=make_user_prompt()

    all_rows=[]
    for p in image_paths:
        data_url,_=load_and_downscale_for_vlm(p)
        txt=client.infer([data_url],SYSTEM_PROMPT,user_prompt)
        try: payload=strict_json_from_text(txt)
        except: payload={"rows":[]}
        rows=rows_from_payload(payload)
        for r in rows: r["_image"]=os.path.basename(p)
        all_rows.extend(rows)

    merged=merge_and_reindex(all_rows)
    with open(args.csv,"w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=["rank_seq","rank","name","count","_image"])
        w.writeheader()
        for r in merged: w.writerow(r)
    print(f"[DONE] {len(merged)} rows -> {args.csv}")

if __name__=="__main__": main()
