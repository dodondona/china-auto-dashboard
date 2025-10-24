# -*- coding: utf-8 -*-
# .github/scripts/stage2_images_to_public.py
#
# csv/*.csv を読み、image列が data:image/... なら public/autohome_images へファイル化。
# image_url 列を公開URL (PUBLIC_PREFIX) + ファイル名 で作成して新CSVを書き出します。

import os, re, base64, hashlib
from pathlib import Path
import pandas as pd

CSV_DIR   = Path("csv")
OUT_DIR   = Path("public/autohome_images")
MERGED_CSV= Path("public/autohome_ranking_with_image_urls.csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)
Path("public").mkdir(exist_ok=True)

PUBLIC_PREFIX = os.environ.get("PUBLIC_PREFIX", "").rstrip("/")
if not PUBLIC_PREFIX:
    # 例: https://<user>.github.io/<repo>/autohome_images
    print("WARNING: PUBLIC_PREFIX is not set. image_url will be relative paths.")
    PUBLIC_PREFIX = "/autohome_images"

def ext_from_data_uri(uri: str) -> str:
    # data:image/avif;base64,...
    m = re.match(r"data:image/([a-zA-Z0-9+.-]+);base64,", uri)
    if not m: return "img"
    mt = m.group(1).lower()
    if "avif" in mt: return "avif"
    if "webp" in mt: return "webp"
    if "jpeg" in mt or "jpg" in mt: return "jpg"
    if "png" in mt: return "png"
    return "img"

def save_data_image(uri: str, hint: str) -> str:
    # 一意ファイル名にする（rank+name でなく、内容ハッシュで衝突回避）
    header, b64 = uri.split(",", 1)
    data = base64.b64decode(b64)
    ext  = ext_from_data_uri(uri)
    sha  = hashlib.sha256(data).hexdigest()[:16]
    fname = f"{sha}_{hint}.{ext}"
    path = OUT_DIR / fname
    path.write_bytes(data)
    return fname

def main():
    frames = []
    for csv in sorted(CSV_DIR.glob("*.csv")):
        df = pd.read_csv(csv)
        # image_url列を追加
        urls = []
        for i, row in df.iterrows():
            img = row.get("image")
            rank = row.get("rank")
            name = row.get("name") or ""
            hint = re.sub(r"[^\w\-]+", "_", f"{int(rank) if pd.notna(rank) else 0}_{name}")[:80]

            if isinstance(img, str) and img.startswith("data:image/"):
                fname = save_data_image(img, hint)
                urls.append(f"{PUBLIC_PREFIX}/{fname}")
            else:
                # http(s) or blank はそのまま
                urls.append(img if isinstance(img, str) and img else "")

        df["image_url"] = urls
        frames.append(df)

    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    merged.to_csv(MERGED_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Saved {MERGED_CSV}  rows={len(merged)}")

if __name__ == "__main__":
    main()
