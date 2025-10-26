import asyncio
import json
import re
import sys
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

OUTPUT_DIR = Path("output/autohome")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def extract_config_json(html: str):
    """
    Extract config JSON object from Autohome car configuration HTML page.
    """
    soup = BeautifulSoup(html, "lxml")
    scripts = soup.find_all("script")
    for script in scripts:
        if not script.string:
            continue
        m = re.search(r"var\s+config\s*=\s*(\{.*?\});", script.string, re.S)
        if m:
            txt = m.group(1)
            try:
                data = json.loads(txt)
                return data
            except Exception:
                # fallback: tolerate trailing commas or invalid JSON
                try:
                    txt = re.sub(r",\s*}", "}", txt)
                    txt = re.sub(r",\s*]", "]", txt)
                    return json.loads(txt)
                except Exception as e:
                    print("Failed to parse config JSON:", e)
                    return None
    return None

def normalize_text(text):
    return re.sub(r"\s+", "", text or "").strip()

def save_csv(series_id: str, data: dict):
    if not data:
        print(f"No config data for {series_id}")
        return
    out_dir = OUTPUT_DIR / series_id
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"config_{series_id}.csv"
    import pandas as pd

    rows = []
    # Extract table data
    if "result" in data and "paramtypeitems" in data["result"]:
        for ptype in data["result"]["paramtypeitems"]:
            ptype_name = ptype.get("name", "")
            for param in ptype.get("paramitems", []):
                key = normalize_text(param.get("name"))
                for i, value in enumerate(param.get("valueitems", [])):
                    rows.append({
                        "type": ptype_name,
                        "name": key,
                        "value": normalize_text(value.get("value")),
                        "model_index": i
                    })
    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(csv_path, index=False)
        print(f"Saved: {csv_path}")
    else:
        print(f"No rows extracted for {series_id}")

def process_series(series_id: str, url: str, browser):
    page = browser.new_page()
    print(f"Loading: {url}")
    try:
        # ✅ リトライ付き goto
        try:
            page.goto(url, wait_until="networkidle", timeout=120000)
        except Exception as e:
            print(f"⚠️ Timeout or error at first attempt: {e}. Retrying once...")
            page.goto(url, wait_until="networkidle", timeout=120000)

        html = page.content()
        data = extract_config_json(html)
        if not data:
            print(f"No config found for {series_id}")
        else:
            save_csv(series_id, data)
    finally:
        page.close()

def main():
    args = sys.argv[1:]
    if not args:
        print("Usage: python autohome_config_to_csv.py <series_id or URL>")
        sys.exit(1)

    arg = args[0]
    if arg.isdigit():
        series_id = arg
        url = f"https://www.autohome.com.cn/config/series/{series_id}.html#pvareaid=3454437"
    elif "autohome.com.cn" in arg:
        series_id_match = re.search(r"/series/(\d+)", arg)
        if not series_id_match:
            print("Cannot extract series_id from URL")
            sys.exit(1)
        series_id = series_id_match.group(1)
        url = arg
    else:
        print("Invalid argument")
        sys.exit(1)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        process_series(series_id, url, browser)
        browser.close()

if __name__ == "__main__":
    main()
