# autohome_config_test.py
from playwright.sync_api import sync_playwright

url = "https://www.autohome.com.cn/config/series/7578.html#pvareaid=3454437"

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    page = browser.new_page()
    print("Loading:", url)
    page.goto(url, wait_until="networkidle", timeout=60000)
    print("Page title:", page.title())
    tables = page.query_selector_all("table")
    print(f"Found {len(tables)} table(s).")
    if tables:
        print("First table snippet:\n", tables[0].inner_text()[:300])
    else:
        print("No table tags detected—content may be loaded by JS.")
    browser.close()
