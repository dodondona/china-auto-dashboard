import re
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

DOT_CLASS_PATTERNS = [
    (re.compile(r"style_col_dot_solid__|dot_solid", re.I), "●"),
    (re.compile(r"style_col_dot_outline__|dot_outline", re.I), "○"),
]

def _detect_dot_class(td):
    for tag in td.select("i"):
        cls = " ".join(tag.get("class", []))
        for pat, sym in DOT_CLASS_PATTERNS:
            if pat.search(cls):
                return sym
    return ""

async def main():
    url = "https://www.autohome.com.cn/config/series/7806.html#pvareaid=3454437"
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = await browser.new_page()
        await page.goto(url, wait_until="networkidle")
        await page.wait_for_timeout(8000)  # JS描画完了まで待機
        html = await page.content()
        await browser.close()

    soup = BeautifulSoup(html, "html.parser")
    rows_out = []
    for tr in soup.select("table tr"):
        cells = tr.select("th,td")
        if not cells:
            continue
        row = []
        for td in cells:
            mark = _detect_dot_class(td)
            text = td.get_text(strip=True)
            if mark:
                text = f"{mark} {text}".strip()
            if text in ("-", "—"):
                text = ""
            row.append(text)
        if any(row):
            rows_out.append(row)

    for r in rows_out:
        print(",".join(r))

if __name__ == "__main__":
    asyncio.run(main())
