import re
import asyncio
from playwright.async_api import async_playwright

# 黒丸・白丸を class から判定
DOT_CLASS_PATTERNS = [
    (re.compile(r"style_col_dot_solid__|dot_solid", re.I), "●"),
    (re.compile(r"style_col_dot_outline__|dot_outline", re.I), "○"),
]

def _detect_dot_by_class(cell):
    try:
        for i in cell.query_selector_all("i"):
            cls = (i.get_attribute("class") or "")
            for pat, sym in DOT_CLASS_PATTERNS:
                if pat.search(cls):
                    return sym
    except Exception:
        pass
    return ""

async def extract_table(page):
    rows = []
    for tr in await page.query_selector_all("table tr"):
        row = []
        for td in await tr.query_selector_all("th, td"):
            mark = _detect_dot_by_class(td)
            if mark:
                try:
                    base = await td.evaluate(
                        """(el) => {
                            const c = el.cloneNode(true);
                            c.querySelectorAll('i').forEach(n=>n.remove());
                            return (c.innerText || '').replace(/\\s+/g, ' ').trim();
                        }"""
                    )
                except Exception:
                    base = ""
                text = f"{mark} {base}".strip() if base else mark
            else:
                try:
                    text = (
                        await td.evaluate(
                            """(el) => (el.innerText || '').replace(/\\s+/g, ' ').trim()"""
                        )
                    ) or ""
                except Exception:
                    text = ""
            if text in ("-", "—"):
                text = ""
            row.append(text)
        if any(x for x in row):
            rows.append(row)
    return rows

async def main():
    url = "https://www.autohome.com.cn/config/series/7806.html#pvareaid=3454437"
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(url)
        await page.wait_for_selector("table")
        rows = await extract_table(page)
        for row in rows:
            print(",".join(row))
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
