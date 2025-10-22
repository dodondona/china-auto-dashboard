# tools/autohome_config_test.py
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

PC_URL = "https://car.autohome.com.cn/config/series/7578.html"   # ← www ではなく car.*
M_URL  = "https://car.m.autohome.com.cn/config/series/7578.html"  # モバイル版

def log(msg): print(msg, flush=True)

def humanize_context(pw):
    browser = pw.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ],
    )
    ctx = browser.new_context(
        locale="zh-CN",
        timezone_id="Asia/Shanghai",
        viewport={"width": 1366, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    )
    # navigator.webdriver を消す／WebGLなど指紋を少し人間寄りに
    ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter){
          // UNMASKED_VENDOR_WEBGL / UNMASKED_RENDERER_WEBGL
          if (parameter === 37445) return "Google Inc.";
          if (parameter === 37446) return "ANGLE (Intel, Intel(R) UHD Graphics, D3D11)";
          return getParameter.call(this, parameter);
        };
        Object.defineProperty(Notification, 'permission', { get: () => 'denied' });
    """)
    return browser, ctx

def ensure_param_tab(page):
    # ページによっては「参数配置」タブを明示クリックしないと表が出ない
    try:
        page.click("text=参数配置", timeout=2000)
    except Exception:
        pass

def wait_for_content(page):
    # 1) まず“テーブル風の要素”を待つ
    selectors = [
        "table",                # PC版は<table>のことが多い
        ".configuration, .config-table, .parameter, .uibox",  # divベースの版もある
        "section:has-text('基本参数')"
    ]
    for sel in selectors:
        try:
            page.wait_for_selector(sel, timeout=6000, state="visible")
            return True
        except PWTimeout:
            continue
    return False

def count_tables(page):
    return len(page.query_selector_all("table"))

def dump_head(page):
    try:
        log("Page title: " + page.title())
    except Exception:
        pass

def try_open(url):
    log(f"Loading: {url}")
    with sync_playwright() as pw:
        browser, ctx = humanize_context(pw)
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        # クッキーの同意などが出た場合は閉じる（存在すれば）
        for text in ("同意", "接受", "我知道了", "关闭", "關閉"):
            try:
                page.get_by_text(text, exact=False).click(timeout=1000)
            except Exception:
                pass

        # ネットワークが一段落するまで待機 → タブ押下 → さらに待機
        page.wait_for_load_state("networkidle", timeout=30000)
        ensure_param_tab(page)
        page.wait_for_load_state("networkidle", timeout=15000)

        # 軽くスクロールして遅延描画を促す
        for _ in range(6):
            page.mouse.wheel(0, 1200)
            page.wait_for_timeout(400)

        ok = wait_for_content(page)
        dump_head(page)
        tables = count_tables(page)
        log(f"Visible content detected: {ok}")
        log(f"Found {tables} table(s).")

        # 参考スニペット
        if tables:
            txt = page.query_selector_all("table")[0].inner_text()[:300]
            log("First table snippet:\n" + txt)

        ctx.close()
        browser.close()
        return ok or tables > 0

if __name__ == "__main__":
    # まず PC 版、ダメならモバイルへフォールバック
    ok = try_open(PC_URL)
    if not ok:
        log("PC版で可視要素が見つからなかったため、モバイル版にフォールバックします。")
        try_open(M_URL)
