"""Quick diagnostic: проверяем что Playwright видит на странице отзывов."""
import time
from playwright.sync_api import sync_playwright

NM_ID = 480055077

with sync_playwright() as pw:
    browser = pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
        ],
    )
    ctx = browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        locale="ru-RU",
        extra_http_headers={"Accept-Language": "ru-RU,ru;q=0.9"},
    )
    ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3]});
        window.chrome = { runtime: {} };
    """)

    page = ctx.new_page()
    url = f"https://www.wildberries.ru/catalog/{NM_ID}/feedbacks?page=1"
    print(f"Navigating to {url} ...")

    try:
        page.goto(url, wait_until="networkidle", timeout=60_000)
        print("goto done (networkidle)")
    except Exception as e:
        print(f"goto exception: {e}")

    # Extra wait
    time.sleep(3)

    title   = page.title()
    n_items = page.evaluate("document.querySelectorAll('.comments__item.feedback').length")
    rstate  = page.evaluate("document.readyState")
    final_url = page.url

    print(f"title      = {title!r}")
    print(f"final_url  = {final_url!r}")
    print(f"readyState = {rstate!r}")
    print(f"reviews    = {n_items}")

    if n_items > 0:
        sample = page.evaluate(r"""
        () => {
            const item = document.querySelector('.comments__item.feedback');
            const starEl = item.querySelector('.stars-line.feedback__rating');
            const ratingMatch = (starEl?.className || '').match(/\bstar(\d)\b/);
            return {
                rating: ratingMatch ? parseInt(ratingMatch[1]) : 0,
                date:   item.querySelector('.feedback__date')?.innerText?.trim() || '',
                text:   (item.querySelector('.feedback__text')?.innerText || '').slice(0,60),
            };
        }
        """)
        print(f"sample[0]  = {sample}")
    else:
        # Dump first 2000 chars of HTML to understand what WB returned
        html = page.evaluate("document.body.innerHTML.slice(0, 2000)")
        print(f"\n--- HTML snippet ---\n{html}\n--- end ---")

    ctx.close()
    browser.close()
    print("Done.")
