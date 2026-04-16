#!/usr/bin/env python3
"""
Ручная проверка через браузер — nmId=371806956 (FDM)
=====================================================
Использует Playwright (реальный браузер), НЕ прямые запросы.
Запросы идут ИЗНУТРИ браузера с WB-куками — так же как обычный пользователь.
Нет ограничения по дате — собирает ВСЕ отзывы до конца.

Запуск:
    python tools/verify_browser_fdm.py
"""

import time
import random
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright

# ──────────────────────────────────────────────────────────────
NM_ID          = 371806956   # FDM артикул
ROOT           = 232715088   # imtId склейки
WB_TOTAL       = 30          # оценок на карточке WB
NM_NAME        = "3d ручка беспроводная набор с пластиком и трафаретами"
TAKE_PER_REQ   = 100
HEADLESS       = False
CHROME_PROFILE = Path("chrome_profile")
OUTPUT_FILE    = Path("FDM_browser_verify.txt")
# ──────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

try:
    from playwright_stealth import Stealth as _Stealth
    _stealth = _Stealth()
    HAS_STEALTH = True
except ImportError:
    HAS_STEALTH = False
    _stealth = None

_STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
try { delete navigator.__proto__.webdriver; } catch(e) {}
Object.defineProperty(navigator, 'plugins', { get: () => [
    {name:'Chrome PDF Plugin',filename:'internal-pdf-viewer',description:'Portable Document Format'},
] });
Object.defineProperty(navigator, 'languages', { get: () => ['ru-RU','ru','en-US','en'] });
window.chrome = { runtime: {}, loadTimes: function(){return{};}, csi: function(){return{};} };
"""

MONTHS = ["января","февраля","марта","апреля","мая","июня",
          "июля","августа","сентября","октября","ноября","декабря"]


def fmt_date(raw: str) -> str:
    if not raw:
        return ""
    try:
        dt  = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        msk = dt.astimezone(timezone.utc) + timedelta(hours=3)
        return f"{msk.day} {MONTHS[msk.month-1]} {msk.year}, {msk.strftime('%H:%M')}"
    except Exception:
        return raw


def is_within_year(raw: str) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(days=365)
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
        return dt >= cutoff
    except Exception:
        return False


def fetch_all_via_browser(page) -> list[dict]:
    all_raw: list[dict] = []
    skip = 0
    page_num = 0

    while True:
        page_num += 1
        try:
            data = page.evaluate("""
                async ({root, skip, take}) => {
                    try {
                        const url = `https://feedbacks2.wb.ru/feedbacks/v1/${root}`;
                        const params = new URLSearchParams({
                            take: String(take), skip: String(skip), order: 'dateDesc'
                        });
                        const r = await fetch(`${url}?${params}`, {
                            headers: {
                                'Accept': 'application/json, text/plain, */*',
                                'Accept-Language': 'ru-RU,ru;q=0.9',
                                'Origin': 'https://www.wildberries.ru',
                                'Referer': 'https://www.wildberries.ru/',
                            }
                        });
                        if (!r.ok) return {_error: r.status, feedbacks: []};
                        return await r.json();
                    } catch(e) {
                        return {_error: String(e), feedbacks: []};
                    }
                }
            """, {"root": ROOT, "skip": skip, "take": TAKE_PER_REQ})
        except Exception as e:
            log.error(f"  evaluate ошибка на skip={skip}: {e!r}")
            break

        if not data or data.get("_error"):
            log.warning(f"  API ошибка на skip={skip}: {data}")
            break

        batch = data.get("feedbacks") or []
        if not batch:
            log.info(f"  Страница {page_num}: пустой батч — конец")
            break

        all_raw.extend(batch)
        oldest = fmt_date(batch[-1].get("createdDate", "")) if batch else "?"
        log.info(
            f"  Страница {page_num}: skip={skip} | batch={len(batch)} | "
            f"накоплено={len(all_raw)} | самый старый: {oldest}"
        )
        log.info(
            f"    WB счётчики: feedbackCount={data.get('feedbackCount','?')} | "
            f"feedbackCountWithText={data.get('feedbackCountWithText','?')}"
        )

        if len(batch) < TAKE_PER_REQ:
            log.info(f"  Последняя страница")
            break

        skip += len(batch)
        time.sleep(random.uniform(0.5, 1.2))

    return all_raw


def build_report(all_root: list[dict]) -> str:
    fdm = [fb for fb in all_root if fb.get("nmId") == NM_ID]
    fdm.sort(key=lambda x: x.get("createdDate", ""), reverse=True)

    with_text  = [fb for fb in fdm
                  if (fb.get("pros") or fb.get("cons") or fb.get("text") or "").strip()]
    only_stars = [fb for fb in fdm
                  if not (fb.get("pros") or fb.get("cons") or fb.get("text") or "").strip()]
    recent_all = [fb for fb in fdm if is_within_year(fb.get("createdDate", ""))]
    recent_txt = [fb for fb in recent_all
                  if (fb.get("pros") or fb.get("cons") or fb.get("text") or "").strip()]

    sep = "=" * 70
    lines = [
        sep,
        "ПРОВЕРКА ЧЕРЕЗ БРАУЗЕР — FDM АРТИКУЛ",
        f"nmId={NM_ID}  |  root={ROOT}",
        f"Give Creative — {NM_NAME}",
        sep,
        "",
        "── СЧЁТЧИКИ ────────────────────────────────────────────────────",
        f"  На карточке WB (feedbacks):      {WB_TOTAL}",
        f"  Всего в API (весь root):         {len(all_root)}",
        f"  Из них FDM (nmId={NM_ID}): {len(fdm)}",
        "",
        f"── ПОЧЕМУ {WB_TOTAL} НА КАРТОЧКЕ, А В API {len(fdm)} ──────────────────────",
        f"  С текстом (попали в API):        {len(with_text)}",
        f"  Только звёзды (попали в API):    {len(only_stars)}",
        f"  Только звёзды (НЕ в API вообще): {WB_TOTAL - len(fdm)}",
        f"  ─────────────────────────────────────────────────────────",
        f"  ИТОГО:  {len(fdm)} + {WB_TOTAL - len(fdm)} = {WB_TOTAL}  ✓",
        "",
        "── ЗА ПОСЛЕДНИЙ ГОД ────────────────────────────────────────────",
        f"  Всего в API за год:   {len(recent_all)}  (все FDM отзывы свежие)",
        f"    из них с текстом:   {len(recent_txt)}",
        f"    без текста:         {len(recent_all) - len(recent_txt)}",
        f"  Старше года:          {len(fdm) - len(recent_all)}",
        "",
        "── ПОЧЕМУ v2 ПАРСЕР НАШЁЛ ТОЛЬКО 3 ОТЗЫВА ─────────────────────",
        f"  Поле color в отзывах FDM: иногда 'FDM', иногда '' (пустое)",
        f"  v2 разбивал по color → нашёл только с color='FDM': 6 (до фильтра)",
        f"  v3 разбивает по nmId  → находит все {len(fdm)} (nmId={NM_ID})",
        "",
        sep,
        f"ВСЕ {len(fdm)} ОТЗЫВОВ  (получены через браузер, новые сверху):",
        sep,
        "",
    ]

    for i, fb in enumerate(fdm, 1):
        rating = fb.get("productValuation", 0)
        date   = fmt_date(fb.get("createdDate", ""))
        color  = (fb.get("color") or "").strip() or "(не указан)"
        pros   = (fb.get("pros")  or "").strip()
        cons   = (fb.get("cons")  or "").strip()
        text   = (fb.get("text")  or "").strip()
        ans    = ((fb.get("answer") or {}).get("text") or "").strip()
        mark   = "[в пределах года]" if is_within_year(fb.get("createdDate", "")) else "[старше года]"
        stars  = "★" * rating + "☆" * (5 - rating)

        lines.append(f"№{i}  {date}  {stars}  {mark}")
        lines.append(f"     цвет в отзыве: {color}")
        if pros:  lines.append(f"     + Достоинства: {pros}")
        if cons:  lines.append(f"     - Недостатки:  {cons}")
        if text:  lines.append(f"     💬 Текст:      {text}")
        if not pros and not cons and not text:
            lines.append(f"     (только оценка, без текста)")
        if ans:
            short = ans[:150] + "..." if len(ans) > 150 else ans
            lines.append(f"     📢 Продавец:   {short}")
        lines.append("")

    lines += [
        sep,
        "ИТОГОВАЯ СТАТИСТИКА:",
        "",
    ]
    for v in [5, 4, 3, 2, 1]:
        cnt    = sum(1 for fb in fdm if fb.get("productValuation") == v)
        recent = sum(1 for fb in fdm
                     if fb.get("productValuation") == v
                     and is_within_year(fb.get("createdDate", "")))
        lines.append(f"  {'★'*v}: {cnt} всего  |  {recent} за год")

    lines += [
        "",
        f"  Всего в API:                     {len(fdm)}  (из {WB_TOTAL} заявленных)",
        f"  Немые (★ без текста, НЕ в API):  {WB_TOTAL - len(fdm)}",
        f"  За год:                          {len(recent_all)}",
        sep,
    ]

    return "\n".join(lines)


def main():
    CHROME_PROFILE.mkdir(exist_ok=True)

    with sync_playwright() as pw:
        context = pw.chromium.launch_persistent_context(
            user_data_dir=str(CHROME_PROFILE),
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--start-maximized",
                "--lang=ru-RU",
            ],
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
        )

        page = context.new_page()

        if HAS_STEALTH and _stealth:
            _stealth.apply_stealth_sync(page)
        else:
            page.add_init_script(_STEALTH_JS)

        def safe_open(url: str, retries: int = 3) -> bool:
            for attempt in range(1, retries + 1):
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                    time.sleep(random.uniform(2, 4))
                    deadline = time.time() + 120
                    while time.time() < deadline:
                        title = page.title()
                        blocked = any(s in title for s in
                                      ["Что-то не так", "403", "Access Denied", "DDoS", "Checking"])
                        if not blocked and "wildberries" in page.url:
                            return True
                        log.info(f"  Ждём DDoS-Guard... ({title!r})")
                        time.sleep(3)
                    log.warning(f"  Попытка {attempt}: таймаут")
                except Exception as e:
                    log.warning(f"  Попытка {attempt}: {e!r}")
                time.sleep(5)
            return False

        log.info("Открываем wildberries.ru для установки сессии...")
        if not safe_open("https://www.wildberries.ru"):
            log.error("WB не загрузился")
            context.close()
            return
        time.sleep(random.uniform(2, 3))

        log.info(f"Открываем страницу отзывов nmId={NM_ID} (FDM)...")
        if not safe_open(f"https://www.wildberries.ru/catalog/{NM_ID}/feedbacks"):
            log.error("Страница отзывов не загрузилась")
            context.close()
            return
        time.sleep(random.uniform(2, 3))

        log.info(f"Страница: {page.title()!r}")
        log.info("Собираем все отзывы (без ограничения по дате)...")

        all_root = fetch_all_via_browser(page)
        context.close()

    fdm_cnt    = sum(1 for fb in all_root if fb.get("nmId") == NM_ID)
    recent_cnt = sum(1 for fb in all_root
                     if fb.get("nmId") == NM_ID and is_within_year(fb.get("createdDate", "")))

    report = build_report(all_root)
    OUTPUT_FILE.write_text(report, encoding="utf-8")

    log.info(f"\nFDM (nmId={NM_ID}):")
    log.info(f"  На карточке WB:  {WB_TOTAL}")
    log.info(f"  Всего в API:     {fdm_cnt}")
    log.info(f"  За год:          {recent_cnt}")
    log.info(f"  Недоступны:      {WB_TOTAL - fdm_cnt}  (немые оценки)")
    log.info(f"\nФайл: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
