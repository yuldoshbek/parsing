#!/usr/bin/env python3
"""
TOTAL SCRAPE PRO v5 — Reviews
==============================
Playwright + Stealth + Page.Request (браузерная сессия) + PRO-Воронка

Архитектура:
  1. Persistent Chrome profile — cookie сохраняются между запусками, WB видит «вернувшегося» пользователя
  2. Stealth-патчи — убираем fingerprint Playwright (navigator.webdriver, plugins, etc.)
  3. page.request.get() — API-запросы через браузерную сессию (cookies + referer = легитимный AJAX)
  4. Пагинация 365 дней — skip/take цикл, стоп по дате
  5. PRO-Воронка — 100% негатив + умный фильтр позитива (17 слов + стоп-фразы)
  6. SKU De-gluing — разбивка по nmId (надёжнее color: WB часто оставляет color='')
  7. 3 формата: TXT (LLM), JSON (IT), CSV (Excel)

Запуск:
  pip install playwright playwright-stealth
  playwright install chromium
  python pro_scraper_reviews.py
"""

import csv
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright, Page, BrowserContext

try:
    from playwright_stealth import Stealth as _Stealth
    _stealth_instance = _Stealth()
    HAS_STEALTH = True
except ImportError:
    HAS_STEALTH = False
    _stealth_instance = None

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════════════════════════════
INPUT_FILE     = "articles.txt"
RESULTS_DIR    = Path(os.getenv("WB_RESULTS_DIR", "results_pro_reviews"))
CHROME_PROFILE = Path("chrome_profile")

DAYS_BACK      = 365        # глубина сбора (последние N дней)
TAKE_PER_REQ   = 100        # размер пакета API (WB обрабатывает до 100)
HEADLESS       = False      # False = видимый браузер (лучше для обхода DDoS-Guard)

# Тайминги
DELAY_PAGE          = (3.0, 6.0)
DELAY_BETWEEN       = (6.0, 14.0)
DELAY_COOLDOWN      = (30.0, 50.0)
COOLDOWN_EVERY_N    = 5
CHALLENGE_TIMEOUT_S = 180    # сколько ждать DDoS-Guard (сек)

# ══════════════════════════════════════════════════════════════════════════════
#  PRO-ВОРОНКА — СТОП-ФРАЗЫ И СЛОВА-БРЕЙКЕРЫ
# ══════════════════════════════════════════════════════════════════════════════

# Категория 1: Синдром «Подарочника» — оценивают коробку, не товар
STOP_GIFT = [
    "в деле не пробовали", "в деле еще не пробовали", "еще не пробовали",
    "еще не открывали", "пока не открывали", "не открывали",
    "ждет своего часа", "ждет нового года", "ждет дня рождения",
    "брали на подарок", "взяли на подарок", "покупали в подарок", "берем на подарок",
    "на вид все целое", "внешне все хорошо",
    "посмотрим как будет работать", "посмотрим как будет",
]

# Категория 2: Ода Логистике — оценивают доставку, не товар
STOP_LOGISTIC = [
    "пришло быстро", "доставка быстрая", "быстро пришло", "быстрая доставка",
    "упаковка целая", "коробка целая", "упаковано хорошо", "упаковка хорошая",
    "спасибо продавцу", "спасибо wb", "спасибо вайлдберриз", "спасибо магазину",
    "все пришло в целости", "пришло в целости", "доставили быстро",
    "курьер быстро", "доставка отличная",
]

# Категория 3: Белый Шум — эмоция без конкретики
STOP_NOISE = [
    "все супер", "всё супер", "все отлично", "всё отлично",
    "все работает", "всё работает",
    "ребенок доволен", "ребёнок доволен", "дети в восторге",
    "рекомендую к покупке", "рекомендую всем",
    "соответствует описанию", "как на фото", "как описание",
    "хорошая ручка", "хороший микроскоп", "хороший товар", "хороший продукт",
    "без брака", "претензий нет", "брак отсутствует",
]

ALL_STOP_PHRASES = STOP_GIFT + STOP_LOGISTIC + STOP_NOISE

# Слова-брейкеры: даже при наличии стоп-фраз → СОХРАНЯЕМ отзыв
BREAK_WORDS = [
    "сломал", "сломала", "сломалась", "не работает", "не включается",
    "забил", "забилась", "расплавил", "расплавилась", "воняет", "запах",
    "брак", "бракован", "возврат", "вернул", "возвращ",
    "минус", "недостат", "проблем", "дефект",
    "не выходит", "застрял", "не заряжает", "не заряд",
    "облез", "отклеил", "треснул", "помял",
]

MIN_WORDS_POSITIVE = 17     # минимум слов для прохождения позитивного отзыва


# ══════════════════════════════════════════════════════════════════════════════
#  PRO-ВОРОНКА — ЛОГИКА
# ══════════════════════════════════════════════════════════════════════════════
def pro_filter(review: dict) -> bool:
    """
    True = отзыв проходит воронку.
    False = мусор.

    ПРАВИЛО 1 — 100% негатива (1-3★) проходит всегда.

    ПРАВИЛО 2 — Позитив (4-5★), удаляем ТОЛЬКО если ОДНОВРЕМЕННО:
        - длина < 17 слов
        - есть стоп-фраза
        - нет слов-брейкеров

    Во всех остальных случаях — сохраняем.
    Иными словами:
        длина >= 17 слов            → СОХРАНИТЬ
        нет стоп-фразы              → СОХРАНИТЬ
        есть брейкер                → СОХРАНИТЬ
        короткий + стоп + нет брейкера → УДАЛИТЬ
    """
    rating = review.get("rating", 0)

    # ПРАВИЛО 1: весь негатив сохраняем
    if rating <= 3:
        return True

    # ПРАВИЛО 2: позитив
    full_text = " ".join(filter(None, [
        review.get("pros", ""),
        review.get("cons", ""),
        review.get("text", ""),
    ])).lower()

    # Длинный отзыв → сразу сохраняем, без проверки стоп-фраз
    if len(full_text.split()) >= MIN_WORDS_POSITIVE:
        return True

    # Короткий, но без стоп-фразы → тоже сохраняем
    has_stop = any(phrase in full_text for phrase in ALL_STOP_PHRASES)
    if not has_stop:
        return True

    # Короткий + стоп-фраза, но есть брейкер → сохраняем
    return any(word in full_text for word in BREAK_WORDS)
    # Короткий + стоп-фраза + нет брейкера → return False (неявно)


def filter_reason(review: dict) -> str:
    """Возвращает причину отсева (для диагностики). Пустая строка = отзыв прошёл."""
    rating = review.get("rating", 0)
    if rating <= 3:
        return ""
    full_text = " ".join(filter(None, [
        review.get("pros", ""), review.get("cons", ""), review.get("text", ""),
    ])).lower()
    wc = len(full_text.split())
    # Проходит: длинный
    if wc >= MIN_WORDS_POSITIVE:
        return ""
    # Проходит: короткий, но без стоп-фразы
    hit_phrase = next((p for p in ALL_STOP_PHRASES if p in full_text), None)
    if hit_phrase is None:
        return ""
    # Проходит: короткий + стоп + но есть брейкер
    if any(b in full_text for b in BREAK_WORDS):
        return ""
    # Удаляем: короткий + стоп + нет брейкера
    return f"short({wc}w) + stop_phrase: '{hit_phrase}'"


# ══════════════════════════════════════════════════════════════════════════════
#  УТИЛИТЫ
# ══════════════════════════════════════════════════════════════════════════════
_MONTHS_RU = [
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]


def rand_sleep(lo: float, hi: float) -> None:
    time.sleep(random.uniform(lo, hi))


def fmt_date(raw: str) -> str:
    """ISO 8601 → 'Д месяца, ЧЧ:ММ' (МСК = UTC+3)."""
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        msk = dt.astimezone(timezone.utc) + timedelta(hours=3)
        return f"{msk.day} {_MONTHS_RU[msk.month - 1]}, {msk.strftime('%H:%M')}"
    except Exception:
        return raw


def parse_dt(raw: str) -> datetime:
    """ISO 8601 → datetime aware (UTC)."""
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def count_words(text: str) -> int:
    return len(text.split()) if text else 0


def safe_filename(name: str) -> str:
    """Убираем символы, запрещённые в именах файлов/папок."""
    return re.sub(r'[\\/:*?"<>|]+', '_', name).strip("_. ")


# ══════════════════════════════════════════════════════════════════════════════
#  STEALTH — ПАТЧИ ПРОТИВ FINGERPRINT PLAYWRIGHT
# ══════════════════════════════════════════════════════════════════════════════
_STEALTH_JS = """
// 1. Убираем главный флаг автоматизации
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
try { delete navigator.__proto__.webdriver; } catch(e) {}

// 2. Реалистичные плагины браузера
const _plugins = [
    { name: 'Chrome PDF Plugin',   filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
    { name: 'Chrome PDF Viewer',   filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
    { name: 'Native Client',       filename: 'internal-nacl-plugin', description: '' },
];
Object.defineProperty(navigator, 'plugins',   { get: () => _plugins });
Object.defineProperty(navigator, 'mimeTypes', { get: () => [{ type: 'application/pdf', description: 'PDF', suffixes: 'pdf' }] });

// 3. Языки
Object.defineProperty(navigator, 'languages', { get: () => ['ru-RU', 'ru', 'en-US', 'en'] });

// 4. Объект window.chrome (есть в настоящем Chrome, нет в Playwright по умолчанию)
window.chrome = {
    app: { isInstalled: false, InstallState: { DISABLED: 'disabled', INSTALLED: 'installed', NOT_INSTALLED: 'not_installed' }, RunningState: { CANNOT_RUN: 'cannot_run', READY_TO_RUN: 'ready_to_run', RUNNING: 'running' } },
    runtime: {},
    loadTimes: function() { return {}; },
    csi: function() { return {}; },
};

// 5. Permissions API (обходим проверку на автоматизацию)
const _origPermQuery = window.navigator.permissions.query.bind(navigator.permissions);
window.navigator.permissions.query = (parameters) =>
    parameters.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : _origPermQuery(parameters);

// 6. Убираем следы HeadlessChrome в UA-строке (резервный патч)
Object.defineProperty(navigator, 'appVersion', {
    get: () => navigator.appVersion.replace('Headless', '')
});
"""


def apply_stealth(page: Page) -> None:
    if HAS_STEALTH and _stealth_instance is not None:
        _stealth_instance.apply_stealth_sync(page)
        log.debug("  [stealth] playwright-stealth v2 применён")
    else:
        page.add_init_script(_STEALTH_JS)
        log.debug("  [stealth] JS-патч применён")


# ══════════════════════════════════════════════════════════════════════════════
#  ЧЕЛОВЕКОПОДОБНОЕ ПОВЕДЕНИЕ
# ══════════════════════════════════════════════════════════════════════════════
def human_scroll(page: Page, scrolls: int = 4) -> None:
    for _ in range(scrolls):
        page.mouse.wheel(0, random.randint(250, 700))
        time.sleep(random.uniform(0.3, 0.9))


def wait_for_challenge(page: Page, timeout_s: int = CHALLENGE_TIMEOUT_S) -> bool:
    """
    Ждёт прохождения DDoS-Guard / капчи.
    Если браузер видимый — пользователь может решить капчу вручную.
    Возвращает True, если страница нормально загрузилась.
    """
    deadline = time.time() + timeout_s
    warned = False
    while time.time() < deadline:
        try:
            title = page.title()
            url   = page.url
        except Exception:
            time.sleep(2)
            continue

        blocked_signs = [
            "Что-то не так", "403", "Access Denied", "DDoS", "Checking",
            "Attention Required", "Just a moment",
        ]
        is_blocked = any(sign in title for sign in blocked_signs)

        if not is_blocked and "wildberries.ru" in url:
            return True

        if not warned:
            log.warning(f"  [challenge] DDoS-Guard обнаружен ({title!r}). Ожидаем до {timeout_s}с...")
            if not HEADLESS:
                log.warning("  [challenge] Если нужна капча — решите её в окне браузера")
            warned = True
        time.sleep(3)

    log.error("  [challenge] Таймаут — страница не прошла проверку")
    return False


def safe_goto(page: Page, url: str, timeout: int = 60_000) -> bool:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        rand_sleep(1.5, 3.0)
        human_scroll(page)
        return wait_for_challenge(page)
    except Exception as e:
        log.error(f"  [goto] ошибка: {e!r}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  ИНФО О ТОВАРЕ — через page.evaluate (fetch изнутри браузера)
# ══════════════════════════════════════════════════════════════════════════════
def get_product_info(page: Page, nm_id: int) -> dict:
    """
    Запрашивает brand, name, total_wb, root (imtId), nm_color.
    Использует page.evaluate(fetch) — запрос идёт ИЗНУТРИ браузера,
    с его cookies и origin. Это не блокируется card.wb.ru.

    Возвращаемые поля:
      total_wb_nm  — feedbacks именно этого nmId (не всей склейки!)
      nm_color     — цвет/вариант этого nmId из card API (например "Оранжевый")
      root         — imtId склейки (нужен для feedbacks API)
    """
    url = (
        f"https://card.wb.ru/cards/v4/detail"
        f"?appType=1&curr=rub&dest=-1257786&nm={nm_id}"
    )
    try:
        data = page.evaluate("""
            async (url) => {
                try {
                    const r = await fetch(url, {
                        headers: {
                            'Accept': 'application/json, text/plain, */*',
                            'Accept-Language': 'ru-RU,ru;q=0.9',
                        }
                    });
                    if (!r.ok) return {_error: r.status};
                    return await r.json();
                } catch(e) {
                    return {_error: String(e)};
                }
            }
        """, url)
    except Exception as e:
        log.error(f"  [product_info] ошибка evaluate: {e!r}")
        return {"brand": "", "name": "", "total_wb_nm": 0, "root": nm_id, "nm_color": ""}

    if not data or data.get("_error"):
        log.error(f"  [product_info] ошибка: {data}")
        return {"brand": "", "name": "", "total_wb_nm": 0, "root": nm_id, "nm_color": ""}

    products = (
        (data.get("data") or {}).get("products")
        or data.get("products")
        or []
    )
    p = products[0] if products else {}

    # Цвет конкретного nmId — WB отдаёт в colors[0].name
    colors   = p.get("colors") or []
    nm_color = colors[0].get("name", "").strip() if colors else ""

    return {
        "brand":      p.get("brand", ""),
        "name":       p.get("name",  ""),
        "total_wb_nm": int(p.get("feedbacks", 0) or p.get("nmFeedbacks", 0) or 0),
        "root":       int(p.get("root", 0) or nm_id),
        "nm_color":   nm_color,   # цвет этого nmId (Оранжевый / FDM / ...)
    }


# ══════════════════════════════════════════════════════════════════════════════
#  СБОР ОТЗЫВОВ — ПАГИНАЦИЯ ЗА 365 ДНЕЙ
# ══════════════════════════════════════════════════════════════════════════════
def fetch_all_reviews(page: Page, root: int,
                      cutoff: datetime, nm_id: int) -> tuple[list[dict], int]:
    """
    Загружает все отзывы за DAYS_BACK дней через page.evaluate(fetch).

    page.evaluate() делает fetch() ИЗНУТРИ браузера — с его cookies,
    origin и referer. feedbacks2.wb.ru видит легитимный запрос от страницы WB.

    Пагинация через skip/take до тех пор пока не встретим отзывы старше cutoff.
    """
    all_raw: list[dict] = []
    skip = 0
    total_wb = 0
    stop_flag = False

    while not stop_flag:
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
            """, {"root": root, "skip": skip, "take": TAKE_PER_REQ})
        except Exception as e:
            log.error(f"  [feedbacks] ошибка evaluate на skip={skip}: {e!r}")
            break

        if not data or data.get("_error"):
            log.warning(f"  [feedbacks] ошибка API на skip={skip}: {data}")
            break

        batch = data.get("feedbacks") or []
        if not batch:
            log.info(f"  [feedbacks] пустой батч на skip={skip} — конец")
            break

        if total_wb == 0:
            total_wb = int(data.get("feedbackCount") or 0)

        kept = 0
        for fb in batch:
            dt = parse_dt(fb.get("createdDate", ""))
            if dt < cutoff:
                stop_flag = True
                break
            all_raw.append(fb)
            kept += 1

        oldest = fmt_date(batch[-1].get("createdDate", "")) if batch else "?"
        log.info(f"  [feedbacks] skip={skip} | batch={len(batch)} | kept={kept} | oldest={oldest}")

        if len(batch) < TAKE_PER_REQ:
            log.info("  [feedbacks] последняя страница")
            break

        skip += len(batch)

        # Защита от бесконечного цикла: feedbacks2.wb.ru у крупных товаров
        # игнорирует skip и всегда возвращает одни и те же свежие отзывы.
        # Если запросили больше, чем всего существует — останавливаемся.
        if total_wb > 0 and skip >= total_wb:
            log.info(f"  [feedbacks] skip={skip} >= total_wb={total_wb} — стоп (API не поддерживает глубокую пагинацию)")
            break

        rand_sleep(0.8, 2.0)

    return all_raw, total_wb


# ══════════════════════════════════════════════════════════════════════════════
#  НОРМАЛИЗАЦИЯ ОТЗЫВА
# ══════════════════════════════════════════════════════════════════════════════
def normalize_review(fb: dict) -> dict:
    pros   = (fb.get("pros")  or "").strip()
    cons   = (fb.get("cons")  or "").strip()
    text   = (fb.get("text")  or "").strip()
    answer = ((fb.get("answer") or {}).get("text") or "").strip()
    color  = (fb.get("color") or "").strip()
    nm_id  = fb.get("nmId") or 0   # nmId конкретного варианта этого отзыва
    rating = int(fb.get("productValuation", 0))

    full_text = " ".join(filter(None, [pros, cons, text]))
    has_text  = bool(full_text.strip())

    return {
        "date":          fmt_date(fb.get("createdDate", "")),
        "date_raw":      fb.get("createdDate", ""),
        "rating":        rating,
        "rating_group":  ("позитив" if rating >= 4 else ("нейтрал" if rating == 3 else "негатив")),
        "sku_variant":   color,   # raw color из API (может быть пустым)
        "nm_id":         nm_id,   # nmId варианта — надёжнее color для расклейки
        "pros":          pros,
        "cons":          cons,
        "text":          text,
        "seller_answer": answer,
        "has_text":      has_text,
        "word_count":    count_words(full_text),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  ВОРОНКА — СТАТИСТИКА
# ══════════════════════════════════════════════════════════════════════════════
def build_funnel_stats(raw: list[dict], filtered: list[dict],
                       total_wb_nm: int, total_wb_root: int = 0) -> dict:
    """
    Строит двухуровневую воронку:
      total_wb_nm → total_parsed → no_text → working_base → after_filter (PRO)

    total_wb_nm  — feedbacks конкретного nmId (из card.wb.ru). Точный знаменатель.
    total_wb_root — feedbackCount всей склейки (из feedbacks API). Для справки.

    % сентимента считаются от after_filter — финальной рабочей базы.
    sum_pct_check = 100.0 ± 0.2 — самопроверка.
    """
    total_parsed = len(raw)
    no_text      = sum(1 for r in raw if not r.get("has_text", False))
    working_base = total_parsed - no_text   # с текстом, до воронки
    after_filter = len(filtered)            # прошло PRO-воронку

    # Знаменатель — только отзывы с текстом, прошедшие воронку
    text_filt = [r for r in filtered if r.get("has_text", False)]
    text_base = len(text_filt)
    positive  = sum(1 for r in text_filt if r.get("rating", 0) >= 4)
    neutral   = sum(1 for r in text_filt if r.get("rating", 0) == 3)
    negative  = sum(1 for r in text_filt if r.get("rating", 0) in (1, 2))

    def pct(n, base):
        return round(n / base * 100, 1) if base > 0 else 0.0

    return {
        "funnel": {
            "total_wb_nm":     total_wb_nm,    # оценок у этого nmId (per-SKU)
            "total_wb_root":   total_wb_root,  # оценок у всей склейки (справочно)
            "total_parsed":    total_parsed,
            "no_text":         no_text,
            "working_base":    working_base,
            "after_filter":    after_filter,
            "text_base":       text_base,       # знаменатель для % (с текстом, после воронки)
            # filter_rate_pct: % текстовых отзывов, отсеянных PRO-воронкой
            # Считаем от working_base (текстовые до воронки) к text_base (текстовые после)
            # after_filter включает no-text 1-3★ (Rule 1), поэтому нельзя делить на working_base
            "filter_rate_pct": round((1 - text_base / working_base) * 100, 1) if working_base else 0.0,
        },
        "sentiment": {
            "positive_count": positive,
            "neutral_count":  neutral,
            "negative_count": negative,
            "positive_pct":   pct(positive, text_base),
            "neutral_pct":    pct(neutral,  text_base),
            "negative_pct":   pct(negative, text_base),
            "sum_pct_check":  round(pct(positive, text_base) + pct(neutral, text_base) + pct(negative, text_base), 1),
        },
        "_note": (
            "total_wb_nm = оценок у этого nmId на WB (из card API). "
            "total_wb_root = оценок всей склейки (из feedbacks API, справочно). "
            "working_base = отзывы с текстом до воронки. "
            "after_filter = прошли PRO-воронку (знаменатель для %). "
            "sum_pct_check должен быть = 100.0 ± 0.2."
        ),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  ЭКСПОРТ — TXT (для LLM / нейросетей)
# ══════════════════════════════════════════════════════════════════════════════
def to_txt_llm(product_id: str, brand: str, name: str, sku: str,
               reviews: list[dict], funnel: dict) -> str:
    f = funnel.get("funnel", {})
    s = funnel.get("sentiment", {})
    SEP = "═" * 70

    lines = [
        SEP,
        f"АРТИКУЛ: {product_id}  |  БРЕНД: {brand}  |  SKU: {sku}",
        f"ТОВАР: {name}",
        SEP,
        "",
        "── ВОРОНКА ──────────────────────────────────────────────────────────",
        f"  Оценок этого SKU на WB: {f.get('total_wb_nm', '?')}  (feedbacks этого nmId)",
        f"  Оценок всей склейки:    {f.get('total_wb_root', '?')}  (feedbackCount root, справочно)",
        f"  Спарсено (год):         {f.get('total_parsed', 0)}",
        f"  Без текста:             {f.get('no_text', 0)}",
        f"  Рабочая база:           {f.get('working_base', 0)}",
        f"  После PRO-фильтра:      {f.get('after_filter', 0)}  (отсеяно {f.get('filter_rate_pct', 0)}%)",
        "",
        f"── СЕНТИМЕНТ (% от {f.get('text_base', 0)} отзывов с текстом) ─────────────────────",
        f"  Позитив (4-5★): {s.get('positive_count', 0)} отз.  = {s.get('positive_pct', 0)}%",
        f"  Нейтрал  (3★):  {s.get('neutral_count',  0)} отз.  = {s.get('neutral_pct',  0)}%",
        f"  Негатив (1-2★): {s.get('negative_count', 0)} отз.  = {s.get('negative_pct', 0)}%",
        f"  СУММА ПРОВЕРКА: {s.get('sum_pct_check', 0)}%",
        "",
        "── ОТЗЫВЫ ───────────────────────────────────────────────────────────",
        "",
    ]

    for i, r in enumerate(reviews, 1):
        rating = r.get("rating", 0)
        lines += [
            f"--- ОТЗЫВ #{i} ---",
            f"<meta date='{r.get('date')}' rating='{rating}' "
            f"group='{r.get('rating_group')}' sku='{r.get('sku_variant')}' "
            f"words='{r.get('word_count')}'>",
        ]
        if r.get("pros"):          lines.append(f"<pros>{r['pros']}</pros>")
        if r.get("cons"):          lines.append(f"<cons>{r['cons']}</cons>")
        if r.get("text"):          lines.append(f"<text>{r['text']}</text>")
        if r.get("seller_answer"): lines.append(f"<seller_answer>{r['seller_answer']}</seller_answer>")
        lines.append("")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  ЭКСПОРТ — CSV (для Excel / аналитики)
# ══════════════════════════════════════════════════════════════════════════════
CSV_FIELDS = [
    "product_id", "brand", "name", "sku_variant",
    "total_wb_nm", "total_wb_root", "total_parsed", "no_text", "working_base", "after_filter", "text_base",
    "positive_pct", "neutral_pct", "negative_pct", "sum_pct_check",
    "date", "rating", "rating_group", "word_count",
    "pros", "cons", "text", "seller_answer",
]


def _clean_cell(val: str) -> str:
    """Убираем переносы строк — они ломают CSV."""
    return re.sub(r"[\r\n]+", " ", val or "").strip()


def reviews_to_csv_rows(product_id: str, brand: str, name: str,
                         reviews: list[dict], funnel: dict) -> list[dict]:
    f = funnel.get("funnel", {})
    s = funnel.get("sentiment", {})
    rows = []
    for r in reviews:
        rows.append({
            "product_id":     product_id,
            "brand":          brand,
            "name":           name,
            "sku_variant":    r.get("sku_variant", ""),
            "total_wb_nm":    f.get("total_wb_nm", ""),    # per-nmId (точный)
            "total_wb_root":  f.get("total_wb_root", ""),  # вся склейка (справочно)
            "total_parsed":   f.get("total_parsed", ""),
            "no_text":        f.get("no_text", ""),
            "working_base":   f.get("working_base", ""),
            "after_filter":   f.get("after_filter", ""),
            "text_base":      f.get("text_base", ""),
            "positive_pct":   s.get("positive_pct", ""),
            "neutral_pct":    s.get("neutral_pct", ""),
            "negative_pct":   s.get("negative_pct", ""),
            "sum_pct_check":  s.get("sum_pct_check", ""),
            "date":           r.get("date", ""),
            "rating":         r.get("rating", ""),
            "rating_group":   r.get("rating_group", ""),
            "word_count":     r.get("word_count", ""),
            "pros":           _clean_cell(r.get("pros", "")),
            "cons":           _clean_cell(r.get("cons", "")),
            "text":           _clean_cell(r.get("text", "")),
            "seller_answer":  _clean_cell(r.get("seller_answer", "")),
        })
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    """UTF-8-SIG + точка с запятой — корректно открывается в русском Excel."""
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=CSV_FIELDS, extrasaction="ignore", delimiter=";"
        )
        writer.writeheader()
        writer.writerows(rows)


# ══════════════════════════════════════════════════════════════════════════════
#  СОХРАНЕНИЕ — 3 ФОРМАТА В ПАПКУ
# ══════════════════════════════════════════════════════════════════════════════
def save_to_dir(out_dir: Path,
                product_id: str, brand: str, name: str, sku: str,
                reviews: list[dict], funnel: dict,
                prefix: str = "PRO_reviews") -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. JSON (для IT / автоматизации)
    payload = {
        "product_id": product_id, "brand": brand, "name": name,
        "sku_variant": sku, "funnel_stats": funnel, "reviews": reviews,
    }
    (out_dir / f"{prefix}_LLM.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # 2. TXT (для LLM / нейросетей)
    txt = to_txt_llm(product_id, brand, name, sku, reviews, funnel)
    (out_dir / f"{prefix}_LLM.txt").write_text(txt, encoding="utf-8")

    # 3. CSV (для Excel / аналитики)
    rows = reviews_to_csv_rows(product_id, brand, name, reviews, funnel)
    write_csv(out_dir / f"{prefix}_Analytics.csv", rows)

    log.info(f"  [save] {out_dir.name}/ — {len(reviews)} отзывов (3 формата)")


# ══════════════════════════════════════════════════════════════════════════════
#  ЗАГРУЗКА АРТИКУЛОВ
# ══════════════════════════════════════════════════════════════════════════════
def load_articles(filepath: str) -> list[int]:
    path = Path(filepath)
    if not path.exists():
        log.error(f"Файл '{filepath}' не найден!")
        return []
    articles: list[int] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and line.isdigit():
            articles.append(int(line))
    log.info(f"Загружено {len(articles)} артикулов из '{filepath}'")
    return articles


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    articles = load_articles(INPUT_FILE)
    if not articles:
        return

    RESULTS_DIR.mkdir(exist_ok=True)
    CHROME_PROFILE.mkdir(exist_ok=True)

    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    log.info(f"Глубина: {DAYS_BACK} дней (с {cutoff.strftime('%d.%m.%Y')})")
    log.info(f"Stealth: {'playwright-stealth' if HAS_STEALTH else 'JS-патч (установи playwright-stealth для лучшей защиты)'}")

    # Накопители для сводных файлов
    all_csv_rows:    list[dict] = []
    all_json_prods:  list[dict] = []
    all_txt_blocks:  list[str]  = []

    with sync_playwright() as p:
        # Persistent context — сохраняет cookies/сессию между запусками
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(CHROME_PROFILE),
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-infobars",
                "--start-maximized",
                "--lang=ru-RU",
                "--disable-extensions-except=",  # убираем следы тестовых расширений
            ],
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            extra_http_headers={
                "Accept-Language":    "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "sec-ch-ua":          '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                "sec-ch-ua-mobile":   "?0",
                "sec-ch-ua-platform": '"Windows"',
            },
        )

        page = context.new_page()
        apply_stealth(page)

        # Первый визит — устанавливаем сессию WB
        log.info("Открываем wildberries.ru...")
        if not safe_goto(page, "https://www.wildberries.ru"):
            log.error("WB не загрузился — проверь интернет")
            context.close()
            return
        rand_sleep(*DELAY_BETWEEN)

        # Кэш по root: если несколько nmId из одной склейки — API запрос делаем один раз
        # root → (all_norm: list[dict], total_wb_root: int)
        root_cache: dict[int, tuple[list[dict], int]] = {}

        for idx, nm_id in enumerate(articles, 1):
            log.info("─" * 60)
            log.info(f"[{idx}/{len(articles)}]  nmId={nm_id}")
            log.info("─" * 60)

            # Кулдаун
            if idx > 1 and (idx - 1) % COOLDOWN_EVERY_N == 0:
                dur = random.uniform(*DELAY_COOLDOWN)
                log.info(f"  ☕ Cooldown {dur:.0f}с...")
                time.sleep(dur)

            # ── Навигация на страницу отзывов (устанавливает сессию WB) ──
            reviews_url = f"https://www.wildberries.ru/catalog/{nm_id}/feedbacks"
            if not safe_goto(page, reviews_url):
                log.warning(f"  Пропускаем {nm_id} — страница не прошла проверку")
                continue
            rand_sleep(*DELAY_PAGE)

            # ── Инфо о товаре — через page.evaluate (после загрузки страницы) ──
            info     = get_product_info(page, nm_id)
            root     = info["root"]
            nm_color = info["nm_color"]          # цвет этого nmId (из card API)
            total_wb_nm = info["total_wb_nm"]    # feedbacks этого nmId

            log.info(f"  Товар: {info['brand']!r} — {info['name']!r}")
            log.info(f"  nmId={nm_id}, root={root}, nm_color={nm_color!r}, total_wb_nm={total_wb_nm}")

            # ── Сбор отзывов: дедупликация по root ────────────────────────
            # Если несколько артикулов из одной склейки — данные уже в кэше
            if root in root_cache:
                all_norm, total_wb_root = root_cache[root]
                log.info(f"  root={root} уже в кэше — повторный запрос к API пропущен")
            else:
                raw_api, total_wb_root = fetch_all_reviews(page, root, cutoff, nm_id)
                log.info(f"  Сырых за {DAYS_BACK} дней: {len(raw_api)}")
                all_norm = [normalize_review(fb) for fb in raw_api]
                root_cache[root] = (all_norm, total_wb_root)

            # ── PRO-Воронка ────────────────────────────────────────────────
            filtered = [r for r in all_norm if pro_filter(r)]
            rejected = len(all_norm) - len(filtered)
            log.info(f"  PRO-воронка: {len(filtered)}/{len(all_norm)} прошли (отсеяно {rejected})")

            # ── Воронка статистики (сводная по root) ──────────────────────
            funnel = build_funnel_stats(all_norm, filtered,
                                        total_wb_nm=total_wb_nm,
                                        total_wb_root=total_wb_root)
            log.info(f"  Воронка: {funnel['funnel']}")
            log.info(f"  Сентимент: {funnel['sentiment']}")

            # ── SKU De-gluing: разбивка по nmId (надёжнее color) ──────────
            # nmId каждого отзыва точно указывает вариант, color — нет:
            # WB часто оставляет color='' даже когда вариант известен.
            #
            # nm_color_map: nmId → display-название (из card API или color из отзыва)
            # Запрашиваем карточки всех nmId, встреченных в отзывах
            unique_nm_ids = {r.get("nm_id") for r in all_norm if r.get("nm_id")}
            nm_color_map: dict[int, str] = {}

            if unique_nm_ids:
                nm_list = ";".join(str(n) for n in unique_nm_ids)
                card_url = (
                    f"https://card.wb.ru/cards/v4/detail"
                    f"?appType=1&curr=rub&dest=-1257786&nm={nm_list}"
                )
                try:
                    card_data = page.evaluate("""
                        async (url) => {
                            try {
                                const r = await fetch(url, {
                                    headers: {'Accept': 'application/json, text/plain, */*',
                                              'Accept-Language': 'ru-RU,ru;q=0.9'}
                                });
                                if (!r.ok) return {_error: r.status};
                                return await r.json();
                            } catch(e) { return {_error: String(e)}; }
                        }
                    """, card_url)
                    siblings = (
                        (card_data.get("data") or {}).get("products")
                        or card_data.get("products") or []
                    )
                    for sp in siblings:
                        s_nm  = int(sp.get("id", 0))
                        s_col = ((sp.get("colors") or [{}])[0]).get("name", "").strip()
                        if not s_col:
                            # fallback: берём часть названия (тип ручки) как метку
                            s_col = sp.get("name", "")[:20].strip()
                        if s_nm:
                            nm_color_map[s_nm] = s_col.capitalize() if s_col else str(s_nm)
                    log.info(f"  nmId→цвет карточки: {nm_color_map}")
                except Exception as e:
                    log.warning(f"  [deglue] не удалось получить карточки nmId: {e!r}")

            # Для nmId без карточки — fallback: color из отзыва (capitalize) или сам nmId
            def resolve_sku(r: dict) -> str:
                nm = r.get("nm_id", 0)
                if nm and nm in nm_color_map:
                    return nm_color_map[nm]
                raw = (r.get("sku_variant") or "").strip()
                return raw.capitalize() if raw else (str(nm) if nm else "Неизвестно")

            # Обновляем sku_variant в каждом отзыве и строим карту
            sku_map: dict[str, list[dict]] = {}
            for r in filtered:
                sku = resolve_sku(r)
                r["sku_variant"] = sku
                sku_map.setdefault(sku, []).append(r)

            # То же для all_norm (нужно для per-SKU воронки)
            for r in all_norm:
                r["sku_variant"] = resolve_sku(r)

            all_skus = sorted(sku_map.keys())
            log.info(f"  SKU-варианты ({len(all_skus)}): {all_skus}")

            # ── Сохранение: сводная папка артикула (все SKU вместе) ────────
            combined_sku = " / ".join(all_skus) if all_skus else ""
            save_to_dir(
                RESULTS_DIR / str(nm_id),
                str(nm_id), info["brand"], info["name"],
                combined_sku, filtered, funnel,
            )

            # ── Сохранение: отдельная папка на каждый SKU ─────────────────
            # total_wb_nm: если nmId из карточки известен → его feedbacks, иначе root
            nm_feedbacks_map: dict[str, int] = {}
            for s_nm, s_color in nm_color_map.items():
                # Для запрошенного nmId уже знаем total_wb_nm из card API
                if s_nm == nm_id:
                    nm_feedbacks_map[s_color.capitalize() if s_color else str(s_nm)] = total_wb_nm

            for sku, sku_reviews in sku_map.items():
                sku_raw_all = [r for r in all_norm if r.get("sku_variant") == sku]
                sku_total_wb_nm = nm_feedbacks_map.get(sku, total_wb_root)
                sku_funnel = build_funnel_stats(sku_raw_all, sku_reviews,
                                                total_wb_nm=sku_total_wb_nm,
                                                total_wb_root=total_wb_root)
                folder_name = f"{nm_id}_{safe_filename(sku)}"
                save_to_dir(
                    RESULTS_DIR / folder_name,
                    str(nm_id), info["brand"], info["name"],
                    sku, sku_reviews, sku_funnel,
                )

            # ── Накопление для сводных файлов ──────────────────────────────
            all_csv_rows.extend(
                reviews_to_csv_rows(str(nm_id), info["brand"], info["name"], filtered, funnel)
            )
            all_json_prods.append({
                "product_id":      str(nm_id),
                "brand":           info["brand"],
                "name":            info["name"],
                "nm_color":        nm_color,
                "all_sku_variants": all_skus,
                "funnel_stats":    funnel,
                "reviews":         filtered,
            })
            all_txt_blocks.append(
                to_txt_llm(str(nm_id), info["brand"], info["name"],
                           combined_sku, filtered, funnel)
            )

            rand_sleep(*DELAY_BETWEEN)

        context.close()

    # ── Сводные файлы — все артикулы в одном месте ───────────────────────────
    log.info("Сохраняем сводные файлы...")
    divider = "\n\n" + "═" * 70 + "\n\n"

    write_csv(RESULTS_DIR / "ALL_reviews_Analytics.csv", all_csv_rows)
    (RESULTS_DIR / "ALL_reviews_LLM.json").write_text(
        json.dumps(all_json_prods, ensure_ascii=False, indent=2), encoding="utf-8")
    (RESULTS_DIR / "ALL_reviews_LLM.txt").write_text(
        divider.join(all_txt_blocks), encoding="utf-8")

    # ── Итог ─────────────────────────────────────────────────────────────────
    total_r = sum(len(p["reviews"]) for p in all_json_prods)
    log.info("=" * 60)
    log.info(f"  Товаров: {len(all_json_prods)}  |  Отзывов (после фильтра): {total_r}")
    log.info(f"  Папки:   {RESULTS_DIR}/{{nmId}}/  и  {{nmId}}_{{color}}/")
    log.info(f"  Сводные: ALL_reviews_Analytics.csv  |  ALL_reviews_LLM.json  |  ALL_reviews_LLM.txt")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
