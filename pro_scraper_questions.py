#!/usr/bin/env python3
"""
TOTAL SCRAPE PRO v5 — Questions
================================
Playwright + Stealth + Page.Request — сбор вопросов покупателей WB

Запуск параллельно с reviews:
  Окно 1: python pro_scraper_reviews.py
  Окно 2: python pro_scraper_questions.py

Фильтрация вопросов:
  Убираем только мусор короче 3 слов (пустые, случайные символы).
  Весь содержательный вопрос — ценная информация, не фильтруем жёстко.

Форматы вывода:
  PRO_questions_LLM.txt       — для ИИ (структурированный markdown)
  PRO_questions_LLM.json      — для IT (чистый JSON)
  PRO_questions_Analytics.csv — для Excel (UTF-8-SIG, разделитель ;)
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
RESULTS_DIR    = Path(os.getenv("WB_RESULTS_DIR_Q", "results_pro_questions"))
CHROME_PROFILE = Path("chrome_profile_q")   # отдельный профиль от reviews

DAYS_BACK      = 365
TAKE_PER_REQ   = 30          # WB Q&A возвращает по 30
HEADLESS       = False

DELAY_PAGE          = (3.0, 6.0)
DELAY_BETWEEN       = (6.0, 14.0)
DELAY_COOLDOWN      = (30.0, 50.0)
COOLDOWN_EVERY_N    = 5
CHALLENGE_TIMEOUT_S = 180

MIN_WORDS_QUESTION  = 3      # вопросы короче 3 слов → мусор

# ══════════════════════════════════════════════════════════════════════════════
#  УТИЛИТЫ (дублированы из reviews для независимости скрипта)
# ══════════════════════════════════════════════════════════════════════════════
_MONTHS_RU = [
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]

_STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
try { delete navigator.__proto__.webdriver; } catch(e) {}
Object.defineProperty(navigator, 'plugins',   { get: () => [{name:'Chrome PDF Plugin',filename:'internal-pdf-viewer',description:'PDF'}] });
Object.defineProperty(navigator, 'languages', { get: () => ['ru-RU','ru','en-US','en'] });
window.chrome = { app:{isInstalled:false}, runtime:{}, loadTimes:function(){return {};}, csi:function(){return {};} };
const _oQ = window.navigator.permissions.query.bind(navigator.permissions);
window.navigator.permissions.query = (p) => p.name==='notifications' ? Promise.resolve({state:Notification.permission}) : _oQ(p);
"""


def rand_sleep(lo: float, hi: float) -> None:
    time.sleep(random.uniform(lo, hi))


def fmt_date(raw: str) -> str:
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        msk = dt.astimezone(timezone.utc) + timedelta(hours=3)
        return f"{msk.day} {_MONTHS_RU[msk.month - 1]}, {msk.strftime('%H:%M')}"
    except Exception:
        return raw


def parse_dt(raw: str) -> datetime:
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def count_words(text: str) -> int:
    return len(text.split()) if text else 0


def safe_filename(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', '_', name).strip("_. ")


# ══════════════════════════════════════════════════════════════════════════════
#  STEALTH
# ══════════════════════════════════════════════════════════════════════════════
def apply_stealth(page: Page) -> None:
    if HAS_STEALTH and _stealth_instance is not None:
        _stealth_instance.apply_stealth_sync(page)
    else:
        page.add_init_script(_STEALTH_JS)


# ══════════════════════════════════════════════════════════════════════════════
#  BROWSER HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def human_scroll(page: Page, scrolls: int = 3) -> None:
    for _ in range(scrolls):
        page.mouse.wheel(0, random.randint(200, 500))
        time.sleep(random.uniform(0.3, 0.8))


def wait_for_challenge(page: Page, timeout_s: int = CHALLENGE_TIMEOUT_S) -> bool:
    deadline = time.time() + timeout_s
    warned = False
    while time.time() < deadline:
        try:
            title = page.title()
            url   = page.url
        except Exception:
            time.sleep(2)
            continue
        blocked = any(s in title for s in ["Что-то не так", "403", "Access Denied", "Just a moment"])
        if not blocked and "wildberries.ru" in url:
            return True
        if not warned:
            log.warning(f"  [challenge] DDoS-Guard ({title!r}) — ждём...")
            warned = True
        time.sleep(3)
    return False


def safe_goto(page: Page, url: str, timeout: int = 60_000) -> bool:
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        rand_sleep(1.5, 3.0)
        human_scroll(page)
        return wait_for_challenge(page)
    except Exception as e:
        log.error(f"  [goto] {e!r}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  ИНФО О ТОВАРЕ
# ══════════════════════════════════════════════════════════════════════════════
def get_product_info(context: BrowserContext, nm_id: int) -> dict:
    url = f"https://card.wb.ru/cards/v4/detail?appType=1&curr=rub&dest=-1257786&nm={nm_id}"
    try:
        resp = context.request.get(url, timeout=20_000)
        data = resp.json()
    except Exception as e:
        log.error(f"  [product_info] {e!r}")
        return {"brand": "", "name": "", "root": nm_id}

    products = (
        (data.get("data") or {}).get("products")
        or data.get("products")
        or []
    )
    p = products[0] if products else {}
    return {
        "brand": p.get("brand", ""),
        "name":  p.get("name",  ""),
        "root":  int(p.get("root", 0) or nm_id),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  СБОР ВОПРОСОВ — ПАГИНАЦИЯ
# ══════════════════════════════════════════════════════════════════════════════
def fetch_all_questions(context: BrowserContext, nm_id: int,
                         cutoff: datetime) -> list[dict]:
    """
    Собирает вопросы через questions.wb.ru API (браузерная сессия).
    Пагинация skip/take, стоп по дате.
    """
    url = "https://questions.wb.ru/api/v1/questions"
    all_raw: list[dict] = []
    skip = 0
    stop_flag = False

    while not stop_flag:
        try:
            resp = context.request.get(
                url,
                params={"nmId": nm_id, "take": TAKE_PER_REQ, "skip": skip, "order": "dateDesc"},
                headers={
                    "Referer":        f"https://www.wildberries.ru/catalog/{nm_id}/questions",
                    "Origin":         "https://www.wildberries.ru",
                    "sec-fetch-site": "cross-site",
                },
                timeout=30_000,
            )
        except Exception as e:
            log.error(f"  [questions] ошибка на skip={skip}: {e!r}")
            break

        if resp.status == 404:
            log.warning(f"  [questions] 404 — вопросов нет для nmId={nm_id}")
            break
        if resp.status != 200:
            log.warning(f"  [questions] HTTP {resp.status} на skip={skip}")
            break

        try:
            data = resp.json()
        except Exception:
            log.error(f"  [questions] Не JSON на skip={skip}")
            break

        batch = data.get("questions") or []
        if not batch:
            log.info(f"  [questions] пустой батч на skip={skip} — конец")
            break

        kept = 0
        for q in batch:
            dt = parse_dt(q.get("createdDate", ""))
            if dt < cutoff:
                stop_flag = True
                break
            all_raw.append(q)
            kept += 1

        oldest = fmt_date(batch[-1].get("createdDate", ""))
        log.info(f"  [questions] skip={skip} | batch={len(batch)} | kept={kept} | oldest={oldest}")

        if len(batch) < TAKE_PER_REQ:
            break
        skip += len(batch)
        rand_sleep(0.5, 1.5)

    return all_raw


# ══════════════════════════════════════════════════════════════════════════════
#  НОРМАЛИЗАЦИЯ ВОПРОСА
# ══════════════════════════════════════════════════════════════════════════════
def normalize_question(q: dict) -> dict:
    question_text = (q.get("text") or "").strip()
    answer_obj    = q.get("answer") or {}
    answer_text   = (answer_obj.get("text") or "").strip()
    answer_date   = fmt_date(answer_obj.get("createdDate", "")) if answer_text else ""

    return {
        "date":         fmt_date(q.get("createdDate", "")),
        "date_raw":     q.get("createdDate", ""),
        "question":     question_text,
        "answer":       answer_text,
        "answer_date":  answer_date,
        "has_answer":   bool(answer_text),
        "word_count":   count_words(question_text),
    }


def question_passes_filter(q: dict) -> bool:
    """Убираем только пустые и односимвольные вопросы (мусор)."""
    return q.get("word_count", 0) >= MIN_WORDS_QUESTION


# ══════════════════════════════════════════════════════════════════════════════
#  СТАТИСТИКА ВОПРОСОВ
# ══════════════════════════════════════════════════════════════════════════════
def build_questions_stats(raw: list[dict], filtered: list[dict]) -> dict:
    answered   = sum(1 for q in filtered if q.get("has_answer"))
    unanswered = len(filtered) - answered

    def pct(n, base):
        return round(n / base * 100, 1) if base > 0 else 0.0

    return {
        "total_parsed":  len(raw),
        "after_filter":  len(filtered),
        "answered":      answered,
        "unanswered":    unanswered,
        "answered_pct":  pct(answered,   len(filtered)),
        "unanswered_pct":pct(unanswered, len(filtered)),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  ЭКСПОРТ — TXT (для LLM)
# ══════════════════════════════════════════════════════════════════════════════
def to_txt_llm(product_id: str, brand: str, name: str,
               questions: list[dict], stats: dict) -> str:
    SEP = "═" * 70
    lines = [
        SEP,
        f"АРТИКУЛ: {product_id}  |  БРЕНД: {brand}",
        f"ТОВАР: {name}",
        SEP,
        "",
        f"ВОПРОСОВ: {stats.get('after_filter')}  |  "
        f"С ответом: {stats.get('answered')} ({stats.get('answered_pct')}%)  |  "
        f"Без ответа: {stats.get('unanswered')} ({stats.get('unanswered_pct')}%)",
        "",
        "── ВОПРОСЫ ──────────────────────────────────────────────────────────",
        "",
    ]

    for i, q in enumerate(questions, 1):
        lines += [
            f"--- ВОПРОС #{i} ---",
            f"<meta date='{q.get('date')}' words='{q.get('word_count')}' answered='{q.get('has_answer')}'>",
            f"<question>{q.get('question')}</question>",
        ]
        if q.get("answer"):
            lines.append(f"<answer date='{q.get('answer_date')}'>{q['answer']}</answer>")
        else:
            lines.append("<answer>БЕЗ ОТВЕТА</answer>")
        lines.append("")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  ЭКСПОРТ — CSV (для Excel)
# ══════════════════════════════════════════════════════════════════════════════
Q_CSV_FIELDS = [
    "product_id", "brand", "name",
    "total_parsed", "after_filter", "answered_pct",
    "date", "word_count", "has_answer",
    "question", "answer", "answer_date",
]


def questions_to_csv_rows(product_id: str, brand: str, name: str,
                            questions: list[dict], stats: dict) -> list[dict]:
    rows = []
    for q in questions:
        def clean(t): return re.sub(r"[\r\n]+", " ", t or "").strip()
        rows.append({
            "product_id":    product_id,
            "brand":         brand,
            "name":          name,
            "total_parsed":  stats.get("total_parsed", ""),
            "after_filter":  stats.get("after_filter", ""),
            "answered_pct":  stats.get("answered_pct", ""),
            "date":          q.get("date", ""),
            "word_count":    q.get("word_count", ""),
            "has_answer":    q.get("has_answer", ""),
            "question":      clean(q.get("question", "")),
            "answer":        clean(q.get("answer", "")),
            "answer_date":   q.get("answer_date", ""),
        })
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=Q_CSV_FIELDS, extrasaction="ignore", delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


# ══════════════════════════════════════════════════════════════════════════════
#  СОХРАНЕНИЕ
# ══════════════════════════════════════════════════════════════════════════════
def save_to_dir(out_dir: Path,
                product_id: str, brand: str, name: str,
                questions: list[dict], stats: dict) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "product_id": product_id, "brand": brand, "name": name,
        "stats": stats, "questions": questions,
    }
    (out_dir / "PRO_questions_LLM.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    txt = to_txt_llm(product_id, brand, name, questions, stats)
    (out_dir / "PRO_questions_LLM.txt").write_text(txt, encoding="utf-8")

    rows = questions_to_csv_rows(product_id, brand, name, questions, stats)
    write_csv(out_dir / "PRO_questions_Analytics.csv", rows)

    log.info(f"  [save] {out_dir.name}/ — {len(questions)} вопросов (3 формата)")


# ══════════════════════════════════════════════════════════════════════════════
#  ЗАГРУЗКА АРТИКУЛОВ
# ══════════════════════════════════════════════════════════════════════════════
def load_articles(filepath: str) -> list[int]:
    path = Path(filepath)
    if not path.exists():
        log.error(f"'{filepath}' не найден")
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

    all_csv_rows:   list[dict] = []
    all_json_prods: list[dict] = []
    all_txt_blocks: list[str]  = []

    with sync_playwright() as p:
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
                "Accept-Language":    "ru-RU,ru;q=0.9",
                "sec-ch-ua":          '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                "sec-ch-ua-mobile":   "?0",
                "sec-ch-ua-platform": '"Windows"',
            },
        )

        page = context.new_page()
        apply_stealth(page)

        log.info("Открываем wildberries.ru...")
        if not safe_goto(page, "https://www.wildberries.ru"):
            log.error("WB не загрузился")
            context.close()
            return
        rand_sleep(*DELAY_BETWEEN)

        for idx, nm_id in enumerate(articles, 1):
            log.info("─" * 60)
            log.info(f"[{idx}/{len(articles)}]  nmId={nm_id}")
            log.info("─" * 60)

            if idx > 1 and (idx - 1) % COOLDOWN_EVERY_N == 0:
                dur = random.uniform(*DELAY_COOLDOWN)
                log.info(f"  ☕ Cooldown {dur:.0f}с...")
                time.sleep(dur)

            info = get_product_info(context, nm_id)
            log.info(f"  Товар: {info['brand']!r} — {info['name']!r}")

            # Навигация на страницу вопросов
            q_url = f"https://www.wildberries.ru/catalog/{nm_id}/questions"
            if not safe_goto(page, q_url):
                log.warning(f"  Пропускаем {nm_id} — страница не прошла проверку")
                continue
            rand_sleep(*DELAY_PAGE)

            # Сбор вопросов
            raw_questions = fetch_all_questions(context, nm_id, cutoff)
            log.info(f"  Сырых вопросов за год: {len(raw_questions)}")

            # Нормализация и фильтрация
            all_norm = [normalize_question(q) for q in raw_questions]
            filtered  = [q for q in all_norm if question_passes_filter(q)]
            log.info(f"  После фильтра: {len(filtered)} / {len(all_norm)}")

            stats = build_questions_stats(all_norm, filtered)
            log.info(f"  Статистика: {stats}")

            # Сохранение
            save_to_dir(
                RESULTS_DIR / str(nm_id),
                str(nm_id), info["brand"], info["name"],
                filtered, stats,
            )

            # Накопление
            all_csv_rows.extend(
                questions_to_csv_rows(str(nm_id), info["brand"], info["name"], filtered, stats)
            )
            all_json_prods.append({
                "product_id": str(nm_id), "brand": info["brand"],
                "name": info["name"], "stats": stats, "questions": filtered,
            })
            all_txt_blocks.append(
                to_txt_llm(str(nm_id), info["brand"], info["name"], filtered, stats)
            )

            rand_sleep(*DELAY_BETWEEN)

        context.close()

    # Сводные файлы
    log.info("Сохраняем сводные файлы...")
    divider = "\n\n" + "═" * 70 + "\n\n"

    write_csv(RESULTS_DIR / "ALL_questions_Analytics.csv", all_csv_rows)
    (RESULTS_DIR / "ALL_questions_LLM.json").write_text(
        json.dumps(all_json_prods, ensure_ascii=False, indent=2), encoding="utf-8")
    (RESULTS_DIR / "ALL_questions_LLM.txt").write_text(
        divider.join(all_txt_blocks), encoding="utf-8")

    total_q = sum(len(p["questions"]) for p in all_json_prods)
    log.info("=" * 60)
    log.info(f"  Товаров: {len(all_json_prods)}  |  Вопросов (после фильтра): {total_q}")
    log.info(f"  Сводные: ALL_questions_Analytics.csv  |  ALL_questions_LLM.json  |  ALL_questions_LLM.txt")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
