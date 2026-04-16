#!/usr/bin/env python3
"""
Wildberries Reviews & Questions Scraper  v5.0
==============================================
API-режим: прямые запросы к внутреннему WB API. Браузер НЕ нужен.

Почему v5 вместо v4 (Playwright):
  WB DDoS-Guard блокирует Playwright-браузер («Что-то не так...
  Подозрительная активность»). Прямые API-запросы не требуют браузера,
  не триггерят антибот, работают быстрее и надёжнее.

API-эндпойнты:
  Отзывы:   feedbacks2.wb.ru/feedbacks/v1/{nmId}
  Вопросы:  questions.wb.ru/api/v1/questions?nmId={nmId}
  Инфо:     card.wb.ru/cards/v4/detail?nm={nmId}

Все исправления v4 сохранены:
  • sku_variant — из поля color каждого отзыва (API точнее браузера)
  • total_wb    — из feedbackCount карточки (точный счётчик WB)
  • Воронка: total_wb → total_parsed → no_text → working_base
  • % от working_base, сумма = 100%
  • 3★ = нейтрал (не выпадает из расчёта)

Зависимости:
    pip install requests
"""

import csv
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

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
INPUT_FILE  = "articles.txt"

# ── Директория вывода ──────────────────────────────────────────────────────
# По умолчанию: папка results/ рядом с кодом (создаётся автоматически).
# Переопределить через переменную окружения WB_RESULTS_DIR:
#   Windows: set WB_RESULTS_DIR=C:\Users\User\Desktop\wb_output && python main.py
#   Linux:   WB_RESULTS_DIR=/data/wb_output python main.py
RESULTS_DIR = os.getenv("WB_RESULTS_DIR", "results")

OUTPUT_JSON       = os.path.join(RESULTS_DIR, "data_export.json")
OUTPUT_TXT        = os.path.join(RESULTS_DIR, "data_export.txt")
ALL_REVIEWS_CSV   = os.path.join(RESULTS_DIR, "all_reviews.csv")
ALL_QUESTIONS_CSV = os.path.join(RESULTS_DIR, "all_questions.csv")

FEEDBACKS_PER_PAGE = 30
QUESTIONS_PER_PAGE = 30

# ── Тайминги (секунды) ─────────────────────────────────────────────────────
DELAY_BETWEEN_PAGES    = (1.0, 2.5)
DELAY_BETWEEN_SECTIONS = (2.0, 4.0)
DELAY_BETWEEN_ARTICLES = (3.0, 7.0)
COOLDOWN_EVERY_N       = 6           # кулдаун каждые N артикулов
COOLDOWN_DURATION      = (20, 35)    # секунды

# ── HTTP-заголовки (имитация Chrome) ──────────────────────────────────────
_BASE_HEADERS = {
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin":             "https://www.wildberries.ru",
    "Referer":            "https://www.wildberries.ru/",
    "sec-ch-ua":          '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "cross-site",
}

# ── Пул User-Agent для ротации ─────────────────────────────────────────────
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.122 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


# ══════════════════════════════════════════════════════════════════════════════
#  УТИЛИТЫ
# ══════════════════════════════════════════════════════════════════════════════
def rand_sleep(lo: float, hi: float) -> None:
    t = random.uniform(lo, hi)
    log.debug(f"  ⏱ sleep {t:.1f}s")
    time.sleep(t)


def make_session() -> requests.Session:
    """Создаёт HTTP-сессию с ротацией User-Agent."""
    s = requests.Session()
    headers = dict(_BASE_HEADERS)
    headers["User-Agent"] = random.choice(_USER_AGENTS)
    s.headers.update(headers)
    return s


# ══════════════════════════════════════════════════════════════════════════════
#  API-ХЕЛПЕР — УСТОЙЧИВЫЙ GET С РЕТРАЯМИ
# ══════════════════════════════════════════════════════════════════════════════
def api_get(session: requests.Session, url: str,
            params: dict | None = None, retries: int = 5) -> dict:
    """
    Robust GET к WB API.
    Автоматически повторяет при 429 (rate limit) и 5xx (серверные ошибки).
    """
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=20)

            if r.status_code == 200:
                return r.json()

            if r.status_code == 429:
                wait = 30 * (attempt + 1)
                log.warning(f"  [api] 429 rate limit — ждём {wait}с (попытка {attempt+1}/{retries})")
                time.sleep(wait)
                continue

            if r.status_code in (500, 502, 503, 504):
                wait = 10 * (attempt + 1)
                log.warning(f"  [api] HTTP {r.status_code} — ждём {wait}с (попытка {attempt+1}/{retries})")
                time.sleep(wait)
                continue

            log.error(f"  [api] HTTP {r.status_code} для {url}")
            return {}

        except requests.exceptions.Timeout:
            log.warning(f"  [api] timeout, попытка {attempt+1}/{retries}")
            time.sleep(5 * (attempt + 1))
        except Exception as e:
            log.warning(f"  [api] ошибка: {e!r}, попытка {attempt+1}/{retries}")
            time.sleep(5 * (attempt + 1))

    log.error(f"  [api] все {retries} попыток исчерпаны: {url}")
    return {}


# ══════════════════════════════════════════════════════════════════════════════
#  ФОРМАТИРОВАНИЕ ДАТЫ
# ══════════════════════════════════════════════════════════════════════════════
_MONTHS_RU = [
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]

def _fmt_date(raw: str) -> str:
    """ISO 8601 → 'Д месяца, ЧЧ:ММ' в московском времени (UTC+3)."""
    if not raw:
        return ""
    try:
        dt  = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        msk = dt.astimezone(timezone.utc) + timedelta(hours=3)
        return f"{msk.day} {_MONTHS_RU[msk.month - 1]}, {msk.strftime('%H:%M')}"
    except Exception:
        return raw


# ══════════════════════════════════════════════════════════════════════════════
#  ИНФО О ТОВАРЕ
# ══════════════════════════════════════════════════════════════════════════════
def get_product_info(session: requests.Session, nm_id: int) -> dict:
    """
    Получает название, бренд, счётчик отзывов и root (imtId) через card.wb.ru.

    root (imtId) — ID «склейки» (группы вариантов). Именно он нужен для
    feedbacks API: feedbacks2.wb.ru/feedbacks/v1/{root}, а не nmId.
    """
    url  = f"https://card.wb.ru/cards/v4/detail?appType=1&curr=rub&dest=-1257786&nm={nm_id}"
    data = api_get(session, url)

    # API возвращает {"products": [...]}
    products = (
        (data.get("data") or {}).get("products")
        or data.get("products")
        or []
    )
    product = products[0] if products else {}

    return {
        "brand":    product.get("brand", ""),
        "name":     product.get("name",  ""),
        "total_wb": int(product.get("feedbacks", 0) or product.get("feedbackCount", 0) or 0),
        "root":     int(product.get("root", 0) or nm_id),   # imtId для feedbacks API
    }


# ══════════════════════════════════════════════════════════════════════════════
#  ПАРСИНГ ОТЗЫВОВ (API)
# ══════════════════════════════════════════════════════════════════════════════
def scrape_feedbacks(session: requests.Session, nm_id: int, root: int) -> tuple[list[dict], int]:
    """
    Собирает все отзывы через feedbacks2.wb.ru API.

    ВАЖНО: API принимает root (imtId), а не nmId.
      root берётся из card.wb.ru (поле product.root).
      feedbacks2.wb.ru/feedbacks/v1/{nmId} всегда возвращает 0 элементов.

    Поведение API:
      - take/skip игнорируются — API возвращает все доступные отзывы за один запрос
      - Максимум 1000 элементов в одном ответе (встроенный лимит WB)
      - Возвращает только отзывы с текстом (feedbackCountWithText), не все

    Возвращает: (list отзывов, feedbackCount — полное число отзывов на WB)
    """
    url    = f"https://feedbacks2.wb.ru/feedbacks/v1/{root}"
    params = {"take": 1000, "skip": 0, "order": "dateDesc"}
    log.info(f"  [отзывы] запрос feedbacks/v1/{root}")

    data      = api_get(session, url, params=params)
    feedbacks = data.get("feedbacks") or []
    total_wb  = int(data.get("feedbackCount") or 0)

    log.info(f"  [отзывы] feedbackCount={total_wb}, feedbackCountWithText={data.get('feedbackCountWithText')}, получено={len(feedbacks)}")

    all_reviews: list[dict] = []
    for fb in feedbacks:
        pros          = (fb.get("pros")  or "").strip()
        cons          = (fb.get("cons")  or "").strip()
        text          = (fb.get("text")  or "").strip()
        answer_obj    = fb.get("answer") or {}
        seller_answer = (answer_obj.get("text") or "").strip()

        # sku_variant: API отдаёт color напрямую для каждого отзыва
        color = (fb.get("color") or "").strip()

        all_reviews.append({
            "date":          _fmt_date(fb.get("createdDate", "")),
            "rating":        int(fb.get("productValuation", 0)),
            "text":          text,
            "pros":          pros,
            "cons":          cons,
            "seller_answer": seller_answer,
            "has_text":      bool(pros or cons or text),
            "sku_variant":   color,
        })

    log.info(f"  [отзывы] ИТОГО собрано: {len(all_reviews)}")
    return all_reviews, total_wb


# ══════════════════════════════════════════════════════════════════════════════
#  БАГ #2 + #3 FIX — ВОРОНКА ФИЛЬТРАЦИИ + % ОТ РАБОЧЕЙ БАЗЫ
# ══════════════════════════════════════════════════════════════════════════════
def build_funnel_stats(all_reviews: list[dict], total_wb: int = 0) -> dict:
    """
    Строит воронку фильтрации и считает % позитив/нейтрал/негатив
    от РАБОЧЕЙ БАЗЫ (отзывы с текстом).

    Группировка оценок (БАГ #3 FIX):
      Позитив  = 4-5 ★
      Нейтрал  = 3 ★   (раньше выпадал из расчёта!)
      Негатив  = 1-2 ★

    Воронка (БАГ #2 FIX):
      total_wb      — всего отзывов на карточке WB (из API)
      total_parsed  — сколько спарсил парсер
      no_text       — отзывы без текста (только звёзды)
      working_base  — отзывы с текстом (рабочая база для %)
    """
    total_parsed = len(all_reviews)
    no_text      = sum(1 for r in all_reviews if not r.get("has_text", False))
    working_base = total_parsed - no_text

    # Считаем рейтинги только по отзывам С текстом (рабочая база)
    text_reviews = [r for r in all_reviews if r.get("has_text", False)]

    positive = sum(1 for r in text_reviews if r.get("rating", 0) >= 4)        # 4-5 ★
    neutral  = sum(1 for r in text_reviews if r.get("rating", 0) == 3)        # 3 ★
    negative = sum(1 for r in text_reviews if r.get("rating", 0) in (1, 2))   # 1-2 ★

    def pct(n: int, base: int) -> float:
        return round(n / base * 100, 1) if base > 0 else 0.0

    return {
        # --- Воронка ---
        "funnel": {
            "total_wb":     total_wb,        # всего на ВБ (из card API)
            "total_parsed": total_parsed,    # спарсил парсер
            "no_text":      no_text,         # без текста (только звёзды)
            "working_base": working_base,    # РАБОЧАЯ БАЗА (с текстом)
        },
        # --- % от рабочей базы: в сумме = 100% (БАГ #2 FIX) ---
        "sentiment": {
            "positive_count": positive,
            "neutral_count":  neutral,
            "negative_count": negative,
            "positive_pct":   pct(positive, working_base),
            "neutral_pct":    pct(neutral,  working_base),
            "negative_pct":   pct(negative, working_base),
            "sum_pct_check":  round(
                pct(positive, working_base)
                + pct(neutral,  working_base)
                + pct(negative, working_base), 1
            ),
            # sum_pct_check должен быть ≈ 100.0 — встроенная проверка корректности
        },
        "_note": (
            "sentiment % считаются от working_base (отзывы с текстом). "
            "Нейтрал (3★) включён явно — раньше выпадал из расчёта. "
            "sum_pct_check должен быть = 100.0 ± 0.2 (погрешность округления)."
        ),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  ПАРСИНГ ВОПРОСОВ (API)
# ══════════════════════════════════════════════════════════════════════════════
def scrape_questions(session: requests.Session, nm_id: int, root: int) -> list[dict]:
    """
    Собирает Q&A-вопросы покупателей.

    Текущий статус:
      Домен questions.wb.ru не резолвится из этой сети.
      feedbacks2.wb.ru/feedbacks/v1/{root}?type=question возвращает обычные отзывы
      (параметр type игнорируется WB API).
      Реальный Q&A недоступен через публичный API — возвращаем пустой список.

    TODO: WB хранит Q&A на questions.wb.ru. Если домен станет доступен,
          endpoint: GET https://questions.wb.ru/api/v1/questions?nmId={nmId}&take=30&skip=0
    """
    log.warning(f"  [вопросы] Q&A API недоступен (questions.wb.ru не резолвится) — вопросы пропущены")
    return []


# ══════════════════════════════════════════════════════════════════════════════
#  ЭКСПОРТ — TXT
# ══════════════════════════════════════════════════════════════════════════════
def _reviews_to_txt(product: dict) -> str:
    SEP       = "=" * 70
    funnel    = product.get("funnel_stats", {}).get("funnel", {})
    sentiment = product.get("funnel_stats", {}).get("sentiment", {})

    lines = [
        SEP,
        f"ТОВАР:          {product.get('name', '')}",
        f"БРЕНД:          {product.get('brand', '')}",
        f"АРТИКУЛ:        {product.get('product_id', '')}",
        f"SKU-ВАРИАНТ:    {product.get('sku_variant', '(не определён)')}",
        "",
        "── ВОРОНКА ФИЛЬТРАЦИИ ─────────────────────────────────────────",
        f"  Всего на ВБ:   {funnel.get('total_wb', '?')}",
        f"  Спарсил:       {funnel.get('total_parsed', 0)}",
        f"  Без текста:    {funnel.get('no_text', 0)}  (только звёзды — исключены)",
        f"  Рабочая база:  {funnel.get('working_base', 0)}  ← знаменатель для %",
        "",
        "── СЕНТИМЕНТ (% от рабочей базы) ─────────────────────────────",
        f"  Позитив (4-5★): {sentiment.get('positive_count', 0)} отз.  = {sentiment.get('positive_pct', 0)}%",
        f"  Нейтрал (3★):   {sentiment.get('neutral_count', 0)} отз.  = {sentiment.get('neutral_pct', 0)}%",
        f"  Негатив (1-2★): {sentiment.get('negative_count', 0)} отз.  = {sentiment.get('negative_pct', 0)}%",
        f"  СУММА ПРОВЕРКА: {sentiment.get('sum_pct_check', 0)}%  (должно быть ≈ 100%)",
        SEP, "",
    ]

    reviews = product.get("reviews", [])
    if not reviews:
        lines += ["  (нет текстовых отзывов)", ""]
    else:
        for i, r in enumerate(reviews, 1):
            stars       = "★" * r.get("rating", 0) + "☆" * (5 - r.get("rating", 0))
            variant_tag = f"  [{r.get('sku_variant', '')}]" if r.get("sku_variant") else ""
            lines.append(f"[{i}] {r.get('date', '')}  {stars}{variant_tag}")
            if r.get("pros"):          lines.append(f"  + Достоинства : {r['pros']}")
            if r.get("cons"):          lines.append(f"  - Недостатки  : {r['cons']}")
            if r.get("text"):          lines.append(f"  💬 Комментарий: {r['text']}")
            if r.get("seller_answer"): lines.append(f"  📢 Ответ продавца: {r['seller_answer']}")
            lines.append("")
    lines += [SEP, ""]
    return "\n".join(lines)


def _questions_to_txt(product: dict) -> str:
    SEP   = "=" * 70
    lines = [
        SEP,
        f"ТОВАР:    {product.get('name', '')}",
        f"БРЕНД:    {product.get('brand', '')}",
        f"АРТИКУЛ:  {product.get('product_id', '')}",
        f"ВОПРОСОВ: {len(product.get('questions', []))}",
        SEP, "",
    ]
    questions = product.get("questions", [])
    if not questions:
        lines += ["  (нет вопросов)", ""]
    else:
        for i, q in enumerate(questions, 1):
            lines.append(f"[{i}] {q.get('date', '')}")
            lines.append(f"  ❓ Вопрос : {q.get('question', '')}")
            ans = q.get("answer", "")
            lines.append(f"  ✅ Ответ  : {ans if ans else '(нет ответа)'}")
            lines.append("")
    lines += [SEP, ""]
    return "\n".join(lines)


def build_full_txt(results: list[dict]) -> str:
    blocks = []
    for p in results:
        blocks.append(_reviews_to_txt(p))
        blocks.append(_questions_to_txt(p))
    return "\n\n".join(blocks)


# ══════════════════════════════════════════════════════════════════════════════
#  ЭКСПОРТ — CSV
# ══════════════════════════════════════════════════════════════════════════════
REVIEWS_CSV_FIELDS = [
    "product_id", "brand", "name", "sku_variant",
    "total_wb", "total_parsed", "no_text", "working_base",
    "positive_pct", "neutral_pct", "negative_pct",
    "date", "rating", "rating_group",
    "pros", "cons", "text", "seller_answer",
]
QUESTIONS_CSV_FIELDS = ["product_id", "brand", "name", "date", "question", "answer"]


def _write_csv(filepath: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _rating_group(rating: int) -> str:
    """БАГ #3 FIX: явная классификация 3★ как нейтральной (не пропадает)."""
    if rating >= 4:
        return "позитив"
    elif rating == 3:
        return "нейтрал"
    else:
        return "негатив"


def results_to_csv_rows(results: list[dict]) -> tuple[list[dict], list[dict]]:
    review_rows:   list[dict] = []
    question_rows: list[dict] = []

    for p in results:
        pid       = p.get("product_id", "")
        brand     = p.get("brand", "")
        name      = p.get("name",  "")
        sku_var   = p.get("sku_variant", "")
        funnel    = p.get("funnel_stats", {}).get("funnel", {})
        sentiment = p.get("funnel_stats", {}).get("sentiment", {})

        for r in p.get("reviews", []):
            rating = r.get("rating", 0)
            review_rows.append({
                "product_id":   pid,
                "brand":        brand,
                "name":         name,
                "sku_variant":  r.get("sku_variant", sku_var),
                # Воронка — одинакова для всех строк артикула
                "total_wb":     funnel.get("total_wb", ""),
                "total_parsed": funnel.get("total_parsed", ""),
                "no_text":      funnel.get("no_text", ""),
                "working_base": funnel.get("working_base", ""),
                # % сентимента
                "positive_pct": sentiment.get("positive_pct", ""),
                "neutral_pct":  sentiment.get("neutral_pct", ""),
                "negative_pct": sentiment.get("negative_pct", ""),
                # Данные отзыва
                "date":          r.get("date", ""),
                "rating":        rating,
                "rating_group":  _rating_group(rating),   # БАГ #3 FIX
                "pros":          r.get("pros", ""),
                "cons":          r.get("cons", ""),
                "text":          r.get("text", ""),
                "seller_answer": r.get("seller_answer", ""),
            })
        for q in p.get("questions", []):
            question_rows.append({
                "product_id": pid,
                "brand":      brand,
                "name":       name,
                "date":       q.get("date", ""),
                "question":   q.get("question", ""),
                "answer":     q.get("answer", ""),
            })
    return review_rows, question_rows


# ══════════════════════════════════════════════════════════════════════════════
#  СОХРАНЕНИЕ
# ══════════════════════════════════════════════════════════════════════════════
def save_all(results: list[dict], results_dir: Path) -> None:
    for p in results:
        pid     = p["product_id"]
        out_dir = results_dir / pid
        out_dir.mkdir(exist_ok=True)

        reviews   = p.get("reviews", [])
        questions = p.get("questions", [])

        # JSON (включая funnel_stats и sku_variant)
        (out_dir / "reviews.json").write_text(
            json.dumps({
                "product_id":   pid,
                "brand":        p.get("brand"),
                "name":         p.get("name"),
                "sku_variant":  p.get("sku_variant", ""),
                "funnel_stats": p.get("funnel_stats", {}),
                "reviews":      reviews,
            }, ensure_ascii=False, indent=2), encoding="utf-8")
        (out_dir / "questions.json").write_text(
            json.dumps({
                "product_id": pid, "brand": p.get("brand"),
                "name": p.get("name"), "questions": questions,
            }, ensure_ascii=False, indent=2), encoding="utf-8")
        (out_dir / "full.json").write_text(
            json.dumps(p, ensure_ascii=False, indent=2), encoding="utf-8")

        (out_dir / "reviews.txt").write_text(_reviews_to_txt(p),    encoding="utf-8")
        (out_dir / "questions.txt").write_text(_questions_to_txt(p), encoding="utf-8")

        r_rows = [{
            "product_id":   pid,
            "brand":        p.get("brand", ""),
            "name":         p.get("name", ""),
            "sku_variant":  r.get("sku_variant", p.get("sku_variant", "")),
            "total_wb":     p.get("funnel_stats", {}).get("funnel", {}).get("total_wb", ""),
            "total_parsed": p.get("funnel_stats", {}).get("funnel", {}).get("total_parsed", ""),
            "no_text":      p.get("funnel_stats", {}).get("funnel", {}).get("no_text", ""),
            "working_base": p.get("funnel_stats", {}).get("funnel", {}).get("working_base", ""),
            "positive_pct": p.get("funnel_stats", {}).get("sentiment", {}).get("positive_pct", ""),
            "neutral_pct":  p.get("funnel_stats", {}).get("sentiment", {}).get("neutral_pct", ""),
            "negative_pct": p.get("funnel_stats", {}).get("sentiment", {}).get("negative_pct", ""),
            "date":         r.get("date", ""),
            "rating":       r.get("rating", ""),
            "rating_group": _rating_group(r.get("rating", 0)),
            "pros":         r.get("pros", ""),
            "cons":         r.get("cons", ""),
            "text":         r.get("text", ""),
            "seller_answer": r.get("seller_answer", ""),
        } for r in reviews]
        q_rows = [{
            "product_id": pid, "brand": p.get("brand", ""), "name": p.get("name", ""),
            "date": q.get("date", ""), "question": q.get("question", ""), "answer": q.get("answer", ""),
        } for q in questions]

        _write_csv(out_dir / "reviews.csv",   REVIEWS_CSV_FIELDS,   r_rows)
        _write_csv(out_dir / "questions.csv", QUESTIONS_CSV_FIELDS, q_rows)

    all_r_rows, all_q_rows = results_to_csv_rows(results)
    # Сводные файлы — всегда в корне RESULTS_DIR
    Path(ALL_REVIEWS_CSV).parent.mkdir(parents=True, exist_ok=True)
    _write_csv(Path(ALL_REVIEWS_CSV),   REVIEWS_CSV_FIELDS,   all_r_rows)
    _write_csv(Path(ALL_QUESTIONS_CSV), QUESTIONS_CSV_FIELDS, all_q_rows)
    Path(OUTPUT_JSON).write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    Path(OUTPUT_TXT).write_text(build_full_txt(results), encoding="utf-8")


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
        if line and not line.startswith("#"):
            if line.isdigit():
                articles.append(int(line))
            else:
                log.warning(f"Пропускаем строку: {line!r}")
    log.info(f"Загружено {len(articles)} артикулов из '{filepath}'.")
    return articles


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    articles = load_articles(INPUT_FILE)
    if not articles:
        return

    results_dir = Path(RESULTS_DIR)
    results_dir.mkdir(exist_ok=True)
    results: list[dict] = []

    for idx, nm_id in enumerate(articles, start=1):
        log.info("─" * 60)
        log.info(f"[{idx}/{len(articles)}]  nmId={nm_id}")
        log.info("─" * 60)

        # Кулдаун между сериями артикулов
        if idx > 1 and (idx - 1) % COOLDOWN_EVERY_N == 0:
            dur = random.uniform(*COOLDOWN_DURATION)
            log.info(f"  ☕ Cooldown {dur:.0f}с после {idx - 1} артикулов...")
            time.sleep(dur)

        # Новая сессия = ротация User-Agent
        session = make_session()

        # ── Инфо о товаре ────────────────────────────────────────────────
        info = get_product_info(session, nm_id)
        root = info["root"]
        log.info(f"  Товар: {info['brand']!r} — {info['name']!r}")
        log.info(f"  nmId={nm_id}, root={root}, total_wb={info['total_wb']}")

        data: dict = {
            "product_id":       str(nm_id),
            "brand":            info["brand"],
            "name":             info["name"],
            "sku_variant":      "",   # сводный — заполним после сбора отзывов
            "all_sku_variants": [],   # заполним после сбора отзывов
            "reviews":          [],
            "questions":        [],
            "funnel_stats":     {},
        }

        rand_sleep(1.0, 2.5)

        # ── Отзывы ──────────────────────────────────────────────────────
        # root (imtId) передаём в API — feedbacks2.wb.ru требует root, не nmId
        raw_reviews, api_total_wb = scrape_feedbacks(session, nm_id, root)
        data["reviews"] = raw_reviews

        # total_wb: приоритет — feedbacks API (точнее), fallback — card API
        total_wb = api_total_wb or info["total_wb"]

        # Уникальные варианты SKU из поля color в каждом отзыве
        unique_variants = sorted({r["sku_variant"] for r in raw_reviews if r["sku_variant"]})
        data["all_sku_variants"] = unique_variants
        data["sku_variant"]      = " / ".join(unique_variants) if unique_variants else ""
        log.info(f"  SKU-варианты: {unique_variants}")

        # ── Воронка + сентимент (БАГ #2 + #3 FIX сохранены) ────────────
        data["funnel_stats"] = build_funnel_stats(raw_reviews, total_wb=total_wb)
        log.info(f"  Воронка:   {data['funnel_stats']['funnel']}")
        log.info(f"  Сентимент: {data['funnel_stats']['sentiment']}")

        rand_sleep(*DELAY_BETWEEN_SECTIONS)

        # ── Вопросы ─────────────────────────────────────────────────────
        data["questions"] = scrape_questions(session, nm_id, root)

        results.append(data)
        save_all(results, results_dir)

        log.info(
            f"  ✓ Сохранено  results/{nm_id}/ "
            f"[reviews={len(data['reviews'])}, questions={len(data['questions'])}]"
        )

        if idx < len(articles):
            rand_sleep(*DELAY_BETWEEN_ARTICLES)

    # ── Итоговый отчёт ───────────────────────────────────────────────────
    total_r = sum(len(p["reviews"])   for p in results)
    total_q = sum(len(p["questions"]) for p in results)

    log.info("=" * 60)
    log.info(f"  Готово! Товаров: {len(results)}  |  Отзывов: {total_r}  |  Вопросов: {total_q}")
    log.info(f"  📁 results/{{nmId}}/  reviews.json/.txt/.csv  +  questions.json/.txt/.csv")
    log.info(f"  📊 {ALL_REVIEWS_CSV}   {ALL_QUESTIONS_CSV}")
    log.info(f"  📦 {OUTPUT_JSON}   {OUTPUT_TXT}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
