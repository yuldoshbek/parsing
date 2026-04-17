# AI Memory — Парсер отзывов WB

> Последнее обновление: 17 апреля 2026 (сессия 4)
> Версия парсера: v5.2 (pro_scraper_reviews.py)
> Агент: Claude Sonnet 4.6

---

## Состояние проекта

### Что это
Парсер отзывов с Wildberries. Два инструмента:
- `pro_scraper_reviews.py` — Playwright + браузер + PRO-фильтр (основной)
- `main.py` — прямые HTTP-запросы без браузера (лёгкий режим)

Запуск: `python pro_scraper_reviews.py` (читает `articles.txt`, пишет в `results_pro_reviews/`)

### GitHub
Репозиторий: https://github.com/yuldoshbek/parsing
Ветка: `main`.

### Структура проекта
| Файл/папка | Роль |
|---|---|
| `pro_scraper_reviews.py` | Основной парсер v5.1 — Playwright + PRO-воронка |
| `main.py` | Лёгкий парсер v5.0 — API-режим без браузера |
| `requirements.txt` | `playwright playwright-stealth requests` |
| `articles.txt` | Список артикулов WB (gitignored — создай свой) |
| `articles.example.txt` | Пример формата articles.txt |
| `run.bat` | Windows-запуск с логом |
| `README.md` | Документация (обновлена под v5.1) |
| `docs/AI_ANALYST_GUIDE.md` | ТЗ для ИИ-аналитика |
| `docs/AUDIT_AND_PLAN.md` | Аудит багов v1-v4 + план |
| `tools/verify_browser.py` | Браузерная проверка всех отзывов Оранжевый (246290927) |
| `tools/verify_browser_fdm.py` | Браузерная проверка всех отзывов FDM (371806956) |
| `tools/convert.py` | Конвертер старых results/*.json |
| `tools/debug_check.py` | Диагностика API-ответов |
| `FILTER_PIPELINE_EXPLAINED.txt` | Полное описание фильтрации с примерами (новый) |
| `DEMO_before_after.txt` | До/после для презентации руководителю |
| `ORANGE_browser_verify.txt` | 55 из 110 Оранжевый — верификация через браузер |
| `FDM_browser_verify.txt` | 18 из 30 FDM — верификация через браузер |

### Разделение кода и данных
Код в `C:\Users\User\Desktop\Отзывы\`.
Данные (`results_pro_reviews/`, `results_test*/`) — gitignored, не коммитятся.

---

## История исправлений

### v5.2 — Исправлена логика PRO-фильтра (17.04.2026)

**Файл:** `pro_scraper_reviews.py` — функции `pro_filter()` и `filter_reason()`

**Проблема:** Старый фильтр резал короткие позитивные отзывы (4-5★) сразу по длине — любой отзыв < 17 слов уходил в мусор. Это неправильно.

**Правильная логика (по указанию руководителя):**
Удаляем 4-5★ ТОЛЬКО если ОДНОВРЕМЕННО три условия:
- длина < 17 слов
- есть стоп-фраза
- нет слов-брейкеров

Во всех остальных случаях — сохраняем:
- длина ≥ 17 слов → СОХРАНИТЬ (без проверки стоп-фраз)
- нет стоп-фразы → СОХРАНИТЬ (даже если короткий)
- есть брейкер → СОХРАНИТЬ (даже если короткий + стоп-фраза)

**Результат на тестовом артикуле 246290927 (Оранжевый):**
- Было: 24 отзывов с текстом → 8 прошло (старый фильтр)
- Стало: 24 отзывов с текстом → 21 прошло (новый фильтр)
- Отсеяно только 3 — реально пустые: «все супер», «все отлично», «упаковано хорошо»

**Изменение в коде:**
```python
# Было (неправильно):
if len(full_text.split()) <= MIN_WORDS_POSITIVE:
    return False  # короткий = сразу мусор

# Стало (правильно):
if len(full_text.split()) >= MIN_WORDS_POSITIVE:
    return True   # длинный = сразу сохраняем
# короткий → проверяем стоп-фразы, только потом решаем
```

---

### v5.1 — SKU De-gluing: исправлен total_wb и дедупликация (16.04.2026)

**Контекст:** Тестовый артикул `246290927` (Give Creative, 3D ручка).
WB склеивает несколько nmId в одну страницу отзывов (root=232715088):
- nmId 246290927 = Оранжевый, feedbacks=**110**
- nmId 371806956 = FDM, feedbacks=**30**
- root возвращает feedbackCount=**151** (баг счётчика WB, 110+30≠151)

**БАГ A — total_wb в per-SKU папках был неверным** ✅ ИСПРАВЛЕН
- Было: `total_wb = 151` (feedbackCount склейки) в каждой per-SKU папке
- Стало: `total_wb_nm = 110` (feedbacks nmId из card API) + `total_wb_root = 151` (справочно)
- card.wb.ru → `p["feedbacks"]` = per-nmId; feedbacks2.wb.ru → `feedbackCount` = root

**БАГ B — Дублирование при нескольких артикулах одной склейки** ✅ ИСПРАВЛЕН
- Было: два nmId → два одинаковых запроса → дублированные данные
- Стало: `root_cache: dict[int, tuple[list, int]]` — если root уже обработан, API не вызывается

**БАГ C — Цвет nmId определялся по ненадёжному полю color** ✅ ИСПРАВЛЕН
- Было: split по `color` — WB часто возвращает `color=''` (из 18 FDM отзывов: 12 с пустым color)
- Стало: split по `nmId` → card API для маппинга nmId→display_name

**Данные верифицированы через браузер:**
- Оранжевый (246290927): 110 на карточке → 55 в API → 27 за год → 8 после PRO-фильтра
- FDM (371806956): 30 на карточке → 18 в API → 18 за год (все свежие) → ~8 после фильтра

---

### v5.0 — TOTAL SCRAPE PRO: Playwright + PRO-воронка (16.04.2026)

**Файл:** `pro_scraper_reviews.py`

**Проблема:** WB DDoS-Guard начал блокировать прямые API-запросы из `main.py`.

**Решение:** Playwright с браузерной сессией. Запросы делаются через `page.evaluate(fetch)` — изнутри браузера с WB-куками. DDoS-Guard видит легитимный AJAX.

**PRO-фильтр:**
- 1-3★: проходит всегда (Rule 1)
- 4-5★: >17 слов + отсутствие стоп-фраз (3 категории) + слова-брейкеры как исключение
- Подробно: `FILTER_PIPELINE_EXPLAINED.txt`

---

### main.py v5.0 — API-режим без браузера (16.04.2026)

**Три эндпоинта:**
- `feedbacks2.wb.ru/feedbacks/v1/{root}` — отзывы
- `questions.wb.ru/api/v1/questions?nmId=` — вопросы (DNS не резолвится!)
- `card.wb.ru/cards/v4/detail?nm=` — инфо о товаре

---

## Что ещё не сделано (из implementation_plan.md)

### Приоритет 3 — Структура аналитической таблицы
- [ ] 3.1: L1 → L1a (кто/кому) + L1b (когда/повод) — разбить на два блока
- [ ] 3.2: Добавить блок «Конструктивный негатив из 4-5★»
- [ ] 3.3: Убрать колонку «цвет/вариант» как информационную

### Приоритет 4 — QA-анализ вопросов
- [ ] 4.1: Применить аналитический пайплайн к вопросам
- [ ] 4.2: Смёрджить боли из вопросов и отзывов в единую Pain Matrix

### Известные ограничения
- **Немые оценки:** WB не возвращает оценки без текста через API (246290927: 55 из 110)
- **API-лимит 1000:** feedbacks2.wb.ru отдаёт максимум 1000 записей
- **Q&A вопросы:** questions.wb.ru не резолвится DNS; scrape_questions() возвращает []

---

## Паттерны кода

```python
# Логирование — всегда с section-тегом
log.info(f"  [section] message {var!r}")

# Воронка данных — всегда text_base как знаменатель
pct = round(n / text_base * 100, 1) if text_base > 0 else 0.0

# PRO-фильтр — негатив всегда проходит
if rating <= 3:
    return True

# Дедупликация по root
if root in root_cache:
    all_norm, total_wb_root = root_cache[root]
else:
    raw_api, total_wb_root = fetch_all_reviews(page, root, cutoff, nm_id)
    root_cache[root] = (all_norm, total_wb_root)

# SKU de-gluing — nmId надёжнее color
nm_id = fb.get("nmId") or 0   # никогда не пустой
# card.wb.ru → colors[0].name = display_name варианта
```

---

## Тестовые данные (gitignored)

- `results_test/` — результаты ДО исправлений v5.1 (старый total_wb=151)
- `results_test_v2/` — результаты ПОСЛЕ исправлений v5.1 (правильный total_wb_nm=110)
- `results_test_v3/` — промежуточные тесты де-глуинга
- `articles_test.txt` — тестовый набор: артикулы 246290927 (Оранжевый) и 371806956 (FDM)
