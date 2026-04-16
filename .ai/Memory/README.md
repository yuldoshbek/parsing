# AI Memory — Парсер отзывов WB

> Последнее обновление: 16 апреля 2026  
> Версия парсера: v4.0  
> Агент: Claude Sonnet 4.6

---

## Состояние проекта

### Что это
Playwright-парсер отзывов и вопросов с Wildberries.  
Запуск: `python main.py` (читает `articles.txt`, пишет в `results/{nmId}/`)

### Файлы проекта
| Файл | Роль |
|---|---|
| `main.py` | Основной парсер v4.0 — stealth Playwright + сбор отзывов/вопросов |
| `convert.py` | Разовый конвертер старых `results/*.json` в новый формат подпапок |
| `debug_check.py` | Диагностический скрипт — проверяет видимость отзывов на странице |
| `requirements.txt` | `playwright>=1.44.0`, `requests>=2.31.0` |
| `articles.txt` | Список артикулов WB (по одному на строку) |
| `implementation_plan.md` | Аудит багов + приоритизированный план исправлений |
| `ИНСТРУКЦИЯ_ДЛЯ_ИИ_АНАЛИТИКА.md` | ТЗ для ИИ-аналитика — формат анализа отзывов |

---

## История исправлений

### v5.0 — Замена Playwright на API-режим (16.04.2026)

**Проблема:** WB DDoS-Guard блокировал Playwright-браузер («Что-то не так... Подозрительная активность»).

**Решение:** Полностью убран Playwright. Парсер теперь работает через прямые HTTP-запросы к внутреннему WB API:
- `feedbacks2.wb.ru/feedbacks/v1/{nmId}` — отзывы
- `questions.wb.ru/api/v1/questions?nmId={nmId}` — вопросы
- `card.wb.ru/cards/v4/detail?nm={nmId}` — инфо о товаре

**Новые функции:**
- `make_session()` — requests.Session с ротацией UA
- `api_get()` — robust GET с ретраями на 429/5xx
- `_fmt_date()` — ISO 8601 → московское время (UTC+3)
- `get_product_info()` — переписан на requests
- `scrape_feedbacks()` — переписан на API
- `scrape_questions()` — переписан на API

**Преимущества v5 vs v4:**
- `sku_variant` — из поля `color` каждого отзыва в API (точнее browser-based определения)
- `total_wb` — из `feedbackCount` карточки (точный счётчик WB)
- Не нужен браузер, нет антибота
- В 5-10 раз быстрее Playwright

**Удалено:** все Playwright-функции (`wait_for_challenge`, `safe_goto`, `human_scroll`, `make_fresh_context`, `get_sku_variants`, все `_JS_*` константы)

**Без изменений:** все экспортные функции (TXT, CSV, JSON, `build_funnel_stats`, `_rating_group`)

---

### v4.0 — Исправлены критические баги (15.04.2026)

**БАГ #1 (КРИТИЧЕСКИЙ) — Склейка SKU** ✅ ИСПРАВЛЕН  
- Добавлена функция `get_sku_variants()` + JS `_JS_PARSE_VARIANTS`  
- Каждый отзыв помечается полем `sku_variant`  
- В JSON добавлено поле `all_sku_variants`

**БАГ #2 (КРИТИЧЕСКИЙ) — Неверный знаменатель %** ✅ ИСПРАВЛЕН  
- Добавлена функция `build_funnel_stats()` — строит воронку:  
  `total_wb → total_parsed → no_text → working_base`  
- % позитив/нейтрал/негатив считаются от `working_base`, сумма = 100%  
- Добавлен `sum_pct_check` для самопроверки

**БАГ #3 (СРЕДНИЙ) — 3★ выпадала из расчёта** ✅ ИСПРАВЛЕН  
- `_rating_group()`: 4-5★ = позитив, 3★ = нейтрал, 1-2★ = негатив  
- Нейтрал теперь явно присутствует в CSV и JSON

**Прочие улучшения v4.0:**  
- `REVIEWS_CSV_FIELDS` расширен: добавлены `sku_variant`, `total_wb`, `total_parsed`, `no_text`, `working_base`, `positive_pct`, `neutral_pct`, `negative_pct`, `rating_group`  
- `wait_for_challenge()` — поддержка таймаута до 5 минут для антибота WB

---

## Что ещё не сделано (из implementation_plan.md)

### Приоритет 3 — Структура аналитической таблицы
- [ ] 3.1: L1 → L1a (кто/кому) + L1b (когда/повод) — разбить на два блока
- [ ] 3.2: Добавить блок «Конструктивный негатив из 4-5★»
- [ ] 3.3: Убрать колонку «цвет/вариант» как информационную

### Приоритет 4 — QA-анализ вопросов
- [ ] 4.1: Применить аналитический пайплайн к вопросам (1868 записей)
- [ ] 4.2: Смёрджить боли из вопросов и отзывов в единую Pain Matrix

### Известные ограничения v5.0
- **Q&A вопросы недоступны**: `questions.wb.ru` не резолвится DNS; `type=question` возвращает обычные отзывы. `scrape_questions()` возвращает `[]` + WARNING. TODO: endpoint `questions.wb.ru/api/v1/questions?nmId={nm}&take=30&skip=0`
- **API-лимит 1000**: feedbacks2.wb.ru отдаёт максимум 1000 записей. Крупные товары (124436959: 1477, 428894982: 1978, 844748724: 2317) берём только первые 1000 (самые новые).

---

## Паттерны кода

```python
# Логирование — всегда с section-тегом
log.info(f"  [section] message {var!r}")

# Функции возвращают dict
def get_something() -> dict:
    return {"key": "", "other": []}

# Stealth-задержки
rand_sleep(*DELAY_BETWEEN_PAGES)   # распаковка кортежа

# Воронка данных — всегда working_base как знаменатель
pct = round(n / working_base * 100, 1) if working_base > 0 else 0.0

# Комментарии объясняют ПОЧЕМУ
# БАГ #1 FIX: помечаем каждый отзыв тем вариантом SKU, который парсили
for item in batch:
    item["sku_variant"] = sku_variant
```

---

## Данные в results/

- **9 артикулов** в подпапках: `124436959`, `196861890`, `224227461`, `227044607`, `246290927`, `333681986`, `428894982`, `480055076`, `480055077`
- Каждая подпапка: `reviews.json`, `questions.json`, `full.json`, `reviews.txt`, `questions.txt`, `reviews.csv`, `questions.csv`
- **Важно**: данные в `results/` собраны ДО v4.0 — в них отсутствуют поля `funnel_stats`, `sku_variant`, `rating_group`
- Артикулы `480055076` и `480055077` имеют одинаковый размер файла — подтверждение бага #1 (склейка)
