#!/usr/bin/env python3
"""
Конвертер старых результатов в новый формат.
Читает results/{nmId}.json → создаёт results/{nmId}/reviews.* + questions.*
Создаёт сводные: all_reviews.csv, all_questions.csv, data_export.json, data_export.txt
"""

import csv
import json
from pathlib import Path

RESULTS_DIR       = Path("results")
OUTPUT_JSON       = "data_export.json"
OUTPUT_TXT        = "data_export.txt"
ALL_REVIEWS_CSV   = "all_reviews.csv"
ALL_QUESTIONS_CSV = "all_questions.csv"

R_FIELDS = ["product_id", "brand", "name", "date", "rating", "pros", "cons", "text", "seller_answer"]
Q_FIELDS = ["product_id", "brand", "name", "date", "question", "answer"]


# ── TXT-форматтеры ────────────────────────────────────────────────────────────
def reviews_to_txt(p: dict) -> str:
    sep = "=" * 70
    lines = [sep,
             f"ТОВАР:    {p.get('name','')}",
             f"БРЕНД:    {p.get('brand','')}",
             f"АРТИКУЛ:  {p.get('product_id','')}",
             f"ОТЗЫВОВ:  {len(p.get('reviews',[]))}",
             sep, ""]
    for i, r in enumerate(p.get("reviews", []), 1):
        stars = "★" * r.get("rating", 0) + "☆" * (5 - r.get("rating", 0))
        lines.append(f"[{i}] {r.get('date','')}  {stars}")
        if r.get("pros"):          lines.append(f"  + Достоинства : {r['pros']}")
        if r.get("cons"):          lines.append(f"  - Недостатки  : {r['cons']}")
        if r.get("text"):          lines.append(f"  Комментарий   : {r['text']}")
        if r.get("seller_answer"): lines.append(f"  Ответ продавца: {r['seller_answer']}")
        lines.append("")
    lines += [sep, ""]
    return "\n".join(lines)


def questions_to_txt(p: dict) -> str:
    sep = "=" * 70
    lines = [sep,
             f"ТОВАР:    {p.get('name','')}",
             f"БРЕНД:    {p.get('brand','')}",
             f"АРТИКУЛ:  {p.get('product_id','')}",
             f"ВОПРОСОВ: {len(p.get('questions',[]))}",
             sep, ""]
    for i, q in enumerate(p.get("questions", []), 1):
        lines.append(f"[{i}] {q.get('date','')}")
        lines.append(f"  Вопрос: {q.get('question','')}")
        lines.append(f"  Ответ : {q.get('answer','') or '(нет ответа)'}")
        lines.append("")
    lines += [sep, ""]
    return "\n".join(lines)


def write_csv(path: Path, fields: list, rows: list):
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


# ── Основная конвертация ──────────────────────────────────────────────────────
old_jsons = sorted(RESULTS_DIR.glob("*.json"))
if not old_jsons:
    print("Нет .json файлов в папке results/")
    exit()

print(f"Найдено файлов: {len(old_jsons)}")

all_results   = []
all_r_rows    = []
all_q_rows    = []

for src in old_jsons:
    p = json.loads(src.read_text(encoding="utf-8"))
    pid       = p.get("product_id", src.stem)
    brand     = p.get("brand", "")
    name      = p.get("name", "")
    reviews   = p.get("reviews",   [])
    questions = p.get("questions", [])

    # Создаём подпапку
    out_dir = RESULTS_DIR / pid
    out_dir.mkdir(exist_ok=True)

    # ── JSON ──
    (out_dir / "reviews.json").write_text(
        json.dumps({"product_id": pid, "brand": brand, "name": name, "reviews": reviews},
                   ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "questions.json").write_text(
        json.dumps({"product_id": pid, "brand": brand, "name": name, "questions": questions},
                   ensure_ascii=False, indent=2), encoding="utf-8")
    (out_dir / "full.json").write_text(
        json.dumps(p, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── TXT ──
    (out_dir / "reviews.txt").write_text(reviews_to_txt(p),   encoding="utf-8")
    (out_dir / "questions.txt").write_text(questions_to_txt(p), encoding="utf-8")

    # ── CSV ──
    r_rows = [{"product_id": pid, "brand": brand, "name": name,
               "date": r.get("date",""), "rating": r.get("rating",""),
               "pros": r.get("pros",""), "cons": r.get("cons",""),
               "text": r.get("text",""), "seller_answer": r.get("seller_answer","")}
              for r in reviews]
    q_rows = [{"product_id": pid, "brand": brand, "name": name,
               "date": q.get("date",""), "question": q.get("question",""),
               "answer": q.get("answer","")}
              for q in questions]
    write_csv(out_dir / "reviews.csv",   R_FIELDS, r_rows)
    write_csv(out_dir / "questions.csv", Q_FIELDS, q_rows)

    all_results.extend([p])
    all_r_rows.extend(r_rows)
    all_q_rows.extend(q_rows)

    print(f"  ✓ {pid:>12}  отзывов={len(reviews):>4}  вопросов={len(questions):>4}  {name[:35]}")

# ── Сводные файлы ─────────────────────────────────────────────────────────────
write_csv(Path(ALL_REVIEWS_CSV),   R_FIELDS, all_r_rows)
write_csv(Path(ALL_QUESTIONS_CSV), Q_FIELDS, all_q_rows)
Path(OUTPUT_JSON).write_text(json.dumps(all_results, ensure_ascii=False, indent=2), encoding="utf-8")
Path(OUTPUT_TXT).write_text(
    "\n\n".join(reviews_to_txt(p) + "\n" + questions_to_txt(p) for p in all_results),
    encoding="utf-8")

total_r = sum(len(p.get("reviews",[])) for p in all_results)
total_q = sum(len(p.get("questions",[])) for p in all_results)
print(f"\nГотово! Товаров: {len(all_results)} | Отзывов: {total_r} | Вопросов: {total_q}")
print(f"Файлы: results/{{nmId}}/reviews.json/txt/csv + questions.json/txt/csv")
print(f"Сводные: {ALL_REVIEWS_CSV}, {ALL_QUESTIONS_CSV}, {OUTPUT_JSON}, {OUTPUT_TXT}")
