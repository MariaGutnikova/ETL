"""
Генерация файлов-источников для ETL (Вариант 2: HR Зарплата)
- salaries_source.xlsx — 50 000 записей (справочник окладов)
- bonuses_source.csv   — 50 000 записей (данные о премиях)
"""

import csv
import random
import os
from datetime import date, timedelta

try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False
    print("⚠  openpyxl не установлен. Установите: pip install openpyxl")
    print("   Excel-файл будет создан как CSV-fallback (salaries_source.csv).\n")

# ── Настройки ──────────────────────────────────────────────
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

SALARIES_COUNT = 50_000    # количество записей в файле окладов
BONUSES_COUNT  = 50_000    # количество записей в файле премий

# emp_id будут в диапазоне 1 .. 1_000_000 (соответствуют PostgreSQL)
MAX_EMP_ID = 1_000_000

# Справочники
DEPARTMENTS = [
    'Бухгалтерия', 'IT-отдел', 'Отдел кадров', 'Отдел продаж',
    'Маркетинг', 'Логистика', 'Юридический отдел', 'Производство',
    'Контроль качества', 'Финансовый отдел', 'Служба безопасности',
    'Административный отдел', 'Отдел закупок', 'R&D', 'Техническая поддержка'
]

GRADES = ['Junior', 'Middle', 'Senior', 'Lead', 'Head']

BONUS_TYPES = [
    'Квартальная премия', 'Годовая премия', 'Проектная премия',
    'Премия за KPI', 'Разовая премия', '13-я зарплата',
    'Премия за наставничество', 'Премия за инновации'
]

# Диапазоны окладов по грейду (руб.)
SALARY_RANGES = {
    'Junior': (30_000, 55_000),
    'Middle': (55_000, 90_000),
    'Senior': (90_000, 140_000),
    'Lead':   (130_000, 200_000),
    'Head':   (180_000, 350_000),
}

random.seed(42)  # воспроизводимость


# ── 1. Генерация salaries_source.xlsx (окладов) ───────────
def generate_salaries():
    """Генерирует файл окладов (50 000 уникальных emp_id)."""
    # Выбираем 50 000 уникальных emp_id
    emp_ids = sorted(random.sample(range(1, MAX_EMP_ID + 1), SALARIES_COUNT))

    rows = []
    for emp_id in emp_ids:
        grade = random.choice(GRADES)
        low, high = SALARY_RANGES[grade]
        base_salary = round(random.uniform(low, high), 2)
        # Надбавка за стаж (0..15%)
        seniority_pct = round(random.uniform(0, 15), 1)
        # Районный коэффициент (1.0 .. 1.5)
        regional_coeff = round(random.choice([1.0, 1.0, 1.0, 1.15, 1.2, 1.3, 1.5]), 2)
        # Дата установления оклада
        effective_date = date(2020, 1, 1) + timedelta(days=random.randint(0, 2000))

        rows.append({
            'emp_id': emp_id,
            'grade': grade,
            'base_salary': base_salary,
            'seniority_pct': seniority_pct,
            'regional_coeff': regional_coeff,
            'effective_date': effective_date.isoformat(),
            'currency': 'RUB',
        })

    if HAS_OPENPYXL:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Оклады"
        headers = list(rows[0].keys())
        ws.append(headers)
        for row in rows:
            ws.append(list(row.values()))
        path = os.path.join(OUTPUT_DIR, "salaries_source.xlsx")
        wb.save(path)
        print(f"✅ Создан файл: {path}  ({SALARIES_COUNT} записей)")
    else:
        # Fallback — CSV
        path = os.path.join(OUTPUT_DIR, "salaries_source.csv")
        with open(path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()), delimiter=';')
            writer.writeheader()
            writer.writerows(rows)
        print(f"✅ Создан файл (CSV-fallback): {path}  ({SALARIES_COUNT} записей)")

    return emp_ids  # вернём для bonuses


# ── 2. Генерация bonuses_source.csv (премий) ─────────────
def generate_bonuses(salary_emp_ids: list):
    """
    Генерирует файл премий (50 000 записей).
    Часть emp_id совпадает с окладами, часть — нет (для тестирования JOIN).
    """
    # 80% записей — emp_id из файла окладов, 20% — случайные
    from_salaries = random.choices(salary_emp_ids, k=int(BONUSES_COUNT * 0.8))
    from_random   = [random.randint(1, MAX_EMP_ID) for _ in range(BONUSES_COUNT - len(from_salaries))]
    all_emp_ids   = from_salaries + from_random
    random.shuffle(all_emp_ids)

    path = os.path.join(OUTPUT_DIR, "bonuses_source.csv")
    with open(path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(['emp_id', 'bonus_type', 'bonus_amount', 'bonus_date', 'comment'])

        for emp_id in all_emp_ids:
            bonus_type = random.choice(BONUS_TYPES)
            # Размер премии зависит от типа
            if bonus_type in ('Годовая премия', '13-я зарплата'):
                amount = round(random.uniform(20_000, 150_000), 2)
            elif bonus_type == 'Разовая премия':
                amount = round(random.uniform(5_000, 30_000), 2)
            else:
                amount = round(random.uniform(10_000, 80_000), 2)

            bonus_date = date(2024, 1, 1) + timedelta(days=random.randint(0, 730))
            comment = f"Начисление: {bonus_type}"
            writer.writerow([emp_id, bonus_type, amount, bonus_date.isoformat(), comment])

    print(f"✅ Создан файл: {path}  ({BONUSES_COUNT} записей)")


# ── Main ──────────────────────────────────────────────────
if __name__ == '__main__':
    print("=" * 60)
    print("Генерация данных для ETL — Вариант 2: HR (Зарплата)")
    print("=" * 60)
    salary_ids = generate_salaries()
    generate_bonuses(salary_ids)
    print("\n🎉 Все файлы успешно сгенерированы в папку:", OUTPUT_DIR)
