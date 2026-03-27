# Лабораторная работа 3.1 — ETL-решение для интеграции данных

## Вариант 2: HR (Зарплата)

**Цель:** Разработать комплексное ETL-решение для интеграции данных из PostgreSQL и файловых источников (CSV/Excel) в целевое хранилище MySQL. Создать единый отчёт по начислениям (оклад + премия), рассчитать налоги, выгрузить в MySQL таблицу выплат.

---

## 📋 Содержание

- [Архитектура решения](#архитектура-решения)
- [Структура репозитория](#структура-репозитория)
- [Источники данных](#источники-данных)
- [Техническое окружение](#техническое-окружение)
- [Инструкция по развёртыванию](#инструкция-по-развёртыванию)
- [ETL-процесс](#etl-процесс)
- [Витрины данных](#витрины-данных)
- [Выводы](#выводы)

---

## Архитектура решения

Архитектура построена на трёхслойной модели:

```
┌─────────────────┐     ┌──────────────────────┐     ┌──────────────────────┐
│  SOURCE LAYER   │     │     ETL PROCESS       │     │   STORAGE LAYER      │
│  (Источники)    │────▶│   (Pentaho PDI 9.4)   │────▶│   (MySQL Target)     │
│                 │     │                        │     │                      │
│ ▪ PostgreSQL    │     │ 1. Table Input (PG)    │     │ ▪ stg_employees      │
│   employees_info│     │ 2. Excel Input         │     │ ▪ hr_payroll_final   │
│   (1 000 000)   │     │ 3. CSV Input           │     │ ▪ fact_bonuses       │
│                 │     │ 4. Merge Join (x2)     │     │ ▪ dim_department     │
│ ▪ Excel         │     │ 5. Filter Rows         │     │ ▪ dim_grade          │
│   salaries.xlsx │     │ 6. Calculator/JS       │     │                      │
│   (50 000)      │     │ 7. Table Output        │     │                      │
│                 │     │                        │     │                      │
│ ▪ CSV           │     └──────────────────────┘     └──────────┬───────────┘
│   bonuses.csv   │                                              │
│   (50 000)      │                                              ▼
└─────────────────┘                               ┌──────────────────────┐
                                                  │   BUSINESS LAYER     │
                                                  │   (Витрины / Views)  │
                                                  │                      │
                                                  │ ▪ view_payroll_by_   │
                                                  │   department         │
                                                  │ ▪ view_payroll_by_   │
                                                  │   grade              │
                                                  │ ▪ view_tax_burden    │
                                                  │ ▪ view_top_earners   │
                                                  │ ▪ view_bonus_analysis│
                                                  │ ▪ view_analytics_    │
                                                  │   report             │
                                                  └──────────────────────┘
```

> Подробная схема в формате Draw.io: [`diagrams/architecture_etl_hr.drawio`](diagrams/architecture_etl_hr.drawio)

---

## Структура репозитория

```
ETL/
├── README.md                              ← Этот файл
├── diagrams/
│   └── architecture_etl_hr.drawio         ← Схема архитектуры (Draw.io)
├── sql/
│   ├── postgresql_create_and_populate.sql  ← Создание и заполнение PG (1 млн)
│   ├── mysql_create_target_tables.sql      ← Целевые таблицы MySQL
│   └── mysql_create_views.sql             ← Аналитические витрины (6 Views)
├── scripts/
│   └── generate_data_files.py             ← Генерация CSV и Excel файлов
├── data/
│   ├── salaries_source.xlsx               ← Справочник окладов (50 000)
│   └── bonuses_source.csv                 ← Данные о премиях (50 000)
└── pentaho/
    └── HR_Payroll_ETL.ktr                 ← Трансформация Pentaho
```

---

## Источники данных

### 1. PostgreSQL — Личные данные сотрудников (1 000 000 записей)

| Поле         | Тип            | Описание                          |
|-------------|----------------|-----------------------------------|
| `emp_id`    | SERIAL (PK)    | Уникальный ID сотрудника          |
| `last_name` | VARCHAR(100)   | Фамилия                           |
| `first_name`| VARCHAR(100)   | Имя                                |
| `middle_name`| VARCHAR(100)  | Отчество                          |
| `birth_date`| DATE           | Дата рождения                     |
| `gender`    | CHAR(1)        | Пол (M/F)                         |
| `department`| VARCHAR(100)   | Отдел (15 вариантов)              |
| `position`  | VARCHAR(100)   | Должность (8 уровней)             |
| `hire_date` | DATE           | Дата приёма на работу             |
| `inn`       | VARCHAR(12)    | ИНН                                |
| `snils`     | VARCHAR(14)    | СНИЛС                              |
| `email`     | VARCHAR(150)   | Электронная почта                 |
| `phone`     | VARCHAR(20)    | Телефон                            |
| `is_active` | BOOLEAN        | Статус активности (95% = TRUE)    |

### 2. Excel — Справочник окладов (50 000 записей, `salaries_source.xlsx`)

| Поле              | Тип     | Описание                         |
|-------------------|---------|----------------------------------|
| `emp_id`          | Integer | ID сотрудника (ключ связи)       |
| `grade`           | String  | Грейд: Junior/Middle/Senior/Lead/Head |
| `base_salary`     | Number  | Базовый оклад (руб.)            |
| `seniority_pct`   | Number  | Надбавка за стаж (0–15%)         |
| `regional_coeff`  | Number  | Районный коэффициент (1.0–1.5)   |
| `effective_date`  | String  | Дата установления оклада         |
| `currency`        | String  | Валюта (RUB)                     |

### 3. CSV — Данные о премиях (50 000 записей, `bonuses_source.csv`)

| Поле           | Тип     | Описание                          |
|----------------|---------|-----------------------------------|
| `emp_id`       | Integer | ID сотрудника (ключ связи)        |
| `bonus_type`   | String  | Тип премии (8 вариантов)          |
| `bonus_amount` | Number  | Сумма премии (руб.)              |
| `bonus_date`   | String  | Дата начисления                   |
| `comment`      | String  | Комментарий                       |

---

## Техническое окружение

| Компонент    | Значение                                   |
|-------------|---------------------------------------------|
| ВМ          | ETL_devops_26                                |
| ETL-инструмент | Pentaho Data Integration (PDI) 9.4       |
| Source DBMS | PostgreSQL (localhost:5432, БД: `st_200`)    |
| Target DBMS | MySQL (95.131.149.21:3306, БД: `mgpu_ico_etl_XX`) |
| Моделирование | Draw.io                                   |
| PG Admin    | http://localhost/login/next=/                 |
| Логин PG    | admin / admin                                |

---

## Инструкция по развёртыванию

### Шаг 1. Подготовка источника (PostgreSQL)

```bash
# Подключиться к PostgreSQL через psql или pgAdmin4
# Выполнить скрипт создания и наполнения таблицы:
psql -h localhost -p 5432 -U admin -d st_200 -f sql/postgresql_create_and_populate.sql
```

> ⏱ Генерация 1 000 000 записей занимает ~2–5 минут (батчами по 10 000).

### Шаг 2. Генерация файловых источников

```bash
# Установить зависимости (если нужен Excel)
pip install openpyxl

# Запустить скрипт генерации
python scripts/generate_data_files.py
```

Результат:
- `data/salaries_source.xlsx` — 50 000 записей
- `data/bonuses_source.csv` — 50 000 записей

### Шаг 3. Создание целевых таблиц (MySQL)

```bash
# Подключиться к MySQL и выполнить скрипт
mysql -h 95.131.149.21 -P 3306 -u YOUR_LOGIN -p mgpu_ico_etl_XX < sql/mysql_create_target_tables.sql
```

### Шаг 4. Запуск ETL в Pentaho (Spoon)

1. Открыть Pentaho Spoon (pan.bat / spoon.bat)
2. Открыть трансформацию: `pentaho/HR_Payroll_ETL.ktr`
3. **Настроить подключения:**
   - `PostgreSQL_Source`: localhost:5432 / st_200 / admin:admin
   - `MySQL_Target`: 95.131.149.21:3306 / mgpu_ico_etl_XX / ваши учётные данные
4. Проверить пути к файлам (Excel, CSV) в шагах Input
5. Нажать **▶ Run** (F9)

### Шаг 5. Создание витрин данных (Views)

```bash
mysql -h 95.131.149.21 -P 3306 -u YOUR_LOGIN -p mgpu_ico_etl_XX < sql/mysql_create_views.sql
```

---

## ETL-процесс

### Схема трансформации в Pentaho

```
PostgreSQL ──▶ Sort ──┐
(employees)           ├──▶ Merge Join 1 ──▶ Sort ──┐
Excel ──────▶ Sort ──┘     (LEFT JOIN)             ├──▶ Merge Join 2 ──▶ Filter ──▶ Calculator ──▶ Table Output
(salaries)                  by emp_id              │     (LEFT JOIN)      NULL       JS: налоги     MySQL
                                                   │      by emp_id     check       и расчёты
CSV ────────▶ Sort ────────────────────────────────┘
(bonuses)
```

### Ключевые шаги

| # | Шаг                | Описание                                              |
|---|---------------------|-------------------------------------------------------|
| 1 | **Table Input**     | `SELECT * FROM employees_info WHERE is_active = TRUE`  |
| 2 | **Excel Input**     | Чтение `salaries_source.xlsx` (лист "Оклады")         |
| 3 | **CSV Input**       | Чтение `bonuses_source.csv` (разделитель `;`)         |
| 4 | **Merge Join 1**    | LEFT JOIN: PG + Excel по `emp_id`                     |
| 5 | **Merge Join 2**    | LEFT JOIN: (PG+Excel) + CSV по `emp_id`               |
| 6 | **Filter Rows**     | Удаление строк с `emp_id IS NULL`                     |
| 7 | **Calculator/JS**   | Расчёт (см. формулы ниже)                             |
| 8 | **Table Output**    | Запись в MySQL `hr_payroll_final` (batch = 10000)     |

### Формулы расчётов

```
adjusted_salary = base_salary × (1 + seniority_pct / 100) × regional_coeff
total_payout    = adjusted_salary + bonus_amount

НДФЛ (13%)   : tax_ndfl    = total_payout × 0.13
ПФР (22%)    : tax_pension  = total_payout × 0.22
ОМС (5.1%)   : tax_medical  = total_payout × 0.051
ФСС (2.9%)   : tax_social   = total_payout × 0.029

net_payout    = total_payout − tax_ndfl          (на руки)
employer_cost = total_payout + tax_pension + tax_medical + tax_social  (полная стоимость)
```

---

## Витрины данных

После загрузки данных в MySQL создаются 6 аналитических представлений (VIEW):

| # | VIEW                           | Описание                                      |
|---|--------------------------------|------------------------------------------------|
| 1 | `view_payroll_by_department`   | Сводка начислений, налогов, чистых выплат по отделам |
| 2 | `view_payroll_by_grade`        | Анализ зарплат по грейдам (min/avg/max)        |
| 3 | `view_tax_burden`              | Налоговая нагрузка: НДФЛ + взносы по отделам  |
| 4 | `view_top_earners`             | Топ-100 сотрудников по общей сумме выплат      |
| 5 | `view_bonus_analysis`          | Анализ премий: количество, суммы по типам      |
| 6 | `view_analytics_report`        | Итоговая витрина: отдел × грейд (для дашборда) |

### Пример запроса к витрине

```sql
-- Топ-5 отделов по фонду оплаты труда
SELECT department, employees_count, total_payout, avg_net_payout
FROM view_payroll_by_department
LIMIT 5;

-- Налоговая нагрузка
SELECT department, gross_payout, total_taxes, tax_burden_pct
FROM view_tax_burden
ORDER BY tax_burden_pct DESC;
```

---

## Выводы

1. **Разработана трёхслойная архитектура** ETL-решения (Source → Storage → Business), обеспечивающая прозрачность потоков данных и разделение ответственности.

2. **Реализован ETL-процесс** в Pentaho PDI, интегрирующий данные из трёх гетерогенных источников:
   - PostgreSQL (1 000 000 записей — личные данные сотрудников)
   - Excel (50 000 записей — справочник окладов с грейдами)
   - CSV (50 000 записей — данные о премиях)

3. **Выполнена валидация и очистка данных**: фильтрация NULL-значений, проверка типов, удаление нерелевантных записей.

4. **Реализованы вычисляемые показатели**: скорректированный оклад (с учётом стажа и районного коэффициента), налоговые отчисления (НДФЛ 13%, ПФР 22%, ОМС 5.1%, ФСС 2.9%), чистая выплата и полная стоимость для работодателя.

5. **Созданы 6 аналитических витрин** (MySQL Views) для бизнес-пользователей, покрывающие ключевые аналитические срезы: по отделам, грейдам, налоговой нагрузке, топ-сотрудникам и типам премий.

---

**Автор:** Студент  
**Дата:** Март 2026  
**Инструменты:** Pentaho PDI 9.4, PostgreSQL, MySQL, Python, Draw.io
