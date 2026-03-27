# Лабораторная работа 3.1 — ETL-решение для интеграции данных

## Вариант 2: HR (Зарплата)

**Цель:** Разработать комплексное ETL-решение для интеграции данных из PostgreSQL и файловых источников (CSV/Excel) в целевое хранилище MySQL. Создать единый отчёт по начислениям (оклад + премия), рассчитать налоги, выгрузить в MySQL таблицу выплат.

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

## Ход работы

### Шаг 1. Подготовка источника (PostgreSQL)

```bash
# Подключиться к PostgreSQL через psql или pgAdmin4
# Выполнить скрипты создания и наполнения таблицы:
```
Результат скрипта создания:
<img width="1916" height="978" alt="image" src="https://github.com/user-attachments/assets/4a0386f3-ac83-446d-bcb9-0967cc7238b9" />

Результат скрипта наполнения:
<img width="1847" height="956" alt="image" src="https://github.com/user-attachments/assets/a8c3dc9b-746d-4b6d-accd-6ade1d88ed77" />

Данные в таблице:
<img width="1847" height="1008" alt="image" src="https://github.com/user-attachments/assets/72a4fc78-03b5-4d90-a0d7-19c8a827aa6d" />

### Шаг 2. Генерация файловых источников

```bash
# Установить зависимости (если нужен Excel)
pip install openpyxl

# Запустить скрипт генерации
python scripts/generate_data_files.py
```
Результата запуска скрипта:
<img width="1564" height="974" alt="image" src="https://github.com/user-attachments/assets/93a0c66d-dab5-4b00-87ab-d58907229d84" />

Результат:
- `data/salaries_source.xlsx` — 50 000 записей
- `data/bonuses_source.csv` — 50 000 записей

### Шаг 3. Создание целевых таблиц (MySQL)

```sql
CREATE TABLE hr_payroll_final (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    emp_id          INT            NOT NULL,
    full_name       VARCHAR(300),
    department      VARCHAR(100),
    position        VARCHAR(100),
    grade           VARCHAR(50),
    base_salary     DECIMAL(12,2)  DEFAULT 0.00,
    seniority_pct   DECIMAL(5,1)   DEFAULT 0.0,
    regional_coeff  DECIMAL(4,2)   DEFAULT 1.00,
    adjusted_salary DECIMAL(12,2)  DEFAULT 0.00,  
    bonus_amount    DECIMAL(12,2)  DEFAULT 0.00,
    bonus_type      VARCHAR(100),
    total_payout    DECIMAL(12,2)  DEFAULT 0.00,  
    tax_ndfl        DECIMAL(12,2)  DEFAULT 0.00,  
    tax_pension     DECIMAL(12,2)  DEFAULT 0.00,  
    tax_medical     DECIMAL(12,2)  DEFAULT 0.00,  
    tax_social      DECIMAL(12,2)  DEFAULT 0.00,   
    net_payout      DECIMAL(12,2)  DEFAULT 0.00,  
    employer_cost   DECIMAL(12,2)  DEFAULT 0.00,   
    processed_at    TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);
```
Результат создания БД в MYSQL:
<img width="1618" height="521" alt="image" src="https://github.com/user-attachments/assets/09f0d9c0-83f8-4e40-8d20-f9ab74940e93" />

### Шаг 4. Запуск ETL в Pentaho (Spoon)

1. Открыть Pentaho Spoon (pan.bat / spoon.bat)
2. Открыть трансформацию: `pentaho/HR_Payroll_ETL.ktr`
3. **Настроить подключения**
   - `MySQL_Target`: 95.131.149.21:3306 / mgpu_ico_etl_02 / fGq5CRv6
4. Проверить пути к файлам (Excel, CSV) в шагах Input
5. Нажать **▶ Run** (F9)

Результат работы:

### Шаг 5. Создание витрин данных (Views)

```SQL
CREATE OR REPLACE VIEW view_payroll_by_department AS
SELECT
    department,
    COUNT(*)                          AS employees_count,
    ROUND(AVG(base_salary), 2)        AS avg_base_salary,
    ROUND(AVG(adjusted_salary), 2)    AS avg_adjusted_salary,
    ROUND(SUM(bonus_amount), 2)       AS total_bonuses,
    ROUND(SUM(total_payout), 2)       AS total_payout,
    ROUND(SUM(tax_ndfl), 2)           AS total_ndfl,
    ROUND(SUM(net_payout), 2)         AS total_net_payout,
    ROUND(SUM(employer_cost), 2)      AS total_employer_cost,
    ROUND(AVG(net_payout), 2)         AS avg_net_payout
FROM hr_payroll_final
GROUP BY department
ORDER BY total_payout DESC;
```
Результат работы:
<img width="1612" height="1044" alt="image" src="https://github.com/user-attachments/assets/5985af61-353a-4cf6-8c41-777f04c164ba" />

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

## ETL-процесс

### Схема трансформации в Pentaho

Обшщий вид:
<img width="1839" height="1046" alt="image" src="https://github.com/user-attachments/assets/c43cc7f9-08d5-4351-93ec-9b8bbe8db344" />


### Ключевые шаги
<img width="977" height="709" alt="image" src="https://github.com/user-attachments/assets/8454f86f-2276-4130-a63d-bcaeb1746cc6" />
<img width="1259" height="621" alt="image" src="https://github.com/user-attachments/assets/579b87bc-3fb8-4606-8644-3778ab6de3f4" />
<img width="892" height="721" alt="image" src="https://github.com/user-attachments/assets/1183c5f8-cb1d-4fd0-8e7a-53ed8cbb53b4" />
<img width="393" height="366" alt="image" src="https://github.com/user-attachments/assets/62801fe8-53f1-4069-885c-29cc54fd2b7e" />
<img width="756" height="387" alt="image" src="https://github.com/user-attachments/assets/301242cc-960f-484d-9af0-b61eb72db557" />
<img width="1540" height="470" alt="image" src="https://github.com/user-attachments/assets/b23a66e0-6e4a-4077-aa56-f4b74f30afca" />
<img width="984" height="1044" alt="image" src="https://github.com/user-attachments/assets/784a5bd0-d7f2-42e8-a3d1-03c523fde08d" />
<img width="1543" height="852" alt="image" src="https://github.com/user-attachments/assets/280767cf-60f4-4f74-abcc-8fa0654f15a7" />
<img width="938" height="881" alt="image" src="https://github.com/user-attachments/assets/43d884aa-1802-49fd-aea6-ef179e1d8b61" />

## Выводы

1. **Разработана трёхслойная архитектура** ETL-решения (Source → Storage → Business), обеспечивающая прозрачность потоков данных и разделение ответственности.

2. **Реализован ETL-процесс** в Pentaho PDI, интегрирующий данные из трёх гетерогенных источников:
   - PostgreSQL (1 000 000 записей — личные данные сотрудников)
   - Excel (50 000 записей — справочник окладов с грейдами)
   - CSV (50 000 записей — данные о премиях)

3. **Выполнена валидация и очистка данных**: фильтрация NULL-значений, проверка типов, удаление нерелевантных записей.

4. **Реализованы вычисляемые показатели**: скорректированный оклад (с учётом стажа и районного коэффициента), налоговые отчисления (НДФЛ 13%, ПФР 22%, ОМС 5.1%, ФСС 2.9%), чистая выплата и полная стоимость для работодателя.


