-- ============================================================
-- Вариант 2: HR (Зарплата)
-- Целевые таблицы в MySQL (Storage Layer)
-- БД: mgpu_ico_etl_XX
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 1. Staging-таблица (промежуточная) — для сырых данных
-- ──────────────────────────────────────────────────────────
DROP TABLE IF EXISTS stg_employees;
CREATE TABLE stg_employees (
    emp_id         INT           NOT NULL,
    last_name      VARCHAR(100),
    first_name     VARCHAR(100),
    middle_name    VARCHAR(100),
    birth_date     DATE,
    gender         CHAR(1),
    department     VARCHAR(100),
    position       VARCHAR(100),
    hire_date      DATE,
    inn            VARCHAR(12),
    snils          VARCHAR(14),
    email          VARCHAR(150),
    phone          VARCHAR(20),
    is_active      TINYINT(1),
    loaded_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (emp_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ──────────────────────────────────────────────────────────
-- 2. Таблица измерений — Справочник отделов
-- ──────────────────────────────────────────────────────────
DROP TABLE IF EXISTS dim_department;
CREATE TABLE dim_department (
    dept_id        INT AUTO_INCREMENT PRIMARY KEY,
    dept_name      VARCHAR(100)  NOT NULL UNIQUE,
    created_at     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ──────────────────────────────────────────────────────────
-- 3. Таблица измерений — Справочник грейдов
-- ──────────────────────────────────────────────────────────
DROP TABLE IF EXISTS dim_grade;
CREATE TABLE dim_grade (
    grade_id       INT AUTO_INCREMENT PRIMARY KEY,
    grade_name     VARCHAR(50)   NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO dim_grade (grade_name) VALUES
    ('Junior'), ('Middle'), ('Senior'), ('Lead'), ('Head');

-- ──────────────────────────────────────────────────────────
-- 4. Таблица фактов — Итоговые начисления (hr_payroll_final)
-- ──────────────────────────────────────────────────────────
DROP TABLE IF EXISTS hr_payroll_final;
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
    adjusted_salary DECIMAL(12,2)  DEFAULT 0.00,   -- base * (1 + seniority/100) * regional
    bonus_amount    DECIMAL(12,2)  DEFAULT 0.00,
    bonus_type      VARCHAR(100),
    total_payout    DECIMAL(12,2)  DEFAULT 0.00,   -- adjusted_salary + bonus
    tax_ndfl        DECIMAL(12,2)  DEFAULT 0.00,   -- total * 0.13
    tax_pension     DECIMAL(12,2)  DEFAULT 0.00,   -- total * 0.22
    tax_medical     DECIMAL(12,2)  DEFAULT 0.00,   -- total * 0.051
    tax_social      DECIMAL(12,2)  DEFAULT 0.00,   -- total * 0.029
    net_payout      DECIMAL(12,2)  DEFAULT 0.00,   -- total - НДФЛ
    employer_cost   DECIMAL(12,2)  DEFAULT 0.00,   -- total + pension + medical + social
    processed_at    TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_payroll_emp   (emp_id),
    INDEX idx_payroll_dept  (department),
    INDEX idx_payroll_grade (grade)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ──────────────────────────────────────────────────────────
-- 5. Таблица фактов — Детализация премий
-- ──────────────────────────────────────────────────────────
DROP TABLE IF EXISTS fact_bonuses;
CREATE TABLE fact_bonuses (
    bonus_id       INT AUTO_INCREMENT PRIMARY KEY,
    emp_id         INT            NOT NULL,
    bonus_type     VARCHAR(100),
    bonus_amount   DECIMAL(12,2),
    bonus_date     DATE,
    comment        VARCHAR(255),
    loaded_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_bonus_emp  (emp_id),
    INDEX idx_bonus_date (bonus_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
