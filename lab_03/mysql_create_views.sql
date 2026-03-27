-- ============================================================
-- Вариант 2: HR (Зарплата)
-- Аналитические витрины (Views) — Business Layer (MySQL)
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- VIEW 1: Сводный отчёт по начислениям по отделам
-- ──────────────────────────────────────────────────────────
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


-- ──────────────────────────────────────────────────────────
-- VIEW 2: Сводный отчёт по грейдам
-- ──────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW view_payroll_by_grade AS
SELECT
    grade,
    COUNT(*)                          AS employees_count,
    ROUND(MIN(base_salary), 2)        AS min_salary,
    ROUND(AVG(base_salary), 2)        AS avg_salary,
    ROUND(MAX(base_salary), 2)        AS max_salary,
    ROUND(AVG(bonus_amount), 2)       AS avg_bonus,
    ROUND(AVG(total_payout), 2)       AS avg_total_payout,
    ROUND(AVG(tax_ndfl), 2)           AS avg_ndfl,
    ROUND(AVG(net_payout), 2)         AS avg_net_payout
FROM hr_payroll_final
GROUP BY grade
ORDER BY avg_salary DESC;


-- ──────────────────────────────────────────────────────────
-- VIEW 3: Налоговая нагрузка — детализация по отделам
-- ──────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW view_tax_burden AS
SELECT
    department,
    COUNT(*)                                                  AS emp_count,
    ROUND(SUM(total_payout), 2)                               AS gross_payout,
    ROUND(SUM(tax_ndfl), 2)                                   AS ndfl_13pct,
    ROUND(SUM(tax_pension), 2)                                AS pension_22pct,
    ROUND(SUM(tax_medical), 2)                                AS medical_5_1pct,
    ROUND(SUM(tax_social), 2)                                 AS social_2_9pct,
    ROUND(SUM(tax_ndfl + tax_pension + tax_medical + tax_social), 2) AS total_taxes,
    ROUND(SUM(employer_cost), 2)                              AS full_employer_cost,
    ROUND(
        SUM(tax_ndfl + tax_pension + tax_medical + tax_social) /
        NULLIF(SUM(total_payout), 0) * 100, 2
    )                                                         AS tax_burden_pct
FROM hr_payroll_final
GROUP BY department
ORDER BY tax_burden_pct DESC;


-- ──────────────────────────────────────────────────────────
-- VIEW 4: Топ сотрудников по выплатам
-- ──────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW view_top_earners AS
SELECT
    emp_id,
    full_name,
    department,
    position,
    grade,
    base_salary,
    bonus_amount,
    total_payout,
    net_payout,
    employer_cost
FROM hr_payroll_final
ORDER BY total_payout DESC
LIMIT 100;


-- ──────────────────────────────────────────────────────────
-- VIEW 5: Анализ премий по типам
-- ──────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW view_bonus_analysis AS
SELECT
    bonus_type,
    COUNT(*)                        AS bonus_count,
    ROUND(MIN(bonus_amount), 2)     AS min_bonus,
    ROUND(AVG(bonus_amount), 2)     AS avg_bonus,
    ROUND(MAX(bonus_amount), 2)     AS max_bonus,
    ROUND(SUM(bonus_amount), 2)     AS total_bonus
FROM fact_bonuses
GROUP BY bonus_type
ORDER BY total_bonus DESC;


-- ──────────────────────────────────────────────────────────
-- VIEW 6: Отчёт для бизнес-пользователя (итоговая витрина)
-- ──────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW view_analytics_report AS
SELECT
    p.department,
    p.grade,
    COUNT(*)                          AS employees_count,
    ROUND(SUM(p.base_salary), 2)      AS total_base_salary,
    ROUND(SUM(p.adjusted_salary), 2)  AS total_adjusted_salary,
    ROUND(SUM(p.bonus_amount), 2)     AS total_bonuses,
    ROUND(SUM(p.total_payout), 2)     AS total_gross,
    ROUND(SUM(p.net_payout), 2)       AS total_net,
    ROUND(SUM(p.employer_cost), 2)    AS total_employer_cost,
    ROUND(AVG(p.net_payout), 2)       AS avg_net_per_employee
FROM hr_payroll_final p
GROUP BY p.department, p.grade
ORDER BY p.department, p.grade;
