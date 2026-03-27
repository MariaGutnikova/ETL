-- ============================================================
-- Вариант 2: HR (Зарплата)
-- Источник: PostgreSQL — Личные данные сотрудников (1 000 000 записей)
-- ============================================================

-- Удаляем таблицу, если существует
DROP TABLE IF EXISTS employees_info;

-- Создание таблицы сотрудников
CREATE TABLE employees_info (
    emp_id       SERIAL PRIMARY KEY,
    last_name    VARCHAR(100)  NOT NULL,
    first_name   VARCHAR(100)  NOT NULL,
    middle_name  VARCHAR(100),
    birth_date   DATE          NOT NULL,
    gender       CHAR(1)       NOT NULL,          -- 'M' / 'F'
    department   VARCHAR(100)  NOT NULL,
    position     VARCHAR(100)  NOT NULL,
    hire_date    DATE          NOT NULL,
    inn          VARCHAR(12)   NOT NULL,           -- ИНН
    snils        VARCHAR(14)   NOT NULL,           -- СНИЛС
    email        VARCHAR(150),
    phone        VARCHAR(20),
    is_active    BOOLEAN       NOT NULL DEFAULT TRUE
);

-- ============================================================
-- Генерация 1 000 000 записей синтетических данных
-- ============================================================
-- Используем DO-блок с generate_series для массовой вставки

DO $$
DECLARE
    -- Справочники
    departments  TEXT[] := ARRAY[
        'Бухгалтерия', 'IT-отдел', 'Отдел кадров', 'Отдел продаж',
        'Маркетинг', 'Логистика', 'Юридический отдел', 'Производство',
        'Контроль качества', 'Финансовый отдел', 'Служба безопасности',
        'Административный отдел', 'Отдел закупок', 'R&D', 'Техническая поддержка'
    ];

    positions_map TEXT[][] := ARRAY[
        ARRAY['Стажёр', 'Специалист', 'Старший специалист', 'Ведущий специалист',
              'Руководитель группы', 'Начальник отдела', 'Заместитель директора', 'Директор']
    ];

    last_names_m TEXT[] := ARRAY[
        'Иванов', 'Петров', 'Сидоров', 'Козлов', 'Новиков',
        'Морозов', 'Волков', 'Алексеев', 'Лебедев', 'Семёнов',
        'Егоров', 'Павлов', 'Кузнецов', 'Степанов', 'Николаев',
        'Орлов', 'Андреев', 'Макаров', 'Никитин', 'Захаров',
        'Зайцев', 'Соловьёв', 'Борисов', 'Яковлев', 'Григорьев',
        'Романов', 'Воробьёв', 'Сергеев', 'Кузьмин', 'Фролов',
        'Дмитриев', 'Тарасов', 'Белов', 'Комаров', 'Поляков',
        'Киселёв', 'Медведев', 'Гусев', 'Титов', 'Антонов'
    ];

    last_names_f TEXT[] := ARRAY[
        'Иванова', 'Петрова', 'Сидорова', 'Козлова', 'Новикова',
        'Морозова', 'Волкова', 'Алексеева', 'Лебедева', 'Семёнова',
        'Егорова', 'Павлова', 'Кузнецова', 'Степанова', 'Николаева',
        'Орлова', 'Андреева', 'Макарова', 'Никитина', 'Захарова',
        'Зайцева', 'Соловьёва', 'Борисова', 'Яковлева', 'Григорьева',
        'Романова', 'Воробьёва', 'Сергеева', 'Кузьмина', 'Фролова',
        'Дмитриева', 'Тарасова', 'Белова', 'Комарова', 'Полякова',
        'Киселёва', 'Медведева', 'Гусева', 'Титова', 'Антонова'
    ];

    first_names_m TEXT[] := ARRAY[
        'Александр', 'Дмитрий', 'Максим', 'Сергей', 'Андрей',
        'Алексей', 'Артём', 'Илья', 'Кирилл', 'Михаил',
        'Никита', 'Матвей', 'Роман', 'Егор', 'Арсений',
        'Иван', 'Денис', 'Евгений', 'Даниил', 'Тимофей',
        'Владислав', 'Игорь', 'Владимир', 'Павел', 'Руслан',
        'Марк', 'Константин', 'Тимур', 'Олег', 'Ярослав'
    ];

    first_names_f TEXT[] := ARRAY[
        'Анна', 'Мария', 'Елена', 'Дарья', 'Алиса',
        'Полина', 'Виктория', 'Екатерина', 'Наталья', 'Ольга',
        'Татьяна', 'Ирина', 'Марина', 'Светлана', 'Юлия',
        'Анастасия', 'Вера', 'Людмила', 'Галина', 'Ксения',
        'Валентина', 'Надежда', 'Лариса', 'Тамара', 'Софья',
        'Варвара', 'Диана', 'Карина', 'Маргарита', 'Евгения'
    ];

    middle_names_m TEXT[] := ARRAY[
        'Александрович', 'Дмитриевич', 'Сергеевич', 'Андреевич', 'Алексеевич',
        'Михайлович', 'Иванович', 'Евгеньевич', 'Николаевич', 'Владимирович',
        'Петрович', 'Олегович', 'Павлович', 'Игоревич', 'Романович'
    ];

    middle_names_f TEXT[] := ARRAY[
        'Александровна', 'Дмитриевна', 'Сергеевна', 'Андреевна', 'Алексеевна',
        'Михайловна', 'Ивановна', 'Евгеньевна', 'Николаевна', 'Владимировна',
        'Петровна', 'Олеговна', 'Павловна', 'Игоревна', 'Романовна'
    ];

    positions TEXT[] := ARRAY[
        'Стажёр', 'Специалист', 'Старший специалист', 'Ведущий специалист',
        'Руководитель группы', 'Начальник отдела', 'Заместитель директора', 'Директор'
    ];

    batch_size  INT := 10000;
    total       INT := 1000000;
    i           INT;
    g           CHAR(1);
    rnd         DOUBLE PRECISION;
BEGIN
    -- Вставляем данные батчами через generate_series
    FOR batch_start IN 0 .. (total / batch_size - 1) LOOP
        INSERT INTO employees_info (
            last_name, first_name, middle_name, birth_date, gender,
            department, position, hire_date, inn, snils,
            email, phone, is_active
        )
        SELECT
            CASE WHEN random() < 0.5
                THEN last_names_m[1 + floor(random() * array_length(last_names_m, 1))::INT]
                ELSE last_names_f[1 + floor(random() * array_length(last_names_f, 1))::INT]
            END AS last_name,
            CASE WHEN random() < 0.5
                THEN first_names_m[1 + floor(random() * array_length(first_names_m, 1))::INT]
                ELSE first_names_f[1 + floor(random() * array_length(first_names_f, 1))::INT]
            END AS first_name,
            CASE WHEN random() < 0.5
                THEN middle_names_m[1 + floor(random() * array_length(middle_names_m, 1))::INT]
                ELSE middle_names_f[1 + floor(random() * array_length(middle_names_f, 1))::INT]
            END AS middle_name,
            -- Дата рождения: 1960-01-01 .. 2002-12-31
            DATE '1960-01-01' + (floor(random() * 15706))::INT AS birth_date,
            -- Пол
            CASE WHEN random() < 0.5 THEN 'M' ELSE 'F' END AS gender,
            -- Отдел
            departments[1 + floor(random() * array_length(departments, 1))::INT] AS department,
            -- Должность
            positions[1 + floor(random() * array_length(positions, 1))::INT] AS position,
            -- Дата приёма: 2005-01-01 .. 2025-12-31
            DATE '2005-01-01' + (floor(random() * 7670))::INT AS hire_date,
            -- ИНН (12 цифр)
            LPAD((floor(random() * 1000000000000)::BIGINT)::TEXT, 12, '0') AS inn,
            -- СНИЛС (формат XXX-XXX-XXX XX)
            LPAD((floor(random() * 1000))::TEXT, 3, '0') || '-' ||
            LPAD((floor(random() * 1000))::TEXT, 3, '0') || '-' ||
            LPAD((floor(random() * 1000))::TEXT, 3, '0') || ' ' ||
            LPAD((floor(random() * 100))::TEXT, 2, '0') AS snils,
            -- Email
            'emp' || (batch_start * batch_size + gs)::TEXT || '@company.ru' AS email,
            -- Телефон
            '+7' || LPAD((floor(random() * 10000000000)::BIGINT)::TEXT, 10, '0') AS phone,
            -- Активность (95% активны)
            CASE WHEN random() < 0.95 THEN TRUE ELSE FALSE END AS is_active
        FROM generate_series(1, batch_size) AS gs;

        -- Прогресс
        RAISE NOTICE 'Вставлено % записей', (batch_start + 1) * batch_size;
    END LOOP;
END $$;

-- Создаём индексы для ускорения JOIN-ов
CREATE INDEX idx_employees_department ON employees_info (department);
CREATE INDEX idx_employees_active     ON employees_info (is_active);
CREATE INDEX idx_employees_hire_date  ON employees_info (hire_date);

-- Проверка количества
SELECT COUNT(*) AS total_employees FROM employees_info;
SELECT department, COUNT(*) AS cnt
FROM employees_info
GROUP BY department
ORDER BY cnt DESC
LIMIT 15;
