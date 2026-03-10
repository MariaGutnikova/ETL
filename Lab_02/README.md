
# Лабораторная работа №2. Динамические соединения с базами данных

**Цель работы.** Получить практические навыки создания сложного ETL-процесса, включающего динамическую загрузку файлов по HTTP, нормализацию базы данных, обработку дубликатов и настройку обработки ошибок с использованием Pentaho Data Integration (PDI).

## Вариант 2 fGq5CRv6

|№ |Основной фильтр для загрузки в БД	 |Доп. задание 1 (Аналитика)	 |Доп. задание 2 (Аналитика)| 
|-|--------------|------------|----|
|2| Регион: только Central |	Статистика по способам доставки| 	Анализ возвратов|

# Ход работы

## Шаг 1. Подготовка базы данных

Перед запуском ETL-процесса необходимо создать структуру таблиц в вашей базе данных (mgpu_ico_etl_14). Выполните следующий SQL-скрипт через phpMyAdmin или DBeaver:
```SQL
-- 1. Таблица заказов (фактов)
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
 row_id INT PRIMARY KEY,
 order_date DATE,
 ship_date DATE,
 ship_mode VARCHAR(50),
 sales DECIMAL(10,2),
 quantity INT,
 discount DECIMAL(4,2),
 profit DECIMAL(10,2),
 returned TINYINT(1) DEFAULT 0 -- 1 = Yes, 0 = No
);
```
Пример Успешного выполнения:
<img width="1682" height="952" alt="image" src="https://github.com/user-attachments/assets/b5b0cf55-7cc2-4bd7-802b-9ff2b6d30117" />


```
-- 2. Таблица клиентов (измерение)
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
 id INT AUTO_INCREMENT PRIMARY KEY,
 customer_id VARCHAR(20) NOT NULL,
 customer_name VARCHAR(100),
 segment VARCHAR(50),
 country VARCHAR(100),
 city VARCHAR(100),
 state VARCHAR(100),
 postal_code VARCHAR(20),
 region VARCHAR(50),
 INDEX idx_customer_id (customer_id),
 INDEX idx_region (region)
);
```
Пример Успешногго выполнения:
<img width="1678" height="952" alt="image" src="https://github.com/user-attachments/assets/19bc8c4e-e43f-4d93-9ab0-7380c91373cf" />


```
-- 3. Таблица продуктов (измерение)
DROP TABLE IF EXISTS products;
CREATE TABLE products (
 id INT AUTO_INCREMENT PRIMARY KEY,
 product_id VARCHAR(20) NOT NULL,
 category VARCHAR(50),
 sub_category VARCHAR(50),
 product_name VARCHAR(255),
 person VARCHAR(100),
 INDEX idx_product_id (product_id),
 INDEX idx_category (category),
 INDEX idx_subcategory (sub_category)
);
```
Пример Успешногго выполнения:
<img width="1677" height="962" alt="image" src="https://github.com/user-attachments/assets/37291fbc-6059-4bd1-be71-ba42e9eca832" />


```
-- 4. Индексы и настройка кодировки
ALTER TABLE orders ADD INDEX idx_order_date (order_date);
ALTER TABLE orders ADD INDEX idx_ship_date (ship_date);

-- ЗАМЕНИТЕ mgpu_ico_etl_02 на имя вашей базы данных!
ALTER DATABASE mgpu_ico_etl_02 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE customers CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE products CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```
Пример Успешногго выполнения:
<img width="1677" height="945" alt="image" src="https://github.com/user-attachments/assets/98c4ccc8-5aab-49ef-9dd1-3bee6adc6627" />

## Шаг 2. Настройка Job (Главного задания)

**Set Variables: Создайте переменную пути к файлу.**

**Check File Exists: Проверка наличия файла ${CSV_FILE_PATH}.**

**HTTP (Download): Загрузка файла, если его нет.**

**Transformation. Последовательный вызов трех трансформаций для загрузки данных.**

## Шаг 3. Реализация Трансформаций (Transformations)
### Трансформация 1. Load Orders

**Select Values. Установите типы данных (Date format: dd.MM.yyyy для дат, Integer для ID).**

**Memory Group By. Используется для дедупликации (группировка по row_id, взятие первых значений по остальным полям).**

**Filter Rows (Валидация)**
* Условие: order_date IS NOT NULL AND ship_date IS NOT NULL AND reterned = YES.
* TRUE -> Table Output (в таблицу orders).
* FALSE -> Write to Log (логирование ошибок).

**Value Mapper. Преобразование поля Returned: Yes -> 1, No -> 0, Empty -> 0.**

### Трансформация 2. Load Customers

**Select Values. Оставьте только поля, относящиеся к клиенту (customer_id, name, city и т.д.).**

**Memory Group By. Группировка по customer_id (устранение дублей клиентов).**

**Table Output. Загрузка в таблицу customers.**

### Трансформация 3. Load Products

**Select Values. Оставьте поля продукта (product_id, category, name и т.д.).**


**Memory Group By. Группировка по product_id.**


**Table Output. Загрузка в таблицу products.**

## Шаг 4 Выполнение доп заданий



## Проверка данных

### Для проверки наличия записей использовался такой скрипт: SELECT * FROM orders,customers,products LIMIT 100;




### Для проверки количества записей использовался такой запрос: SELECT COUNT(*) FROM orders,customers,products;



# Файлы

[Файл Job]()

[Файл Transformations orders]()

[Файл Transformations products]()

[Файл Transformations customers]()
