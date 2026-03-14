
# Лабораторная работа №2. Динамические соединения с базами данных

**Цель работы.** Получить практические навыки создания сложного ETL-процесса, включающего динамическую загрузку файлов по HTTP, нормализацию базы данных, обработку дубликатов и настройку обработки ошибок с использованием Pentaho Data Integration (PDI).

## Вариант 2

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
<img width="1852" height="987" alt="image" src="https://github.com/user-attachments/assets/62c994e7-8dc5-4be0-8f1d-e8ad00a259c2" />


**Check File Exists: Проверка наличия файла ${CSV_FILE_PATH}.**
<img width="1848" height="985" alt="image" src="https://github.com/user-attachments/assets/169d60a6-a06e-45f5-9771-745d702ab17b" />

**HTTP (Download): Загрузка файла, если его нет.**
<img width="1853" height="985" alt="image" src="https://github.com/user-attachments/assets/2accc96c-a455-416f-b977-4b0e47a19bba" />

**Transformation. Последовательный вызов трех трансформаций для загрузки данных.**
<img width="1626" height="893" alt="image" src="https://github.com/user-attachments/assets/d1612c55-e61b-4810-9f4d-7665e7bcbd17" />

## Шаг 3. Реализация Трансформаций (Transformations)
### Трансформация 1. Load Orders

**Select Values. Установите типы данных (Date format: dd.MM.yyyy для дат, Integer для ID).**
<img width="1853" height="990" alt="image" src="https://github.com/user-attachments/assets/dde45c57-5a72-4666-bb72-75783c6050f8" />

**Memory Group By. Используется для дедупликации (группировка по row_id, взятие первых значений по остальным полям).**
<img width="1856" height="997" alt="image" src="https://github.com/user-attachments/assets/b8527948-a064-4486-a15c-bcd36ed2d326" />

**Filter Rows (Валидация)**
* Условие: order_date IS NOT NULL AND ship_date IS NOT NULL AND region = Central.
* TRUE -> Table Output (в таблицу orders).
* FALSE -> Write to Log (логирование ошибок).
<img width="1084" height="718" alt="image" src="https://github.com/user-attachments/assets/3deab2a8-7f4a-43b7-aed9-f7b10b135cf7" />

**Value Mapper. Преобразование поля Returned: Yes -> 1, No -> 0, Empty -> 0.**
<img width="852" height="362" alt="image" src="https://github.com/user-attachments/assets/6770e134-035c-47b1-bfb8-e0f518aa4cae" />

### Трансформация 2. Load Customers

**Select Values. Оставьте только поля, относящиеся к клиенту (customer_id, name, city и т.д.).**
<img width="1552" height="812" alt="image" src="https://github.com/user-attachments/assets/d7158789-96e3-46fb-9470-f0557fea6961" />

**Memory Group By. Группировка по customer_id (устранение дублей клиентов).**
<img width="1134" height="835" alt="image" src="https://github.com/user-attachments/assets/b7a293bd-36cc-409a-ba94-3833a77070fb" />

**Table Output. Загрузка в таблицу customers.**
<img width="936" height="712" alt="image" src="https://github.com/user-attachments/assets/caabc273-0d46-4d9d-a960-03494fb22d29" />

### Трансформация 3. Load Products

**Select Values. Оставьте поля продукта (product_id, category, name и т.д.).**
<img width="1547" height="812" alt="image" src="https://github.com/user-attachments/assets/cc8114d9-adfd-4a2f-95ab-69d34f9cc719" />

**Memory Group By. Группировка по product_id.**
<img width="1853" height="986" alt="image" src="https://github.com/user-attachments/assets/20d0c068-c442-4aa5-8df7-86fa40aeeb38" />

**Table Output. Загрузка в таблицу products.**
<img width="1852" height="977" alt="image" src="https://github.com/user-attachments/assets/413c5fcc-2b33-40da-a575-acfd0e86b906" />

## Шаг 4 Выполнение доп заданий
Общиий вид:
<img width="1535" height="559" alt="image" src="https://github.com/user-attachments/assets/14610d1e-4d23-4f9d-8974-20ccfb642eee" />

Выполнение задания 1:
<img width="1847" height="983" alt="image" src="https://github.com/user-attachments/assets/e0b730ef-e40e-4d3d-96c7-8b20e11e0010" />

Общиий вид 2:
<img width="1844" height="987" alt="image" src="https://github.com/user-attachments/assets/1c3622f5-0df5-4cc7-8b86-e1455f2ff015" />

Выполнение задания 2:
<img width="1845" height="991" alt="image" src="https://github.com/user-attachments/assets/925aa0ab-3449-4faa-b9ec-287971fb812b" />


## Проверка данных

### Для проверки наличия записей использовался такой скрипт: SELECT * FROM orders,customers,products LIMIT 100;




### Для проверки количества записей использовался такой запрос: SELECT COUNT(*) FROM orders,customers,products;



# Файлы

[Файл Job](Files/Job%20CSV.kjb)

[Файл Transformations orders](Files/lab_2_1_csv_orders.ktr)

[Файл Transformations products](Files/lab_2_2_csv_to_Customers.ktr)

[Файл Transformations customers](Files/lab_2_3_csv_to_products.ktr)

[Файл задания 1](/Files/zadanie_1.ktr)

[Файл задания 2](Files/zadanie_2.ktr)
