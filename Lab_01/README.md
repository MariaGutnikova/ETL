# Лабораторная работа №1. Установка и настройка ETL-инструмента. Создание конвейеров данных

**Цель работы.** Изучение основных принципов работы с ETL-инструментами на примере Pentaho Data Integration (PDI), настройка среды, создание конвейера обработки данных (фильтрация, очистка, замена значений) и выгрузка результатов в базу данных MySQL.

# Описание данных


# Задание на лабораторную работу
### Общая задача
1.  Выбрать вариант задания из таблицы ниже.
2.  Скачать CSV-датасет (если ссылка Kaggle недоступна — использовать VPN, найти зеркало, сгенерировать синтетические данные или использовать анонимизированные рабочие данные).
3.  Скачать шаблоны конвейеров для примера: [GitHub Repository](https://github.com/BosenkoTM/workshop-on-ETL/tree/main/lectures/L_01).
4.  Создать трансформацию (`.ktr`), реализующую:
    *   **CSV File Input.** Чтение данных.
    *   **Filter Rows / Value Mapper / String Operations.** Очистка данных, фильтрация битых записей, замена значений.
    *   **Table Output.** Загрузка очищенных данных в таблицу MySQL в базе `mgpu_ico_etl_XX`.
5.  Проверить результат SQL-запросом через phpMyAdmin.
### Варианты заданий
| 02 | 	**E-commerce:** очистка данных о заказах, сегментация клиентов. | [E-coomerce Dataset](https://www.kaggle.com/datasets/carrie1/ecommerce-data)

# Ход работы

## Созданный конвейер в Spoon (общий вид)
<img width="1850" height="982" alt="image" src="https://github.com/user-attachments/assets/54097406-cafa-48b3-a4be-4b8d5acf9759" />

## Настройки ключевых шагов (Input, Filter, Output)
Input:
<img width="1232" height="733" alt="image" src="https://github.com/user-attachments/assets/90029d5d-8536-4301-bd92-0fa9c3f53543" />

Group by:
<img width="1129" height="823" alt="image" src="https://github.com/user-attachments/assets/ae396616-e186-45f6-8eb8-e06f920b0364" />

Filter:
<img width="1094" height="729" alt="image" src="https://github.com/user-attachments/assets/802c2a5a-c13c-4b50-9719-f8be61ff2954" />

Output:
<img width="942" height="725" alt="image" src="https://github.com/user-attachments/assets/158663cc-6cd4-40be-95ab-141898672d85" />


## SQL-запросы, использованные для проверки загрузки данных, и скриншот результата SELECT из phpMyAdmin

### Для проверки наличия записей использовался такой скрипт: SELECT * FROM `invoices` LIMIT 100;
<img width="1682" height="955" alt="image" src="https://github.com/user-attachments/assets/8ac154f4-519e-46b8-af32-c5fd4f5572a1" />

### Для проверки количества записей использовался такой запрос: SELECT COUNT(*) FROM stock_prices;
<img width="1678" height="950" alt="image" src="https://github.com/user-attachments/assets/b67f17a8-b345-45ac-b09a-d688dec844a7" />


## Файлы:
[Исходный CSV](https://cloud.mail.ru/public/GUeW/Sb3pvkD8u)

[Файл трансформации](Lab_01/Lab_01.ktr)
