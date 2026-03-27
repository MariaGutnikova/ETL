# Лабораторная работа №4. Анализ и обработка больших данных с Dask (ETL-пайплайн)

**Вариант 2**: `Parking_Violations_Issued_-_Fiscal_Year_2015.csv` (2.8 ГБ)

**Цель работы:** изучить инструменты Dask для обработки Big Data, освоить построение ETL-пайплайнов с «ленивыми вычислениями» и визуализировать графы выполнения задач (DAG).

## Шаг 1. Extract (Извлечение данных)

Для работы с файлом объемом 2.8 ГБ (около 11 млн строк) используется `dask.dataframe`. Настраиваем локальный кластер для параллельной обработки.

```python
# Словарь типов данных для NYC Parking Violations 2015
dtype_dict = {
    'Summons Number': 'int64',
    'Plate ID': 'object',
    'Registration State': 'object',
    'Plate Type': 'object',
    'Issue Date': 'object',
    'Violation Code': 'int64',
    'Vehicle Body Type': 'object',
    'Vehicle Make': 'object',
    'Issuing Agency': 'object',
    'Issuer Code': 'int64',
    'Issuer Command': 'object',
    'Issuer Squad': 'object',
    'Violation Time': 'object',
    'Violation County': 'object',
    'Violation In Existence Only Flag': 'object',
    'House Number': 'object',
    'Street Name': 'object',
    'Intersecting Street': 'object',
    'Sub Division': 'object',
    'Violation Legal Code': 'object',
    'Days Parking In Effect    ': 'object',
    'From Hours In Effect': 'object',
    'To Hours In Effect': 'object',
    'Vehicle Color': 'object',
    'Meter Number': 'object',
    'Violation Post Code': 'object',
    'Violation Description': 'object'
}

# Укажите путь к файлу. Для Colab это обычно '/content/ИМЯ_ФАЙЛА.csv'
filename = 'Parking_Violations_Issued_-_Fiscal_Year_2015.csv'

if not os.path.exists(filename):
    print(f"ВНИМАНИЕ: Файл {filename} не найден в текущей директории Colab.")

# Чтение CSV (Ленивая загрузка)
df = dd.read_csv(filename, dtype=dtype_dict, blocksize="64MB", on_bad_lines='skip', assume_missing=True)

# Просмотр метаданных (структуры)
df
```
Результат:
<img width="1905" height="987" alt="image" src="https://github.com/user-attachments/assets/21a1f129-caa0-42ca-8626-2a02f0c83129" />
<img width="1913" height="1027" alt="image" src="https://github.com/user-attachments/assets/4fef487f-f340-4f17-9075-e944b63490e5" />
<img width="1893" height="988" alt="image" src="https://github.com/user-attachments/assets/847ff71c-ea8a-4b54-9ec2-300e2dc077be" />
<img width="1403" height="839" alt="image" src="https://github.com/user-attachments/assets/27f2a6ca-587d-4943-a77c-381c021a08ed" />


## Шаг 2. Transform (Трансформация и очистка данных)

Проведено профилирование качества данных (подсчет пропусков). Столбцы с пропуском более 55% удаляются. Также удаляются технические столбцы, не несущие смысловой нагрузки для анализа.

```python
missing_values = df.isnull().sum()
mysize = df.index.size
missing_count_percent = (missing_values / mysize) * 100

with ProgressBar():
    missing_percent_computed = missing_count_percent.compute()

columns_to_drop = list(missing_percent_computed[missing_percent_computed > 60].index)
df_dropped = df.drop(columns=columns_to_drop)
df_dropped.head()
```

**Результат очистки:**
<img width="1919" height="1038" alt="image" src="https://github.com/user-attachments/assets/7712ba7c-e624-474a-a5b8-02ab22dca35c" />


## Шаг 3. Load (Загрузка / Сохранение результатов)

Очищенный датасет сохраняется в формате Parquet, который является стандартом де-факто для больших данных благодаря колоночному хранению и высокой скорости чтения/записи в Dask.

```python
output_path = '/content/drive/MyDrive/cleaned_data_2015_v2.parquet'
with ProgressBar():
    df_dropped.to_parquet(output_path, engine='pyarrow', overwrite=True)

csv_output = '/content/drive/MyDrive/cleaned_violations_2015_v2.csv'
with ProgressBar():
    df_dropped.to_csv(csv_output, single_file=True, index=False)

print("Данные сохранены в Parquet и CSV.")
```
Результат:
<img width="1412" height="369" alt="image" src="https://github.com/user-attachments/assets/a7269ea2-fd52-46a6-bcc1-2fc118b62bd7" />

## Визуализация DAG

### 1. Простой граф (Аналитика марок ТС)

Граф визуализирует процесс подсчета общего числа строк, уникальных марок автомобилей и вычисление среднего значения нарушений на марку.

```python
from dask import delayed
from IPython.display import Image
import dask.dataframe as dd

# Функции для анализа
def get_total_violations():
    return len(df_dropped)

def get_unique_makes():
    # Вычисляем уникальные марки
    return df_dropped['Vehicle Make'].nunique().compute()

def avg_violations_per_make(total, unique):
    if unique == 0: 
        return 0
    return round(total / unique, 2)

# Создаем узлы DAG
x = delayed(get_total_violations)()
y = delayed(get_unique_makes)()
z = delayed(avg_violations_per_make)(x, y)

# Визуализация
try:
    # Визуализируем граф
    z.visualize(filename='simple_violation_analysis.png', rankdir='TB')
    display(Image('simple_violation_analysis.png'))
    print("Граф сохранен как 'simple_violation_analysis.png'")
except Exception as e:
    print(f"Graphviz ошибка: {e}")
    print("Для визуализации установите graphviz: !apt-get install graphviz")

print("Результат вычисления DAG:", z.compute())
print(f"Среднее количество нарушений на марку: {z.compute()}")
```
Результат: 

<img width="491" height="774" alt="simple_violation_analysis" src="https://github.com/user-attachments/assets/5bcac103-24f1-49c7-a685-d4cdb89fa678" />
<img width="1306" height="220" alt="image" src="https://github.com/user-attachments/assets/abe024b4-188c-45cc-b1af-d8d5e07e56f6" />

### 2. Сложный граф (Анализ по районам NYC)

Построение многоуровневого графа для анализа доли нарушений в часы пик (8:00 - 10:00) в разрезе округов (Violation County).

```python
from dask import delayed
from IPython.display import Image
import pandas as pd

# Список районов для анализа
districts = ['NY', 'K', 'Q', 'BX', 'R']
district_names = {
    'NY': 'Manhattan',
    'K': 'Brooklyn', 
    'Q': 'Queens',
    'BX': 'Bronx',
    'R': 'Staten Island'
}

def load_district_data(district):
    """Загрузка данных по району"""
    return df_dropped[df_dropped['Violation County'] == district]

def count_violations(district_data):
    """Подсчет общего количества нарушений"""
    if district_data is None or len(district_data) == 0:
        return 0
    return len(district_data)

def count_peak_hours(district_data):
    """Подсчет нарушений в часы пик (8-10 утра)"""
    if district_data is None or len(district_data) == 0:
        return 0
    
    # Извлекаем часы из Violation Time
    # Преобразуем в строку и берем первые 2 символа
    hours = district_data['Violation Time'].astype(str).str[:2]
    
    # Часы пик: 08, 09, 10
    peak_hours = hours[hours.isin(['08', '09', '10'])]
    return len(peak_hours)

def calculate_peak_percentage(total, peak):
    """Расчет процента нарушений в часы пик"""
    if total == 0:
        return 0
    return round((peak / total) * 100, 2)

# Создаем многоуровневый DAG
# Уровень 1: Загрузка данных по каждому району
layer1 = [delayed(load_district_data)(d) for d in districts]

# Уровень 2: Параллельный подсчет нарушений
layer2 = [delayed(count_violations)(d) for d in layer1]

# Уровень 3: Параллельный подсчет нарушений в часы пик
layer3 = [delayed(count_peak_hours)(d) for d in layer1]

# Уровень 4: Расчет процентов
layer4 = [delayed(calculate_peak_percentage)(t, p) for t, p in zip(layer2, layer3)]

# Собираем результаты
results = delayed(list)(layer4)

# Визуализация сложного графа
try:
    # Сохраняем граф с иерархической структурой
    results.visualize(
        filename='complex_district_analysis.png',
        rankdir='TB',  # Top to Bottom orientation
        color='lightblue',
        fontsize='10'
    )
    display(Image('complex_district_analysis.png'))
    print("Сложный граф сохранен как 'complex_district_analysis.png'")
except Exception as e:
    print(f"Graphviz ошибка: {e}")

# Вычисляем результаты
final_results = results.compute()

print("\n=== Результаты анализа по районам ===")
print("Район\t\tОбщее количество\tЧасы пик (%)\t\tРайон")
print("-" * 70)
for i, district in enumerate(districts):
    district_name = district_names.get(district, district)
    total = layer2[i].compute()  # Вычисляем общее количество
    peak_percent = final_results[i]
    
    print(f"{district_name:<15}\t{total:<16}\t{peak_percent}%\t\t{district}")

# Дополнительный анализ: находим район с наибольшим процентом нарушений в часы пик
max_idx = final_results.index(max(final_results))
print(f"\nНаибольший процент нарушений в часы пик: {district_names[districts[max_idx]]} ({final_results[max_idx]}%)")
```
Результат:

<img width="2318" height="1345" alt="image" src="https://github.com/user-attachments/assets/386b199f-81ad-449f-8456-7f6b9552de2c" />

## Аналитика
```python
import altair as alt
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Включаем режим работы с большими данными Altair
alt.data_transformers.enable('default', max_rows=None)

print(f"Всего записей: {len(df_viz):,}")

# График 1: Топ-10 марок (Altair)
top_makes = df_viz['Vehicle Make'].value_counts().reset_index().head(10)
top_makes.columns = ['Make', 'Count']

chart1 = alt.Chart(top_makes).mark_bar().encode(
    x=alt.X('Count:Q', title='Количество нарушений'),
    y=alt.Y('Make:N', sort='-x', title='Марка автомобиля'),
    color='Count:Q',
    tooltip=['Make', 'Count']
).properties(title='Топ-10 марок автомобилей по количеству нарушений', width=600)

display(chart1)

# График 2: Нарушения по дням недели (Altair с агрегацией)
daily_counts = df_viz.groupby('Day of Week').size().reset_index(name='count')
daily_counts['Day of Week'] = pd.Categorical(
    daily_counts['Day of Week'], 
    categories=day_order, 
    ordered=True
)
daily_counts = daily_counts.sort_values('Day of Week')

chart2 = alt.Chart(daily_counts).mark_bar().encode(
    x=alt.X('Day of Week:N', title='День недели'),
    y=alt.Y('count:Q', title='Количество нарушений'),
    color='Day of Week:N',
    tooltip=['Day of Week', 'count']
).properties(title='Распределение нарушений по дням недели', width=600)

display(chart2)

# График 3: Топ-10 типов нарушений (Altair)
top_violations = df_viz['Violation Description'].value_counts().reset_index().head(10)
top_violations.columns = ['Violation', 'Count']

chart3 = alt.Chart(top_violations).mark_bar().encode(
    x=alt.X('Count:Q', title='Количество нарушений'),
    y=alt.Y('Violation:N', sort='-x', title='Тип нарушения'),
    color='Count:Q',
    tooltip=['Violation', 'Count']
).properties(title='Топ-10 типов нарушений', width=600)

display(chart3)

# График 4: Дополнительный - распределение по часам (matplotlib)
plt.figure(figsize=(12, 6))

# Извлекаем часы из Violation Time
hours = pd.to_numeric(df_viz['Violation Time'].astype(str).str[:2], errors='coerce')
hours = hours.dropna()
hour_counts = hours.value_counts().sort_index()

plt.bar(hour_counts.index, hour_counts.values, color='lightgreen', edgecolor='darkgreen', alpha=0.7)
plt.xlabel('Час дня')
plt.ylabel('Количество нарушений')
plt.title('Распределение нарушений по часам')
plt.xticks(range(0, 24))
plt.grid(axis='y', alpha=0.3)

# Выделяем часы пик (8-10)
peak_hours = [8, 9, 10]
for hour in peak_hours:
    if hour in hour_counts.index:
        plt.bar(hour, hour_counts[hour], color='orange', edgecolor='darkorange', alpha=0.7)

plt.legend(['Обычные часы', 'Часы пик (8-10)'], loc='upper right')
plt.tight_layout()
plt.show()

print("\n📊 Статистика:")
print(f"Общее количество нарушений: {len(df_viz):,}")
print(f"Среднее количество нарушений в день: {len(df_viz)/7:.0f}")
print(f"Наиболее активный день: {daily_counts['count'].idxmax()} ({daily_counts['count'].max():,} нарушений)")
print(f"Наименее активный день: {daily_counts['count'].idxmin()} ({daily_counts['count'].min():,} нарушений)")
```
<img width="913" height="323" alt="image" src="https://github.com/user-attachments/assets/79b86ae2-e8d8-4eca-b741-823b527586ac" />
<img width="1046" height="323" alt="image" src="https://github.com/user-attachments/assets/b3dac54f-afc8-49ce-86a9-46cf32c1e2de" />
<img width="1189" height="590" alt="image" src="https://github.com/user-attachments/assets/b6dd4ecb-b2dc-430a-be2c-c66ae64e1758" />





