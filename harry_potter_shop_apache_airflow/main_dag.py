# Получить ответ на вопрос, Сколько продаж по каждому дому приходится на определенную дату и указать наиболее свежий верштамп для каждой строки
import os
import requests
import pandas as pd
import dask.dataframe as dd
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook

# region Переменные
# Путь где лежит файл в котором храним наиболее свежий verstamp
file_path_key_verstamp = "/usr/local/airflow/dags/harry_potter_shop_apache_airflow/key_verstamp/key_verstamp.csv"
# Путь где лежит файл в котором сохранем результат работы процедуры
file_path_house_sales = "/usr/local/airflow/dags/harry_potter_shop_apache_airflow/house_sales/house_sales"
# Путь где лежат данные для исходного файла
file_path_sales = "/usr/local/airflow/dags/harry_potter_shop_apache_airflow/sales.csv"
# Url по которому обращаемся к API
url_api_hp = "https://hp-api.onrender.com/api"


# endregion

# region Функции
# Функция проверки существует ли файл который хранит актуальный
# is_restart - позволяет пересоздать key_verstamp.csv принудительно и перезапустить логику
def create_file_key_verstamp(path, is_restart, **kwargs):
    if not os.path.exists(path) or is_restart:
        # создаём папку, если её нет
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # создаём CSV с дефолтным verstamp
        pd.DataFrame({"verstamp": ["0x00000000"]}).to_csv(path, index=False)


def compute_house_sales(path_verstamp, path_sales, path_house_sales, url_api, **kwargs):
    # Читаем старый verstamp
    key_verstamp_old = dd.read_csv(path_verstamp)
    key_verstamp_old = key_verstamp_old["verstamp"].max().compute()

    # Читаем файл и фильтруем по key_verstamp_old
    sales = dd.read_csv(path_sales)
    sales = sales[sales["verstamp"] > key_verstamp_old].compute()

    key_verstamp_new = sales[sales["verstamp"] > key_verstamp_old].max()

    # Проверяем пустой ли DataFrame у нас на запись дальнейшую
    if not sales.empty:
        # Загружаем JSON из API
        url_api = url_api + "/characters"
        response = requests.get(url_api)
        characters = response.json()
        characters = pd.DataFrame(characters)
        characters = characters[["id", "name", "house"]]

        # переименовываем чтобы дублированных столбцов не образовывалось
        characters = characters.rename(columns={"id": "character_id"})
        result = sales.merge(characters, on="character_id", how="left")
        # result = duckdb.sql("""
        #                                         SELECT
        #                                         "sale_date",
        #                                         "house",
        #                                         SUM("amount") AS "amount",
        #                                         MAX("verstamp") AS "last_verstamp"
        #                                         FROM result
        #                                         GROUP BY "sale_date", "house"
        #                                         ORDER BY "sale_date", "house"
        #                                         """).df()
        result = result.groupby(['sale_date', 'house'], as_index=False).agg({
            'amount': 'sum',
            'verstamp': 'max'
        }).rename(columns={'verstamp': 'last_verstamp'})
        result = result.sort_values(['sale_date', 'house'])

        path_house_sales = path_house_sales + "_" + key_verstamp_new["verstamp"] + ".csv"

        if not os.path.exists(path_house_sales):
            # создаём папку, если её нет
            os.makedirs(os.path.dirname(path_house_sales), exist_ok=True)

        # создаём CSV с результатом запроса
        result.to_csv(path_house_sales, index=False)


def update_actual_key_verstamp(path_verstamp, path_sales, **kwargs):
    # Читаем старый verstamp
    key_verstamp_old = dd.read_csv(path_verstamp)
    key_verstamp_old = key_verstamp_old["verstamp"].max().compute()

    # Вычисляем наиболее актуальный key_verstamp_new текущей выгрузки и записываем его в key_verstamp.csv
    sales = dd.read_csv(path_sales)
    key_verstamp_new = sales[sales["verstamp"] > key_verstamp_old].compute()
    key_verstamp_new = key_verstamp_new["verstamp"].max()
    pd.DataFrame({"verstamp": [key_verstamp_new]}).to_csv(path_verstamp, index=False)


# endregion

# region запуск без AirFlow
# is_restart - позволяет пересоздать key_verstamp.csv принудительно и перезапустить логику
# create_file_key_verstamp(path=file_path_key_verstamp, is_restart=True)
# compute_house_sales(path_house_sales=file_path_house_sales
#                     , path_verstamp=file_path_key_verstamp
#                     , path_sales=file_path_sales
#                     , url_api=url_api_hp
#                     )
# update_actual_key_verstamp(path_verstamp=file_path_key_verstamp
#                            , path_sales=file_path_sales
#                            )
# endregion

# region Часть AirFlow
dag = DAG(
    'harry_potter_shop_apache_airflow',
    schedule_interval='@daily',  # Запуск каждый день
    start_date=datetime(2025, 9, 27)
)

# Операторы для выполнения шагов
task_create_file_key_verstamp = PythonOperator(
    task_id='create_file_key_verstamp',
    python_callable=create_file_key_verstamp,
    # Передача аргументов через словарь, а не список
    op_kwargs={
        'path': file_path_key_verstamp,
        'is_restart': True
    },
    dag=dag
)

task_compute_house_sales = PythonOperator(
    task_id='compute_house_sales',
    python_callable=compute_house_sales,
    # Передача аргументов через словарь, а не список
    op_kwargs={
        'path_house_sales': file_path_house_sales,
        'path_verstamp': file_path_key_verstamp,
        'path_sales': file_path_sales,
        'url_api': url_api_hp
    },
    dag=dag
)

task_update_actual_key_verstamp = PythonOperator(
    task_id='update_actual_key_verstamp',
    python_callable=update_actual_key_verstamp,
    # Передача аргументов через словарь, а не список
    op_kwargs={
        'path_verstamp': file_path_key_verstamp,
        'path_sales': file_path_sales
    },
    dag=dag
)

# Определение зависимостей между задачами
task_create_file_key_verstamp >> task_compute_house_sales >> task_update_actual_key_verstamp
# endregion
