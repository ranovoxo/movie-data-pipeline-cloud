from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from docker.types import Mount
from datetime import datetime
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from extract import extract_data
from transform_silver_layer import transform_to_silver
from transform_gold_layer import transform_to_gold
from load import load_data_to_postgres
from logger import log_extract_start, log_extract_end, log_transform_start, log_transform_end, log_load_start, log_load_end

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'movie_data_etl',
    default_args=default_args,
    description='ETL pipeline for movie data',
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 5),
    catchup=False,
)

def extract_movies(**context):
    data = extract_data()  # should return JSON-serializable result or save to file/DB
    log_extract_end()
    context['ti'].xcom_push(key='raw_data', value=data)

def transform_gold_layer():
    log_transform_start()
    transform_to_gold()
    log_transform_end()
    #context['ti'].xcom_push(key='gold_data', value=gold_data)

def load_movies(**context):
    log_load_start()
    gold_data = context['ti'].xcom_pull(task_ids='transform_movies_gold', key='gold_data')
    db_url = "postgresql://airflow:airflow@localhost:5432/movie-ratings-db"
    load_data_to_postgres(gold_data, "gold_movies", db_url)
    log_load_end()

extract_task = PythonOperator(
    task_id='extract_movies',
    python_callable=extract_movies,
    provide_context=True,
    dag=dag,
)


transform_silver_task = PythonOperator(
    task_id='transform_silver_task',
    python_callable=transform_to_silver,
    provide_context=True,  # Optional, if you're not using context vars, you can remove this
    dag=dag,
)


transform_gold_task = PythonOperator(
    task_id='transform_movies_gold',
    python_callable=transform_gold_layer,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_movies',
    python_callable=load_movies,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_silver_task >> transform_gold_task >> load_task