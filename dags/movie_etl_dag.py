from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import os
import sys

# get path of python files to run
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from extract import extract_data
from transform_silver_layer import transform_to_silver
from transform_gold_layer import transform_to_gold
from logger import *

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

def extract_movies():
    log_extract_start()
    extract_data()  
    log_extract_end()

def transform_to_silver_layer():
    log_transform_start()
    transform_to_silver()
    log_transform_end()
    
def transform_gold_layer():
    log_transform_start()
    transform_to_gold()
    log_transform_end()

extract_task = PythonOperator(
    task_id='extract_movies',
    python_callable=extract_movies,
    provide_context=True,
    dag=dag,
)


transform_silver_task = PythonOperator(
    task_id='transform_silver_task',
    python_callable=transform_to_silver_layer,
    provide_context=True,  # Optional, if you're not using context vars, you can remove this
    dag=dag,
)


transform_gold_task = PythonOperator(
    task_id='transform_movies_gold',
    python_callable=transform_gold_layer,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_silver_task >> transform_gold_task