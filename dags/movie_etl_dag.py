import os
import sys
# get path of python files to run
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../ml')))

from src.extract_movies import extract_movies
from src.extract_genres import extract_genres
from src.extract_budget_revenue import extract_movie_financials
from src.transform_silver_layer import transform_to_silver
from src.transform_gold_layer import transform_to_gold
from ml.multi_label_classification.preprocess_text import preprocess_text
from ml.multi_label_classification.train_genre_multilabel import start_training
from ml.multi_label_classification.predict_genre import start_genre_predictions
from src.logger import *
from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime


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

def get_movies():
    extract_movies()  

def get_genres():
    extract_genres()

def get_budget_revenue():
    extract_movie_financials()

def transform_to_silver_layer():
    log_transform_start()
    transform_to_silver()
    log_transform_end()
    
def transform_gold_layer():
    log_transform_start()
    transform_to_gold()
    log_transform_end()

extract_movies_task = PythonOperator(
    task_id='extract_movies_task',
    python_callable=get_movies,
    provide_context=True,
    dag=dag,
)
extract_genres_task = PythonOperator(
    task_id='extract_genres_task',
    python_callable=get_genres,
    provide_context=True,
    dag=dag,
)

extract_budget_revenue_task = PythonOperator(
    task_id='extract_budget_revenue_task',
    python_callable=get_budget_revenue,
    provide_context=True,
    dag=dag,
)

transform_movies_silver_task = PythonOperator(
    task_id='transform_movies_silver_task',
    python_callable=transform_to_silver_layer,
    provide_context=True,  # Optional, if you're not using context vars, you can remove this
    dag=dag,
)

transform_movies_gold_task = PythonOperator(
    task_id='transform_movies_gold_task',
    python_callable=transform_gold_layer,
    provide_context=True,
    dag=dag,
)

preprocess_text_task = PythonOperator(
    task_id='preprocess_text_task',
    python_callable=preprocess_text,
    provide_context=True,
    dag=dag,
)

train_genre_ml = PythonOperator(
    task_id='train_genre_ml',
    python_callable=start_training,
    provide_context=True,
    dag=dag,
)

start_genre_predictions_ml = PythonOperator(
    task_id='start_genre_predictions_ml',
    python_callable=start_genre_predictions,
    provide_context=True,
    dag=dag,
)

extract_movies_task >> extract_genres_task >> extract_budget_revenue_task >> transform_movies_silver_task >> transform_movies_gold_task >> preprocess_text_task >> train_genre_ml >> start_genre_predictions_ml