import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# get path of python files to run
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ml.preprocess_text import preprocess_text
from ml.train_genre_multilabel import start_training
from ml.predict_genre import start_genre_predictions


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'movie_genre_ml',
    default_args=default_args,
    description='ML pipeline for movie genre prediction',
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 5),
    catchup=False,
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

preprocess_text_task >> train_genre_ml >> start_genre_predictions_ml
