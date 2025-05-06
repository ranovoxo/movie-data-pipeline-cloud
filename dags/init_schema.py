from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Start as soon as possible
    'retries': 1,
}

dag = DAG(
    'init_schema',
    default_args=default_args,
    description='Initial Schema Setup for Movie ETL',
    schedule_interval=None,  # This is a one-time setup task
)

# Function to run the SQL script
def create_tables():
    # Path to your create_tables.sql file
    sql_file_path = os.path.join(os.path.dirname(__file__), '../sql/create_tables.sql')

    # Ensure the file exists
    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"{sql_file_path} not found!")

    # Read the SQL file
    with open(sql_file_path, 'r') as file:
        sql_script = file.read()

    # Create a Postgres hook to execute the SQL on your PostgreSQL DB
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    # Run the SQL script to create the tables
    pg_hook.run(sql_script)

# Define the task to create tables
create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag,
)

create_tables_task
