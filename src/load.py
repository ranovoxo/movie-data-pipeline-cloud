import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import logging
import psycopg2.extras
from sqlalchemy import create_engine
from airflow.models import Variable


def load_data_to_postgres(df: DataFrame):

    """Load PySpark DataFrame to PostgreSQL"""
    POSTGRES_USER=Variable.get("POSTGRES_USER")
    POSTGRES_PW=Variable.get("POSTGRES_PW")
    table_name = "movies_silver"

    DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PW}@postgres:5432/movie-ratings-db"
    # Initialize a Spark session
    spark = SparkSession.builder.appName("MovieETL").getOrCreate()  
    try:
        logging.info(f"Starting to load data into {table_name} table...")

        # Convert PySpark DataFrame to Pandas DataFrame
        pandas_df = df.toPandas()

        # Use SQLAlchemy engine for PostgreSQL connection
        engine = create_engine(DB_URL)

        # Load the data into the table (if table exists, append the data)
        pandas_df.to_sql(table_name, engine, if_exists='append', index=False)

        logging.info(f"Data successfully loaded into {table_name} table.")
    except Exception as e:
        logging.error(f"Error loading data to {table_name}: {e}")