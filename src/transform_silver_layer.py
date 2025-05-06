from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import logging
import os
import sys

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PW = os.getenv("POSTGRES_PW")
DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PW}@postgres:5432/movie-ratings-db"
SOURCE_TABLE = "raw_movies"
TARGET_TABLE = "movies_silver"


def setup_logger(name):
    # Use Airflow's logger (which will be visible in the UI)
    logger = logging.getLogger('airflow.task')
    logger.setLevel(logging.INFO)
    return logger

extract_logger = setup_logger('extract_logger')

def transform_to_silver():
    """Transform raw data to silver-level cleaned data using PySpark and write to Postgres"""
    logger = setup_logger()
    logger.info("transform", "Starting transformation to silver...")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("MovieETL").getOrCreate()
    # Get the Airflow logger
    log = logging.getLogger('airflow.task')
    try:
        # Read raw data
        df = spark.read \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", SOURCE_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PW) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        log.info(f"Rows read from source table `{SOURCE_TABLE}`: {df.count()}")

        log.info("Raw data read from PostgreSQL.")
        log.info(f"Original row count: {df.count()}")
        # Clean and transform
        df_silver = df.select(
            'id',
            'title',
            'release_date',
            'genre_ids',
            'vote_average',
            'vote_count',
            'popularity',
            'overview',
            'original_language'
        ).dropna()
        log.info(f"After dropna: {df_silver.count()}")

        df_silver = df_silver.withColumn('release_date', col('release_date').cast('date'))
        df_silver = df_silver.dropDuplicates()
        df_silver.printSchema()

        log.info("Data transformation completed. Writing to PostgreSQL...")

        # Write to silver table
        # TODO: pyspark not writing to table, need to investigate.
        df_silver.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", TARGET_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PW) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

        log.info(f"Data written to table `{TARGET_TABLE}` successfully.")

    except Exception as e:
        log.error(f"Error during transformation or loading: {str(e)}")
        raise
