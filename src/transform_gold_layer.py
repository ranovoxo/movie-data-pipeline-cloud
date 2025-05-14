import pandas as pd
import logging
import os
from sqlalchemy import create_engine
from logger import setup_logger
from airflow.models import Variable

POSTGRES_DB = Variable.get("POSTGRES_DB")
POSTGRES_USER = Variable.get("POSTGRES_USER")
POSTGRES_PW = Variable.get("POSTGRES_PW")

DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PW}@postgres:5432/{POSTGRES_DB}"


def transform_to_gold():
    """Aggregate silver data and write gold-level insights to PostgreSQL"""
    #log_info("Starting transformation to gold...")

    try:
        engine = create_engine(DB_URL)

        # Load silver data
        df = pd.read_sql("SELECT * FROM movies_silver", engine)
        #logger.info(f"Loaded {len(df)} rows from silver table")

        # Sample gold transformations
        top_movies = df.sort_values(by="vote_average", ascending=False).head(10)

        avg_rating_by_lang = df.groupby("original_language")["vote_average"].mean().reset_index()
        avg_rating_by_lang.columns = ["language", "avg_vote"]

        yearly_counts = df['release_date'].dt.year.value_counts().reset_index()
        yearly_counts.columns = ['release_year', 'movie_count']

        # Write to gold tables
        top_movies.to_sql("gold_top_movies", engine, if_exists="replace", index=False)
        avg_rating_by_lang.to_sql("gold_avg_rating_by_language", engine, if_exists="replace", index=False)
        yearly_counts.to_sql("gold_yearly_counts", engine, if_exists="replace", index=False)

        #logger.info("Gold tables written successfully.")

    except Exception as e:
        #log_.error(f"Gold transformation failed: {str(e)}")
        raise
