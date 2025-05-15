import pandas as pd
import logging
import os
from sqlalchemy import create_engine
from logger import *
from airflow.models import Variable

POSTGRES_DB = Variable.get("POSTGRES_DB")
POSTGRES_USER = Variable.get("POSTGRES_USER")
POSTGRES_PW = Variable.get("POSTGRES_PW")

DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PW}@postgres:5432/{POSTGRES_DB}"

# Create output directory if it doesn't exist
#CSV_DIR = os.path.join(os.path.dirname(__file__), "..", "tableau", "hyper_exports")
#os.makedirs(CSV_DIR, exist_ok=True)

CSV_DIR = "/opt/airflow/src/tableau/hyper_exports"


def export_csv_for_tableau(tables_dict, csv_dir):
    
    for name, df in tables_dict.items():
        csv_path = os.path.join(csv_dir, f"{name}.csv")
        df.to_csv(csv_path, index=False)
        print(f"✅ Exported `{name}` to CSV: {csv_path}")


def transform_to_gold():
    """Aggregate silver data and write gold-level insights to PostgreSQL"""
    log_info("gold_layer", "Starting transformation to gold...")

    try:
        engine = create_engine(DB_URL)

        # Load silver data
        df = pd.read_sql("SELECT * FROM movies_silver", engine)
        log_info("gold_layer", f"Loaded {len(df)} rows from silver table")

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

        log_info("gold_layer", "Gold tables written successfully.")

        export_csv_for_tableau(
                    {
                "gold_top_movies": top_movies,
                "avg_rating_by_lang": avg_rating_by_lang,
                "yearly_counts": yearly_counts
                },
                CSV_DIR
            )

    except Exception as e:
        log_error(f"Gold transformation failed: {str(e)}")
        raise
