import pandas as pd
from src.logger import *
from db.db_connector import get_engine


def transform_to_gold():
    """Aggregate silver data and write gold-level insights to PostgreSQL"""
    log_info("gold_layer", "Starting transformation to gold...")

    try:
        engine = get_engine()

        # Load silver data
        df = pd.read_sql("SELECT * FROM movies_silver", engine)
        log_info("gold_layer", f"Loaded {len(df)} rows from silver table")

        # TODO: add more and useful gold layer tables

        # using a weighted movie scoring approach for top movies
        C = df["vote_average"].mean()
        m = 500  # minimum votes required
        top_movies = df[df["vote_count"] >= m].copy()
        top_movies["weighted_score"] = (
        (top_movies["vote_count"] / (top_movies["vote_count"] + m)) * top_movies["vote_average"]
        + (m / (top_movies["vote_count"] + m)) * C
        )
        top_movies = top_movies.sort_values(by="weighted_score", ascending=False).head(15)

        avg_rating_by_lang = df.groupby("original_language")["vote_average"].mean().reset_index()
        avg_rating_by_lang.columns = ["language", "avg_vote"]

        release_date_dt = pd.to_datetime(df['release_date'], errors='coerce')
        yearly_counts = release_date_dt.dt.year.value_counts().reset_index()
        yearly_counts.columns = ['year', 'count']

        # Write to gold tables
        top_movies.to_sql("gold_top_movies", engine, if_exists="replace", index=False)
        avg_rating_by_lang.to_sql("gold_avg_rating_by_language", engine, if_exists="replace", index=False)
        yearly_counts.to_sql("gold_yearly_counts", engine, if_exists="replace", index=False)

        log_info("gold_layer", "Gold tables written successfully.")

    except Exception as e:
        log_error("gold_layer", f"Gold transformation failed: {str(e)}")
        raise
