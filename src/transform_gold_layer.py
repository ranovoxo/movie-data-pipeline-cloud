import pandas as pd
import logging
import os
from logger import *
from airflow.models import Variable
from db.db_connector import get_engine
from pathlib import Path

# Create output directory if it doesn't exist
#CSV_DIR = os.path.join(os.path.dirname(__file__), "..", "tableau", "hyper_exports")
#os.makedirs(CSV_DIR, exist_ok=True)

import os

JSON_DIR = "/opt/airflow/src/tableau/hyper_exports"

# making this path more robust, so independent of where the script is run from 
SQL_TOP_ACTORS = sql_file_path = Path(__file__).parent / "sql" / "actor_top_movies.sql"
PROFITABILITY_SQL = sql_file_path = Path(__file__).parent / "sql" / "profitability_gold.sql"

PROFITABILITY_QUERY = query = """
SELECT 
    id,
    original_title,
    release_date,
    EXTRACT(YEAR FROM release_date) AS release_year,
    budget,
    revenue,
    revenue - budget AS profit,
    ROUND((revenue - budget) * 1.0 / NULLIF(budget, 0), 2) AS roi,
    vote_average,
    genres
FROM silver_movies
WHERE 
    budget > 0 AND revenue > 0
ORDER BY profit DESC
"""

def export_json_for_tableau(tables_dict, json_dir):
    for name, df in tables_dict.items():
        json_path = os.path.join(json_dir, f"{name}.json")
        df.to_json(json_path, orient="records", lines=True)  # or lines=False for pretty JSON
        print(f"✅ Exported `{name}` to JSON: {json_path}")

from sqlalchemy import text

def run_query(filename, engine):
    with open(filename, 'r') as file:
        query = file.read()

    # checking if query is SELECT (or WITH)
    if query.strip().lower().startswith(("select", "with")):
        # For SELECT queries, return DataFrame
        df = pd.read_sql(query, engine)
        return df
    else:
        # if just a DDL/DML query, just execute
        with engine.connect() as conn:
            conn.execute(text(query))
        return None  # No DataFrame to return


def transform_to_gold():
    """Aggregate silver data and write gold-level insights to PostgreSQL"""
    log_info("gold_layer", "Starting transformation to gold...")

    try:
        engine = get_engine()

        # Load silver data
        movies_df = pd.read_sql("SELECT * FROM movies_silver", engine)
        finances_df = pd.read_sql("SELECT * FROM raw_finances", engine)

        log_info("gold_layer", f"Loaded {len(movies_df)} rows from silver table")

        # datatype might be saved differently in Postgres, so need to change befor exporting for tableau.
        # TODO: verify datatype in postgress
        #df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        # TODO: add more and useful gold layer tables

        """ Using a weighted movie scoring approach for top movies"""
        C = movies_df["vote_average"].mean()
        m = 500  # minimum votes required
        top_movies = movies_df[movies_df["vote_count"] >= m].copy()
        top_movies["weighted_score"] = (
        (top_movies["vote_count"] / (top_movies["vote_count"] + m)) * top_movies["vote_average"]
        + (m / (top_movies["vote_count"] + m)) * C
        )
        top_movies = top_movies.sort_values(by="weighted_score", ascending=False).head(15)
        """---------------------------------------------------------------------------------"""
        
        """Profitability Gold Table Process fin_df -> finances_df"""
        movies_w_finances_df = pd.merge(movies_df, finances_df, on='id', how='inner')

        print("Columns before cleaning", movies_w_finances_df.columns)
        movies_w_finances_df.drop(columns=['title_y', 'release_date_y'], inplace=True)
        movies_w_finances_df.rename(columns={'title_x': 'title','release_date_x': 'release_date'}, inplace=True)
        print("Columns after cleaning", movies_w_finances_df.columns)

        fin_df = movies_w_finances_df[(movies_w_finances_df['budget'] > 0) & (movies_w_finances_df['revenue'] > 0)].copy()

        # calculate profit and ROI
        fin_df['profit'] = fin_df['revenue'] - fin_df['budget']
        fin_df['roi'] = round((fin_df['profit'] / fin_df['budget']), 2)

        # Extract year from release_date (ensure it's datetime)
        fin_df['release_date'] = pd.to_datetime(fin_df['release_date'], errors='coerce')
        fin_df['release_year'] = fin_df['release_date'].dt.year
        # Sort by ROI descending and take top 20
        top_roi_movies = fin_df.sort_values(by='roi', ascending=False).head(20)
        """---------------------------------------------------------------------------------"""

        avg_rating_by_lang = movies_df.groupby("original_language")["vote_average"].mean().reset_index()
        avg_rating_by_lang.columns = ["language", "avg_vote"]

        release_date_dt = pd.to_datetime(movies_df['release_date'], errors='coerce')
        yearly_counts = release_date_dt.dt.year.value_counts().reset_index()
        yearly_counts.columns = ['year', 'count']

        run_query(SQL_TOP_ACTORS, engine)
        """Actor Top Movies"""
        run_query(PROFITABILITY_SQL, engine)
        """---------------------------------------------------------------------------------"""


        # Write to gold tables
        top_movies.to_sql("gold_top_movies", engine, if_exists="replace", index=False)
        avg_rating_by_lang.to_sql("gold_avg_rating_by_language", engine, if_exists="replace", index=False)
        yearly_counts.to_sql("gold_yearly_counts", engine, if_exists="replace", index=False)
        top_roi_movies.to_sql("gold_top_roi_movies", engine, if_exists="replace", index=False)

        log_info("gold_layer", "Gold tables written successfully.")

        export_json_for_tableau(
            {
                "gold_top_movies": top_movies,
                "avg_rating_by_lang": avg_rating_by_lang,
                "yearly_counts": yearly_counts,
                "gold_top_roi_movies": top_roi_movies
            },
            JSON_DIR
        )


    except Exception as e:
        log_error("extract", f"Gold transformation failed: {str(e)}")
        raise
