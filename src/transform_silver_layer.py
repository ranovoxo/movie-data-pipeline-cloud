import pandas as pd
from logger import *
import os
from sqlalchemy import create_engine
from airflow.models import Variable
import requests

POSTGRES_DB = Variable.get("POSTGRES_DB")
POSTGRES_USER = Variable.get("POSTGRES_USER")
POSTGRES_PW = Variable.get("POSTGRES_PW")
TMDB_API_KEY = Variable.get("MY_API_KEY")

# SQLAlchemy connection string
DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PW}@postgres:5432/{POSTGRES_DB}"

SOURCE_TABLE = "raw_movies"
TARGET_TABLE = "movies_silver"

# getting the genere's and what numbers they map to to include into dataset
def get_genere_id_mapping():

    url = "https://api.themoviedb.org/3/genre/movie/list"
    params = {
        "api_key": TMDB_API_KEY,
        "language": "en-US"
    }
    
    response = requests.get(url, params=params)
    genres = response.json()["genres"]

    # convert to dictionary
    genre_map = {genre["id"]: genre["name"] for genre in genres}

    return genre_map


def transform_to_silver():
    """Transform raw data to silver-level cleaned data using Pandas and write to Postgres"""

    log_info("Transform","Starting transformation to silver...")

    try:
        # create database engine
        engine = create_engine(DB_URL)

        # read raw data from PostgreSQL
        df = pd.read_sql(f"SELECT * FROM {SOURCE_TABLE}", engine)

        log_info("transform", f"{len(df)} Rows read from source table `{SOURCE_TABLE}`")

        # Select and clean relevant columns
        df_silver = df[[
            'id',
            'title',
            'release_date',
            'genre_ids',
            'vote_average',
            'vote_count',
            'popularity',
            'overview',
            'original_language'
        ]].dropna()
        log_info("transform", f"After dropna: {len(df_silver)}")

        # convert release_date to datetime
        df_silver['release_date'] = pd.to_datetime(df_silver['release_date'], errors='coerce')

        # drop duplicates
        df_silver = df_silver.drop_duplicates()

        log_info("transform", f"Count after dropping duplicates: {len(df_silver)}")

        log_info("transform", "Data transformation completed. Writing to PostgreSQL...")

        # TODO: get genere mappings and join with silver table:
        
        # write to silver table (overwrite)
        df_silver.to_sql(TARGET_TABLE, engine, if_exists='replace', index=False)

        log_info("transform", f"Data written to table `{TARGET_TABLE}` successfully.")
        log_load_end()

    except Exception as e:
        log_error("transform", f"Error during transformation or loading: {str(e)}")
        raise
