import pandas as pd
import os
import requests
import ast  # helps parse stringified listsfrom airflow.models import Variable
from db.db_connector import get_engine
from logger import *
from airflow.models import Variable

TMDB_API_KEY = Variable.get("MY_API_KEY")

SOURCE_TABLE = "raw_movies"
TARGET_TABLE = "movies_silver"

# getting the genere's and what numbers they map to to include into dataset
def get_genre_id_df():
    """
    Read genre ID-to-name mapping from a Postgres table and return as a DataFrame.
    """
    engine = get_engine()  # assumes DB_URL is already defined

    genre_df = pd.read_sql("SELECT id, name FROM raw_genres", engine)

    return genre_df


def transform_to_silver():
    """Transform raw data to silver-level cleaned data using Pandas and write to Postgres"""

    log_info("Transform","Starting transformation to silver...")

    try:
        # create database engine
        engine = get_engine()

        """read raw data from PostgreSQL, group records by id, do a ranking on vote_count 
        and get one with highest which should be latest record
        eliminating duplicates"""
        df = pd.read_sql(f"""SELECT r.*
                            FROM (
                            SELECT *, ROW_NUMBER() OVER (
                                PARTITION BY id 
                                ORDER BY vote_count DESC, load_timestamp DESC  -- fallback to timestamp
                            ) AS rn
                            FROM {SOURCE_TABLE}
                        ) r
                        WHERE r.rn = 1;""", engine)

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

        # --- NEW: Convert genre_ids from string to list if needed ---
        df_silver['genre_ids'] = df_silver['genre_ids'].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x
        )

        log_info("transform", f"Count after dropping duplicates: {len(df_silver)}")

        log_info("transform", "Data transformation completed. Writing to PostgreSQL...")

        # get genere mappings and join with silver table:
        genere_mappings = get_genre_id_df()

        # create a copy with exploded genre_ids
        df_genres = df_silver[['id', 'genre_ids']].explode('genre_ids').copy()

        # convert to dictionary
        genere_mapping_dict = dict(zip(genere_mappings['id'], genere_mappings['name']))

        # map genre_id to genre_name
        df_genres['name'] = df_genres['genre_ids'].map(genere_mapping_dict)

        # group back into comma-separated genre names
        df_genre_names = df_genres.groupby('id')['name'].apply(lambda x: ', '.join(sorted(set(x.dropna())))).reset_index()

        # merge back into df_silver
        df_silver = df_silver.drop(columns=['genre_ids']).merge(df_genre_names, on='id', how='left')

        # rename genre_name column to genres
        df_silver = df_silver.rename(columns={'name': 'genres'})

        # replace missing or blank genres with 'Unknown'
        df_silver_final = df_silver.copy()
        df_silver_final['genres'] = df_silver_final['genres'].fillna('Unknown')
        df_silver_final.loc[df_silver_final['genres'].str.strip() == '', 'genres'] = 'Unknown'


        # write to silver table (overwrite)
        df_silver_final.to_sql(TARGET_TABLE, engine, if_exists='replace', index=False)

        log_info("transform", f"Data written to table `{TARGET_TABLE}` successfully.")
        log_load_end()

    except Exception as e:
        log_error("transform", f"Error during transformation or loading: {str(e)}")
        raise
