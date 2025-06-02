import os
import requests
import pandas as pd
from logger import log_extract_start, log_extract_end, log_error, log_info
from db.db_connector import get_engine
from airflow.models import Variable

TMDB_API_KEY = Variable.get("MY_API_KEY")
GENRE_MOVIE_LIST_URL = "https://api.themoviedb.org/3/genre/movie/list"


def extract_genres():

    params = {
        "api_key": TMDB_API_KEY,
        "language": "en-US"
    }
    
    response = requests.get(GENRE_MOVIE_LIST_URL, params=params)
    genres = response.json().get("genres", [])

    # Convert to DataFrame
    df_genres = pd.DataFrame(genres)

    # ensure genre_id is int
    df_genres['id'] = df_genres['id'].astype(int)

    engine = get_engine()
    
    try:
        log_info('extract', f"Inserting {len(df_genres)} records into 'raw_genres' table")

        df_genres.to_sql('raw_genres', engine, if_exists='replace', index=False)

        log_info('extract', "Data successfully written to 'raw_genres' table")
    
    except Exception as e:
        log_error('extract', f"Failed to write data to 'raw_generes' table: {str(e)}")
        raise