import requests
import pandas as pd
from logger import log_extract_start, log_extract_end, log_error, log_info
from sqlalchemy import create_engine
from airflow.models import Variable
import os

TMDB_API_KEY = Variable.get("MY_API_KEY")
POSTGRES_USER=Variable.get("POSTGRES_USER")
POSTGRES_PW=Variable.get("POSTGRES_PW")
URL = "https://api.themoviedb.org/3/discover/movie"
DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PW}@postgres:5432/movie-ratings-db"


def extract_movies():
    # Database connection settings (replace with actual values or load from .env)

    log_extract_start()
    all_movies = []
    page = 1
    total_pages = 1  # will be updated from the API response

    try:
        while page <= total_pages and page <= 500:  # TMDB allows max 500 pages
            params = {
                "api_key": TMDB_API_KEY,
                "page": page
            }

            response = requests.get(URL, params=params)

            if response.status_code != 200:
                log_error('extract', f"Failed to fetch data. Status Code: {response.status_code}")
                log_error('extract', f"Response Text: {response.text}")
                raise Exception(f"Failed to fetch data, status code: {response.status_code}")
            
            log_info("extract",  f"Response Code Text: {response.text} on page {page}")
            
            data = response.json()
            total_pages = data.get("total_pages", 1)
            movies = data.get("results", [])
            all_movies.extend(movies)
            page += 1

    except Exception as e:
        log_error('extract', f"Exception occurred during movie fetch: {str(e)}")
        raise

    # Convert to DataFrame and save to PostgreSQL
    log_info("extract",  f"Loading data into dataframe")

    df = pd.DataFrame(all_movies)
    engine = create_engine(DB_URL)

    log_info("extract",  f"Writing data to postgress database")


    try:
        log_info('extract', f"Inserting {len(df)} records into 'raw_movies' table")

        df.to_sql('raw_movies', engine, if_exists='replace', index=False)

        log_info('extract', "Data successfully written to 'raw_movies' table")

    except Exception as e:
        log_error('extract', f"Failed to write data to 'raw_movies' table: {str(e)}")
        raise

    log_extract_end()