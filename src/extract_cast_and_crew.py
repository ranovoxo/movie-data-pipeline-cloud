import os
import requests
import pandas as pd
import time
from logger import log_extract_start, log_extract_end, log_error, log_info
from airflow.models import Variable
from db.db_connector import get_engine
from datetime import timezone
from concurrent.futures import ThreadPoolExecutor, as_completed


TMDB_API_KEY = Variable.get("MY_API_KEY")
CAST_AND_CREW_URL = "https://api.themoviedb.org/3/discover/movie"
SOURCE_TABLE = "movies_silver"
DELAY = 0.25
MAX_WORKERS = 5   # TMDB's rate limit is ~40 requests every 10 seconds with API key

#SELECT id, title, popularity release_date, overview, genres FROM movies_silver
#WHERE CAST(release_date AS DATE) > CURRENT_DATE;

def get_all_movie_ids():
    engine = get_engine()
    try: 
        df = pd.read_sql(f"SELECT id FROM movies_silver;", engine)
        
    except Exception as e:
        return f"Could not read from the database: {e}"
    
    return df

def extract_cast_and_crew(movie_id):
    movie_url = f'https://api.themoviedb.org/3/movie/{movie_id}/credits'
    
    params = {
        "api_key": TMDB_API_KEY,
        "language": "en-US"
    }
    
    try: 
        response = requests.get(movie_url, params=params)
        if response.status_code == 200:
            return response.json()
        
    except Exception as e:
        print(f"Error fetching movie data from API: {e}")


def process_movie(movie_id):
    try:
        credits = extract_cast_and_crew(movie_id)
        if not credits:
            return None

        # sort cast by popularity
        sorted_cast = sorted(
            [c for c in credits['cast'] if c.get('known_for_department') == 'Acting'],
            key=lambda c: c.get('popularity', 0),
            reverse=True
        )
        top_cast = [(member['name'], member['popularity']) for member in sorted_cast[:5]]

        # get producers and directors
        key_crew = [(member['name'], member['job']) for member in credits['crew'] if member['job'] in ['Director', 'Producer']]

        return {
            'id': movie_id,
            'title': credits.get('title', None),
            'top_cast': top_cast,
            'key_crew': key_crew
        }

    except Exception as e:
        log_error(f"Failed to process movie_id={movie_id}: {e}")
        return None
    
def get_all_cast_and_crew_parallel():
    movies_df = get_all_movie_ids()
    all_movies_cast_crew = []

    # to process data faster and efficiently, doing multiple requests at once. 
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_movie, movie_id): movie_id for movie_id in movies_df['id']}

        for future in as_completed(futures):
            result = future.result()
            if result:
                all_movies_cast_crew.append(result)

    df_cast_crew = pd.DataFrame(all_movies_cast_crew)
    save_cast_crew_to_db(df_cast_crew)


def save_cast_crew_to_db(df_cast_crew):

    engine = get_engine()
    try:
        movie_info = df_cast_crew[['id', 'title']].drop_duplicates()
    
        cast_rows = []
        for _, row in df_cast_crew.iterrows():
            for name, popularity in row['top_cast']:
                cast_rows.append({
                    'movie_id': row['id'],
                    'name': name,
                    'popularity': popularity
                })
        df_cast = pd.DataFrame(cast_rows)
        df_cast.to_sql('cast_members', engine, if_exists='replace', index=False)

        # Flatten crew
        crew_rows = []
        for _, row in df_cast_crew.iterrows():
            for name, job in row['key_crew']:
                crew_rows.append({
                    'movie_id': row['id'],
                    'name': name,
                    'job': job
                })
        df_crew = pd.DataFrame(crew_rows)
        df_crew.to_sql('crew_members', engine, if_exists='replace', index=False)

        print("✅ Successfully saved cast and crew data to PostgreSQL.")

    except Exception as e:
        print(f"❌ Failed to save cast and crew data: {e}")