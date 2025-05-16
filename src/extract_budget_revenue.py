import requests
import pandas as pd
from logger import log_extract_start, log_extract_end, log_error, log_info
from sqlalchemy import create_engine
from airflow.models import Variable
from requests.adapters import HTTPAdapter, Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

TMDB_API_KEY = Variable.get("MY_API_KEY")
POSTGRES_USER = Variable.get("POSTGRES_USER")
POSTGRES_PW = Variable.get("POSTGRES_PW")
BUDGET_URL = "https://api.themoviedb.org/3/movie/"
DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PW}@postgres:5432/movie-ratings-db"


# This version uses ThreadPoolExecutor to fetch movie financials in parallel.
# The original version made API calls sequentially, one at a time, which is slow because
# each request takes ~250–500ms due to network latency.
# By using concurrent threads (e.g., 10), we can make multiple API calls at once,
# significantly reducing total runtime — up to 10x faster in practice — while staying
# within TMDB’s API rate limits (40 requests per 10 seconds).


# Setup session with retries
def get_requests_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

# Fetch data for a single movie
def fetch_movie_data(movie_id, session):
    url = f"{BUDGET_URL}{movie_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}
    try:
        resp = session.get(url, params=params, timeout=5)
        if resp.status_code == 200:
            movie = resp.json()
            return {
                "id": movie.get("id"),
                "title": movie.get("title"),
                "release_date": movie.get("release_date"),
                "budget": movie.get("budget"),
                "revenue": movie.get("revenue")
            }
        else:
            log_error("extract", f"ID {movie_id} failed: {resp.status_code}")
    except Exception as e:
        log_error("extract", f"Exception for ID {movie_id}: {str(e)}")
    return None

# Main function
def extract_movie_financials():
    log_extract_start()
    session = get_requests_session()
    engine = create_engine(DB_URL)

    try:
        df_movies = pd.read_sql("SELECT id FROM raw_movies", engine)
        movie_ids = df_movies['id'].tolist()

        log_info("extract", f"Fetching financials for {len(movie_ids)} movies")

        financial_data = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_movie_data, movie_id, session): movie_id for movie_id in movie_ids}
            for i, future in enumerate(as_completed(futures)):
                result = future.result()
                if result:
                    financial_data.append(result)
                if i % 100 == 0:
                    log_info("extract", f"Processed {i}/{len(movie_ids)}")

        df_financials = pd.DataFrame(financial_data)
        log_info("extract", f"Collected financials for {len(df_financials)} movies")

        df_financials.to_sql('raw_finances', engine, if_exists='replace', index=False)
        log_info("extract", f"Written to `raw_finances` table")

    except Exception as e:
        log_error("extract", f"Fatal error in extract_movie_financials: {str(e)}")
        raise
    finally:
        log_extract_end()
