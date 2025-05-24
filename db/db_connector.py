import os
import logging
from sqlalchemy import create_engine
import psycopg2
from airflow.models import Variable
from dotenv import load_dotenv

# Load .env if available (for local development)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

# Try to get from Airflow Variables, fallback to .env
def get_env_var(key, fallback=None):
    try:
        return Variable.get(key)
    except Exception:
        return os.getenv(key, fallback)

# Config values
POSTGRES_DB = get_env_var("POSTGRES_DB", "movie-ratings-db")
POSTGRES_USER = get_env_var("POSTGRES_USER", "postgres")
POSTGRES_PW = get_env_var("POSTGRES_PW", "postgres")
POSTGRES_HOST = get_env_var("POSTGRES_HOST", "postgres")  # default to docker service name
POSTGRES_PORT = get_env_var("POSTGRES_PORT", "5432")

# Use Airflow's home directory or fallback to a safe path
LOG_DIR = os.path.join(os.getenv("AIRFLOW_HOME", "/opt/airflow"), "logs")

# Ensure directory exists
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename='/app/logs/database.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# SQLAlchemy Engine
def get_engine():
    try:
        db_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PW}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        engine = create_engine(db_url)
        logging.info("SQLAlchemy engine created.")
        return engine
    except Exception as e:
        logging.error(f"Failed to create SQLAlchemy engine: {e}")
        raise

# psycopg2 connection (for cursor usage)
def get_pg_connection():
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PW,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        logging.info("psycopg2 connection successful.")
        return conn
    except Exception as e:
        logging.error(f"psycopg2 connection failed: {e}")
        raise
