Here's the **README.md** file for your Movie Data ETL Pipeline project:

---

# Movie Data ETL Pipeline with Airflow & PostgreSQL (Dockerized)

This is a fully Dockerized data engineering pipeline that extracts movie data from a public API, stores raw data in PostgreSQL, transforms it into silver and gold tables, and orchestrates the entire process with Apache Airflow. The project also includes structured logging to track ETL stages in detail.

---
# Movie Analytics Reporting

The pipeline creates curated reporting tables for a future web dashboard with insights on movie ratings, genres, and yearly trends.

## Project Structure
```
movie_data_pipeline/
│
├── dags/
│   ├── init_schema.py              # Creates DB schema on Airflow start
│   ├── movie_etl_dag.py            # Daily ETL Airflow DAG definition
│   └── movie_genre_ml_dag.py       # Weekly ML genre prediction DAG definition
│
├── src/
│   ├── extract_movies.py           # Extract movie metadata
│   ├── extract_genres.py           # Extract genre data
│   ├── extract_budget_revenue.py   # Extract budget and revenue data
│   ├── transform_silver_layer.py   # Clean and deduplicate raw data
│   ├── transform_gold_layer.py     # Enrich and finalize analytics-ready data
│   ├── logger.py                   # Custom logger for ETL steps
│   └── tableau/
│       └── hyper_exports/          # Legacy local exports
│           ├── avg_rating_by_lang.csv
│           ├── gold_top_movies.csv
│           └── yearly_counts.csv
│
├── ml/
│   ├── predict_genre.py 
│   ├── preprocess_text.py          # Preprocess text for overview column for machine learning training
│   ├── train_genre_multilabel.py   # Train the model
│   ├── utils.py
├── sql/
│   └── create_table.sql            # PostgreSQL table creation script
│
├── db/
│   └── db_connector.py             # DB connection helper
│
├── logs/                           # ETL & Airflow logs
│   ├── etl/
│   │   ├── extract.log
│   │   ├── transform.log
│   │   └── load.log
│   ├── scheduler/                  # Airflow scheduler logs
│   ├── dag_id=movie_data_etl/     # Logs by DAG run and task
│   └── dag_processor_manager/     # DAG processor logs
│
├── jars/
│   └── postgresql-42.7.1.jar       # JDBC driver for PostgreSQL
│
├── docker-compose.yml             # Docker config for Airflow, Postgres, etc.
├── Dockerfile                     # Custom Airflow image with dependencies
├── requirements.txt               # Python dependencies
├── .env                           # Environment variables (DB, secrets)
├── README.md                      # Project documentation
└── command_line_notes.md          # Helpful CLI commands during development
```

---

## Pipeline Stages
[API] → [Extract Scripts] → [PostgreSQL Raw Tables] → [Silver & Gold Tables] → [Website/API]

[Silver/Gold Tables] → [Preprocess Text] → [Train Genre Model] → [Generate Genre Predictions]

### Extract

* Pulls movie data from a public API or scrape source.

### Load Raw

* Inserts unprocessed data into the `raw_movies` table in PostgreSQL.

### Transform Silver

* Cleans and standardizes the data into `silver_movies`.

### Transform Gold

* Aggregates insights into `gold_movies`.

### Orchestration

* Apache Airflow manages the ETL and ML workflows as separate DAGs.
* `movie_data_etl` runs daily at 2:00 AM.
* `movie_genre_ml` runs weekly on Sunday at 4:00 AM, after the ETL data is available.

### Machine Learning

  Preprocesses plot summaries and trains a multi-label classification model to predict movie genres from the overview text.

  - `preprocess_text.py`  
    Cleans and tokenizes the `overview` text column to prepare it for model training.

  - `train_genre_multilabel.py`  
    Trains a multi-label classifier to predict genres based on preprocessed plot summaries.

  - `predict_genre.py`  
    Applies the trained model to generate genre predictions.

  - `utils.py`  
    Contains shared helper functions used across the ML pipeline.

---

## Technologies Used

* **Python 3.10+**
* **PostgreSQL**
* **Apache Airflow 2.7+**
* **Docker & Docker Compose**
* **Requests** 
* **Pandas** (for data wrangling)
* **Custom Logging** (per ETL step)

---

## Requirements

* `apache-airflow==2.7.1`
* `psycopg2-binary`
* `requests`
* `beautifulsoup4`
* `pandas`
* `python-dotenv`

---

## Setup Instructions (Dockerized)

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/movie-data-pipeline.git
cd movie-data-pipeline
```

### 2. Create `.env`

Create an `.env` file in the root of the project directory with the following content:

```plaintext
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=movies
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
```

### 3. Build and Start Services

Run the following command to build and start the Docker services:

```bash
docker-compose up --build
```

This starts:

## Services Overview

- **Airflow Webserver**  
  Access the Airflow UI at: [http://localhost:8080](http://localhost:8080)

- **Airflow Scheduler**  
  Responsible for scheduling and triggering DAG tasks.

- **PostgreSQL Database**  
  Runs on `localhost:5432`. Used as the main data warehouse for storing:
  - `raw_movies`
  - `raw_genres`
  - `raw_finances`
  - `movies_silver`
  - `movies_gold`
  - `ml_genre_predictions`
  - `gold_avg_rating_by_language`
  - `gold_top_movies`
  - `gold_yearly_counts`


### Default Airflow credentials:

* **Username**: airflow
* **Password**: airflow

### 4. Initialize Airflow

Run the following commands to initialize the Airflow database and create the default user:

```bash
docker-compose exec airflow-webserver airflow db init
docker-compose exec airflow-webserver airflow users create \
  --username airflow --password airflow \
  --firstname Air --lastname Flow --role Admin --email airflow@example.com
```

### 5. Create Database Tables

Run the scripts in the `sql/` directory using a database client like **DBeaver**, or with a DAG task to auto-init the schema in PostgreSQL.

---

## Airflow DAGs

### `movie_data_etl`

Runs daily at 2:00 AM and owns the data extraction and transformation workflow.

- **`extract_movies`**  
  Fetches raw movie metadata from the TMDB API and stores it in the `raw_movies` table.

- **`extract_genres`**  
  Retrieves the latest genre mappings from TMDB and stores them in the `raw_genres` table.

- **`extract_financials`**  
  Collects budget and revenue details for each movie and saves them to the `raw_finances` table.

- **`transform_to_silver`**  
  Cleans, normalizes, and enriches the raw data (e.g., mapping genre IDs to names) and writes the results to the `movies_silver` table.

- **`transform_movies_gold_task`**  
Generates curated gold-layer analytics tables based on cleaned movie data, enabling downstream reporting and insights.

  - `gold_avg_rating_by_language`  
  Provides the average movie rating grouped by the primary spoken language. Useful for analyzing how movies perform across different linguistic audiences.

  - `gold_top_movies`  
  Contains the top-rated movies, typically filtered by vote count to ensure quality. Highlights the most critically acclaimed or popular films in the dataset.

  - `gold_yearly_counts`  
  Summarizes the number of movies released each year. Useful for visualizing historical trends in movie production volume over time.

### `movie_genre_ml`

Runs weekly on Sunday at 4:00 AM and owns the machine learning workflow for genre prediction.

- **`preprocess_text_task`**  
  Cleans and prepares movie overview text for model training.

- **`train_genre_ml`**  
  Trains the multi-label genre classification model.

- **`start_genre_predictions_ml`**  
  Uses the trained model to generate genre predictions.


---

## Logging

Each ETL step uses a shared `logger.py` utility for structured logging into the `logs/etl/` directory:

* `logs/etl/extract.log`
* `logs/etl/transform.log`
* `logs/etl/load.log`

These logs help you monitor step-by-step progress, errors, and runtime metrics. The `logs/` folder is mounted as a Docker volume so logs persist on the host machine even after container shutdown.

### Sample log entry:

```plaintext
2025-05-05 12:00:00 - INFO - Starting extraction task...
```
---
### 🎯 Genre Prediction Model
- **Model:** Logistic Regression (multi-label)
- **Input:** Tokenized `overview` text
- **Target:** One-hot encoded genres
- **Performance:** ~XX% accuracy on validation set

## Improvements & Ideas

* Add **data validation** (using tools like **Great Expectations** or custom validation rules).
* Dockerize for **production** (multi-container deployment).
* Integrate a **dashboard** (using **Streamlit** or **Superset**) to visualize gold-layer insights.
* Schedule with **cron** + Airflow variables.
* Add **unit tests** for `src/` logic.

---

## License

This project is licensed under the **MIT License**.
