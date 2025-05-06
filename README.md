Here's the **README.md** file for your Movie Data ETL Pipeline project:

---

# Movie Data ETL Pipeline with Airflow & PostgreSQL (Dockerized)

This is a fully Dockerized data engineering pipeline that extracts movie data from a public API, stores raw data in PostgreSQL, transforms it into silver and gold tables, and orchestrates the entire process with Apache Airflow. The project also includes structured logging to track ETL stages in detail.

---

## Project Structure

```
movie_data_pipeline/
│
├── dags/
│   └── movie_etl_dag.py         # Airflow DAG definition
│
├── src/
│   ├── extract.py               # Extract movie data
│   ├── transform.py             # Clean and transform raw data
│   ├── load.py                  # Load data into PostgreSQL
│   └── logger.py                # Custom logger for ETL steps
│
├── sql/
│   ├── create_tables.sql        # Raw, silver, and gold schema
│   ├── transform_silver.sql     # Transform raw to silver
│   └── transform_gold.sql       # Transform silver to gold
│
├── logs/                        # ETL logs (host-mounted in Docker)
│   └── etl/
│       ├── extract.log
│       ├── transform.log
│       └── load.log
│
├── docker-compose.yml           # Docker config for Airflow & Postgres
├── Dockerfile                   # Custom Airflow image with requirements
├── requirements.txt             # Python dependencies
├── .env                         # Environment variables for DB
└── README.md                    # Project documentation
```

---

## Pipeline Stages

### Extract

* Pulls movie data from a public API or scrape source.

### Load Raw

* Inserts unprocessed data into the `raw_movies` table in PostgreSQL.

### Transform Silver

* Cleans and standardizes the data into `silver_movies`.

### Transform Gold

* Aggregates insights into `gold_movies`.

### Orchestration

* Apache Airflow manages and schedules all tasks.

---

## Technologies Used

* **Python 3.10+**
* **PostgreSQL**
* **Apache Airflow 2.7+**
* **Docker & Docker Compose**
* **BeautifulSoup / Requests** (for scraping)
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

* **Airflow Webserver**: [http://localhost:8080](http://localhost:8080)
* **Scheduler**
* **PostgreSQL**

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

Run the scripts in the `sql/` directory using a tool like **DBeaver**, **pgAdmin**, or with a DAG task to auto-init the schema in PostgreSQL.

---

## Airflow DAG Tasks

* **extract\_movies**: Grabs raw data via API/scraping.
* **load\_raw**: Loads the raw data into `raw_movies` table.
* **transform\_to\_silver**: Transforms raw data into `silver_movies`.
* **transform\_to\_gold**: Aggregates data to produce `gold_movies`.

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

## Improvements & Ideas

* Add **data validation** (using tools like **Great Expectations** or custom validation rules).
* Dockerize for **production** (multi-container deployment).
* Integrate a **dashboard** (using **Streamlit** or **Superset**) to visualize gold-layer insights.
* Schedule with **cron** + Airflow variables.
* Add **unit tests** for `src/` logic.

---

## License

This project is licensed under the **MIT License**.

