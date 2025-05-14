# Clone the repo and enter project folder
git clone https://github.com/yourusername/movie-data-pipeline.git
cd movie-data-pipeline

# Airflow localhost:

# Create the .env file with PostgreSQL environment variables
echo "POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=movies
POSTGRES_HOST=postgres
POSTGRES_PORT=5432" > .env

# Build Docker containers and start services
docker-compose up --build  # Starts Airflow webserver, scheduler, and PostgreSQL

# Wait until services are running, then initialize Airflow DB
docker-compose exec airflow-webserver airflow db init  # Initializes Airflow metadata DB

# Create an Airflow admin user
docker-compose exec airflow-webserver airflow users create \
  --username airflow --password airflow \
  --firstname Air --lastname Flow --role Admin --email airflow@example.com

# (Optional) Access Airflow UI at http://localhost:8080
# Default login: airflow / airflow

# Enter the Postgres container to run SQL files (Option A: manual setup)
docker-compose exec postgres psql -U airflow -d movies  # Enters psql shell

# Inside psql shell, run:
# \i sql/create_tables.sql
# \i sql/transform_silver.sql
# \i sql/transform_gold.sql

# (Optional) List running containers
docker ps

# Build containers but dont start
docker-compose up --build --no-start

# (Optional) Stop containers
docker-compose down           # Stops and removes containers
docker-compose down -v        # Also removes volumes (DB data)

# (Optional) Clear ETL log files
rm -rf logs/etl/*.log

# (Optional) View logs for debugging
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler
docker-compose logs postgres

# (Optional) Rebuild images if requirements or Dockerfile changes
docker-compose build --no-cache
docker-compose up

# (Optional - for local testing without Docker)
pip install -r requirements.txt
