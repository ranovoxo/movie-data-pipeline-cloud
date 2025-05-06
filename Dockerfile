FROM apache/airflow:2.7.2-python3.10

# Switch to root user to install dependencies
USER root

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y openjdk-11-jdk

# Set JAVA_HOME environment variable for Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install PySpark and other dependencies from requirements.txt
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Optional: Check if Java and Spark installation is correct (for debugging)
# RUN java -version
