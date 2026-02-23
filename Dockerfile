FROM apache/airflow:2.10.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy pipeline source code
COPY src/ /opt/airflow/dags/src/
COPY dags/ /opt/airflow/dags/
COPY sql/ /opt/airflow/sql/
