"""
=============================================================================
  DAG: clinical_trial_backfill
=============================================================================
  One-time bulk load from CSV file (496,615 historical studies).
  Run this ONCE manually to populate the database, then never again.

  Flow: start → ingest_csv → clean → validate → load → analytics → cleanup → end

  Connection: Cloudberry_dwh_dev_data_power_db
=============================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared_tasks import (
    clean_and_transform, validate_data, load_to_database,
    run_analytical_queries, cleanup_temp_files,
)

logger = logging.getLogger(__name__)

CSV_FILE_PATH = "/opt/airflow/data/clinical_trials.csv"


# =============================================================================
# INGESTION TASK (CSV-specific)
# =============================================================================
def ingest_csv(**context):
    """
    Read clinical trial data from CSV file.
    The CSV is a static Kaggle dataset — 496,615 historical studies.
    """
    from src.ingestion.csv_ingestor import CSVIngestor

    ingestor = CSVIngestor(CSV_FILE_PATH)
    df = ingestor.ingest()
    df["data_source"] = "csv"

    logger.info(f"CSV ingested: {len(df):,} rows")

    tmp_path = "/tmp/raw_data.parquet"
    df.to_parquet(tmp_path, index=False)
    context["ti"].xcom_push(key="data_path", value=tmp_path)
    context["ti"].xcom_push(key="row_count", value=len(df))
    context["ti"].xcom_push(key="has_locations", value=False)


# =============================================================================
# DAG DEFINITION
# =============================================================================
default_args = {
    "owner": "Andres",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="clinical_trial_backfill",
    default_args=default_args,
    description="One-time bulk load from CSV file — run manually, then never again",
    schedule_interval=None,           # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["clinical_trials", "Andres", "backfill"],
    doc_md="""
    ## Clinical Trial Backfill (One-Time)

    Loads the full historical dataset from CSV (496,615 studies).
    **Run this once** to populate the database, then use the incremental DAG.

    Connection: `Cloudberry_dwh_dev_data_power_db`
    """,
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: logger.info("Backfill pipeline started"),
    )

    task_ingest = PythonOperator(
        task_id="ingest_csv",
        python_callable=ingest_csv,
    )

    task_clean = PythonOperator(
        task_id="clean_and_transform",
        python_callable=clean_and_transform,
        op_kwargs={"ingest_task_id": "ingest_csv"},
    )

    task_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    task_load = PythonOperator(
        task_id="load_to_database",
        python_callable=load_to_database,
        op_kwargs={"ingest_task_id": "ingest_csv"},
    )

    task_analytics = PythonOperator(
        task_id="run_analytical_queries",
        python_callable=run_analytical_queries,
    )

    task_cleanup = PythonOperator(
        task_id="cleanup_temp_files",
        python_callable=cleanup_temp_files,
    )

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: logger.info("Backfill complete"),
    )

    start >> task_ingest >> task_clean >> task_validate >> task_load >> task_analytics >> task_cleanup >> end
