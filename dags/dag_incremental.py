"""
=============================================================================
  DAG: clinical_trial_incremental
=============================================================================
  Weekly incremental load from ClinicalTrials.gov API v2.
  Fetches new/updated studies every Monday at 06:00 UTC.

  Flow: start → ingest_api → clean → validate → load → analytics → cleanup → end

  The API provides ~572K+ studies (vs 496K in CSV), including:
    - Newly registered trials
    - Updated trial statuses
    - Geographic location data (not available in CSV)

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

API_MAX_STUDIES = 1000


# =============================================================================
# INGESTION TASK (API-specific)
# =============================================================================
def ingest_api(**context):
    """
    Fetch NEW clinical trial data from ClinicalTrials.gov API v2.
    The API is the live data source for periodic incremental updates.
    """
    from src.ingestion.api_ingestor import APIIngestor

    ingestor = APIIngestor(
        condition=None,           # fetch all conditions
        max_studies=API_MAX_STUDIES,
    )
    df = ingestor.ingest()

    if len(df) == 0:
        logger.warning("No data returned from API")
        context["ti"].xcom_push(key="data_path", value=None)
        context["ti"].xcom_push(key="row_count", value=0)
        context["ti"].xcom_push(key="has_locations", value=False)
        return

    # Extract locations (API bonus — enables geographic analytics)
    locations_df = APIIngestor.extract_locations(df)
    df = df.drop(columns=["_locations"], errors="ignore")

    tmp_data_path = "/tmp/raw_data.parquet"
    df.to_parquet(tmp_data_path, index=False)

    has_locations = len(locations_df) > 0
    if has_locations:
        locations_df.to_parquet("/tmp/api_locations.parquet", index=False)

    context["ti"].xcom_push(key="data_path", value=tmp_data_path)
    context["ti"].xcom_push(key="row_count", value=len(df))
    context["ti"].xcom_push(key="has_locations", value=has_locations)

    logger.info(f"API ingested: {len(df):,} studies, {len(locations_df):,} locations")


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
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="clinical_trial_incremental",
    default_args=default_args,
    description="Weekly incremental load from ClinicalTrials.gov API",
    schedule_interval="0 6 * * 1",    # Every Monday at 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["clinical_trials", "Andres", "incremental"],
    doc_md="""
    ## Clinical Trial Incremental Update (Weekly)

    Fetches new/updated studies from ClinicalTrials.gov API v2.
    Runs every Monday at 06:00 UTC.

    Connection: `Cloudberry_dwh_dev_data_power_db`
    """,
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: logger.info("Incremental pipeline started"),
    )

    task_ingest = PythonOperator(
        task_id="ingest_api",
        python_callable=ingest_api,
    )

    task_clean = PythonOperator(
        task_id="clean_and_transform",
        python_callable=clean_and_transform,
        op_kwargs={"ingest_task_id": "ingest_api"},
    )

    task_validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    task_load = PythonOperator(
        task_id="load_to_database",
        python_callable=load_to_database,
        op_kwargs={"ingest_task_id": "ingest_api"},
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
        python_callable=lambda: logger.info("Incremental update complete"),
    )

    start >> task_ingest >> task_clean >> task_validate >> task_load >> task_analytics >> task_cleanup >> end
