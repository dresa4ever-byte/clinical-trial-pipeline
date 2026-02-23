"""
Shared task functions used by both backfill and incremental DAGs.
Keeps logic in one place — no duplication.
"""

import pandas as pd
import numpy as np
import logging
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

AIRFLOW_CONN_ID = "Cloudberry_dwh_dev_data_power_db"


def clean_and_transform(ingest_task_id: str, **context):
    """
    Clean and transform data — applies 12 cleaning rules from EDA.
    Works identically for both CSV and API data (same DataFrame format).
    """
    from src.processing.cleaner import DataCleaner

    ti = context["ti"]
    data_path = ti.xcom_pull(task_ids=ingest_task_id, key="data_path")

    if not data_path:
        logger.warning("No data to clean — skipping")
        return

    df = pd.read_parquet(data_path)
    logger.info(f"Cleaning {len(df):,} rows...")

    cleaner = DataCleaner(df)
    cleaned_data = cleaner.clean_all()

    # Save all cleaned DataFrames
    cleaned_data["studies"].to_parquet("/tmp/clean_studies.parquet", index=False)
    cleaned_data["organizations"].to_parquet("/tmp/clean_organizations.parquet", index=False)
    cleaned_data["conditions"].to_parquet("/tmp/clean_conditions.parquet", index=False)
    cleaned_data["interventions"].to_parquet("/tmp/clean_interventions.parquet", index=False)
    cleaned_data["age_groups"].to_parquet("/tmp/clean_age_groups.parquet", index=False)
    cleaned_data["mesh_terms"].to_parquet("/tmp/clean_mesh_terms.parquet", index=False)

    ti.xcom_push(key="cleaning_stats", value=cleaned_data["stats"])
    ti.xcom_push(key="studies_count", value=len(cleaned_data["studies"]))

    logger.info(f"Cleaning complete: {len(cleaned_data['studies']):,} studies")


def validate_data(**context):
    """
    Validate cleaned data before loading.
    Acts as a quality gate — pipeline stops if critical errors found.
    """
    from src.processing.validator import DataValidator

    cleaned_data = {
        "studies": pd.read_parquet("/tmp/clean_studies.parquet"),
        "organizations": pd.read_parquet("/tmp/clean_organizations.parquet"),
        "conditions": pd.read_parquet("/tmp/clean_conditions.parquet"),
        "interventions": pd.read_parquet("/tmp/clean_interventions.parquet"),
        "age_groups": pd.read_parquet("/tmp/clean_age_groups.parquet"),
        "mesh_terms": pd.read_parquet("/tmp/clean_mesh_terms.parquet"),
    }

    validator = DataValidator()
    is_valid, report = validator.validate_all(cleaned_data)

    if not is_valid:
        raise ValueError(f"Data validation failed!\n{report}")

    logger.info("Validation passed")


def load_to_database(ingest_task_id: str, **context):
    """
    Load cleaned data into Cloudberry/PostgreSQL.
    Uses Airflow connection: Cloudberry_dwh_dev_data_power_db
    """
    from src.loading.loader import DatabaseLoader
    from src.config import generate_run_id

    ti = context["ti"]

    hook = PostgresHook(postgres_conn_id=AIRFLOW_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    cleaned_data = {
        "studies": pd.read_parquet("/tmp/clean_studies.parquet"),
        "organizations": pd.read_parquet("/tmp/clean_organizations.parquet"),
        "conditions": pd.read_parquet("/tmp/clean_conditions.parquet"),
        "interventions": pd.read_parquet("/tmp/clean_interventions.parquet"),
        "age_groups": pd.read_parquet("/tmp/clean_age_groups.parquet"),
        "mesh_terms": pd.read_parquet("/tmp/clean_mesh_terms.parquet"),
    }

    # Add locations if available (from API ingestion)
    has_locations = ti.xcom_pull(task_ids=ingest_task_id, key="has_locations")
    if has_locations and os.path.exists("/tmp/api_locations.parquet"):
        cleaned_data["locations"] = pd.read_parquet("/tmp/api_locations.parquet")

    run_id = generate_run_id()
    loader = DatabaseLoader(engine=engine)
    stats = loader.load_all(cleaned_data, run_id)

    ti.xcom_push(key="load_stats", value=stats)
    ti.xcom_push(key="run_id", value=run_id)
    logger.info(f"Database load complete. Run ID: {run_id}")


def run_analytical_queries(**context):
    """
    Run analytical queries and log results.
    Demonstrates that the loaded data supports real analytics.
    """
    hook = PostgresHook(postgres_conn_id=AIRFLOW_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    queries = {
        "trials_by_type_phase": """
            SELECT study_type, phase, COUNT(*) AS trial_count
            FROM studies GROUP BY study_type, phase
            ORDER BY trial_count DESC LIMIT 10
        """,
        "top_conditions": """
            SELECT sc.condition_name, COUNT(DISTINCT sc.study_id) AS num_studies
            FROM study_conditions sc GROUP BY sc.condition_name
            ORDER BY num_studies DESC LIMIT 10
        """,
        "completion_by_year": """
            SELECT EXTRACT(YEAR FROM start_date) AS yr, COUNT(*) AS total,
                   SUM(CASE WHEN overall_status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed
            FROM studies WHERE start_date IS NOT NULL
            GROUP BY EXTRACT(YEAR FROM start_date)
            ORDER BY yr DESC LIMIT 10
        """,
    }

    with engine.connect() as conn:
        for name, sql in queries.items():
            result = pd.read_sql(sql, conn)
            logger.info(f"\n--- {name} ---\n{result.to_string()}")

    logger.info("Analytical queries complete")


def cleanup_temp_files(**context):
    """Remove temporary parquet files to free disk space."""
    import glob
    for f in glob.glob("/tmp/*.parquet"):
        os.remove(f)
        logger.info(f"Cleaned up: {f}")
