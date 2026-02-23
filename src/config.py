"""
Configuration module for the Clinical Trial Data Pipeline.

Update the DATABASE section below with your PostgreSQL/Cloudberry connection details.
"""

import os
from datetime import datetime
import uuid


# =============================================================================
# DATABASE CONNECTION
# =============================================================================
# Two modes:
#   1. DIRECT: Uses DB_CONFIG below (for local testing without Airflow)
#   2. AIRFLOW: Uses Airflow connection ID (for DAG execution)
#
# Set USE_AIRFLOW_CONNECTION = True when running inside an Airflow DAG.
# =============================================================================

USE_AIRFLOW_CONNECTION = False    # ← Set to True when running in Airflow
AIRFLOW_CONN_ID = "Cloudberry_dwh_dev_data_power_db"   # ← Your Airflow connection

# Direct connection (used when USE_AIRFLOW_CONNECTION = False)
DB_CONFIG = {
    "host": os.environ.get("PIPELINE_DB_HOST", "localhost"),
    "port": int(os.environ.get("PIPELINE_DB_PORT", 5432)),
    "database": os.environ.get("PIPELINE_DB_NAME", "your_db_name"),
    "user": os.environ.get("PIPELINE_DB_USER", "your_username"),
    "password": os.environ.get("PIPELINE_DB_PASSWORD", "your_password"),
}


def get_connection_string():
    """Get SQLAlchemy connection string (direct mode only)."""
    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


def get_engine():
    """
    Get SQLAlchemy engine — works in both direct and Airflow modes.
    
    Returns:
        sqlalchemy.Engine
    """
    if USE_AIRFLOW_CONNECTION:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=AIRFLOW_CONN_ID)
        return hook.get_sqlalchemy_engine()
    else:
        from sqlalchemy import create_engine
        return create_engine(get_connection_string())


# =============================================================================
# FILE PATHS
# =============================================================================
# Path to your full clinical trials CSV (390MB)
CSV_FILE_PATH = os.environ.get("CSV_FILE_PATH", r"C:\path\to\your\clinical_trials.csv")  # ← CHANGE THIS!

# Path where pipeline logs and reports will be saved
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "output")


# =============================================================================
# PIPELINE SETTINGS
# =============================================================================
# Batch size for database inserts (how many rows per INSERT)
# Larger = faster but uses more memory
BATCH_SIZE = 10000

# Generate a unique ID for each pipeline run
def generate_run_id():
    return str(uuid.uuid4())[:8] + "-" + datetime.now().strftime("%Y%m%d-%H%M%S")


# =============================================================================
# DATA CLEANING RULES
# =============================================================================
# Values that should be treated as NULL across all columns
HIDDEN_NULL_VALUES = [
    "Unknown", "unknown", "UNKNOWN",
    "None", "none", "NONE",
    "null", "Null", "NULL",
    "N/A", "n/a", "NA", "na",
    "Not Applicable", "not applicable",
    "Not Available", "not available",
    "Missing", "missing",
    "No phases listed",
    "-", "--", "---", ".", "..",
]

# Date boundaries — dates outside this range are flagged as suspicious
DATE_MIN_YEAR = 1950
DATE_MAX_YEAR = 2026

# Columns that are expected to have enum-like values
VALID_ORG_CLASSES = ["OTHER", "INDUSTRY", "NIH", "OTHER_GOV", "FED", "NETWORK", "INDIV"]
VALID_STUDY_TYPES = ["INTERVENTIONAL", "OBSERVATIONAL", "EXPANDED_ACCESS"]
VALID_STATUSES = [
    "COMPLETED", "RECRUITING", "TERMINATED", "NOT_YET_RECRUITING",
    "ACTIVE_NOT_RECRUITING", "WITHDRAWN", "ENROLLING_BY_INVITATION",
    "SUSPENDED", "WITHHELD", "NO_LONGER_AVAILABLE", "AVAILABLE",
    "APPROVED_FOR_MARKETING", "TEMPORARILY_NOT_AVAILABLE",
]
VALID_RESPONSIBLE_PARTIES = ["SPONSOR", "PRINCIPAL_INVESTIGATOR", "SPONSOR_INVESTIGATOR"]
VALID_AGE_GROUPS = ["CHILD", "ADULT", "OLDER_ADULT"]
