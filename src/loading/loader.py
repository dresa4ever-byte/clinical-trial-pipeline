"""
Database Loader — Loads cleaned data into PostgreSQL/Cloudberry tables.

Handles:
    - Organization insertion with ID mapping
    - Studies insertion with foreign key resolution
    - Bridge table bulk insertion
    - Batch processing for memory efficiency
    - Pipeline logging to pipeline_log table
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from sqlalchemy import text

from src.config import get_engine, BATCH_SIZE

logger = logging.getLogger(__name__)


class DatabaseLoader:
    """
    Loads cleaned DataFrames into the database.
    
    Works in both modes:
        - Direct connection (local testing)
        - Airflow connection (DAG execution via Cloudberry_dwh_dev_data_power_db)
    
    Usage:
        loader = DatabaseLoader()
        loader.load_all(cleaned_data, run_id="abc-20260220")
    """
    
    def __init__(self, engine=None):
        """
        Args:
            engine: SQLAlchemy engine. If None, uses config.get_engine()
                    which auto-detects direct vs Airflow mode.
        """
        self.engine = engine or get_engine()
        self.stats = {}
    
    def load_all(self, data: dict, run_id: str) -> dict:
        """
        Load all cleaned DataFrames into the database.
        
        Order matters:
            1. Organizations first (studies reference them)
            2. Studies second (bridge tables reference them)
            3. Bridge tables last
        
        Args:
            data: dict from DataCleaner.clean_all()
            run_id: unique identifier for this pipeline run
        
        Returns:
            dict with loading statistics
        """
        logger.info(f"Starting database load (run_id: {run_id})...")
        
        # Step 1: Clear existing data (full refresh approach)
        self._truncate_tables()
        
        # Step 2: Load organizations → get org_id mapping
        org_mapping = self._load_organizations(data["organizations"], run_id)
        
        # Step 3: Load studies → get study_id mapping
        study_mapping = self._load_studies(data["studies"], org_mapping, run_id)
        
        # Step 4: Load bridge tables using study_id mapping
        self._load_bridge_table(
            data["conditions"], study_mapping,
            table_name="study_conditions",
            value_column="condition_name",
            run_id=run_id
        )
        self._load_bridge_table(
            data["interventions"], study_mapping,
            table_name="study_interventions",
            value_column="intervention_name",
            run_id=run_id
        )
        self._load_bridge_table(
            data["age_groups"], study_mapping,
            table_name="study_age_groups",
            value_column="age_group",
            run_id=run_id
        )
        self._load_bridge_table(
            data["mesh_terms"], study_mapping,
            table_name="study_mesh_terms",
            value_column="mesh_term",
            run_id=run_id
        )
        
        logger.info("Database load complete!")
        self._log_summary()
        
        return self.stats
    
    # =========================================================================
    # TRUNCATE (full refresh)
    # =========================================================================
    def _truncate_tables(self):
        """
        Clear all data tables before loading.
        
        Design Decision: Using full refresh (truncate + reload) rather than 
        incremental/upsert for the prototype. In production, you'd implement
        upsert (INSERT ON CONFLICT) for efficiency.
        """
        tables = [
            "study_mesh_terms", "study_age_groups",
            "study_interventions", "study_conditions",
            "studies", "organizations"
        ]
        
        with self.engine.connect() as conn:
            for table in tables:
                conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
            conn.execute(text("COMMIT"))
        
        logger.info("Truncated all data tables")
    
    # =========================================================================
    # LOAD ORGANIZATIONS
    # =========================================================================
    def _load_organizations(self, orgs_df: pd.DataFrame, run_id: str) -> dict:
        """
        Insert organizations and return a mapping of org_name → org_id.
        
        Args:
            orgs_df: DataFrame with [org_name, org_class]
            run_id: pipeline run ID for logging
        
        Returns:
            dict mapping org_name → org_id (database-assigned)
        """
        start_time = datetime.now()
        logger.info(f"Loading {len(orgs_df):,} organizations...")
        
        # Replace NaN with None for proper SQL NULL insertion
        orgs_clean = orgs_df.where(orgs_df.notna(), None)
        
        # Insert in batches
        total_inserted = 0
        for start in range(0, len(orgs_clean), BATCH_SIZE):
            batch = orgs_clean.iloc[start:start + BATCH_SIZE]
            batch.to_sql(
                "organizations",
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            total_inserted += len(batch)
            if total_inserted % 10000 == 0:
                logger.info(f"  Organizations: {total_inserted:,} / {len(orgs_clean):,}")
        
        # Read back the org_id mapping
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT org_id, org_name FROM organizations"))
            org_mapping = {row[1]: row[0] for row in result}
        
        self.stats["organizations_loaded"] = total_inserted
        self._log_pipeline_step(run_id, "load_organizations", "SUCCESS",
                                total_inserted, 0, start_time)
        
        logger.info(f"Organizations loaded: {total_inserted:,} rows")
        return org_mapping
    
    # =========================================================================
    # LOAD STUDIES
    # =========================================================================
    def _load_studies(self, studies_df: pd.DataFrame, org_mapping: dict,
                      run_id: str) -> dict:
        """
        Insert studies and return a mapping of source_row_index → study_id.
        
        Args:
            studies_df: DataFrame from cleaner with all study columns
            org_mapping: dict of org_name → org_id
            run_id: pipeline run ID
        
        Returns:
            dict mapping source_row_index → study_id (database-assigned)
        """
        start_time = datetime.now()
        logger.info(f"Loading {len(studies_df):,} studies...")
        
        # Map org_name → org_id
        studies_df = studies_df.copy()
        studies_df["org_id"] = studies_df["org_name"].map(org_mapping)
        
        # Select only columns that match the database table
        db_columns = [
            "org_id", "responsible_party", "brief_title", "full_title",
            "overall_status", "start_date", "start_date_raw", "start_date_is_approx",
            "primary_purpose", "study_type", "phase", "outcome_measure",
            "intervention_description",
        ]
        
        studies_for_db = studies_df[db_columns].copy()
        
        # Replace NaN with None for proper SQL NULL
        studies_for_db = studies_for_db.where(studies_for_db.notna(), None)
        
        # Convert start_date to proper date objects
        # (pandas NaT needs to become None for SQL)
        studies_for_db["start_date"] = studies_for_db["start_date"].apply(
            lambda x: x.date() if pd.notna(x) else None
        )
        
        # Insert in batches
        total_inserted = 0
        for start in range(0, len(studies_for_db), BATCH_SIZE):
            batch = studies_for_db.iloc[start:start + BATCH_SIZE]
            batch.to_sql(
                "studies",
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            total_inserted += len(batch)
            if total_inserted % 50000 == 0:
                logger.info(f"  Studies: {total_inserted:,} / {len(studies_for_db):,}")
        
        # Read back study_id mapping
        # Since we inserted in order, study_id corresponds to row order
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT study_id FROM studies ORDER BY study_id"))
            study_ids = [row[0] for row in result]
        
        # Map: source_row_index (original DataFrame index) → study_id
        source_indices = studies_df["source_row_index"].values
        study_mapping = dict(zip(source_indices, study_ids))
        
        self.stats["studies_loaded"] = total_inserted
        self._log_pipeline_step(run_id, "load_studies", "SUCCESS",
                                total_inserted, 0, start_time)
        
        logger.info(f"Studies loaded: {total_inserted:,} rows")
        return study_mapping
    
    # =========================================================================
    # LOAD BRIDGE TABLES
    # =========================================================================
    def _load_bridge_table(self, bridge_df: pd.DataFrame, study_mapping: dict,
                           table_name: str, value_column: str, run_id: str):
        """
        Insert rows into a bridge table, mapping source_row_index → study_id.
        
        Args:
            bridge_df: DataFrame with [source_row_index, {value_column}]
            study_mapping: dict of source_row_index → study_id
            table_name: target table name (e.g., 'study_conditions')
            value_column: name of the value column (e.g., 'condition_name')
            run_id: pipeline run ID
        """
        start_time = datetime.now()
        logger.info(f"Loading {len(bridge_df):,} rows into {table_name}...")
        
        # Map source_row_index → study_id
        bridge_df = bridge_df.copy()
        bridge_df["study_id"] = bridge_df["source_row_index"].map(study_mapping)
        
        # Drop rows where mapping failed (shouldn't happen, but safety check)
        unmapped = bridge_df["study_id"].isna().sum()
        if unmapped > 0:
            logger.warning(f"  {unmapped:,} rows could not be mapped to study_id — skipping")
            bridge_df = bridge_df.dropna(subset=["study_id"])
        
        bridge_df["study_id"] = bridge_df["study_id"].astype(int)
        
        # Select only the columns for the database
        db_df = bridge_df[["study_id", value_column]].copy()
        
        # Replace NaN with None
        db_df = db_df.where(db_df.notna(), None)
        
        # Insert in batches
        total_inserted = 0
        for start in range(0, len(db_df), BATCH_SIZE):
            batch = db_df.iloc[start:start + BATCH_SIZE]
            batch.to_sql(
                table_name,
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            total_inserted += len(batch)
            if total_inserted % 100000 == 0:
                logger.info(f"  {table_name}: {total_inserted:,} / {len(db_df):,}")
        
        self.stats[f"{table_name}_loaded"] = total_inserted
        self._log_pipeline_step(run_id, f"load_{table_name}", "SUCCESS",
                                total_inserted, unmapped, start_time)
        
        logger.info(f"{table_name} loaded: {total_inserted:,} rows")
    
    # =========================================================================
    # PIPELINE LOGGING
    # =========================================================================
    def _log_pipeline_step(self, run_id: str, step_name: str, status: str,
                           rows_processed: int, rows_rejected: int,
                           start_time: datetime, error_message: str = None):
        """Write a log entry to the pipeline_log table."""
        completed_at = datetime.now()
        duration = (completed_at - start_time).total_seconds()
        
        log_entry = pd.DataFrame([{
            "run_id": run_id,
            "step_name": step_name,
            "status": status,
            "rows_processed": rows_processed,
            "rows_rejected": rows_rejected,
            "error_message": error_message,
            "started_at": start_time,
            "completed_at": completed_at,
            "duration_seconds": round(duration, 2),
        }])
        
        log_entry.to_sql("pipeline_log", self.engine, if_exists="append",
                         index=False, method="multi")
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    def _log_summary(self):
        """Print loading summary."""
        logger.info("=" * 60)
        logger.info("LOADING SUMMARY")
        logger.info("=" * 60)
        for key, value in self.stats.items():
            logger.info(f"  {key:<35s}: {value:>12,}")
        logger.info("=" * 60)
