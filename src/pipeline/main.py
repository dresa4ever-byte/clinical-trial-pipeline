"""
=============================================================================
  CLINICAL TRIAL DATA PIPELINE — Main Entry Point
=============================================================================

  This script runs the complete ETL pipeline:
    1. INGEST  — Read clinical trial data from CSV
    2. CLEAN   — Apply 12 cleaning rules (nulls, dates, splitting, etc.)
    3. VALIDATE — Check data quality before loading
    4. LOAD    — Insert into PostgreSQL/Cloudberry tables

  Usage:
    python -m src.pipeline.main
    
    Or in Spyder:
    Run this file directly (F5)

  Prerequisites:
    - Database tables created (run sql/create_tables.sql first)
    - Update config.py with your DB credentials and CSV file path
    - Install dependencies: pip install pandas sqlalchemy psycopg2-binary
=============================================================================
"""

import sys
import os
import logging
from datetime import datetime

# Add project root to path so imports work from any location
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.config import CSV_FILE_PATH, generate_run_id
from src.ingestion.csv_ingestor import CSVIngestor
from src.ingestion.api_ingestor import APIIngestor
from src.processing.cleaner import DataCleaner
from src.processing.validator import DataValidator
from src.loading.loader import DatabaseLoader


# =============================================================================
# LOGGING SETUP
# =============================================================================
def setup_logging():
    """Configure logging to console and file."""
    log_format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    
    # Create output directory if it doesn't exist
    os.makedirs("output", exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                f"output/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
                encoding="utf-8"
            ),
        ],
    )


# =============================================================================
# MAIN PIPELINE
# =============================================================================
def run_pipeline(source: str = "csv", api_condition: str = None, 
                 api_max_studies: int = 500):
    """
    Execute the full ETL pipeline.
    
    Supports multiple data sources through the same pipeline:
        - source="csv"  → reads from CSV file (bulk historical load)
        - source="api"  → fetches from ClinicalTrials.gov API (live/incremental)
        - source="both" → runs CSV first, then API for new studies
    
    Args:
        source: Data source — "csv", "api", or "both"
        api_condition: Condition to filter API results (e.g., "Breast Cancer")
        api_max_studies: Max studies to fetch from API
    """
    setup_logging()
    logger = logging.getLogger("pipeline")
    
    run_id = generate_run_id()
    pipeline_start = datetime.now()
    
    logger.info("=" * 70)
    logger.info(f"  CLINICAL TRIAL DATA PIPELINE")
    logger.info(f"  Run ID:  {run_id}")
    logger.info(f"  Source:  {source}")
    logger.info(f"  Started: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    try:
        # =====================================================================
        # STEP 1: INGEST (from CSV, API, or both)
        # =====================================================================
        logger.info("\n" + "=" * 40)
        logger.info("STEP 1/4: INGESTION")
        logger.info("=" * 40)
        
        if source in ("csv", "both"):
            logger.info("--- CSV Ingestion ---")
            csv_ingestor = CSVIngestor(CSV_FILE_PATH)
            raw_df = csv_ingestor.ingest()
            raw_df["data_source"] = "csv"
            logger.info(f"CSV: {raw_df.shape[0]:,} rows ingested")
        
        if source in ("api", "both"):
            logger.info("--- API Ingestion ---")
            api_ingestor = APIIngestor(
                condition=api_condition,
                max_studies=api_max_studies,
            )
            api_df = api_ingestor.ingest()
            
            # Extract locations before dropping the _locations column
            locations_df = APIIngestor.extract_locations(api_df)
            api_df = api_df.drop(columns=["_locations"], errors="ignore")
            
            if source == "api":
                raw_df = api_df
            elif source == "both":
                # Combine CSV + API data
                logger.info("Combining CSV and API data...")
                raw_df = pd.concat([raw_df, api_df], ignore_index=True)
                logger.info(f"Combined: {raw_df.shape[0]:,} total rows")
        
        logger.info(f"Ingested: {raw_df.shape[0]:,} rows × {raw_df.shape[1]} columns")
        
        # =====================================================================
        # STEP 2: CLEAN & TRANSFORM
        # =====================================================================
        logger.info("\n" + "=" * 40)
        logger.info("STEP 2/4: CLEANING & TRANSFORMATION")
        logger.info("=" * 40)
        
        cleaner = DataCleaner(raw_df)
        cleaned_data = cleaner.clean_all()
        
        # Add locations if we have them from API
        if source in ("api", "both") and len(locations_df) > 0:
            cleaned_data["locations"] = locations_df
        
        logger.info(f"  Studies:        {len(cleaned_data['studies']):,} rows")
        logger.info(f"  Organizations:  {len(cleaned_data['organizations']):,} rows")
        logger.info(f"  Conditions:     {len(cleaned_data['conditions']):,} rows")
        logger.info(f"  Interventions:  {len(cleaned_data['interventions']):,} rows")
        logger.info(f"  Age Groups:     {len(cleaned_data['age_groups']):,} rows")
        logger.info(f"  MeSH Terms:     {len(cleaned_data['mesh_terms']):,} rows")
        if "locations" in cleaned_data:
            logger.info(f"  Locations:      {len(cleaned_data['locations']):,} rows")
        
        # =====================================================================
        # STEP 3: VALIDATE
        # =====================================================================
        logger.info("\n" + "=" * 40)
        logger.info("STEP 3/4: VALIDATION")
        logger.info("=" * 40)
        
        validator = DataValidator()
        is_valid, report = validator.validate_all(cleaned_data)
        
        if not is_valid:
            logger.error("Validation FAILED. Check the report above.")
            logger.error("Pipeline aborted. Fix data issues and re-run.")
            return False
        
        # =====================================================================
        # STEP 4: LOAD TO DATABASE
        # =====================================================================
        logger.info("\n" + "=" * 40)
        logger.info("STEP 4/4: DATABASE LOADING")
        logger.info("=" * 40)
        
        loader = DatabaseLoader()
        load_stats = loader.load_all(cleaned_data, run_id)
        
        # =====================================================================
        # DONE
        # =====================================================================
        pipeline_end = datetime.now()
        duration = (pipeline_end - pipeline_start).total_seconds()
        
        logger.info("\n" + "=" * 70)
        logger.info("  PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"  Run ID:   {run_id}")
        logger.info(f"  Source:   {source}")
        logger.info(f"  Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        logger.info(f"  Studies loaded:  {load_stats.get('studies_loaded', 0):,}")
        logger.info("=" * 70)
        
        return True
        
    except FileNotFoundError as e:
        logger.error(f"FILE NOT FOUND: {e}")
        logger.error("Check CSV_FILE_PATH in config.py")
        return False
        
    except Exception as e:
        logger.error(f"PIPELINE FAILED: {type(e).__name__}: {e}")
        logger.error("Check the log file for details", exc_info=True)
        return False


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    import pandas as pd
    
    # -------------------------------------------------------
    # Choose your mode here:
    # -------------------------------------------------------
    
    # MODE 1: CSV only (bulk load from file)
    success = run_pipeline(source="csv")
    
    # MODE 2: API only (fetch live data)
    # success = run_pipeline(source="api", api_condition="Breast Cancer", api_max_studies=200)
    
    # MODE 3: Both CSV + API
    # success = run_pipeline(source="both", api_condition="Breast Cancer", api_max_studies=200)
    
    # -------------------------------------------------------
    
    if success:
        print("\n✓ Pipeline completed successfully! Check your database tables.")
    else:
        print("\n✗ Pipeline failed. Check the logs above for details.")
