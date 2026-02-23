"""
CSV Ingestor — Reads clinical trial data from CSV files.

This is the primary data source for the pipeline.
Handles the 390MB ClinicalTrials.gov dataset.
"""

import pandas as pd
import logging
import os

from src.ingestion.base_ingestor import BaseIngestor

logger = logging.getLogger(__name__)


class CSVIngestor(BaseIngestor):
    """
    Reads clinical trial data from a CSV file.
    
    Handles:
        - Large files (reads in chunks if needed)
        - Encoding issues
        - Column name standardization
    """
    
    # Columns we expect in the ClinicalTrials.gov dataset
    EXPECTED_COLUMNS = [
        "Organization Full Name",
        "Organization Class",
        "Responsible Party",
        "Brief Title",
        "Full Title",
        "Overall Status",
        "Start Date",
        "Standard Age",
        "Conditions",
        "Primary Purpose",
        "Interventions",
        "Intervention Description",
        "Study Type",
        "Phases",
        "Outcome Measure",
        "Medical Subject Headings",
    ]
    
    def __init__(self, file_path: str):
        """
        Args:
            file_path: Full path to the CSV file
        """
        super().__init__(source_name=os.path.basename(file_path))
        self.file_path = file_path
    
    def ingest(self) -> pd.DataFrame:
        """
        Read the CSV file into a pandas DataFrame.
        
        Returns:
            pd.DataFrame with raw clinical trial data
            
        Raises:
            FileNotFoundError: if CSV file doesn't exist
            ValueError: if required columns are missing
        """
        # --- Step 1: Verify file exists ---
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")
        
        file_size_mb = os.path.getsize(self.file_path) / (1024 * 1024)
        logger.info(f"Reading CSV: {self.file_path} ({file_size_mb:.1f} MB)")
        
        # --- Step 2: Read CSV ---
        # low_memory=False prevents mixed type warnings on large files
        # keep_default_na=False prevents pandas from converting strings like "NA" to NaN
        # (we handle null detection ourselves in the cleaner)
        df = pd.read_csv(
            self.file_path,
            low_memory=False,
            keep_default_na=True,      # let pandas handle obvious NaN/None
            na_values=[""],             # treat empty strings as NaN too
            encoding="utf-8",
        )
        
        logger.info(f"CSV loaded: {df.shape[0]:,} rows × {df.shape[1]} columns")
        
        # --- Step 3: Drop the unnamed index column if present ---
        unnamed_cols = [c for c in df.columns if c.startswith("Unnamed")]
        if unnamed_cols:
            df = df.drop(columns=unnamed_cols)
            logger.info(f"Dropped index column(s): {unnamed_cols}")
        
        # --- Step 4: Validate schema ---
        self.validate_schema(df, self.EXPECTED_COLUMNS)
        
        # --- Step 5: Store metadata ---
        self.row_count = len(df)
        self.column_count = len(df.columns)
        
        logger.info(f"Ingestion complete: {self.row_count:,} rows ready for processing")
        
        return df
