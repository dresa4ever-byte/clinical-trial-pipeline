"""
Base Ingestor — Abstract base class for all data source ingestors.

Design Pattern: Strategy pattern — all ingestors share the same interface,
making it easy to add new data sources (API, SQL, Parquet, etc.) without
changing the pipeline logic.

Usage:
    All ingestors must implement the `ingest()` method which returns a pandas DataFrame.
"""

from abc import ABC, abstractmethod
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BaseIngestor(ABC):
    """
    Abstract base class for data ingestors.
    
    Every data source (CSV, JSON API, SQL database) implements this interface.
    This ensures the pipeline can swap data sources without changing downstream logic.
    """
    
    def __init__(self, source_name: str):
        """
        Args:
            source_name: Human-readable name for logging (e.g., 'clinical_trials.csv')
        """
        self.source_name = source_name
        self.row_count = 0
        self.column_count = 0
    
    @abstractmethod
    def ingest(self) -> pd.DataFrame:
        """
        Read data from the source and return as a pandas DataFrame.
        
        Returns:
            pd.DataFrame with raw data (no cleaning applied yet)
        
        Raises:
            FileNotFoundError: if source file doesn't exist
            ConnectionError: if database/API is unreachable
            ValueError: if data format is unexpected
        """
        pass
    
    def validate_schema(self, df: pd.DataFrame, required_columns: list) -> bool:
        """
        Check that the DataFrame contains all expected columns.
        
        Args:
            df: DataFrame to validate
            required_columns: list of column names that must be present
        
        Returns:
            True if all columns present, raises ValueError otherwise
        """
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(
                f"Source '{self.source_name}' is missing required columns: {missing}"
            )
        logger.info(f"Schema validation passed for '{self.source_name}' — {len(df.columns)} columns found")
        return True
    
    def get_summary(self) -> dict:
        """Return summary stats about the ingested data."""
        return {
            "source": self.source_name,
            "rows": self.row_count,
            "columns": self.column_count,
        }
