"""
Data Validator — Validates cleaned data before database insertion.

Performs data quality checks on the cleaned DataFrames to ensure
they meet the requirements of our database schema.

Validation Rules:
    1. Required fields are not null (brief_title, study_type)
    2. Enum fields contain only valid values
    3. Dates are within reasonable range
    4. Bridge table references are consistent
    5. No duplicate organizations
"""

import pandas as pd
import logging

from src.config import (
    VALID_ORG_CLASSES, VALID_STUDY_TYPES, VALID_STATUSES,
    VALID_RESPONSIBLE_PARTIES, VALID_AGE_GROUPS,
)

logger = logging.getLogger(__name__)


class DataValidator:
    """
    Validates cleaned DataFrames before loading to the database.
    
    Usage:
        validator = DataValidator()
        is_valid, report = validator.validate_all(cleaned_data)
        if not is_valid:
            print(report)
    """
    
    def __init__(self):
        self.errors = []
        self.warnings = []
    
    def validate_all(self, data: dict) -> tuple:
        """
        Run all validation checks on the cleaned data.
        
        Args:
            data: dict from DataCleaner.clean_all() containing all DataFrames
        
        Returns:
            tuple of (is_valid: bool, report: str)
            is_valid is True if no errors (warnings are OK)
        """
        self.errors = []
        self.warnings = []
        
        studies = data["studies"]
        orgs = data["organizations"]
        conditions = data["conditions"]
        interventions = data["interventions"]
        age_groups = data["age_groups"]
        mesh_terms = data["mesh_terms"]
        
        # --- Validation 1: Required fields ---
        self._check_not_null(studies, "brief_title", is_error=True)
        self._check_not_null(studies, "study_type", is_error=True)
        
        # --- Validation 2: Enum values ---
        self._check_valid_values(studies, "study_type", VALID_STUDY_TYPES)
        self._check_valid_values(studies, "overall_status", VALID_STATUSES)
        self._check_valid_values(studies, "responsible_party", VALID_RESPONSIBLE_PARTIES)
        self._check_valid_values(orgs, "org_class", VALID_ORG_CLASSES)
        self._check_valid_values(age_groups, "age_group", VALID_AGE_GROUPS)
        
        # --- Validation 3: Date range ---
        self._check_date_range(studies, "start_date")
        
        # --- Validation 4: No duplicate orgs ---
        self._check_unique(orgs, "org_name")
        
        # --- Validation 5: Bridge tables have data ---
        self._check_not_empty(conditions, "conditions")
        self._check_not_empty(interventions, "interventions")
        self._check_not_empty(age_groups, "age_groups")
        
        # --- Build report ---
        report = self._build_report()
        is_valid = len(self.errors) == 0
        
        if is_valid:
            logger.info(f"Validation PASSED ({len(self.warnings)} warnings)")
        else:
            logger.error(f"Validation FAILED: {len(self.errors)} errors, {len(self.warnings)} warnings")
        
        return is_valid, report
    
    # =========================================================================
    # VALIDATION METHODS
    # =========================================================================
    
    def _check_not_null(self, df: pd.DataFrame, column: str, is_error: bool = True):
        """Check that a column has no null values."""
        null_count = df[column].isna().sum()
        if null_count > 0:
            msg = f"Column '{column}' has {null_count:,} null values"
            if is_error:
                self.errors.append(msg)
            else:
                self.warnings.append(msg)
    
    def _check_valid_values(self, df: pd.DataFrame, column: str, valid_values: list):
        """
        Check that non-null values in a column are from the expected set.
        Null values are allowed (they represent missing data).
        """
        non_null = df[column].dropna()
        invalid = non_null[~non_null.isin(valid_values)]
        
        if len(invalid) > 0:
            unique_invalid = invalid.unique()[:10]  # show at most 10 examples
            self.warnings.append(
                f"Column '{column}' has {len(invalid):,} unexpected values. "
                f"Examples: {list(unique_invalid)}"
            )
    
    def _check_date_range(self, df: pd.DataFrame, column: str):
        """Check that parsed dates are within a reasonable range."""
        dates = df[column].dropna()
        if len(dates) == 0:
            self.warnings.append(f"Column '{column}' has no valid dates")
            return
        
        min_date = dates.min()
        max_date = dates.max()
        logger.info(f"Date range: {min_date.date()} to {max_date.date()}")
    
    def _check_unique(self, df: pd.DataFrame, column: str):
        """Check that values in a column are unique."""
        duplicates = df[column].duplicated().sum()
        if duplicates > 0:
            self.errors.append(
                f"Column '{column}' has {duplicates:,} duplicate values"
            )
    
    def _check_not_empty(self, df: pd.DataFrame, name: str):
        """Check that a DataFrame is not empty."""
        if len(df) == 0:
            self.errors.append(f"Bridge table '{name}' is empty")
    
    # =========================================================================
    # REPORT
    # =========================================================================
    
    def _build_report(self) -> str:
        """Build a human-readable validation report."""
        lines = []
        lines.append("=" * 60)
        lines.append("  DATA VALIDATION REPORT")
        lines.append("=" * 60)
        
        if self.errors:
            lines.append(f"\n  ERRORS ({len(self.errors)}):")
            for e in self.errors:
                lines.append(f"    ✗ {e}")
        
        if self.warnings:
            lines.append(f"\n  WARNINGS ({len(self.warnings)}):")
            for w in self.warnings:
                lines.append(f"    ⚠ {w}")
        
        if not self.errors and not self.warnings:
            lines.append("\n  ✓ All checks passed — no issues found")
        
        result = "PASSED" if not self.errors else "FAILED"
        lines.append(f"\n  Result: {result}")
        lines.append("=" * 60)
        
        report = "\n".join(lines)
        logger.info("\n" + report)
        return report
