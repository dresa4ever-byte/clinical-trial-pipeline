"""
Data Cleaner — Applies all cleaning and transformation rules.

Based on EDA findings from 496,615 rows of clinical trial data.
Each cleaning rule is a separate method for testability and clarity.

Cleaning Rules Applied:
    1.  Standardize hidden nulls ("Unknown", "UNKNOWN", etc.) → None
    2.  Fix organization class case inconsistency
    3.  Parse dates (YYYY-MM-DD and YYYY-MM formats)
    4.  Flag suspicious dates (before 1950 or after 2026)
    5.  Mark approximate dates (YYYY-MM → set day to 01, flag as approx)
    6.  Normalize phase values ("No phases listed" → None)
    7.  Trim whitespace on all string columns
    8.  Split multi-value: Conditions (comma-separated)
    9.  Split multi-value: Interventions (comma-separated)
    10. Split multi-value: Age groups (space-separated)
    11. Split multi-value: MeSH terms (comma-separated)
    12. Extract and deduplicate organizations
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime

from src.config import HIDDEN_NULL_VALUES, DATE_MIN_YEAR, DATE_MAX_YEAR

logger = logging.getLogger(__name__)


class DataCleaner:
    """
    Cleans and transforms raw clinical trial data.
    
    Usage:
        cleaner = DataCleaner(raw_df)
        result = cleaner.clean_all()
        # result contains: studies_df, orgs_df, conditions_df, etc.
    """
    
    def __init__(self, df: pd.DataFrame):
        """
        Args:
            df: Raw DataFrame from the ingestor (496K rows, 16 columns)
        """
        self.df = df.copy()
        self.stats = {
            "rows_input": len(df),
            "hidden_nulls_replaced": 0,
            "dates_parsed": 0,
            "dates_approximate": 0,
            "dates_suspicious": 0,
            "dates_unparseable": 0,
        }
    
    # =========================================================================
    # MAIN ENTRY POINT
    # =========================================================================
    def clean_all(self) -> dict:
        """
        Run all cleaning steps in order and return cleaned DataFrames.
        
        Returns:
            dict with keys:
                'studies'        → pd.DataFrame (main table, cleaned)
                'organizations'  → pd.DataFrame (unique orgs)
                'conditions'     → pd.DataFrame (study_id, condition_name)
                'interventions'  → pd.DataFrame (study_id, intervention_name)
                'age_groups'     → pd.DataFrame (study_id, age_group)
                'mesh_terms'     → pd.DataFrame (study_id, mesh_term)
                'stats'          → dict with cleaning statistics
        """
        logger.info(f"Starting cleaning pipeline on {len(self.df):,} rows...")
        
        # Step 1: Trim whitespace first (prevents issues in later steps)
        self._trim_whitespace()
        
        # Step 2: Replace hidden nulls across all columns
        self._replace_hidden_nulls()
        
        # Step 3: Clean dates
        self._clean_dates()
        
        # Step 4: Normalize phase values
        self._clean_phases()
        
        # Step 5: Extract organizations (before we modify org columns)
        orgs_df = self._extract_organizations()
        
        # Step 6: Split multi-value columns into bridge table DataFrames
        conditions_df = self._split_conditions()
        interventions_df = self._split_interventions()
        age_groups_df = self._split_age_groups()
        mesh_terms_df = self._split_mesh_terms()
        
        # Step 7: Build the final studies DataFrame
        studies_df = self._build_studies_df()
        
        self.stats["rows_output"] = len(studies_df)
        self._log_summary()
        
        return {
            "studies": studies_df,
            "organizations": orgs_df,
            "conditions": conditions_df,
            "interventions": interventions_df,
            "age_groups": age_groups_df,
            "mesh_terms": mesh_terms_df,
            "stats": self.stats,
        }
    
    # =========================================================================
    # CLEANING STEP 1: Trim whitespace
    # =========================================================================
    def _trim_whitespace(self):
        """Remove leading/trailing whitespace from all string columns."""
        string_cols = self.df.select_dtypes(include=["object"]).columns
        for col in string_cols:
            self.df[col] = self.df[col].apply(
                lambda x: x.strip() if isinstance(x, str) else x
            )
        logger.info(f"Trimmed whitespace on {len(string_cols)} string columns")
    
    # =========================================================================
    # CLEANING STEP 2: Replace hidden nulls
    # =========================================================================
    def _replace_hidden_nulls(self):
        """
        Convert hidden null values ('Unknown', 'UNKNOWN', 'N/A', etc.) to proper NaN.
        
        EDA Finding: 'Unknown' appears across 12 columns, 'UNKNOWN' in 2 columns.
        Combined, ~600K+ hidden nulls exist in the dataset.
        """
        count_before = self.df.isna().sum().sum()
        
        # Replace hidden nulls in all object columns
        string_cols = self.df.select_dtypes(include=["object"]).columns
        for col in string_cols:
            self.df[col] = self.df[col].replace(HIDDEN_NULL_VALUES, np.nan)
        
        count_after = self.df.isna().sum().sum()
        self.stats["hidden_nulls_replaced"] = count_after - count_before
        logger.info(f"Replaced {self.stats['hidden_nulls_replaced']:,} hidden null values → NaN")
    
    # =========================================================================
    # CLEANING STEP 3: Clean and parse dates
    # =========================================================================
    def _clean_dates(self):
        """
        Parse Start Date column handling two formats:
            - YYYY-MM-DD (55.7% of data) → parse directly
            - YYYY-MM    (43.3% of data) → append '-01', flag as approximate
            - NaN/null   ( 1.0% of data) → keep as NaN
        
        Also flags suspicious dates (before 1950 or after 2026).
        """
        date_col = "Start Date"
        
        # Preserve original for audit trail
        self.df["start_date_raw"] = self.df[date_col].copy()
        
        # Detect format: YYYY-MM (no day) — exactly 7 chars like "2004-10"
        is_year_month = (
            self.df[date_col]
            .fillna("")
            .str.match(r"^\d{4}-\d{2}$")
        )
        self.stats["dates_approximate"] = is_year_month.sum()
        
        # For YYYY-MM dates, append '-01' to make them parseable
        self.df.loc[is_year_month, date_col] = (
            self.df.loc[is_year_month, date_col] + "-01"
        )
        
        # Store the approximation flag
        self.df["start_date_is_approx"] = is_year_month
        
        # Parse all dates
        self.df["start_date_parsed"] = pd.to_datetime(
            self.df[date_col], format="mixed", errors="coerce", dayfirst=False
        )
        
        # Count parse failures (excluding already-null rows)
        was_not_null = self.df["start_date_raw"].notna()
        is_null_after_parse = self.df["start_date_parsed"].isna()
        self.stats["dates_unparseable"] = (was_not_null & is_null_after_parse).sum()
        
        # Flag suspicious dates
        parsed = self.df["start_date_parsed"]
        suspicious_mask = (
            (parsed.dt.year < DATE_MIN_YEAR) | (parsed.dt.year > DATE_MAX_YEAR)
        ) & parsed.notna()
        
        self.stats["dates_suspicious"] = suspicious_mask.sum()
        self.stats["dates_parsed"] = self.df["start_date_parsed"].notna().sum()
        
        # Set suspicious dates to NaN (but raw value is preserved)
        self.df.loc[suspicious_mask, "start_date_parsed"] = pd.NaT
        
        logger.info(
            f"Dates: {self.stats['dates_parsed']:,} parsed, "
            f"{self.stats['dates_approximate']:,} approximate (YYYY-MM), "
            f"{self.stats['dates_suspicious']:,} suspicious (nulled), "
            f"{self.stats['dates_unparseable']:,} unparseable"
        )
    
    # =========================================================================
    # CLEANING STEP 4: Normalize phases
    # =========================================================================
    def _clean_phases(self):
        """
        Clean the Phases column.
        
        EDA Finding: 60.1% missing (183K true NULL + 114K 'Unknown' + 885 'No phases listed').
        After hidden null replacement, only true NULLs remain.
        Combined values like 'PHASE1, PHASE2' are kept as-is.
        """
        # "No phases listed" should already be caught by hidden nulls,
        # but just in case:
        self.df["Phases"] = self.df["Phases"].replace("No phases listed", np.nan)
        self.df["Study Type"] = self.df["Study Type"].fillna("UNKNOWN")
        logger.info(f"Phases: {self.df['Phases'].notna().sum():,} non-null values remain")
    
    # =========================================================================
    # CLEANING STEP 5: Extract organizations
    # =========================================================================
    def _extract_organizations(self) -> pd.DataFrame:
        """
        Create a deduplicated organizations DataFrame.
        
        EDA Finding: 28,083 unique organizations, with org_class having
        case inconsistency ('Unknown' vs 'UNKNOWN' — both now NaN).
        
        Returns:
            DataFrame with columns: [org_name, org_class]
        """
        orgs = (
            self.df[["Organization Full Name", "Organization Class"]]
            .drop_duplicates(subset=["Organization Full Name"])
            .rename(columns={
                "Organization Full Name": "org_name",
                "Organization Class": "org_class",
            })
            .reset_index(drop=True)
        )
        
        logger.info(f"Extracted {len(orgs):,} unique organizations")
        return orgs
    
    # =========================================================================
    # CLEANING STEPS 6-9: Split multi-value columns
    # =========================================================================
    def _split_multi_value(self, column: str, separator: str, 
                           value_col_name: str) -> pd.DataFrame:
        """
        Generic method to split a multi-value column into a bridge table DataFrame.
        
        Args:
            column: Name of the source column (e.g., 'Conditions')
            separator: Delimiter (e.g., ',' or ' ')
            value_col_name: Name for the value column in output (e.g., 'condition_name')
        
        Returns:
            DataFrame with columns: [source_row_index, {value_col_name}]
            source_row_index maps to the original DataFrame index (used to link to study_id later)
        """
        # Only process non-null rows
        non_null = self.df[column].dropna()
        
        # Split and explode
        if separator == " ":
            split_series = non_null.str.split()
        else:
            split_series = non_null.str.split(separator)
        
        exploded = split_series.explode().str.strip()
        
        # Remove empty strings that can result from splitting
        exploded = exploded[exploded != ""]
        
        # Build the bridge DataFrame
        result = pd.DataFrame({
            "source_row_index": exploded.index,
            value_col_name: exploded.values,
        })
        
        logger.info(
            f"Split '{column}': {len(non_null):,} rows → {len(result):,} individual values "
            f"({result[value_col_name].nunique():,} unique)"
        )
        
        return result
    
    def _split_conditions(self) -> pd.DataFrame:
        """Split comma-separated Conditions into bridge table rows."""
        return self._split_multi_value("Conditions", ",", "condition_name")
    
    def _split_interventions(self) -> pd.DataFrame:
        """Split comma-separated Interventions into bridge table rows."""
        return self._split_multi_value("Interventions", ",", "intervention_name")
    
    def _split_age_groups(self) -> pd.DataFrame:
        """Split space-separated Standard Age into bridge table rows."""
        return self._split_multi_value("Standard Age", " ", "age_group")
    
    def _split_mesh_terms(self) -> pd.DataFrame:
        """Split comma-separated Medical Subject Headings into bridge table rows."""
        return self._split_multi_value("Medical Subject Headings", ",", "mesh_term")
    
    # =========================================================================
    # FINAL STEP: Build the studies DataFrame
    # =========================================================================
    def _build_studies_df(self) -> pd.DataFrame:
        """
        Build the final studies DataFrame ready for database insertion.
        
        Maps original column names to database column names.
        Keeps only the columns that go into the studies table.
        """
        studies = pd.DataFrame({
            "org_name":                 self.df["Organization Full Name"],
            "responsible_party":        self.df["Responsible Party"],
            "brief_title":              self.df["Brief Title"],
            "full_title":               self.df["Full Title"],
            "overall_status":           self.df["Overall Status"],
            "start_date":               self.df["start_date_parsed"],
            "start_date_raw":           self.df["start_date_raw"],
            "start_date_is_approx":     self.df["start_date_is_approx"],
            "primary_purpose":          self.df["Primary Purpose"],
            "study_type":               self.df["Study Type"],
            "phase":                    self.df["Phases"],
            "outcome_measure":          self.df["Outcome Measure"],
            "intervention_description": self.df["Intervention Description"],
        })
        
        # Keep the original index as source_row_index for linking bridge tables
        studies["source_row_index"] = studies.index
        
        logger.info(f"Built studies DataFrame: {len(studies):,} rows × {len(studies.columns)} columns")
        return studies
    
    # =========================================================================
    # LOGGING
    # =========================================================================
    def _log_summary(self):
        """Print a summary of all cleaning operations."""
        logger.info("=" * 60)
        logger.info("CLEANING SUMMARY")
        logger.info("=" * 60)
        for key, value in self.stats.items():
            if isinstance(value, int):
                logger.info(f"  {key:<30s}: {value:>12,}")
            else:
                logger.info(f"  {key:<30s}: {value}")
        logger.info("=" * 60)
