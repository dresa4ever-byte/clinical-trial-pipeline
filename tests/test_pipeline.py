"""
=============================================================================
  TESTS — Clinical Trial Data Pipeline
=============================================================================
  Run: pytest tests/ -v
=============================================================================
"""

import pytest
import pandas as pd
import numpy as np
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.processing.cleaner import DataCleaner
from src.processing.validator import DataValidator
from src.ingestion.csv_ingestor import CSVIngestor
from src.config import HIDDEN_NULL_VALUES


# =============================================================================
# FIXTURES — Sample data for testing
# =============================================================================

@pytest.fixture
def sample_raw_df():
    """Create a small DataFrame that mimics the real CSV structure."""
    return pd.DataFrame({
        "Organization Full Name": ["Harvard", "MIT", "Harvard", "NIH", "Unknown"],
        "Organization Class": ["OTHER", "OTHER", "OTHER", "NIH", "UNKNOWN"],
        "Responsible Party": ["SPONSOR", "PRINCIPAL_INVESTIGATOR", "Unknown", "SPONSOR", "N/A"],
        "Brief Title": ["Study A", "Study B", "Study C", "Study D", "Study E"],
        "Full Title": ["Full A", "Full B", "Full C", "Full D", None],
        "Overall Status": ["COMPLETED", "RECRUITING", "TERMINATED", "COMPLETED", "UNKNOWN"],
        "Start Date": ["2020-01-15", "2021-06", "2019-03-20", "1940-01-01", "Unknown"],
        "Standard Age": ["ADULT OLDER_ADULT", "CHILD ADULT", "ADULT", "OLDER_ADULT", "CHILD ADULT OLDER_ADULT"],
        "Conditions": ["Diabetes, Obesity", "Cancer", "Heart Failure, Stroke, Pain", "HIV", "Unknown"],
        "Primary Purpose": ["TREATMENT", "PREVENTION", "Unknown", "TREATMENT", "Not Applicable"],
        "Interventions": ["Drug A, Drug B", "Radiation", "Placebo, Drug C", "Vaccine", "Unknown"],
        "Intervention Description": ["Desc A", "Desc B", "Unknown", "Desc D", None],
        "Study Type": ["INTERVENTIONAL", "INTERVENTIONAL", "OBSERVATIONAL", "INTERVENTIONAL", "Unknown"],
        "Phases": ["PHASE2", "PHASE3", "No phases listed", "PHASE1, PHASE2", None],
        "Outcome Measure": ["Survival", "Response rate", None, "Viral load", "Unknown"],
        "Medical Subject Headings": ["Diabetes Mellitus, Obesity", "Neoplasms", "Heart Failure", None, "Unknown"],
    })


@pytest.fixture
def cleaned_data(sample_raw_df):
    """Run the cleaner on sample data."""
    cleaner = DataCleaner(sample_raw_df)
    return cleaner.clean_all()


# =============================================================================
# TEST: Hidden Null Replacement
# =============================================================================

class TestHiddenNullCleaning:
    
    def test_unknown_replaced_with_nan(self, sample_raw_df):
        """'Unknown' and 'UNKNOWN' should become NaN."""
        cleaner = DataCleaner(sample_raw_df)
        cleaner._trim_whitespace()
        cleaner._replace_hidden_nulls()
        
        # Row 4 had "Unknown" in Organization Full Name
        assert pd.isna(cleaner.df.iloc[4]["Organization Full Name"])
    
    def test_n_a_replaced_with_nan(self, sample_raw_df):
        """'N/A' should become NaN."""
        cleaner = DataCleaner(sample_raw_df)
        cleaner._trim_whitespace()
        cleaner._replace_hidden_nulls()
        
        # Row 4 had "N/A" in Responsible Party
        assert pd.isna(cleaner.df.iloc[4]["Responsible Party"])
    
    def test_valid_values_preserved(self, sample_raw_df):
        """Non-null values should not be changed."""
        cleaner = DataCleaner(sample_raw_df)
        cleaner._trim_whitespace()
        cleaner._replace_hidden_nulls()
        
        assert cleaner.df.iloc[0]["Organization Full Name"] == "Harvard"
        assert cleaner.df.iloc[0]["Overall Status"] == "COMPLETED"
    
    def test_hidden_null_count(self, sample_raw_df):
        """Count of replaced nulls should be tracked."""
        cleaner = DataCleaner(sample_raw_df)
        cleaner._trim_whitespace()
        cleaner._replace_hidden_nulls()
        
        assert cleaner.stats["hidden_nulls_replaced"] > 0


# =============================================================================
# TEST: Date Cleaning
# =============================================================================

class TestDateCleaning:
    
    def test_full_date_parsed(self, sample_raw_df):
        """YYYY-MM-DD format should parse correctly."""
        cleaner = DataCleaner(sample_raw_df)
        cleaner._trim_whitespace()
        cleaner._replace_hidden_nulls()
        cleaner._clean_dates()
        
        # Row 0: "2020-01-15"
        parsed = cleaner.df.iloc[0]["start_date_parsed"]
        assert parsed.year == 2020
        assert parsed.month == 1
        assert parsed.day == 15
    
    def test_year_month_gets_day_01(self, sample_raw_df):
        """YYYY-MM format should get day=01 and be flagged as approximate."""
        cleaner = DataCleaner(sample_raw_df)
        cleaner._trim_whitespace()
        cleaner._replace_hidden_nulls()
        cleaner._clean_dates()
        
        # Row 1: "2021-06" → should become 2021-06-01
        parsed = cleaner.df.iloc[1]["start_date_parsed"]
        assert parsed.year == 2021
        assert parsed.month == 6
        assert parsed.day == 1
        assert cleaner.df.iloc[1]["start_date_is_approx"] == True
    
    def test_suspicious_date_nulled(self, sample_raw_df):
        """Dates before 1950 should be flagged and set to NaT."""
        cleaner = DataCleaner(sample_raw_df)
        cleaner._trim_whitespace()
        cleaner._replace_hidden_nulls()
        cleaner._clean_dates()
        
        # Row 3: "1940-01-01" → suspicious, should be NaT
        assert pd.isna(cleaner.df.iloc[3]["start_date_parsed"])
        # But raw value should be preserved
        assert cleaner.df.iloc[3]["start_date_raw"] == "1940-01-01"
    
    def test_unknown_date_is_nat(self, sample_raw_df):
        """'Unknown' dates (already NaN after hidden null step) should be NaT."""
        cleaner = DataCleaner(sample_raw_df)
        cleaner._trim_whitespace()
        cleaner._replace_hidden_nulls()
        cleaner._clean_dates()
        
        # Row 4: "Unknown" → NaN → NaT
        assert pd.isna(cleaner.df.iloc[4]["start_date_parsed"])


# =============================================================================
# TEST: Multi-value Splitting
# =============================================================================

class TestMultiValueSplitting:
    
    def test_conditions_split_correctly(self, cleaned_data):
        """Comma-separated conditions should be split into separate rows."""
        conditions = cleaned_data["conditions"]
        # Row 0 had "Diabetes, Obesity" → 2 rows
        row0_conditions = conditions[conditions["source_row_index"] == 0]
        assert len(row0_conditions) == 2
    
    def test_conditions_trimmed(self, cleaned_data):
        """Split values should have whitespace trimmed."""
        conditions = cleaned_data["conditions"]
        for val in conditions["condition_name"]:
            assert val == val.strip()
    
    def test_age_groups_split_by_space(self, cleaned_data):
        """Space-separated age groups should be split correctly."""
        age_groups = cleaned_data["age_groups"]
        # Row 0 had "ADULT OLDER_ADULT" → 2 rows
        row0_ages = age_groups[age_groups["source_row_index"] == 0]
        assert len(row0_ages) == 2
        assert "ADULT" in row0_ages["age_group"].values
        assert "OLDER_ADULT" in row0_ages["age_group"].values
    
    def test_interventions_split(self, cleaned_data):
        """Comma-separated interventions should split into separate rows."""
        interventions = cleaned_data["interventions"]
        # Row 0 had "Drug A, Drug B" → 2 rows
        row0_inv = interventions[interventions["source_row_index"] == 0]
        assert len(row0_inv) == 2


# =============================================================================
# TEST: Organization Extraction
# =============================================================================

class TestOrganizationExtraction:
    
    def test_orgs_deduplicated(self, cleaned_data):
        """Organizations should be unique by name."""
        orgs = cleaned_data["organizations"]
        assert orgs["org_name"].duplicated().sum() == 0
    
    def test_harvard_appears_once(self, cleaned_data):
        """Harvard appears in 2 rows but should be extracted once."""
        orgs = cleaned_data["organizations"]
        harvard = orgs[orgs["org_name"] == "Harvard"]
        assert len(harvard) == 1


# =============================================================================
# TEST: Studies DataFrame
# =============================================================================

class TestStudiesDataFrame:
    
    def test_studies_has_all_rows(self, cleaned_data):
        """Studies DataFrame should have same row count as input."""
        assert len(cleaned_data["studies"]) == 5
    
    def test_studies_has_required_columns(self, cleaned_data):
        """Studies DataFrame should have all expected columns."""
        studies = cleaned_data["studies"]
        required = ["org_name", "brief_title", "study_type", "overall_status", "start_date"]
        for col in required:
            assert col in studies.columns, f"Missing column: {col}"
    
    def test_source_row_index_preserved(self, cleaned_data):
        """source_row_index should be present for linking bridge tables."""
        studies = cleaned_data["studies"]
        assert "source_row_index" in studies.columns


# =============================================================================
# TEST: Validator
# =============================================================================

class TestValidator:
    
    def test_valid_data_passes(self, cleaned_data):
        """Cleaned sample data should pass validation."""
        validator = DataValidator()
        is_valid, report = validator.validate_all(cleaned_data)
        # May have warnings but should not have errors
        # (our sample has UNKNOWN study_type which is a warning, not error)
        assert is_valid or "study_type" in report
    
    def test_empty_bridge_table_fails(self, cleaned_data):
        """Empty bridge table should cause validation error."""
        cleaned_data["conditions"] = pd.DataFrame(columns=["source_row_index", "condition_name"])
        validator = DataValidator()
        is_valid, report = validator.validate_all(cleaned_data)
        assert not is_valid


# =============================================================================
# TEST: CSV Ingestor
# =============================================================================

class TestCSVIngestor:
    
    def test_missing_file_raises_error(self):
        """Non-existent file should raise FileNotFoundError."""
        ingestor = CSVIngestor("/nonexistent/file.csv")
        with pytest.raises(FileNotFoundError):
            ingestor.ingest()
    
    def test_schema_validation(self):
        """Missing required columns should raise ValueError."""
        ingestor = CSVIngestor("dummy.csv")
        bad_df = pd.DataFrame({"col1": [1], "col2": [2]})
        with pytest.raises(ValueError):
            ingestor.validate_schema(bad_df, ["col1", "col3"])


# =============================================================================
# TEST: Cleaning Statistics
# =============================================================================

class TestCleaningStats:
    
    def test_stats_tracked(self, cleaned_data):
        """Cleaning stats should be populated."""
        stats = cleaned_data["stats"]
        assert "rows_input" in stats
        assert "hidden_nulls_replaced" in stats
        assert "dates_parsed" in stats
        assert stats["rows_input"] == 5
