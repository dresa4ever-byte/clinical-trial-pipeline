"""
=============================================================================
  CLINICAL TRIALS DATASET — EXPLORATORY DATA ANALYSIS (EDA)
=============================================================================
  Purpose:  Run this on your FULL dataset (390MB CSV) before building the pipeline.
            It produces a text report you can paste back to Claude for schema refinement.
  
  How to run:  Open in Spyder → change FILE_PATH below → Run (F5)
  
  Output:  Prints everything to console AND saves to 'eda_report.txt'
           Copy/paste the contents of eda_report.txt back to Claude.
  
  Estimated runtime:  1-3 minutes for 390MB depending on your machine.
=============================================================================
"""

import pandas as pd
import sys
from datetime import datetime
from io import StringIO

# ============================================================================
# CONFIGURATION — CHANGE THIS TO YOUR FILE PATH
# ============================================================================
FILE_PATH = r"C:\path\to\your\clinical_trials.csv"   # <-- CHANGE THIS!
# Example: FILE_PATH = r"C:\Users\YourName\Downloads\clinical_trials.csv"
# ============================================================================


def capture_output():
    """Capture all print output to both console and file."""
    class Tee:
        def __init__(self, *files):
            self.files = files
        def write(self, text):
            for f in self.files:
                f.write(text)
                f.flush()
        def flush(self):
            for f in self.files:
                f.flush()
    
    report_file = open("eda_report.txt", "w", encoding="utf-8")
    sys.stdout = Tee(sys.__stdout__, report_file)
    return report_file


def section(title):
    """Print a section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def main():
    report_file = capture_output()
    
    print(f"EDA Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"File: {FILE_PATH}")
    
    # ========================================================================
    # 1. LOAD DATA
    # ========================================================================
    section("1. LOADING DATA")
    
    print("Reading CSV... (this may take 30-60 seconds for 390MB)")
    df = pd.read_csv(FILE_PATH, low_memory=False)
    
    print(f"Rows:    {df.shape[0]:,}")
    print(f"Columns: {df.shape[1]}")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    
    print(f"\nColumn names:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i:2d}. '{col}'")
    
    # ========================================================================
    # 2. DATA TYPES & BASIC INFO
    # ========================================================================
    section("2. DATA TYPES")
    
    for col in df.columns:
        non_null = df[col].notna().sum()
        print(f"  {col:<35s} | dtype: {str(df[col].dtype):<10s} | non-null: {non_null:>10,} / {len(df):,}")
    
    # ========================================================================
    # 3. DUPLICATE CHECK
    # ========================================================================
    section("3. DUPLICATE ANALYSIS")
    
    total_exact_dupes = df.duplicated().sum()
    print(f"Exact duplicate rows: {total_exact_dupes:,}")
    
    # Check for duplicate titles (possible same study entered twice)
    if 'Brief Title' in df.columns:
        title_dupes = df['Brief Title'].duplicated().sum()
        print(f"Duplicate 'Brief Title' values: {title_dupes:,}")
        if title_dupes > 0 and title_dupes < 20:
            print("  Sample duplicated titles:")
            duped = df[df['Brief Title'].duplicated(keep=False)]['Brief Title'].unique()[:5]
            for t in duped:
                print(f"    → '{t[:80]}...'")
    
    # Check if first column is an ID
    first_col = df.columns[0]
    print(f"\nFirst column '{first_col}':")
    print(f"  Unique values: {df[first_col].nunique():,}")
    print(f"  Is unique (potential ID)? {df[first_col].is_unique}")
    if df[first_col].dtype in ['int64', 'float64']:
        print(f"  Min: {df[first_col].min()}, Max: {df[first_col].max()}")
        # Check if sequential
        if df[first_col].is_unique:
            expected_range = df[first_col].max() - df[first_col].min() + 1
            actual_count = df[first_col].nunique()
            print(f"  Sequential? {'Yes' if expected_range == actual_count else 'No (has gaps)'}")
    
    # ========================================================================
    # 4. NULL / MISSING VALUES AUDIT (including hidden nulls)
    # ========================================================================
    section("4. NULL & HIDDEN-NULL AUDIT")
    
    # Define what counts as "hidden null"
    hidden_null_values = ['Unknown', 'unknown', 'UNKNOWN', 
                          'None', 'none', 'NONE',
                          'null', 'Null', 'NULL',
                          'N/A', 'n/a', 'NA', 'na',
                          'Not Applicable', 'not applicable',
                          '-', '--', '---', '.', '..', 
                          'Not Available', 'not available',
                          'Missing', 'missing']
    
    print(f"{'Column':<35s} | {'True Null':>10s} | {'Hidden Null':>12s} | {'Empty Str':>10s} | {'Whitespace':>10s} | {'TOTAL Missing':>14s} | {'% Missing':>9s}")
    print("-" * 120)
    
    for col in df.columns:
        true_null = df[col].isna().sum()
        
        if df[col].dtype == 'object':
            hidden_null = df[col].isin(hidden_null_values).sum()
            empty_str = (df[col] == '').sum()
            whitespace = ((df[col].str.strip() == '') & (df[col] != '') & df[col].notna()).sum()
        else:
            hidden_null = 0
            empty_str = 0
            whitespace = 0
        
        total_missing = true_null + hidden_null + empty_str + whitespace
        pct = (total_missing / len(df)) * 100
        
        print(f"  {col:<33s} | {true_null:>10,} | {hidden_null:>12,} | {empty_str:>10,} | {whitespace:>10,} | {total_missing:>14,} | {pct:>8.1f}%")
    
    # Detail: what hidden null values were found?
    print(f"\nHidden null values detected per column:")
    for col in df.columns:
        if df[col].dtype == 'object':
            found = []
            for val in hidden_null_values:
                count = (df[col] == val).sum()
                if count > 0:
                    found.append(f"'{val}' ({count:,})")
            if found:
                print(f"  {col}: {', '.join(found)}")
    
    # ========================================================================
    # 5. CATEGORICAL COLUMN PROFILING
    # ========================================================================
    section("5. CATEGORICAL COLUMN PROFILING")
    
    # These are the columns we expect to be enum-like
    categorical_candidates = [
        'Organization Class', 'Responsible Party', 'Overall Status',
        'Standard Age', 'Primary Purpose', 'Study Type', 'Phases'
    ]
    
    for col in categorical_candidates:
        if col not in df.columns:
            print(f"  WARNING: Column '{col}' not found in dataset!")
            continue
        
        print(f"\n--- {col} ---")
        print(f"  Unique values: {df[col].nunique()}")
        print(f"  Null count: {df[col].isna().sum():,}")
        
        vc = df[col].value_counts(dropna=False).head(20)
        for val, count in vc.items():
            pct = (count / len(df)) * 100
            val_display = repr(val) if pd.isna(val) else f"'{val}'"
            print(f"    {val_display:<40s}  {count:>10,}  ({pct:5.1f}%)")
        
        if df[col].nunique() > 20:
            print(f"    ... and {df[col].nunique() - 20} more values (showing top 20)")
        
        # Check for case inconsistencies
        if df[col].dtype == 'object':
            unique_vals = df[col].dropna().unique()
            lower_map = {}
            for v in unique_vals:
                key = v.lower().strip()
                if key not in lower_map:
                    lower_map[key] = []
                lower_map[key].append(v)
            case_issues = {k: v for k, v in lower_map.items() if len(v) > 1}
            if case_issues:
                print(f"  ⚠️  CASE INCONSISTENCIES FOUND:")
                for k, variants in case_issues.items():
                    print(f"    '{k}' appears as: {variants}")
    
    # ========================================================================
    # 6. DATE COLUMN ANALYSIS
    # ========================================================================
    section("6. DATE ANALYSIS (Start Date)")
    
    date_col = 'Start Date'
    if date_col in df.columns:
        print(f"Total values: {df[date_col].notna().sum():,}")
        print(f"Null values: {df[date_col].isna().sum():,}")
        
        # Detect date formats
        sample = df[date_col].dropna()
        
        # Pattern 1: MM/DD/YYYY or M/D/YYYY
        pattern_slash = sample.str.match(r'^\d{1,2}/\d{1,2}/\d{4}$')
        # Pattern 2: YYYY-MM (no day)
        pattern_ym = sample.str.match(r'^\d{4}-\d{2}$')
        # Pattern 3: YYYY-MM-DD
        pattern_ymd = sample.str.match(r'^\d{4}-\d{2}-\d{2}$')
        # Anything else
        pattern_other = ~(pattern_slash | pattern_ym | pattern_ymd)
        
        print(f"\nDate format distribution:")
        print(f"  MM/DD/YYYY (slash format):  {pattern_slash.sum():>10,}  ({pattern_slash.sum()/len(sample)*100:.1f}%)")
        print(f"  YYYY-MM (year-month only):  {pattern_ym.sum():>10,}  ({pattern_ym.sum()/len(sample)*100:.1f}%)")
        print(f"  YYYY-MM-DD (ISO format):    {pattern_ymd.sum():>10,}  ({pattern_ymd.sum()/len(sample)*100:.1f}%)")
        print(f"  OTHER/UNRECOGNIZED:         {pattern_other.sum():>10,}  ({pattern_other.sum()/len(sample)*100:.1f}%)")
        
        if pattern_other.sum() > 0:
            print(f"\n  ⚠️  UNRECOGNIZED DATE FORMATS (sample):")
            others = sample[pattern_other].head(10)
            for v in others:
                print(f"    → '{v}'")
        
        # Try to parse all dates and get range
        print(f"\nAttempting to parse dates for range analysis...")
        parsed = pd.to_datetime(df[date_col], format='mixed', errors='coerce', dayfirst=False)
        parse_failures = parsed.isna().sum() - df[date_col].isna().sum()
        print(f"  Parse failures: {parse_failures:,}")
        print(f"  Earliest date: {parsed.min()}")
        print(f"  Latest date:   {parsed.max()}")
        
        # Check for suspicious dates
        if parsed.min().year < 1990:
            old = parsed[parsed.dt.year < 1990].count()
            print(f"  ⚠️  {old:,} dates before 1990 (suspicious?)")
        if parsed.max() > pd.Timestamp.now():
            future = parsed[parsed > pd.Timestamp.now()].count()
            print(f"  ⚠️  {future:,} dates in the future (suspicious?)")
        
        # Year distribution (top 10)
        print(f"\n  Year distribution (top 10):")
        year_counts = parsed.dt.year.value_counts().head(10)
        for year, count in year_counts.items():
            print(f"    {int(year)}: {count:>10,}")
    else:
        print(f"  Column '{date_col}' not found!")
    
    # ========================================================================
    # 7. MULTI-VALUE COLUMN ANALYSIS
    # ========================================================================
    section("7. MULTI-VALUE COLUMN ANALYSIS")
    
    multi_value_cols = {
        'Conditions': ',',
        'Interventions': ',',
        'Medical Subject Headings': ',',
        'Standard Age': ' '  # space-separated!
    }
    
    for col, separator in multi_value_cols.items():
        if col not in df.columns:
            print(f"  Column '{col}' not found!")
            continue
        
        print(f"\n--- {col} (separator: '{separator}') ---")
        
        non_null = df[col].dropna()
        print(f"  Non-null rows: {len(non_null):,}")
        
        # Split and count
        if separator == ' ':
            all_values = non_null.str.split().explode().str.strip()
        else:
            all_values = non_null.str.split(separator).explode().str.strip()
        
        # Remove empty strings after splitting
        all_values = all_values[all_values != '']
        
        print(f"  Total individual values (after split): {len(all_values):,}")
        print(f"  Unique individual values: {all_values.nunique():,}")
        
        # Values per row
        if separator == ' ':
            counts_per_row = non_null.str.split().apply(len)
        else:
            counts_per_row = non_null.str.split(separator).apply(len)
        
        print(f"  Values per row: min={counts_per_row.min()}, avg={counts_per_row.mean():.1f}, max={counts_per_row.max()}")
        
        if counts_per_row.max() > 20:
            print(f"  ⚠️  Some rows have {counts_per_row.max()} values — check for parsing issues!")
            # Show the row with most values
            worst_idx = counts_per_row.idxmax()
            print(f"    Row {worst_idx}: '{str(df.loc[worst_idx, col])[:100]}...'")
        
        # Top 15 most common values
        print(f"  Top 15 most common:")
        for val, count in all_values.value_counts().head(15).items():
            print(f"    '{val[:60]}': {count:,}")
    
    # ========================================================================
    # 8. TEXT COLUMN LENGTH ANALYSIS
    # ========================================================================
    section("8. TEXT COLUMN LENGTHS")
    
    text_cols = ['Brief Title', 'Full Title', 'Outcome Measure', 'Intervention Description']
    
    for col in text_cols:
        if col not in df.columns:
            continue
        
        lengths = df[col].dropna().str.len()
        print(f"\n--- {col} ---")
        print(f"  Non-null: {df[col].notna().sum():,}")
        print(f"  Length — min: {lengths.min()}, avg: {lengths.mean():.0f}, max: {lengths.max()}")
        
        # Suspiciously short values
        short = df[col][df[col].str.len() < 5].dropna()
        if len(short) > 0:
            print(f"  ⚠️  {len(short):,} values shorter than 5 characters:")
            for v in short.head(5):
                print(f"    → '{v}'")
    
    # ========================================================================
    # 9. ORGANIZATION ANALYSIS
    # ========================================================================
    section("9. ORGANIZATION OVERVIEW")
    
    if 'Organization Full Name' in df.columns:
        print(f"Unique organizations: {df['Organization Full Name'].nunique():,}")
        print(f"\nTop 20 organizations by number of studies:")
        for org, count in df['Organization Full Name'].value_counts().head(20).items():
            print(f"  {org[:60]:<60s}  {count:>8,}")
    
    # ========================================================================
    # 10. SUMMARY FOR SCHEMA DECISIONS
    # ========================================================================
    section("10. SUMMARY — KEY NUMBERS FOR SCHEMA DESIGN")
    
    print(f"Total rows in dataset:           {len(df):>12,}")
    print(f"Total columns:                   {len(df.columns):>12,}")
    
    if 'Conditions' in df.columns:
        cond_count = df['Conditions'].dropna().str.split(',').explode().str.strip().nunique()
        print(f"Unique conditions:               {cond_count:>12,}")
    
    if 'Interventions' in df.columns:
        int_count = df['Interventions'].dropna().str.split(',').explode().str.strip().nunique()
        print(f"Unique interventions:            {int_count:>12,}")
    
    if 'Medical Subject Headings' in df.columns:
        mesh_count = df['Medical Subject Headings'].dropna().str.split(',').explode().str.strip().nunique()
        print(f"Unique MeSH terms:               {mesh_count:>12,}")
    
    if 'Organization Full Name' in df.columns:
        print(f"Unique organizations:            {df['Organization Full Name'].nunique():>12,}")
    
    for col in categorical_candidates:
        if col in df.columns:
            print(f"Unique '{col}': {df[col].nunique():>6,}")
    
    print(f"\n{'='*70}")
    print(f"  END OF REPORT")
    print(f"  Saved to: eda_report.txt")
    print(f"  Please paste the contents of eda_report.txt back to Claude!")
    print(f"{'='*70}")
    
    # Restore stdout and close file
    sys.stdout = sys.__stdout__
    report_file.close()
    print("\nDone! Report saved to 'eda_report.txt'")
    print("Copy the contents of that file and paste it back to our chat.")


if __name__ == "__main__":
    main()
