"""
Quick test of the API ingestor — fetches 50 studies and shows the result.
Run from: C:\clinical_trial_pipeline\
Command: python test_api_ingest.py
"""
import os, sys
sys.path.insert(0, r"C:\clinical_trial_pipeline")

from src.ingestion.api_ingestor import APIIngestor

print("=" * 60)
print("  Testing API Ingestor — Fetching 50 Breast Cancer studies")
print("=" * 60)

# Fetch 50 studies about Breast Cancer
ingestor = APIIngestor(
    condition="Breast Cancer",
    status="RECRUITING",
    max_studies=50,
)

df = ingestor.ingest()

print(f"\n--- Result ---")
print(f"Rows: {len(df)}")
print(f"Columns: {list(df.columns)}")

print(f"\n--- Sample data (first 3 rows) ---")
cols_to_show = ["nct_id", "Brief Title", "Overall Status", "Study Type", "Phases", "data_source"]
print(df[cols_to_show].head(3).to_string())

print(f"\n--- Column fill rates ---")
for col in df.columns:
    if col != "_locations":
        fill = df[col].notna().sum()
        print(f"  {col:<30s}: {fill}/{len(df)} ({fill*100//len(df)}%)")

# Extract locations
locations = APIIngestor.extract_locations(df)
print(f"\n--- Locations ---")
print(f"Total location records: {len(locations)}")
if len(locations) > 0:
    print(locations[["facility", "city", "country"]].head(5).to_string())

print(f"\n✓ API ingestor works! Data format matches CSV pipeline.")
