"""
API Ingestor — Fetches clinical trial data from ClinicalTrials.gov API v2.

This is the second data source for our multi-source pipeline.
Demonstrates:
    - REST API integration with pagination
    - JSON → flat DataFrame transformation
    - Rate limiting and error handling
    - Same output format as CSVIngestor (pipeline-agnostic)

API Details:
    - Base URL: https://clinicaltrials.gov/api/v2/studies
    - Format: JSON
    - Rate limit: ~50 requests/minute (no auth needed)
    - Max page size: 1000 studies per request
    - Pagination: token-based (nextPageToken)
"""

import pandas as pd
import requests
import time
import logging
from typing import Optional

from src.ingestion.base_ingestor import BaseIngestor

logger = logging.getLogger(__name__)


class APIIngestor(BaseIngestor):
    """
    Fetches clinical trial data from the ClinicalTrials.gov REST API v2.
    
    Transforms the nested JSON response into a flat DataFrame that matches
    the same structure as CSVIngestor output — so the rest of the pipeline
    (cleaner → validator → loader) works identically for both sources.
    
    Usage:
        # Fetch 100 breast cancer studies
        ingestor = APIIngestor(
            condition="Breast Cancer",
            max_studies=100
        )
        df = ingestor.ingest()
    """
    
    BASE_URL = "https://clinicaltrials.gov/api/v2/studies"
    PAGE_SIZE = 100          # studies per API request (max 1000)
    RATE_LIMIT_DELAY = 1.2   # seconds between requests (stay under 50/min)
    
    def __init__(
        self,
        condition: Optional[str] = None,
        status: Optional[str] = None,
        phase: Optional[str] = None,
        max_studies: int = 500,
    ):
        """
        Args:
            condition: Filter by condition/disease (e.g., "Breast Cancer")
            status: Filter by status (e.g., "RECRUITING")
            phase: Filter by phase (e.g., "PHASE2")
            max_studies: Maximum number of studies to fetch (default 500)
        """
        source_desc = f"API(cond={condition}, max={max_studies})"
        super().__init__(source_name=source_desc)
        
        self.condition = condition
        self.status = status
        self.phase = phase
        self.max_studies = max_studies
    
    def ingest(self) -> pd.DataFrame:
        """
        Fetch studies from the API and return as a flat DataFrame.
        
        Returns:
            pd.DataFrame with same columns as CSVIngestor output,
            PLUS bonus columns: nct_id, enrollment, data_source
        """
        logger.info(f"Fetching up to {self.max_studies} studies from ClinicalTrials.gov API...")
        
        # --- Step 1: Fetch all pages of JSON data ---
        raw_studies = self._fetch_all_pages()
        
        if not raw_studies:
            logger.warning("No studies returned from API")
            return pd.DataFrame()
        
        logger.info(f"Fetched {len(raw_studies)} studies from API")
        
        # --- Step 2: Transform nested JSON → flat DataFrame ---
        records = []
        for study in raw_studies:
            record = self._flatten_study(study)
            if record:
                records.append(record)
        
        df = pd.DataFrame(records)
        
        # --- Step 3: Store metadata ---
        self.row_count = len(df)
        self.column_count = len(df.columns)
        
        logger.info(
            f"API ingestion complete: {self.row_count:,} studies "
            f"transformed into {self.column_count} columns"
        )
        
        return df
    
    # =========================================================================
    # API FETCHING WITH PAGINATION
    # =========================================================================
    def _fetch_all_pages(self) -> list:
        """
        Fetch all pages of results, respecting rate limits.
        
        Uses token-based pagination (nextPageToken).
        
        Returns:
            list of raw study dicts from the API
        """
        all_studies = []
        page_token = None
        page_num = 0
        
        while len(all_studies) < self.max_studies:
            page_num += 1
            remaining = self.max_studies - len(all_studies)
            page_size = min(self.PAGE_SIZE, remaining)
            
            # Build request parameters
            params = {
                "format": "json",
                "pageSize": page_size,
                "countTotal": "true" if page_num == 1 else "false",
            }
            
            # Add optional filters
            if self.condition:
                params["query.cond"] = self.condition
            if self.status:
                params["filter.overallStatus"] = self.status
            if self.phase:
                params["filter.phase"] = self.phase
            if page_token:
                params["pageToken"] = page_token
            
            # Make the request
            try:
                response = requests.get(
                    self.BASE_URL,
                    params=params,
                    headers={
                        "User-Agent": "ClinicalTrialPipeline/1.0",
                        "Accept": "application/json",
                    },
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API request failed on page {page_num}: {e}")
                break
            
            # Extract studies from response
            studies = data.get("studies", [])
            if not studies:
                logger.info(f"No more studies to fetch (page {page_num})")
                break
            
            all_studies.extend(studies)
            
            # Log progress
            total_available = data.get("totalCount", "?")
            if page_num == 1:
                logger.info(f"Total matching studies on server: {total_available}")
            logger.info(
                f"  Page {page_num}: fetched {len(studies)} studies "
                f"(total so far: {len(all_studies)})"
            )
            
            # Check for next page
            page_token = data.get("nextPageToken")
            if not page_token:
                logger.info("Reached last page")
                break
            
            # Rate limiting
            time.sleep(self.RATE_LIMIT_DELAY)
        
        return all_studies[:self.max_studies]
    
    # =========================================================================
    # JSON → FLAT RECORD TRANSFORMATION
    # =========================================================================
    def _flatten_study(self, study: dict) -> dict:
        """
        Transform one nested API study JSON into a flat dict matching CSV format.
        
        The API returns deeply nested JSON like:
            study.protocolSection.identificationModule.briefTitle
        
        We flatten it to match our CSV column names so the same cleaner
        and loader work for both sources.
        
        Args:
            study: Raw study dict from the API
        
        Returns:
            Flat dict with keys matching CSV column names + bonus fields
        """
        try:
            protocol = study.get("protocolSection", {})
            
            # Extract each module
            ident = protocol.get("identificationModule", {})
            org = ident.get("organization", {})
            status_mod = protocol.get("statusModule", {})
            design = protocol.get("designModule", {})
            cond_mod = protocol.get("conditionsModule", {})
            interv_mod = protocol.get("armsInterventionsModule", {})
            elig = protocol.get("eligibilityModule", {})
            outcomes = protocol.get("outcomesModule", {})
            sponsor = protocol.get("sponsorCollaboratorsModule", {})
            contacts = protocol.get("contactsLocationsModule", {})
            
            # --- Build interventions string (comma-separated, like CSV) ---
            interventions_list = interv_mod.get("interventions", [])
            intervention_names = [i.get("name", "") for i in interventions_list if i.get("name")]
            intervention_descs = [i.get("description", "") for i in interventions_list if i.get("description")]
            
            # --- Build outcome measures string ---
            primary_outcomes = outcomes.get("primaryOutcomes", [])
            outcome_measures = [o.get("measure", "") for o in primary_outcomes if o.get("measure")]
            
            # --- Build the flat record ---
            record = {
                # === Columns matching CSV format (same names) ===
                "Organization Full Name":   org.get("fullName"),
                "Organization Class":       org.get("class"),
                "Responsible Party":        sponsor.get("responsibleParty", {}).get("type"),
                "Brief Title":              ident.get("briefTitle"),
                "Full Title":               ident.get("officialTitle"),
                "Overall Status":           status_mod.get("overallStatus"),
                "Start Date":               status_mod.get("startDateStruct", {}).get("date"),
                "Standard Age":             " ".join(elig.get("stdAges", [])),
                "Conditions":               ", ".join(cond_mod.get("conditions", [])),
                "Primary Purpose":          design.get("designInfo", {}).get("primaryPurpose"),
                "Interventions":            ", ".join(intervention_names),
                "Intervention Description": " | ".join(intervention_descs),
                "Study Type":               design.get("studyType"),
                "Phases":                   ", ".join(design.get("phases", [])),
                "Outcome Measure":          " | ".join(outcome_measures),
                "Medical Subject Headings": ", ".join(cond_mod.get("keywords", [])),
                
                # === BONUS columns (API only) ===
                "nct_id":                   ident.get("nctId"),
                "enrollment":               design.get("enrollmentInfo", {}).get("count"),
                "data_source":              "api",
            }
            
            # === Extract locations for separate table ===
            locations = contacts.get("locations", [])
            record["_locations"] = [
                {
                    "facility": loc.get("facility"),
                    "city":     loc.get("city"),
                    "state":    loc.get("state"),
                    "country":  loc.get("country"),
                    "zip_code": loc.get("zip"),
                }
                for loc in locations
            ]
            
            return record
            
        except Exception as e:
            nct = study.get("protocolSection", {}).get("identificationModule", {}).get("nctId", "?")
            logger.warning(f"Failed to flatten study {nct}: {e}")
            return None
    
    # =========================================================================
    # UTILITY: Extract locations separately
    # =========================================================================
    @staticmethod
    def extract_locations(df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract the locations from the _locations column into a separate DataFrame.
        
        Call this after ingest() to get data for study_locations table.
        
        Args:
            df: DataFrame from ingest() (contains _locations column)
        
        Returns:
            DataFrame with columns: [source_row_index, facility, city, state, country, zip_code]
        """
        rows = []
        for idx, locs in df["_locations"].items():
            if locs:
                for loc in locs:
                    loc["source_row_index"] = idx
                    rows.append(loc)
        
        if not rows:
            return pd.DataFrame(columns=["source_row_index", "facility", "city", "state", "country", "zip_code"])
        
        locations_df = pd.DataFrame(rows)
        logger.info(f"Extracted {len(locations_df):,} location records from {df['_locations'].notna().sum()} studies")
        
        return locations_df
