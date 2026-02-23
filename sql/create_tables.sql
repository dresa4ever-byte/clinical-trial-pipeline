-- =============================================================================
-- CLINICAL TRIAL DATA PIPELINE — DATABASE SCHEMA
-- =============================================================================
-- Author:  Andres
-- Created: 2026-02-20
-- Database: PostgreSQL 15+
-- 
-- Design Decisions:
--   1. Normalized schema: multi-value fields (conditions, interventions, 
--      age groups, MeSH terms) split into bridge tables to satisfy 1NF
--   2. Organizations extracted to separate table (28K unique orgs)
--      to reduce redundancy across 496K study rows
--   3. Phases kept as string on studies table (only 9 distinct values
--      including combinations like "PHASE1, PHASE2") — bridge table 
--      would be over-engineering for this cardinality
--   4. Date handling: parsed date + raw original + approximation flag
--      to handle the two formats (YYYY-MM-DD and YYYY-MM) transparently
--   5. Indexes chosen based on the required analytical queries:
--      trials by phase/type, top conditions, completion rates, 
--      geographic distribution (N/A in this dataset), timeline analysis
-- =============================================================================


-- =============================================================================
-- STEP 0: Clean slate (drop tables if re-running)
-- Drop in reverse dependency order to respect foreign keys
-- =============================================================================
DROP TABLE IF EXISTS study_mesh_terms CASCADE;
DROP TABLE IF EXISTS study_age_groups CASCADE;
DROP TABLE IF EXISTS study_interventions CASCADE;
DROP TABLE IF EXISTS study_conditions CASCADE;
DROP TABLE IF EXISTS study_locations CASCADE;
DROP TABLE IF EXISTS studies CASCADE;
DROP TABLE IF EXISTS organizations CASCADE;
DROP TABLE IF EXISTS pipeline_log CASCADE;

-- Optional: create a dedicated schema for isolation
-- CREATE SCHEMA IF NOT EXISTS clinical_trials;
-- SET search_path TO clinical_trials, public;


-- =============================================================================
-- TABLE 1: organizations
-- =============================================================================
-- Rationale: 28,083 unique orgs repeated across 496K rows.
--            Normalizing saves storage and enables org-level analytics.
-- =============================================================================
CREATE TABLE organizations (
    org_id          SERIAL          PRIMARY KEY,
    org_name        VARCHAR(500)    NOT NULL,
    org_class       VARCHAR(50),                -- OTHER, INDUSTRY, NIH, OTHER_GOV, FED, NETWORK, INDIV
                                                -- NULL when original was 'Unknown'/'UNKNOWN'
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure no duplicate org names
    CONSTRAINT uq_org_name UNIQUE (org_name)
);

COMMENT ON TABLE organizations IS 'Sponsoring organizations for clinical trials. Extracted from study data to reduce redundancy.';
COMMENT ON COLUMN organizations.org_class IS 'Organization classification: OTHER, INDUSTRY, NIH, OTHER_GOV, FED, NETWORK, INDIV. NULL if unknown.';


-- =============================================================================
-- TABLE 2: studies (main fact table)
-- =============================================================================
-- This is the central table. Each row = one clinical trial.
-- All single-value attributes live here.
-- Multi-value attributes (conditions, interventions, etc.) are in bridge tables.
-- =============================================================================
CREATE TABLE studies (
    study_id                SERIAL          PRIMARY KEY,
    org_id                  INTEGER         REFERENCES organizations(org_id),
    responsible_party       VARCHAR(50),    -- SPONSOR, PRINCIPAL_INVESTIGATOR, SPONSOR_INVESTIGATOR
                                            -- NULL when original was 'Unknown'
    brief_title             TEXT            NOT NULL,
    full_title              TEXT,           -- NULL when original was 'Unknown' (9,990 rows)
    overall_status          VARCHAR(50),    -- COMPLETED, RECRUITING, TERMINATED, etc.
                                            -- NULL when original was 'UNKNOWN' (67K rows)
    start_date              DATE,           -- Parsed and cleaned date
                                            -- NULL when unparseable or suspicious (before 1950 / after 2025)
    start_date_raw          VARCHAR(20),    -- Original value preserved for audit trail
    start_date_is_approx    BOOLEAN         DEFAULT FALSE,  
                                            -- TRUE when original was YYYY-MM format (no day)
                                            -- We default to 1st of month for start_date
    primary_purpose         VARCHAR(50),    -- TREATMENT, PREVENTION, DIAGNOSTIC, etc.
                                            -- NULL when original was 'Unknown' (122K rows)
    study_type              VARCHAR(50)     NOT NULL,   
                                            -- INTERVENTIONAL, OBSERVATIONAL, EXPANDED_ACCESS
    phase                   VARCHAR(30),    -- PHASE1, PHASE2, PHASE3, PHASE4, EARLY_PHASE1
                                            -- Also combined: 'PHASE1, PHASE2', 'PHASE2, PHASE3'
                                            -- NULL when original was NULL, 'Unknown', or 'No phases listed'
    outcome_measure         TEXT,           -- Free text, can be very long (up to 21K chars)
    intervention_description TEXT,          -- Free text (up to 13K chars)
                                            -- NULL when original was 'Unknown'
    nct_id                  VARCHAR(20),    -- ClinicalTrials.gov identifier (from API data)
    enrollment              INTEGER,        -- Number of participants (from API data)
    data_source             VARCHAR(10)     DEFAULT 'csv',  -- 'csv' or 'api'
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE studies IS 'Core fact table: one row per clinical trial study. Contains all single-value attributes.';
COMMENT ON COLUMN studies.start_date IS 'Cleaned date. NULL if original was Unknown, before 1950, or after 2025.';
COMMENT ON COLUMN studies.start_date_raw IS 'Original date string from source data, preserved for audit.';
COMMENT ON COLUMN studies.start_date_is_approx IS 'TRUE if original date was YYYY-MM format (day defaulted to 01).';
COMMENT ON COLUMN studies.phase IS 'Trial phase. Can be combined (PHASE1, PHASE2). NULL if unknown.';


-- =============================================================================
-- TABLE 3: study_conditions (bridge table — many-to-many)
-- =============================================================================
-- Rationale: Studies have 1-334 conditions (avg 1.9). Comma-separated in source.
--            110K unique conditions across 923K total associations.
-- =============================================================================
CREATE TABLE study_conditions (
    id              SERIAL          PRIMARY KEY,
    study_id        INTEGER         NOT NULL REFERENCES studies(study_id) ON DELETE CASCADE,
    condition_name  VARCHAR(500)    NOT NULL
);

COMMENT ON TABLE study_conditions IS 'Bridge table: maps studies to their conditions. One row per study-condition pair.';


-- =============================================================================
-- TABLE 4: study_interventions (bridge table — many-to-many)
-- =============================================================================
-- Rationale: Studies have 1-97 interventions (avg 1.9). Comma-separated in source.
--            443K unique interventions across 923K total associations.
-- Note: Case inconsistencies exist (Placebo vs placebo) — cleaned in pipeline.
-- =============================================================================
CREATE TABLE study_interventions (
    id                  SERIAL          PRIMARY KEY,
    study_id            INTEGER         NOT NULL REFERENCES studies(study_id) ON DELETE CASCADE,
    intervention_name   VARCHAR(500)    NOT NULL
);

COMMENT ON TABLE study_interventions IS 'Bridge table: maps studies to their interventions. One row per study-intervention pair.';


-- =============================================================================
-- TABLE 5: study_age_groups (bridge table — many-to-many)
-- =============================================================================
-- Rationale: Studies target 1-3 age groups. Space-separated in source.
--            Only 3 unique values: CHILD, ADULT, OLDER_ADULT.
-- =============================================================================
CREATE TABLE study_age_groups (
    id              SERIAL          PRIMARY KEY,
    study_id        INTEGER         NOT NULL REFERENCES studies(study_id) ON DELETE CASCADE,
    age_group       VARCHAR(20)     NOT NULL  
                                    -- CHILD, ADULT, OLDER_ADULT
);

COMMENT ON TABLE study_age_groups IS 'Bridge table: maps studies to target age groups (CHILD, ADULT, OLDER_ADULT).';


-- =============================================================================
-- TABLE 6: study_mesh_terms (bridge table — many-to-many)
-- =============================================================================
-- Rationale: Studies have 1-53 MeSH terms (avg 1.3). Comma-separated in source.
--            63K unique terms across 657K total associations.
-- Note: Some source data has duplicated terms (e.g., "Carcinoma Carcinoma")
--       which are cleaned in the pipeline.
-- =============================================================================
CREATE TABLE study_mesh_terms (
    id              SERIAL          PRIMARY KEY,
    study_id        INTEGER         NOT NULL REFERENCES studies(study_id) ON DELETE CASCADE,
    mesh_term       TEXT            NOT NULL
);

COMMENT ON TABLE study_mesh_terms IS 'Bridge table: maps studies to Medical Subject Heading (MeSH) terms.';


-- =============================================================================
-- TABLE 7: study_locations (from API data — enables geographic analytics)
-- =============================================================================
-- Rationale: The CSV dataset lacks location data. The API provides facility,
--            city, state, country for each study site. This enables geographic
--            distribution queries impossible with CSV alone.
-- =============================================================================
CREATE TABLE study_locations (
    id              SERIAL          PRIMARY KEY,
    study_id        INTEGER         NOT NULL REFERENCES studies(study_id) ON DELETE CASCADE,
    facility        VARCHAR(500),
    city            VARCHAR(200),
    state           VARCHAR(200),
    country         VARCHAR(100),
    zip_code        VARCHAR(20)
);

COMMENT ON TABLE study_locations IS 'Geographic locations of clinical trial sites. Populated from API data.';


-- =============================================================================
-- TABLE 8: pipeline_log (operational — tracks pipeline runs)
-- =============================================================================
-- Rationale: Every production pipeline needs observability.
--            Tracks when data was loaded, how many rows, any errors.
-- =============================================================================
CREATE TABLE pipeline_log (
    log_id          SERIAL          PRIMARY KEY,
    run_id          VARCHAR(50)     NOT NULL,       -- UUID for each pipeline run
    step_name       VARCHAR(100)    NOT NULL,       -- e.g., 'ingest_csv', 'clean_dates', 'load_studies'
    status          VARCHAR(20)     NOT NULL,       -- SUCCESS, FAILED, RUNNING
    rows_processed  INTEGER,
    rows_rejected   INTEGER,
    error_message   TEXT,
    started_at      TIMESTAMP       NOT NULL,
    completed_at    TIMESTAMP,
    duration_seconds NUMERIC(10,2)
);

COMMENT ON TABLE pipeline_log IS 'Operational logging for pipeline runs. Tracks each step, row counts, and errors.';


-- =============================================================================
-- INDEXES
-- =============================================================================
-- Strategy: indexes chosen to support the 5 required analytical queries:
--   1. Trials by study type and phase       → idx on phase, study_type
--   2. Most common conditions               → idx on condition_name
--   3. Completion rates by intervention     → idx on overall_status + intervention join
--   4. Geographic distribution              → N/A (no location data in this dataset)
--   5. Timeline / duration analysis         → idx on start_date
-- Plus: foreign key indexes for efficient JOINs
-- =============================================================================

-- Studies table indexes
CREATE INDEX idx_studies_phase           ON studies(phase);
CREATE INDEX idx_studies_status          ON studies(overall_status);
CREATE INDEX idx_studies_type            ON studies(study_type);
CREATE INDEX idx_studies_start_date      ON studies(start_date);
CREATE INDEX idx_studies_purpose         ON studies(primary_purpose);
CREATE INDEX idx_studies_org_id          ON studies(org_id);
CREATE INDEX idx_studies_nct_id          ON studies(nct_id);

-- Bridge table indexes (FK + value lookups)
CREATE INDEX idx_conditions_study_id     ON study_conditions(study_id);
CREATE INDEX idx_conditions_name         ON study_conditions(condition_name);

CREATE INDEX idx_interventions_study_id  ON study_interventions(study_id);
CREATE INDEX idx_interventions_name      ON study_interventions(intervention_name);

CREATE INDEX idx_age_groups_study_id     ON study_age_groups(study_id);
CREATE INDEX idx_age_groups_group        ON study_age_groups(age_group);

CREATE INDEX idx_mesh_study_id           ON study_mesh_terms(study_id);
CREATE INDEX idx_mesh_term               ON study_mesh_terms(mesh_term);

-- Location indexes (for geographic queries on API data)
CREATE INDEX idx_locations_study_id      ON study_locations(study_id);
CREATE INDEX idx_locations_country       ON study_locations(country);
CREATE INDEX idx_locations_city          ON study_locations(city);

-- Pipeline log indexes
CREATE INDEX idx_pipeline_log_run_id     ON pipeline_log(run_id);
CREATE INDEX idx_pipeline_log_status     ON pipeline_log(status);


-- =============================================================================
-- VERIFICATION: Check everything was created
-- =============================================================================
SELECT 
    table_name, 
    (SELECT COUNT(*) FROM information_schema.columns c 
     WHERE c.table_name = t.table_name 
     AND c.table_schema = t.table_schema) AS column_count
FROM information_schema.tables t
WHERE table_schema = 'public'  -- change if using a custom schema
AND table_type = 'BASE TABLE'
AND table_name IN (
    'organizations', 'studies', 'study_conditions', 
    'study_interventions', 'study_age_groups', 
    'study_mesh_terms', 'study_locations', 'pipeline_log'
)
ORDER BY table_name;


-- =============================================================================
-- EXPECTED OUTPUT:
--   organizations        | 4
--   pipeline_log         | 10
--   studies              | 18
--   study_age_groups     | 3
--   study_conditions     | 3
--   study_interventions  | 3
--   study_locations      | 7
--   study_mesh_terms     | 3
-- =============================================================================
