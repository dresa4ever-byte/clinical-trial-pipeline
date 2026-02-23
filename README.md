# Clinical Trial Data Pipeline

**Multi-source ETL pipeline** that ingests clinical trial data from CSV files and the ClinicalTrials.gov REST API, applies comprehensive data cleaning, and loads into a normalized PostgreSQL/Cloudberry data warehouse.

Built as a take-home assessment for the Data Engineer.

---

## Architecture

```
┌─────────────────────┐     ┌─────────────────────┐
│   CSV File (390MB)  │     │  ClinicalTrials.gov  │
│   496,615 studies   │     │    REST API v2       │
└────────┬────────────┘     └────────┬─────────────┘
         │                           │
    CSVIngestor               APIIngestor
         │                           │
         └──────────┬────────────────┘
                    │
              DataFrame (unified format)
                    │
              ┌─────▼──────┐
              │  DataCleaner │  12 cleaning rules
              │  (cleaner.py)│  based on EDA findings
              └─────┬──────┘
                    │
              ┌─────▼──────┐
              │  Validator  │  Data quality gate
              └─────┬──────┘
                    │
              ┌─────▼──────┐
              │   Loader    │  Batch inserts
              └─────┬──────┘
                    │
         ┌──────────▼───────────┐
         │   PostgreSQL /       │
         │   Cloudberry DWH     │
         │                      │
         │  organizations       │   28,083 rows
         │  studies             │  496,615 rows
         │  study_conditions    │  923,632 rows
         │  study_interventions │  874,253 rows
         │  study_age_groups    │  933,847 rows
         │  study_mesh_terms    │  571,482 rows
         │  study_locations     │  API data
         │  pipeline_log        │  Observability
         └──────────────────────┘
```

**Key design decision:** Both data sources produce an identical DataFrame format, so the same cleaning, validation, and loading logic works for both. Adding a third source (e.g., SQLite, Parquet) requires only a new ingestor class — zero changes to downstream code.

---

## Quick Start

### Option 1: Docker (recommended for reviewers)

```bash
# 1. Clone the repository
git clone <repo_url>
cd clinical-trial-pipeline

# 2. Place the CSV dataset in the data/ folder
cp /path/to/clinical_trials.csv data/

# 3. Start everything (PostgreSQL + Airflow + pipeline)
docker-compose up -d

# 4. Watch the pipeline run (~12 minutes for full CSV load)
docker-compose logs -f pipeline

# 5. Access Airflow UI to see the DAGs
#    URL: http://localhost:8080
#    Login: admin / admin
#    You will see two DAGs:
#      - clinical_trial_backfill   (one-time CSV load, manual trigger)
#      - clinical_trial_incremental (weekly API updates, scheduled)

# 6. Query the loaded data
docker exec -it clinical-trial-pipeline-postgres-data-1 \
  psql -U pipeline -d clinical_trials -c "SELECT COUNT(*) FROM studies;"

# 7. Run analytical queries
docker exec -it clinical-trial-pipeline-postgres-data-1 \
  psql -U pipeline -d clinical_trials -f /opt/airflow/sql/analytical_queries.sql

# 8. Stop everything
docker-compose down -v
```

### Option 2: Local execution

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Edit src/config.py with your database credentials

# 3. Create tables by running sql/create_tables.sql in your database

# 4. Run the pipeline
python run.py

# 5. Run analytical queries from sql/analytical_queries.sql
```

### Option 3: Code review only

If you prefer to review the code without running it:

| What to Review | File(s) |
|---|---|
| **Start here** → README | `README.md` |
| EDA & data exploration | `eda_clinical_trials.py` |
| Database schema | `sql/create_tables.sql` |
| Pipeline entry point | `run.py` → `src/pipeline/main.py` |
| Data ingestion (CSV) | `src/ingestion/csv_ingestor.py` |
| Data ingestion (API) | `src/ingestion/api_ingestor.py` |
| Cleaning (12 rules) | `src/processing/cleaner.py` |
| Validation | `src/processing/validator.py` |
| Database loading | `src/loading/loader.py` |
| Airflow DAGs | `dags/dag_backfill.py`, `dags/dag_incremental.py` |
| Shared DAG logic | `dags/shared_tasks.py` |
| Analytical SQL queries | `sql/analytical_queries.sql` |
| Tests (22 passing) | `tests/test_pipeline.py` |
| Docker setup | `Dockerfile`, `docker-compose.yml` |

---

## Project Structure

```
clinical_trial_pipeline/
├── README.md
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── run.py                          ← Entry point (local execution)
│
├── src/
│   ├── config.py                   ← DB credentials, settings, cleaning rules
│   ├── ingestion/
│   │   ├── base_ingestor.py        ← Abstract base class (Strategy pattern)
│   │   ├── csv_ingestor.py         ← CSV file reader (390MB, 496K rows)
│   │   └── api_ingestor.py         ← ClinicalTrials.gov API v2 client
│   ├── processing/
│   │   ├── cleaner.py              ← 12 data cleaning rules from EDA
│   │   └── validator.py            ← Data quality checks before loading
│   ├── loading/
│   │   └── loader.py               ← PostgreSQL batch loader with logging
│   └── pipeline/
│       └── main.py                 ← Pipeline orchestrator (csv/api/both modes)
│
├── dags/
│   ├── shared_tasks.py             ← Common functions
│   ├── dag_backfill.py             ← One-time CSV load
│   └── dag_incremental.py          ← Weekly API updates
│
├── sql/
│   ├── create_tables.sql           ← PostgreSQL DDL (Docker/production)
│   ├── create_tables_cloudberry.sql← Cloudberry DDL (development)
│   └── analytical_queries.sql      ← 5 required + 4 bonus analytical queries
│
├── tests/
│   └── test_pipeline.py            ← pytest suite (22 tests)
│
├── output/                         ← Pipeline logs
└── data/                           ← Place CSV file here (for Docker)
```

---

## Data Sources

### Source 1: CSV (Kaggle dataset)
- **File:** ClinicalTrials.gov dataset (390MB, 496,615 rows, 17 columns)
- **Use case:** Bulk historical load

### Source 2: ClinicalTrials.gov API v2
- **Endpoint:** `https://clinicaltrials.gov/api/v2/studies`
- **Format:** JSON with nested structure
- **Rate limit:** ~50 requests/minute, no authentication needed
- **Pagination:** Token-based (`nextPageToken`)
- **Use case:** Live/incremental data, provides bonus fields (NCT ID, enrollment, locations)
- **Bonus fields over CSV:** `nct_id`, `enrollment`, geographic `locations` (facility, city, country)

---

## Database Schema

Normalized design reducing redundancy from 496K rows of repeated org data to 28K unique organizations.

### Entity Relationship:

```
organizations (1) ──→ (N) studies (1) ──→ (N) study_conditions
                                    (1) ──→ (N) study_interventions
                                    (1) ──→ (N) study_age_groups
                                    (1) ──→ (N) study_mesh_terms
                                    (1) ──→ (N) study_locations
```

### Design Decisions:
- **Normalized bridge tables** for multi-value fields (conditions, interventions, etc.) — enables efficient querying and prevents data duplication
- **Phases kept as string** on studies table (only 9 distinct values) — a bridge table would be over-engineering
- **Date handling:** Parsed date + raw original + approximation flag for YYYY-MM format dates (43% of data)
- **Pipeline_log table** for operational observability — tracks every run with row counts and timings
- **data_source column** on studies table — tracks whether each row came from CSV or API

### Data Types and Constraints:
- **SERIAL PRIMARY KEY** on all main tables (org_id, study_id) — auto-incrementing surrogate keys
- **VARCHAR(n)** with appropriate lengths based on EDA: e.g., `overall_status VARCHAR(50)` since max observed was 30 chars, `brief_title VARCHAR(1000)` for long study titles
- **TEXT** for unbounded fields (mesh_terms, outcome_measure) — EDA revealed some values exceed 500 chars due to concatenation
- **DATE** for parsed dates, **VARCHAR(20)** for raw date strings (preserves original format)
- **BOOLEAN** for `start_date_is_approx` flag — marks YYYY-MM dates that were approximated to day 01
- **NOT NULL** on critical columns: study_type, overall_status, brief_title — validated before loading
- **FOREIGN KEY** from studies → organizations in PostgreSQL DDL (relaxed in Cloudberry due to distributed table limitations)

### Indexing Strategy:
- **Primary keys** auto-indexed on all tables
- **`idx_studies_org_id`** — frequent JOINs between studies and organizations
- **`idx_studies_status`** — filtering by overall_status (COMPLETED, RECRUITING, etc.)
- **`idx_studies_type`** — filtering by study_type (INTERVENTIONAL, OBSERVATIONAL)
- **`idx_studies_start_date`** — timeline analysis queries, range scans by year
- **`idx_studies_nct_id`** — fast lookup by ClinicalTrials.gov identifier (API deduplication)
- **`idx_study_locations_country/city`** — geographic distribution queries
- Bridge table indexes on `study_id` — fast JOINs from studies to conditions/interventions/etc.
- **Trade-off:** No indexes on rarely queried columns (full_title, intervention_description) to keep INSERT performance fast for 496K+ row loads

---

## Data Cleaning Rules (EDA-Driven)

The cleaning logic was derived from comprehensive EDA of the full 496,615-row dataset. Key findings and corresponding rules:

| # | Issue Found in EDA | Rule Applied | Rows Affected |
|---|---|---|---|
| 1 | "Unknown"/"UNKNOWN" used as NULL across 12 columns | Replace with proper NULL | 596,170 |
| 2 | YYYY-MM dates (no day) in 43% of rows | Append "-01", flag as approximate | 214,975 |
| 3 | Dates before 1950 and after 2026 | Flag as suspicious, NULL the parsed date | 33 |
| 4 | "No phases listed" string | Convert to NULL | 885 |
| 5 | Organization Class: "Unknown" vs "UNKNOWN" | Both → NULL (case inconsistency resolved) | 1,557 |
| 6 | Comma-separated Conditions (avg 1.9 per study) | Split into bridge table rows | → 923,632 |
| 7 | Comma-separated Interventions | Split into bridge table rows | → 874,253 |
| 8 | Space-separated Standard Age | Split into bridge table rows | → 933,847 |
| 9 | Comma-separated MeSH Terms (with duplicates) | Split into bridge table rows | → 571,482 |
| 10 | Unnamed:0 index column | Drop | 496,615 |
| 11 | Leading/trailing whitespace | Trim all string columns | Preventive |
| 12 | NULL study_type (after hidden null removal) | Set to "UNKNOWN" | 885 |

---

## Analytical Queries

### Required Queries:

1. **Trials by study type and phase** — Shows research distribution; 37% of interventional trials have no listed phase
2. **Most common conditions** — "Healthy" (volunteer studies) leads with 10,312 studies; Breast Cancer is #1 disease at 7,723
3. **Completion rates by intervention** — HIV studies have 77.5% completion; COVID-19 studies lowest at 44.6%
4. **Geographic distribution** — By organization (CSV data) and by country (API data)
5. **Timeline analysis** — Trials per year showing growth trends and COVID-19 impact

### Bonus Queries:
- Phase progression funnel (completion rates by phase)
- Organization class breakdown (Industry vs NIH vs Academic)
- Late-stage conditions (what's in Phase 3?)
- Pipeline health check (pipeline_log data)

---

## Airflow DAGs

Two separate DAGs with different purposes:

### DAG 1: `clinical_trial_backfill` (manual trigger)
```
start → ingest_csv → clean → validate → load → analytics → cleanup → end
```
- **Purpose:** One-time bulk load of 496K historical studies from CSV
- **Schedule:** None — triggered manually once, then never again
- **When to use:** Initial database population

### DAG 2: `dag_incremental` (weekly schedule)
```
start → ingest_api → clean → validate → load → analytics → cleanup → end
```
- **Purpose:** Fetch new/updated studies from ClinicalTrials.gov API
- **Schedule:** Every Monday at 06:00 UTC
- **When to use:** Keeps the database up to date with live data

### Design Decisions:
- **Separate DAGs** because CSV is a one-time source, API is recurring — different lifecycle
- **Same cleaning/loading logic** for both — proves the pipeline is source-agnostic
- **Parquet temp files** for inter-task data passing (XCom can't handle 496K-row DataFrames)
- **Validation gate** — pipeline stops if data quality checks fail
- **Cleanup task** — removes temp files after each run
- **Connection:** `Cloudberry_dwh_dev_data_power_db`

---

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=src
```

**22 tests covering:**
- Hidden null replacement logic
- Date parsing (full dates, partial dates, suspicious dates)
- Multi-value column splitting (conditions, interventions, age groups)
- Organization deduplication
- Validator error/warning detection
- CSV ingestor error handling
- Cleaning statistics tracking

---

## Performance

| Metric | Value |
|---|---|
| Total pipeline runtime | ~12 minutes |
| CSV ingestion | 6 seconds (390MB file) |
| Cleaning + transformation | 19 seconds |
| Database loading | ~11 minutes (3.8M total rows) |
| API ingestion (50 studies) | ~3 seconds |

---

## Bonus Questions

### 1. Scalability: How would you handle 100x more data?

Partition the studies table by year, use chunked reads for CSV ingestion, and move to Kubernetes with KubernetesExecutor in Airflow for horizontal scaling. Add ClickHouse as the analytical layer for faster aggregations.

### 2. Data Quality: What additional validation rules?

Cross-field checks (e.g., completed studies must have a start date), date logic (completion after start), and duplicate detection based on title + organization.

### 3. Compliance: GxP environment considerations?

Audit trail for every data transformation with timestamps, role-based access controls to the database, and formal change control process for schema and code changes.

### 4. Monitoring: How would you monitor in production?

The pipeline_log table already tracks row counts and errors per step. Add Airflow SLA alerts for slow DAGs, data freshness checks (alert if no new data in 7+ days), and row count trend monitoring to catch silent issues.

### 5. Security: What measures for sensitive clinical data?

Encryption at rest and in transit (SSL), credentials stored in Airflow Connections and Variables (not in code), network isolation for the database, separate DB users for pipeline writes vs analyst reads.

---

## Time Allocation

| Phase | Hours | Activities |
|---|---|---|
| EDA & data exploration | ~1.5 hrs | Profiled 496K rows, discovered data quality issues |
| Schema design & SQL | ~1 hr | Normalized design, indexing strategy, DDL |
| Pipeline development | ~3 hrs | Ingestors (CSV + API), cleaner, validator, loader |
| Airflow DAGs & Docker | ~1.5 hrs | Two-DAG architecture, docker-compose setup |
| Testing & debugging | ~1.5 hrs | pytest suite (22 tests), end-to-end validation |
| Documentation | ~1 hr | README, code comments |
| **Total** | **~10 hrs** | Extended scope (API integration, multi-source architecture) added ~3 hrs beyond the basic requirements |

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.11.1 |
| Database | PostgreSQL 15 / Cloudberry |
| Data Processing | pandas 2.1.4, NumPy 1.24.4|
| ORM / DB Access | SQLAlchemy 2.0.44 |
| API Client | requests |
| Orchestration | Apache Airflow 2.10 |
| Testing | pytest |
| Containerization | Docker, Docker Compose |

---

## AI Tools Used

This project was built with AI assistance, as encouraged by the task guidelines.

**Tools:**
- **Claude Opus 4.6 (Anthropic)** — Primary assistant. Used throughout the project via conversational dialog for architecture decisions, code generation, debugging, and documentation.
- **Codex 5.3** — Used to review and verify specific scripts.

**How AI was used:**
- Architecture and schema design were discussed collaboratively — I described the data and requirements, Claude suggested the normalized schema approach, and I made the final decisions on table structure and trade-offs.
- Pipeline code was generated iteratively: Claude wrote initial drafts, I tested them against the real dataset, and we debugged issues together (e.g., Cloudberry constraint limitations, module import paths, VARCHAR overflow on mesh terms).
- The EDA findings drove all cleaning rules — AI helped translate those findings into code, but the data exploration and interpretation were mine.

**Prompting approach:**
- Mostly free-form conversational dialog rather than structured prompts. I described what I needed, Claude proposed solutions, and I validated them by running the code against the actual 496K-row dataset and the live ClinicalTrials.gov API.
- For specific tasks (schema design, API integration), I provided concrete context: database platform, existing Airflow connections, column names, and error messages.

---
## Author

Built with a data-first approach: EDA → Schema Design → Pipeline → Validation → Analytics.
