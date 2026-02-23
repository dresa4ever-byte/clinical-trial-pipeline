-- =============================================================================
-- ANALYTICAL SQL QUERIES — Clinical Trial Data Pipeline
-- =============================================================================
-- These queries answer the 5 analytical questions required by the task.
-- Run them after the pipeline has loaded data into the database.
-- =============================================================================


-- =============================================================================
-- QUERY 1: Number of trials by study type and phase
-- =============================================================================
-- Business value: Shows the distribution of clinical trial activity across
-- different study types and phases, helping identify where research effort
-- is concentrated.
-- =============================================================================

SELECT 
    study_type,
    phase,
    COUNT(*)                                    AS trial_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)  AS pct_of_total
FROM studies
GROUP BY study_type, phase
ORDER BY trial_count DESC;


-- =============================================================================
-- QUERY 2: Most common conditions studied
-- =============================================================================
-- Business value: Identifies which diseases/conditions receive the most
-- research attention. Useful for pharma companies to spot competitive
-- landscapes and underserved therapeutic areas.
-- =============================================================================

SELECT 
    sc.condition_name,
    COUNT(DISTINCT sc.study_id)                 AS num_studies,
    COUNT(DISTINCT s.org_id)                    AS num_organizations,
    ROUND(
        SUM(CASE WHEN s.overall_status = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 
        / COUNT(*), 1
    )                                           AS completion_rate_pct
FROM study_conditions sc
JOIN studies s ON s.study_id = sc.study_id
GROUP BY sc.condition_name
ORDER BY num_studies DESC
LIMIT 20;


-- =============================================================================
-- QUERY 3: Completion rates by intervention type
-- =============================================================================
-- Business value: Shows which interventions have the highest/lowest 
-- completion rates. High withdrawal or termination rates may indicate
-- safety issues or recruitment challenges.
-- =============================================================================

SELECT 
    si.intervention_name,
    COUNT(DISTINCT si.study_id)                 AS num_studies,
    SUM(CASE WHEN s.overall_status = 'COMPLETED' THEN 1 ELSE 0 END)     AS completed,
    SUM(CASE WHEN s.overall_status = 'TERMINATED' THEN 1 ELSE 0 END)    AS terminated,
    SUM(CASE WHEN s.overall_status = 'WITHDRAWN' THEN 1 ELSE 0 END)     AS withdrawn,
    SUM(CASE WHEN s.overall_status = 'RECRUITING' THEN 1 ELSE 0 END)    AS recruiting,
    ROUND(
        SUM(CASE WHEN s.overall_status = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 
        / NULLIF(COUNT(DISTINCT si.study_id), 0), 1
    )                                           AS completion_rate_pct
FROM study_interventions si
JOIN studies s ON s.study_id = si.study_id
GROUP BY si.intervention_name
HAVING COUNT(DISTINCT si.study_id) >= 50
ORDER BY num_studies DESC
LIMIT 20;


-- =============================================================================
-- QUERY 4: Geographic distribution of trials
-- =============================================================================
-- Note: The CSV dataset does not include location data.
-- This query works with API-sourced data (study_locations table).
-- For CSV-only loads, we provide an alternative: distribution by organization.
-- =============================================================================

-- 4a: By organization (works with CSV data)
SELECT 
    o.org_name,
    o.org_class,
    COUNT(s.study_id)                           AS num_studies,
    SUM(CASE WHEN s.overall_status = 'COMPLETED' THEN 1 ELSE 0 END)    AS completed,
    SUM(CASE WHEN s.overall_status = 'RECRUITING' THEN 1 ELSE 0 END)   AS recruiting,
    MIN(s.start_date)                           AS earliest_study,
    MAX(s.start_date)                           AS latest_study
FROM organizations o
JOIN studies s ON s.org_id = o.org_id
GROUP BY o.org_name, o.org_class
ORDER BY num_studies DESC
LIMIT 20;

-- 4b: By country (works when API data with locations is loaded)
-- Uncomment and run after loading API data:
/*
SELECT 
    sl.country,
    COUNT(DISTINCT sl.study_id)                 AS num_studies,
    COUNT(DISTINCT sl.city)                     AS num_cities,
    COUNT(DISTINCT sl.facility)                 AS num_facilities
FROM study_locations sl
GROUP BY sl.country
ORDER BY num_studies DESC
LIMIT 20;
*/


-- =============================================================================
-- QUERY 5: Timeline analysis — trials over time
-- =============================================================================
-- Business value: Shows trends in clinical trial activity over time.
-- Useful for understanding growth patterns, impact of events (e.g., COVID),
-- and predicting future research volumes.
-- =============================================================================

SELECT 
    EXTRACT(YEAR FROM start_date)               AS start_year,
    COUNT(*)                                    AS num_studies,
    SUM(CASE WHEN study_type = 'INTERVENTIONAL' THEN 1 ELSE 0 END)     AS interventional,
    SUM(CASE WHEN study_type = 'OBSERVATIONAL' THEN 1 ELSE 0 END)      AS observational,
    SUM(CASE WHEN overall_status = 'COMPLETED' THEN 1 ELSE 0 END)      AS completed,
    SUM(CASE WHEN overall_status = 'TERMINATED' THEN 1 ELSE 0 END)     AS terminated,
    ROUND(
        SUM(CASE WHEN overall_status = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 
        / NULLIF(COUNT(*), 0), 1
    )                                           AS completion_rate_pct
FROM studies
WHERE start_date IS NOT NULL
GROUP BY EXTRACT(YEAR FROM start_date)
ORDER BY start_year;


-- =============================================================================
-- BONUS QUERIES — Additional insights for the README/presentation
-- =============================================================================

-- BONUS 1: Phase progression funnel
SELECT 
    phase,
    COUNT(*)                                    AS total_studies,
    SUM(CASE WHEN overall_status = 'COMPLETED' THEN 1 ELSE 0 END)      AS completed,
    ROUND(
        SUM(CASE WHEN overall_status = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 
        / NULLIF(COUNT(*), 0), 1
    )                                           AS completion_rate_pct
FROM studies
WHERE phase IS NOT NULL
GROUP BY phase
ORDER BY phase;


-- BONUS 2: Organization class breakdown
SELECT 
    o.org_class,
    COUNT(DISTINCT o.org_id)                    AS num_orgs,
    COUNT(s.study_id)                           AS num_studies,
    ROUND(COUNT(s.study_id) * 100.0 / SUM(COUNT(s.study_id)) OVER(), 1) AS pct_of_total
FROM organizations o
JOIN studies s ON s.org_id = o.org_id
WHERE o.org_class IS NOT NULL
GROUP BY o.org_class
ORDER BY num_studies DESC;


-- BONUS 3: Top conditions by phase (what diseases are in late-stage trials?)
SELECT 
    sc.condition_name,
    COUNT(DISTINCT sc.study_id)                 AS num_phase3_studies
FROM study_conditions sc
JOIN studies s ON s.study_id = sc.study_id
WHERE s.phase IN ('PHASE3', 'PHASE2, PHASE3')
GROUP BY sc.condition_name
ORDER BY num_phase3_studies DESC
LIMIT 15;


-- BONUS 4: Pipeline health check
SELECT 
    run_id,
    step_name,
    status,
    rows_processed,
    rows_rejected,
    duration_seconds,
    completed_at
FROM pipeline_log
ORDER BY completed_at;
