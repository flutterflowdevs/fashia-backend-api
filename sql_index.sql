-- PRIORITY 1: Most critical indexes for your subqueries
-- These directly fix the 25-second subquery problem

CREATE INDEX IF NOT EXISTS idx_provider_entities_npi_or_ccn 
ON provider_entities(npi_or_ccn);

CREATE INDEX IF NOT EXISTS idx_provider_taxonomies_npi 
ON provider_taxonomies(npi);

CREATE INDEX IF NOT EXISTS idx_provider_taxonomies_nucc_code 
ON provider_taxonomies(nucc_code);

CREATE INDEX IF NOT EXISTS idx_roles_specialties_nucc_code 
ON roles_specialties_classification(nucc_code);

-- PRIORITY 2: Index for employer joins
CREATE INDEX IF NOT EXISTS idx_pfel_facility_npi_or_ccn 
ON provider_facility_employer_linked(facility_npi_or_ccn);

CREATE INDEX IF NOT EXISTS idx_pfel_provider_id 
ON provider_facility_employer_linked(provider_id);

CREATE INDEX IF NOT EXISTS idx_entities_enriched_ccn_npi 
ON entities_enriched(ccn_or_npi);


-- =====================================================================
-- FIX FOR COUNT QUERY SLOWDOWN
-- =====================================================================

-- CRITICAL: Index on is_employer (most important!)
-- CREATE INDEX IF NOT EXISTS idx_entities_enriched_is_employer 
-- ON entities_enriched(is_employer);

-- -- RECOMMENDED: Index on state_id
-- CREATE INDEX IF NOT EXISTS idx_entities_enriched_state_id 
-- ON entities_enriched(state_id);

-- -- OPTIONAL: Composite index for common patterns
-- CREATE INDEX IF NOT EXISTS idx_entities_is_employer_state 
-- ON entities_enriched(is_employer, state_id);

-- UPDATE STATISTICS
ANALYZE;

--############# Lockup Tables and Indexes for Performance Optimization #############

-- ========================================================================
-- PERFORMANCE OPTIMIZATION: Materialized Lookup Tables and Indexes
-- ========================================================================
-- This script creates denormalized tables and optimized indexes to dramatically
-- improve query performance from ~4 minutes to <1 second
-- ========================================================================

-- Step 1: Create materialized lookup table for facility-role-specialty mappings
-- This eliminates the need for complex EXISTS subqueries in searches
DROP TABLE IF EXISTS facility_roles_specialties_lookup;

CREATE TABLE facility_roles_specialties_lookup (
    facility_ccn_or_npi TEXT NOT NULL,
    role TEXT NOT NULL,
    specialty TEXT NOT NULL,
    provider_count INTEGER NOT NULL,
    PRIMARY KEY (facility_ccn_or_npi, role, specialty)
) WITHOUT ROWID;

-- Populate the lookup table
INSERT INTO facility_roles_specialties_lookup
SELECT 
    pe.npi_or_ccn AS facility_ccn_or_npi,
    LOWER(COALESCE(rsc.role, '')) AS role,
    LOWER(COALESCE(rsc.specialty, '')) AS specialty,
    COUNT(DISTINCT pe.provider_id) AS provider_count
FROM provider_entities pe
INNER JOIN provider_taxonomies pt ON pt.npi = pe.provider_id
INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code
GROUP BY pe.npi_or_ccn, LOWER(rsc.role), LOWER(rsc.specialty);

-- Create indexes on the lookup table
CREATE INDEX idx_frsl_role ON facility_roles_specialties_lookup(role);
CREATE INDEX idx_frsl_specialty ON facility_roles_specialties_lookup(specialty);
CREATE INDEX idx_frsl_role_specialty ON facility_roles_specialties_lookup(role, specialty);

-- Step 2: Create facility-employer lookup table
DROP TABLE IF EXISTS facility_employers_lookup;

CREATE TABLE facility_employers_lookup (
    facility_ccn_or_npi TEXT NOT NULL,
    employer_ccn_or_npi TEXT NOT NULL,
    employer_name TEXT NOT NULL,
    role TEXT NOT NULL,
    specialty TEXT NOT NULL,
    PRIMARY KEY (facility_ccn_or_npi, employer_ccn_or_npi, role, specialty)
) WITHOUT ROWID;

-- Populate the employer lookup table
INSERT INTO facility_employers_lookup
SELECT DISTINCT
    pfel.facility_npi_or_ccn AS facility_ccn_or_npi,
    emp.ccn_or_npi AS employer_ccn_or_npi,
    LOWER(emp.name) AS employer_name,
    LOWER(COALESCE(rsc.role, '')) AS role,
    LOWER(COALESCE(rsc.specialty, '')) AS specialty
FROM provider_facility_employer_linked pfel
INNER JOIN entities_enriched emp ON emp.ccn_or_npi = pfel.employer_npi_or_ccn AND emp.is_employer = 1
INNER JOIN provider_taxonomies pt ON pt.npi = pfel.provider_id
INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code;

-- Create indexes on employer lookup table
CREATE INDEX idx_fel_employer_name ON facility_employers_lookup(employer_name);
CREATE INDEX idx_fel_role_specialty ON facility_employers_lookup(role, specialty);

-- Step 3: Create provider name lookup for faster filtering
DROP TABLE IF EXISTS facility_providers_lookup;

CREATE TABLE facility_providers_lookup (
    facility_ccn_or_npi TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    role TEXT NOT NULL,
    specialty TEXT NOT NULL,
    PRIMARY KEY (facility_ccn_or_npi, provider_id, role, specialty)
) WITHOUT ROWID;

-- Populate provider name lookup
INSERT INTO facility_providers_lookup
SELECT DISTINCT
    pe.npi_or_ccn AS facility_ccn_or_npi,
    pe.provider_id,
    LOWER(COALESCE(pe.first_name, '')) AS first_name,
    LOWER(COALESCE(pe.last_name, '')) AS last_name,
    LOWER(COALESCE(rsc.role, '')) AS role,
    LOWER(COALESCE(rsc.specialty, '')) AS specialty
FROM provider_entities pe
INNER JOIN provider_taxonomies pt ON pt.npi = pe.provider_id
INNER JOIN roles_specialties_classification rsc ON rsc.nucc_code = pt.nucc_code;

-- Create indexes on provider lookup
CREATE INDEX idx_fpl_first_name ON facility_providers_lookup(first_name);
CREATE INDEX idx_fpl_last_name ON facility_providers_lookup(last_name);
CREATE INDEX idx_fpl_full_name ON facility_providers_lookup(first_name, last_name);
CREATE INDEX idx_fpl_role_specialty ON facility_providers_lookup(role, specialty);

-- Step 4: Add critical missing indexes on main tables
CREATE INDEX IF NOT EXISTS idx_entities_enriched_is_employer 
ON entities_enriched(is_employer) 
WHERE is_employer = 0;

CREATE INDEX IF NOT EXISTS idx_entities_enriched_is_employer_name
ON entities_enriched(is_employer, name)
WHERE is_employer = 0;

-- Composite covering index for provider lookups
CREATE INDEX IF NOT EXISTS idx_provider_entities_covering
ON provider_entities(npi_or_ccn, provider_id, first_name, last_name);

-- Composite covering index for taxonomy joins
CREATE INDEX IF NOT EXISTS idx_provider_taxonomies_covering
ON provider_taxonomies(npi, nucc_code);

-- Composite covering index for role/specialty lookups
CREATE INDEX IF NOT EXISTS idx_roles_specialties_covering
ON roles_specialties_classification(nucc_code, role, specialty);

-- Step 5: Create summary statistics table for facilities
DROP TABLE IF EXISTS facility_summary_stats;

CREATE TABLE facility_summary_stats (
    facility_ccn_or_npi TEXT,
    subtype TEXT,
    total_providers INTEGER NOT NULL,
    total_employers INTEGER NOT NULL,
    roles_json TEXT,  -- JSON array of distinct roles
    specialties_json TEXT,  -- JSON array of distinct specialties
    employers_json TEXT,  -- JSON array of employer objects
    PRIMARY KEY (facility_ccn_or_npi, subtype)
) WITHOUT ROWID;

-- Populate summary stats (this helps for sorting and display)
INSERT INTO facility_summary_stats
SELECT 
    e.ccn_or_npi,
    COALESCE(e.subtype, 'Unknown') as subtype,
    COALESCE(
        (SELECT COUNT(DISTINCT pe.provider_id)
         FROM provider_entities pe
         WHERE pe.npi_or_ccn = e.ccn_or_npi), 0
    ) AS total_providers,
    COALESCE(
        (SELECT COUNT(DISTINCT pfel.employer_npi_or_ccn)
         FROM provider_facility_employer_linked pfel
         WHERE pfel.facility_npi_or_ccn = e.ccn_or_npi), 0
    ) AS total_employers,
    NULL AS roles_json,
    NULL AS specialties_json,
    NULL AS employers_json
FROM (
    SELECT DISTINCT ccn_or_npi, subtype
    FROM entities_enriched 
    WHERE is_employer = 0
) e;

-- Convert Null to Unknown as default value

-- Step 6: Analyze tables for query optimization
ANALYZE facility_roles_specialties_lookup;
ANALYZE facility_employers_lookup;
ANALYZE facility_providers_lookup;
ANALYZE facility_summary_stats;
ANALYZE entities_enriched;
ANALYZE provider_entities;
ANALYZE provider_taxonomies;
ANALYZE roles_specialties_classification;

-- ========================================================================
-- Performance Notes:
-- ========================================================================
-- 1. The materialized tables should be rebuilt periodically (e.g., nightly)
-- 2. For incremental updates, use triggers or scheduled jobs
-- 3. The lookup tables are stored WITHOUT ROWID for better performance
-- 4. All text searches should now use lowercase comparison
-- 5. Expected performance improvement: 200x+ faster (4 min -> <1 sec)
-- ========================================================================

-- Example rebuild script (run periodically):
-- DELETE FROM facility_roles_specialties_lookup;
-- INSERT INTO facility_roles_specialties_lookup SELECT ...;
-- ANALYZE facility_roles_specialties_lookup;