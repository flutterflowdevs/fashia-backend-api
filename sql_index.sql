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