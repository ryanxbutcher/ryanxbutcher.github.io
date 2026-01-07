-- ============================================================================
-- EMS Data Warehouse - Index Definitions
-- Author: J. Ryan Butcher
-- Purpose: Performance optimization indexes for analytical queries
-- ============================================================================

-- ============================================================================
-- Columnstore Index for Analytical Workloads
-- Provides excellent compression and fast aggregation queries
-- ============================================================================
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_FACT_EMS_COLUMNSTORE
    ON dw.FACT_EMS_INCIDENT (
        date_key
        ,county_key
        ,chief_complaint_key
        ,provider_org_key
        ,service_level_key
        ,provider_to_scene_mins
        ,provider_to_dest_mins
        ,injury_flg
        ,naloxone_given_flg
        ,incident_count
    );
GO

-- ============================================================================
-- Additional Dimension Indexes
-- ============================================================================

-- DIM_DATE: Year/Month hierarchy for drill-down
CREATE NONCLUSTERED INDEX IX_DIM_DATE_HIERARCHY
    ON dw.DIM_DATE (year_num, quarter_num, month_num, date_key);
GO

-- DIM_CHIEF_COMPLAINT: Category browsing
CREATE NONCLUSTERED INDEX IX_DIM_COMPLAINT_CATEGORY
    ON dw.DIM_CHIEF_COMPLAINT (complaint_category, complaint_subcategory);
GO

-- DIM_SYMPTOM: Critical symptom filtering
CREATE NONCLUSTERED INDEX IX_DIM_SYMPTOM_CRITICAL
    ON dw.DIM_SYMPTOM (is_critical_symptom)
    WHERE is_critical_symptom = 1;
GO

-- DIM_PROVIDER_ORGANIZATION: 911 service filtering
CREATE NONCLUSTERED INDEX IX_DIM_PROVIDER_911
    ON dw.DIM_PROVIDER_ORGANIZATION (is_911_service, _is_current)
    WHERE is_911_service = 1 AND _is_current = 1;
GO

-- DIM_SERVICE_LEVEL: Scope tier ranking
CREATE NONCLUSTERED INDEX IX_DIM_SERVICE_SCOPE
    ON dw.DIM_SERVICE_LEVEL (scope_tier DESC, service_level_name);
GO

-- ============================================================================
-- Statistics for Query Optimizer
-- ============================================================================
CREATE STATISTICS STAT_FACT_EMS_DATE_COUNTY
    ON dw.FACT_EMS_INCIDENT (date_key, county_key)
    WITH FULLSCAN;
GO

CREATE STATISTICS STAT_FACT_EMS_DATE_PROVIDER
    ON dw.FACT_EMS_INCIDENT (date_key, provider_org_key)
    WITH FULLSCAN;
GO

CREATE STATISTICS STAT_FACT_EMS_NALOXONE_DATE
    ON dw.FACT_EMS_INCIDENT (naloxone_given_flg, date_key)
    WITH FULLSCAN;
GO

PRINT '✅ Indexes and statistics created successfully';
GO
