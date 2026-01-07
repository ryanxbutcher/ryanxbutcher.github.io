-- ============================================================================
-- EMS Data Warehouse - Staging Tables
-- Author: J. Ryan Butcher
-- Purpose: Raw data landing zone preserving source values for traceability
-- ============================================================================

-- Drop existing staging table if exists
IF OBJECT_ID('staging.STG_EMS_INCIDENT', 'U') IS NOT NULL
    DROP TABLE staging.STG_EMS_INCIDENT;
GO

-- Create staging schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
GO

-- ============================================================================
-- STG_EMS_INCIDENT: Raw EMS incident data from CSV
-- Grain: One row per EMS incident/run
-- All columns stored as VARCHAR to preserve raw values before transformation
-- ============================================================================
CREATE TABLE staging.STG_EMS_INCIDENT (
    -- Audit columns
    stg_load_id             BIGINT IDENTITY(1,1) PRIMARY KEY
    ,_load_datetime         DATETIME2 DEFAULT GETDATE()
    ,_source_file           VARCHAR(500)
    ,_source_row_num        INT

    -- Source columns (preserved as raw VARCHAR)
    ,INCIDENT_DT                        VARCHAR(50)
    ,INCIDENT_COUNTY                    VARCHAR(200)
    ,CHIEF_COMPLAINT_DISPATCH           VARCHAR(500)
    ,CHIEF_COMPLAINT_ANATOMIC_LOC       VARCHAR(200)
    ,PRIMARY_SYMPTOM                    VARCHAR(500)
    ,PROVIDER_IMPRESSION_PRIMARY        VARCHAR(500)
    ,DISPOSITION_ED                     VARCHAR(500)
    ,DISPOSITION_HOSPITAL               VARCHAR(500)
    ,INJURY_FLG                         VARCHAR(50)
    ,NALOXONE_GIVEN_FLG                 VARCHAR(50)
    ,MEDICATION_GIVEN_OTHER_FLG         VARCHAR(50)
    ,DESTINATION_TYPE                   VARCHAR(200)
    ,PROVIDER_TYPE_STRUCTURE            VARCHAR(200)
    ,PROVIDER_TYPE_SERVICE              VARCHAR(500)
    ,PROVIDER_TYPE_SERVICE_LEVEL        VARCHAR(200)
    ,PROVIDER_TO_SCENE_MINS             VARCHAR(50)
    ,PROVIDER_TO_DESTINATION_MINS       VARCHAR(50)
    ,UNIT_NOTIFIED_BY_DISPATCH_DT       VARCHAR(50)
    ,UNIT_ARRIVED_ON_SCENE_DT           VARCHAR(50)
    ,UNIT_ARRIVED_TO_PATIENT_DT         VARCHAR(50)
    ,UNIT_LEFT_SCENE_DT                 VARCHAR(50)
    ,PATIENT_ARRIVED_DESTINATION_DT     VARCHAR(50)
);
GO

-- Index for load tracking
CREATE NONCLUSTERED INDEX IX_STG_EMS_INCIDENT_LOAD
    ON staging.STG_EMS_INCIDENT (_load_datetime, _source_file);
GO

-- ============================================================================
-- SQLite Version (for portable demo)
-- ============================================================================
/*
-- SQLite equivalent:
CREATE TABLE IF NOT EXISTS STG_EMS_INCIDENT (
    stg_load_id             INTEGER PRIMARY KEY AUTOINCREMENT
    ,_load_datetime         TEXT DEFAULT (datetime('now'))
    ,_source_file           TEXT
    ,_source_row_num        INTEGER

    ,INCIDENT_DT                        TEXT
    ,INCIDENT_COUNTY                    TEXT
    ,CHIEF_COMPLAINT_DISPATCH           TEXT
    ,CHIEF_COMPLAINT_ANATOMIC_LOC       TEXT
    ,PRIMARY_SYMPTOM                    TEXT
    ,PROVIDER_IMPRESSION_PRIMARY        TEXT
    ,DISPOSITION_ED                     TEXT
    ,DISPOSITION_HOSPITAL               TEXT
    ,INJURY_FLG                         TEXT
    ,NALOXONE_GIVEN_FLG                 TEXT
    ,MEDICATION_GIVEN_OTHER_FLG         TEXT
    ,DESTINATION_TYPE                   TEXT
    ,PROVIDER_TYPE_STRUCTURE            TEXT
    ,PROVIDER_TYPE_SERVICE              TEXT
    ,PROVIDER_TYPE_SERVICE_LEVEL        TEXT
    ,PROVIDER_TO_SCENE_MINS             TEXT
    ,PROVIDER_TO_DESTINATION_MINS       TEXT
    ,UNIT_NOTIFIED_BY_DISPATCH_DT       TEXT
    ,UNIT_ARRIVED_ON_SCENE_DT           TEXT
    ,UNIT_ARRIVED_TO_PATIENT_DT         TEXT
    ,UNIT_LEFT_SCENE_DT                 TEXT
    ,PATIENT_ARRIVED_DESTINATION_DT     TEXT
);

CREATE INDEX IF NOT EXISTS IX_STG_EMS_INCIDENT_LOAD
    ON STG_EMS_INCIDENT (_load_datetime, _source_file);
*/
