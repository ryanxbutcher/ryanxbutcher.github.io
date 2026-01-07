-- ============================================================================
-- EMS Data Warehouse - Fact Tables
-- Author: J. Ryan Butcher
-- Purpose: Kimball-style fact table for EMS operational analytics
-- ============================================================================

-- ============================================================================
-- FACT_EMS_INCIDENT: Central fact table for EMS incidents
-- Grain: One row per EMS incident/run
-- ============================================================================
CREATE TABLE dw.FACT_EMS_INCIDENT (
    -- Primary Key (surrogate)
    ems_incident_key            BIGINT IDENTITY(1,1) PRIMARY KEY

    -- Foreign Keys to Dimensions
    ,date_key                   INT NOT NULL                    -- FK to DIM_DATE
    ,time_of_day_key            INT NOT NULL                    -- FK to DIM_TIME_OF_DAY
    ,county_key                 BIGINT NOT NULL                 -- FK to DIM_COUNTY
    ,chief_complaint_key        BIGINT NOT NULL                 -- FK to DIM_CHIEF_COMPLAINT
    ,anatomic_location_key      BIGINT NOT NULL                 -- FK to DIM_ANATOMIC_LOCATION
    ,symptom_key                BIGINT NOT NULL                 -- FK to DIM_SYMPTOM
    ,provider_impression_key    BIGINT NOT NULL                 -- FK to DIM_PROVIDER_IMPRESSION
    ,disposition_ed_key         BIGINT NOT NULL                 -- FK to DIM_DISPOSITION (ED)
    ,disposition_hospital_key   BIGINT NOT NULL                 -- FK to DIM_DISPOSITION (Hospital)
    ,destination_type_key       BIGINT NOT NULL                 -- FK to DIM_DESTINATION_TYPE
    ,provider_org_key           BIGINT NOT NULL                 -- FK to DIM_PROVIDER_ORGANIZATION
    ,service_level_key          BIGINT NOT NULL                 -- FK to DIM_SERVICE_LEVEL

    -- Measures (Additive)
    ,provider_to_scene_mins     DECIMAL(10,2)                   -- Response time to scene
    ,provider_to_dest_mins      DECIMAL(10,2)                   -- Transport time
    ,dispatch_to_arrival_mins   DECIMAL(10,2)                   -- Calculated: arrival - dispatch
    ,arrival_to_patient_mins    DECIMAL(10,2)                   -- Calculated: patient contact - arrival
    ,scene_time_mins            DECIMAL(10,2)                   -- Calculated: left scene - arrived
    ,total_call_time_mins       DECIMAL(10,2)                   -- Calculated: dest arrival - dispatch

    -- Flags (Semi-Additive - sum for counts)
    ,injury_flg                 BIT DEFAULT 0
    ,naloxone_given_flg         BIT DEFAULT 0
    ,medication_given_flg       BIT DEFAULT 0
    ,incident_count             TINYINT DEFAULT 1               -- Always 1, for easy counting

    -- Event Timestamps (For detailed drill-down)
    ,unit_notified_dt           DATETIME2
    ,unit_arrived_scene_dt      DATETIME2
    ,unit_arrived_patient_dt    DATETIME2
    ,unit_left_scene_dt         DATETIME2
    ,patient_arrived_dest_dt    DATETIME2

    -- Audit Columns
    ,_source_file               VARCHAR(500)
    ,_source_row_num            INT
    ,_row_created_dt            DATETIME2 DEFAULT GETDATE()
    ,_row_updated_dt            DATETIME2 DEFAULT GETDATE()
    ,_is_deleted                BIT DEFAULT 0
);
GO

-- ============================================================================
-- Indexes for Common Query Patterns
-- ============================================================================

-- Date-based queries (most common analytical filter)
CREATE NONCLUSTERED INDEX IX_FACT_EMS_DATE
    ON dw.FACT_EMS_INCIDENT (date_key)
    INCLUDE (county_key, provider_org_key, injury_flg, naloxone_given_flg, incident_count);
GO

-- County analysis
CREATE NONCLUSTERED INDEX IX_FACT_EMS_COUNTY
    ON dw.FACT_EMS_INCIDENT (county_key, date_key)
    INCLUDE (provider_to_scene_mins, provider_to_dest_mins, incident_count);
GO

-- Provider performance
CREATE NONCLUSTERED INDEX IX_FACT_EMS_PROVIDER
    ON dw.FACT_EMS_INCIDENT (provider_org_key, date_key)
    INCLUDE (provider_to_scene_mins, service_level_key, incident_count);
GO

-- Naloxone/Opioid analysis (public health focus)
CREATE NONCLUSTERED INDEX IX_FACT_EMS_NALOXONE
    ON dw.FACT_EMS_INCIDENT (naloxone_given_flg, date_key)
    WHERE naloxone_given_flg = 1;
GO

-- Injury analysis
CREATE NONCLUSTERED INDEX IX_FACT_EMS_INJURY
    ON dw.FACT_EMS_INCIDENT (injury_flg, date_key, anatomic_location_key)
    WHERE injury_flg = 1;
GO

-- ============================================================================
-- Foreign Key Constraints
-- ============================================================================
ALTER TABLE dw.FACT_EMS_INCIDENT
    ADD CONSTRAINT FK_FACT_EMS_DATE
    FOREIGN KEY (date_key) REFERENCES dw.DIM_DATE(date_key);

ALTER TABLE dw.FACT_EMS_INCIDENT
    ADD CONSTRAINT FK_FACT_EMS_TIME
    FOREIGN KEY (time_of_day_key) REFERENCES dw.DIM_TIME_OF_DAY(time_of_day_key);

ALTER TABLE dw.FACT_EMS_INCIDENT
    ADD CONSTRAINT FK_FACT_EMS_COUNTY
    FOREIGN KEY (county_key) REFERENCES dw.DIM_COUNTY(county_key);

ALTER TABLE dw.FACT_EMS_INCIDENT
    ADD CONSTRAINT FK_FACT_EMS_COMPLAINT
    FOREIGN KEY (chief_complaint_key) REFERENCES dw.DIM_CHIEF_COMPLAINT(chief_complaint_key);

ALTER TABLE dw.FACT_EMS_INCIDENT
    ADD CONSTRAINT FK_FACT_EMS_ANATOMIC
    FOREIGN KEY (anatomic_location_key) REFERENCES dw.DIM_ANATOMIC_LOCATION(anatomic_location_key);

ALTER TABLE dw.FACT_EMS_INCIDENT
    ADD CONSTRAINT FK_FACT_EMS_SYMPTOM
    FOREIGN KEY (symptom_key) REFERENCES dw.DIM_SYMPTOM(symptom_key);

ALTER TABLE dw.FACT_EMS_INCIDENT
    ADD CONSTRAINT FK_FACT_EMS_IMPRESSION
    FOREIGN KEY (provider_impression_key) REFERENCES dw.DIM_PROVIDER_IMPRESSION(provider_impression_key);

ALTER TABLE dw.FACT_EMS_INCIDENT
    ADD CONSTRAINT FK_FACT_EMS_DISP_ED
    FOREIGN KEY (disposition_ed_key) REFERENCES dw.DIM_DISPOSITION(disposition_key);

ALTER TABLE dw.FACT_EMS_INCIDENT
    ADD CONSTRAINT FK_FACT_EMS_DISP_HOSP
    FOREIGN KEY (disposition_hospital_key) REFERENCES dw.DIM_DISPOSITION(disposition_key);

ALTER TABLE dw.FACT_EMS_INCIDENT
    ADD CONSTRAINT FK_FACT_EMS_DEST
    FOREIGN KEY (destination_type_key) REFERENCES dw.DIM_DESTINATION_TYPE(destination_type_key);

ALTER TABLE dw.FACT_EMS_INCIDENT
    ADD CONSTRAINT FK_FACT_EMS_PROVIDER
    FOREIGN KEY (provider_org_key) REFERENCES dw.DIM_PROVIDER_ORGANIZATION(provider_org_key);

ALTER TABLE dw.FACT_EMS_INCIDENT
    ADD CONSTRAINT FK_FACT_EMS_SERVICE
    FOREIGN KEY (service_level_key) REFERENCES dw.DIM_SERVICE_LEVEL(service_level_key);
GO

PRINT '✅ Fact table and indexes created successfully';
GO
