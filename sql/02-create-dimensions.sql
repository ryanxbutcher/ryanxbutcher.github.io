-- ============================================================================
-- EMS Data Warehouse - Dimension Tables
-- Author: J. Ryan Butcher
-- Purpose: Kimball-style star schema dimensions for EMS operational analytics
-- ============================================================================

-- Create DW schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dw')
    EXEC('CREATE SCHEMA dw');
GO

-- ============================================================================
-- DIM_DATE: Conformed date dimension
-- SCD Type: 0 (Fixed - pre-populated calendar)
-- ============================================================================
CREATE TABLE dw.DIM_DATE (
    date_key                INT PRIMARY KEY             -- YYYYMMDD format
    ,date_value             DATE NOT NULL
    ,day_of_week            VARCHAR(10)                 -- Monday, Tuesday, etc.
    ,day_of_week_num        TINYINT                     -- 1=Sunday, 7=Saturday
    ,day_of_month           TINYINT
    ,day_of_year            SMALLINT
    ,week_of_year           TINYINT
    ,month_num              TINYINT
    ,month_name             VARCHAR(15)
    ,month_abbrev           VARCHAR(3)
    ,quarter_num            TINYINT
    ,quarter_name           VARCHAR(10)
    ,year_num               SMALLINT
    ,is_weekend             BIT DEFAULT 0
    ,is_holiday             BIT DEFAULT 0
    ,holiday_name           VARCHAR(50)
);
GO

-- ============================================================================
-- DIM_TIME_OF_DAY: Time dimension for dispatch/arrival analysis
-- SCD Type: 0 (Fixed - static time periods)
-- ============================================================================
CREATE TABLE dw.DIM_TIME_OF_DAY (
    time_of_day_key         INT PRIMARY KEY             -- Minutes from midnight (0-1439)
    ,time_value             TIME
    ,hour_24                TINYINT
    ,hour_12                TINYINT
    ,minute_of_hour         TINYINT
    ,am_pm                  VARCHAR(2)
    ,time_period            VARCHAR(20)                 -- Early Morning, Morning, etc.
    ,shift_name             VARCHAR(20)                 -- Day, Swing, Night
    ,display_12hr           VARCHAR(10)
    ,display_24hr           VARCHAR(8)
);
GO

-- ============================================================================
-- DIM_COUNTY: Geographic dimension
-- SCD Type: 2 (Counties may rename/reclassify)
-- ============================================================================
CREATE TABLE dw.DIM_COUNTY (
    county_key              BIGINT IDENTITY(1,1) PRIMARY KEY
    ,county_name            VARCHAR(100) NOT NULL
    ,county_name_clean      VARCHAR(100)
    ,state_code             VARCHAR(2) DEFAULT 'IN'
    ,state_name             VARCHAR(50) DEFAULT 'Indiana'
    ,region_name            VARCHAR(50)
    ,urban_rural_class      VARCHAR(20)
    -- SCD Type 2 columns
    ,_effective_dt          DATE DEFAULT '1900-01-01'
    ,_expiration_dt         DATE DEFAULT '9999-12-31'
    ,_is_current            BIT DEFAULT 1
    ,_row_hash              VARBINARY(32)
);
GO

CREATE NONCLUSTERED INDEX IX_DIM_COUNTY_CURRENT
    ON dw.DIM_COUNTY (county_name, _is_current) WHERE _is_current = 1;
GO

-- ============================================================================
-- DIM_CHIEF_COMPLAINT: Dispatch complaint categorization
-- SCD Type: 1 (Category refinements apply retroactively)
-- ============================================================================
CREATE TABLE dw.DIM_CHIEF_COMPLAINT (
    chief_complaint_key     BIGINT IDENTITY(1,1) PRIMARY KEY
    ,chief_complaint_code   VARCHAR(100)                -- Natural key
    ,chief_complaint_name   VARCHAR(500) NOT NULL
    ,complaint_category     VARCHAR(100)
    ,complaint_subcategory  VARCHAR(100)
    ,severity_tier          VARCHAR(20)
    ,is_time_sensitive      BIT DEFAULT 0
    ,_row_created_dt        DATETIME2 DEFAULT GETDATE()
    ,_row_updated_dt        DATETIME2 DEFAULT GETDATE()
);
GO

CREATE NONCLUSTERED INDEX IX_DIM_CHIEF_COMPLAINT_NAME
    ON dw.DIM_CHIEF_COMPLAINT (chief_complaint_name);
GO

-- ============================================================================
-- DIM_ANATOMIC_LOCATION: Body location dimension
-- SCD Type: 1 (Reference data)
-- ============================================================================
CREATE TABLE dw.DIM_ANATOMIC_LOCATION (
    anatomic_location_key   BIGINT IDENTITY(1,1) PRIMARY KEY
    ,anatomic_location_name VARCHAR(200) NOT NULL
    ,body_region            VARCHAR(50)
    ,body_system            VARCHAR(50)
    ,_row_created_dt        DATETIME2 DEFAULT GETDATE()
    ,_row_updated_dt        DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- DIM_SYMPTOM: Primary symptom dimension
-- SCD Type: 1 (Reference data standardization)
-- ============================================================================
CREATE TABLE dw.DIM_SYMPTOM (
    symptom_key             BIGINT IDENTITY(1,1) PRIMARY KEY
    ,symptom_name           VARCHAR(500) NOT NULL
    ,symptom_category       VARCHAR(100)
    ,symptom_system         VARCHAR(100)
    ,is_critical_symptom    BIT DEFAULT 0
    ,_row_created_dt        DATETIME2 DEFAULT GETDATE()
    ,_row_updated_dt        DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- DIM_PROVIDER_IMPRESSION: Clinical impression dimension
-- SCD Type: 1 (Coding updates apply uniformly)
-- ============================================================================
CREATE TABLE dw.DIM_PROVIDER_IMPRESSION (
    provider_impression_key BIGINT IDENTITY(1,1) PRIMARY KEY
    ,impression_name        VARCHAR(500) NOT NULL
    ,impression_category    VARCHAR(100)
    ,impression_acuity      VARCHAR(20)
    ,_row_created_dt        DATETIME2 DEFAULT GETDATE()
    ,_row_updated_dt        DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- DIM_DISPOSITION: Shared ED/Hospital disposition
-- SCD Type: 1 (Disposition codes are relatively stable)
-- ============================================================================
CREATE TABLE dw.DIM_DISPOSITION (
    disposition_key         BIGINT IDENTITY(1,1) PRIMARY KEY
    ,disposition_name       VARCHAR(500) NOT NULL
    ,disposition_category   VARCHAR(50)
    ,disposition_context    VARCHAR(20)                 -- ED, Hospital, Both
    ,is_admission           BIT DEFAULT 0
    ,is_transfer            BIT DEFAULT 0
    ,is_deceased            BIT DEFAULT 0
    ,_row_created_dt        DATETIME2 DEFAULT GETDATE()
    ,_row_updated_dt        DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- DIM_DESTINATION_TYPE: Transport destination dimension
-- SCD Type: 1 (Facility classifications)
-- ============================================================================
CREATE TABLE dw.DIM_DESTINATION_TYPE (
    destination_type_key    BIGINT IDENTITY(1,1) PRIMARY KEY
    ,destination_type_name  VARCHAR(200) NOT NULL
    ,destination_category   VARCHAR(50)
    ,is_acute_care          BIT DEFAULT 0
    ,_row_created_dt        DATETIME2 DEFAULT GETDATE()
    ,_row_updated_dt        DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- DIM_PROVIDER_ORGANIZATION: EMS provider structure dimension
-- SCD Type: 2 (Organizations may restructure)
-- ============================================================================
CREATE TABLE dw.DIM_PROVIDER_ORGANIZATION (
    provider_org_key        BIGINT IDENTITY(1,1) PRIMARY KEY
    ,provider_structure     VARCHAR(200) NOT NULL
    ,provider_service       VARCHAR(500)
    ,organization_category  VARCHAR(50)
    ,is_911_service         BIT DEFAULT 0
    ,is_transport_capable   BIT DEFAULT 0
    -- SCD Type 2 columns
    ,_effective_dt          DATE DEFAULT '1900-01-01'
    ,_expiration_dt         DATE DEFAULT '9999-12-31'
    ,_is_current            BIT DEFAULT 1
    ,_row_hash              VARBINARY(32)
);
GO

CREATE NONCLUSTERED INDEX IX_DIM_PROVIDER_ORG_CURRENT
    ON dw.DIM_PROVIDER_ORGANIZATION (provider_structure, _is_current)
    WHERE _is_current = 1;
GO

-- ============================================================================
-- DIM_SERVICE_LEVEL: EMT certification level dimension
-- SCD Type: 1 (Certification standards)
-- ============================================================================
CREATE TABLE dw.DIM_SERVICE_LEVEL (
    service_level_key       BIGINT IDENTITY(1,1) PRIMARY KEY
    ,service_level_name     VARCHAR(200) NOT NULL
    ,certification_level    VARCHAR(50)
    ,scope_tier             TINYINT                     -- 1=Basic, 2=Int, 3=Adv, 4=CC
    ,can_administer_iv      BIT DEFAULT 0
    ,can_administer_meds    BIT DEFAULT 0
    ,can_intubate           BIT DEFAULT 0
    ,_row_created_dt        DATETIME2 DEFAULT GETDATE()
    ,_row_updated_dt        DATETIME2 DEFAULT GETDATE()
);
GO

PRINT '✅ All dimension tables created successfully';
GO
