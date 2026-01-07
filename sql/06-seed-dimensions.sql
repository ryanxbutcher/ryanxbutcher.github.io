-- ============================================================================
-- EMS Data Warehouse - Dimension Seeding
-- Author: J. Ryan Butcher
-- Purpose: Seed unknown members and pre-populate date/time dimensions
-- ============================================================================

-- ============================================================================
-- UNKNOWN MEMBERS (Surrogate Key = -1)
-- Used when source data is NULL or unmatchable
-- ============================================================================

SET IDENTITY_INSERT dw.DIM_COUNTY ON;
INSERT INTO dw.DIM_COUNTY (county_key, county_name, county_name_clean, state_code, state_name, region_name, urban_rural_class)
VALUES (-1, 'Unknown', 'Unknown', 'XX', 'Unknown', 'Unknown', 'Unknown');
SET IDENTITY_INSERT dw.DIM_COUNTY OFF;

SET IDENTITY_INSERT dw.DIM_CHIEF_COMPLAINT ON;
INSERT INTO dw.DIM_CHIEF_COMPLAINT (chief_complaint_key, chief_complaint_code, chief_complaint_name, complaint_category, severity_tier)
VALUES (-1, 'UNK', 'Unknown/Not Recorded', 'Unknown', 'Unknown');
SET IDENTITY_INSERT dw.DIM_CHIEF_COMPLAINT OFF;

SET IDENTITY_INSERT dw.DIM_ANATOMIC_LOCATION ON;
INSERT INTO dw.DIM_ANATOMIC_LOCATION (anatomic_location_key, anatomic_location_name, body_region, body_system)
VALUES (-1, 'Unknown', 'Unknown', 'Unknown');
SET IDENTITY_INSERT dw.DIM_ANATOMIC_LOCATION OFF;

SET IDENTITY_INSERT dw.DIM_SYMPTOM ON;
INSERT INTO dw.DIM_SYMPTOM (symptom_key, symptom_name, symptom_category, symptom_system, is_critical_symptom)
VALUES (-1, 'Unknown/Not Recorded', 'Unknown', 'Unknown', 0);
SET IDENTITY_INSERT dw.DIM_SYMPTOM OFF;

SET IDENTITY_INSERT dw.DIM_PROVIDER_IMPRESSION ON;
INSERT INTO dw.DIM_PROVIDER_IMPRESSION (provider_impression_key, impression_name, impression_category, impression_acuity)
VALUES (-1, 'Unknown/Not Recorded', 'Unknown', 'Unknown');
SET IDENTITY_INSERT dw.DIM_PROVIDER_IMPRESSION OFF;

SET IDENTITY_INSERT dw.DIM_DISPOSITION ON;
INSERT INTO dw.DIM_DISPOSITION (disposition_key, disposition_name, disposition_category, disposition_context)
VALUES (-1, 'Unknown/Not Recorded', 'Unknown', 'Both');
SET IDENTITY_INSERT dw.DIM_DISPOSITION OFF;

SET IDENTITY_INSERT dw.DIM_DESTINATION_TYPE ON;
INSERT INTO dw.DIM_DESTINATION_TYPE (destination_type_key, destination_type_name, destination_category, is_acute_care)
VALUES (-1, 'Unknown/Not Recorded', 'Unknown', 0);
SET IDENTITY_INSERT dw.DIM_DESTINATION_TYPE OFF;

SET IDENTITY_INSERT dw.DIM_PROVIDER_ORGANIZATION ON;
INSERT INTO dw.DIM_PROVIDER_ORGANIZATION (provider_org_key, provider_structure, provider_service, organization_category, is_911_service, is_transport_capable)
VALUES (-1, 'Unknown', 'Unknown', 'Unknown', 0, 0);
SET IDENTITY_INSERT dw.DIM_PROVIDER_ORGANIZATION OFF;

SET IDENTITY_INSERT dw.DIM_SERVICE_LEVEL ON;
INSERT INTO dw.DIM_SERVICE_LEVEL (service_level_key, service_level_name, certification_level, scope_tier)
VALUES (-1, 'Unknown/Not Recorded', 'Unknown', 0);
SET IDENTITY_INSERT dw.DIM_SERVICE_LEVEL OFF;

PRINT '✅ Unknown members seeded for all dimensions';
GO

-- ============================================================================
-- DIM_DATE: Pre-populate calendar from 2014-01-01 to 2030-12-31
-- ============================================================================
DECLARE @start_date DATE = '2014-01-01';
DECLARE @end_date DATE = '2030-12-31';
DECLARE @current_date DATE = @start_date;

-- Insert unknown date
INSERT INTO dw.DIM_DATE (date_key, date_value, day_of_week, day_of_week_num, day_of_month,
    day_of_year, week_of_year, month_num, month_name, month_abbrev, quarter_num, quarter_name, year_num)
VALUES (-1, '1900-01-01', 'Unknown', 0, 0, 0, 0, 0, 'Unknown', 'UNK', 0, 'Unknown', 1900);

WHILE @current_date <= @end_date
BEGIN
    INSERT INTO dw.DIM_DATE (
        date_key
        ,date_value
        ,day_of_week
        ,day_of_week_num
        ,day_of_month
        ,day_of_year
        ,week_of_year
        ,month_num
        ,month_name
        ,month_abbrev
        ,quarter_num
        ,quarter_name
        ,year_num
        ,is_weekend
        ,is_holiday
    )
    VALUES (
        CONVERT(INT, FORMAT(@current_date, 'yyyyMMdd'))
        ,@current_date
        ,DATENAME(WEEKDAY, @current_date)
        ,DATEPART(WEEKDAY, @current_date)
        ,DAY(@current_date)
        ,DATEPART(DAYOFYEAR, @current_date)
        ,DATEPART(WEEK, @current_date)
        ,MONTH(@current_date)
        ,DATENAME(MONTH, @current_date)
        ,LEFT(DATENAME(MONTH, @current_date), 3)
        ,DATEPART(QUARTER, @current_date)
        ,'Q' + CAST(DATEPART(QUARTER, @current_date) AS VARCHAR)
        ,YEAR(@current_date)
        ,CASE WHEN DATEPART(WEEKDAY, @current_date) IN (1, 7) THEN 1 ELSE 0 END
        ,0
    );

    SET @current_date = DATEADD(DAY, 1, @current_date);
END;

PRINT '✅ Date dimension populated from 2014 to 2030';
GO

-- ============================================================================
-- DIM_TIME_OF_DAY: Pre-populate all 1440 minutes of the day
-- ============================================================================

-- Insert unknown time
INSERT INTO dw.DIM_TIME_OF_DAY (time_of_day_key, time_value, hour_24, hour_12, minute_of_hour,
    am_pm, time_period, shift_name, display_12hr, display_24hr)
VALUES (-1, '00:00:00', 0, 12, 0, 'AM', 'Unknown', 'Unknown', 'Unknown', 'Unknown');

DECLARE @minute INT = 0;
WHILE @minute < 1440
BEGIN
    DECLARE @time TIME = DATEADD(MINUTE, @minute, '00:00:00');
    DECLARE @hour24 INT = @minute / 60;
    DECLARE @hour12 INT = CASE WHEN @hour24 = 0 THEN 12 WHEN @hour24 > 12 THEN @hour24 - 12 ELSE @hour24 END;
    DECLARE @min INT = @minute % 60;
    DECLARE @ampm VARCHAR(2) = CASE WHEN @hour24 < 12 THEN 'AM' ELSE 'PM' END;
    DECLARE @period VARCHAR(20) = CASE
        WHEN @hour24 >= 0 AND @hour24 < 5 THEN 'Late Night'
        WHEN @hour24 >= 5 AND @hour24 < 8 THEN 'Early Morning'
        WHEN @hour24 >= 8 AND @hour24 < 12 THEN 'Morning'
        WHEN @hour24 >= 12 AND @hour24 < 17 THEN 'Afternoon'
        WHEN @hour24 >= 17 AND @hour24 < 21 THEN 'Evening'
        ELSE 'Night'
    END;
    DECLARE @shift VARCHAR(20) = CASE
        WHEN @hour24 >= 7 AND @hour24 < 15 THEN 'Day Shift'
        WHEN @hour24 >= 15 AND @hour24 < 23 THEN 'Swing Shift'
        ELSE 'Night Shift'
    END;

    INSERT INTO dw.DIM_TIME_OF_DAY (
        time_of_day_key, time_value, hour_24, hour_12, minute_of_hour,
        am_pm, time_period, shift_name, display_12hr, display_24hr
    )
    VALUES (
        @minute, @time, @hour24, @hour12, @min,
        @ampm, @period, @shift,
        FORMAT(@time, 'h:mm tt'), FORMAT(@time, 'HH:mm:ss')
    );

    SET @minute = @minute + 1;
END;

PRINT '✅ Time of day dimension populated (1440 minutes)';
GO

PRINT '🎉 All dimension seeding complete!';
GO
