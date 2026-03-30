-- ========================================================
-- BRONZE LAYER TABLES
-- Raw ingestion layer (no transformations)
-- ========================================================

USE AUWeatherPipeline;
GO

-- ========================================================
-- INGESTION LOG
-- Tracks each pipeline run (batch)
-- ========================================================

IF OBJECT_ID('bronze.ingestion_log', 'U') IS NOT NULL
    DROP TABLE bronze.ingestion_log;
GO

CREATE TABLE bronze.ingestion_log (
    batch_id UNIQUEIDENTIFIER PRIMARY KEY,
    start_time DATETIME2 NOT NULL,
    end_time DATETIME2 NULL,
    status VARCHAR(20) NOT NULL,  -- STARTED / SUCCESS / FAILED
    records_loaded INT DEFAULT 0,
    source VARCHAR(50),           -- 'open-meteo' / 'csv'
    error_message NVARCHAR(MAX) NULL
);
GO

-- ==================================================
-- WEATHER RAW (API JSON)
-- One row per API call per city
-- ==================================================

IF OBJECT_ID('bronze.weather_raw', 'U') IS NOT NULL
    DROP TABLE bronze.weather_raw;
GO

CREATE TABLE bronze.weather_raw (
    raw_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    ingestion_timestamp DATETIME2 DEFAULT GETUTCDATE(),
    batch_id UNIQUEIDENTIFIER NOT NULL,
    city_name VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    source VARCHAR(50) DEFAULT 'open-meteo',

    -- Full raw API response
    response_json NVARCHAR(MAX)
);
GO

-- ==================================================
-- CITY METADATA RAW (CSV INGESTION)
-- Stored as-is (all NVARCHAR for flexibility)
-- ==================================================

IF OBJECT_ID('bronze.city_metadata_raw', 'U') IS NOT NULL
    DROP TABLE bronze.city_metadata_raw;
GO

CREATE TABLE bronze.city_metadata_raw (
    raw_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    ingestion_timestamp DATETIME2 DEFAULT GETUTCDATE(),
    batch_id UNIQUEIDENTIFIER NOT NULL,

    -- Raw CSV columns — all strings
    city_id                  NVARCHAR(10),        
    city_name                NVARCHAR(100),       
    state                    NVARCHAR(50),        
    latitude                 NVARCHAR(30),        
    longitude                NVARCHAR(30),        
    timezone                 NVARCHAR(100),       
    population_2021          NVARCHAR(20),        
    area_km2                 NVARCHAR(20),        
    avg_house_price_aud      NVARCHAR(20),        
    median_weekly_rent_aud   NVARCHAR(20),        
    cost_of_living_index     NVARCHAR(10),       
    climate_zone             NVARCHAR(100),       
    beach_access             NVARCHAR(10),
    avg_annual_sunshine_hrs  NVARCHAR(20),
    walkability_score        NVARCHAR(10),
    public_transport_score   NVARCHAR(10),
    avg_annual_rainy_days    NVARCHAR(20),
    colloquial_name          NVARCHAR(100)
);
GO
