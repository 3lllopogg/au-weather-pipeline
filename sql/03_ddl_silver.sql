-- ==============================================================
-- SILVER LAYER TABLES
-- Transformation layer
-- ==============================================================

USE AUWeatherPipeline;
GO

-- ==============================================================
-- DAILY WEATHER derived from flattened JSON in bronze.weather_raw
-- GRAIN: 1 row per city per day
-- ==============================================================

IF OBJECT_ID('silver.weather_daily', 'U') IS NOT NULL
    DROP TABLE silver.weather_daily;
GO

CREATE TABLE silver.weather_daily (
	city_name            VARCHAR(50) NOT NULL,
	date                 DATE NOT NULL,
	temp_max             FLOAT,
	temp_min             FLOAT,
	precipitation_sum    FLOAT,
	precipitation_hours  FLOAT,
	windspeed_max        FLOAT,
	weathercode          INT,
	weather_type         VARCHAR(50),
	sunshine_hours       FLOAT,
	batch_id             UNIQUEIDENTIFIER,

    -- Composite PK
	CONSTRAINT PK_weather PRIMARY KEY (city_name, date),

    -- Validity check on temperature
    CONSTRAINT CHK_temp_valid CHECK (temp_max >= temp_min)
);

-- ==============================================================
-- CITY METADATA derived from bronze.city_metadata_raw (latest batch)
-- GRAIN: 1 row per city
-- ==============================================================

IF OBJECT_ID('silver.city_metadata', 'U') IS NOT NULL
    DROP TABLE silver.city_metadata;
GO

CREATE TABLE silver.city_metadata (
    city_id                  INT UNIQUE NOT NULL,        
    city_name                NVARCHAR(50) NOT NULL,       
    state                    NVARCHAR(5),        
    latitude                 FLOAT,        
    longitude                FLOAT,        
    timezone_iana            NVARCHAR(100),       
    population_2021          INT,        
    area_km2                 FLOAT,        
    avg_house_price          FLOAT,
    median_weekly_rent       FLOAT,        
    cost_of_living_score     INT,       
    climate_zone             NVARCHAR(100),       
    beach_access             INT,
    avg_annual_sunshine_hrs  FLOAT,
    walkability_score        INT,
    public_transport_score   INT,
    avg_annual_rainy_days    FLOAT,
    colloquial_name          NVARCHAR(100),
	batch_id                 UNIQUEIDENTIFIER,

    -- PK on city_id
	CONSTRAINT PK_city_metadata PRIMARY KEY(city_id),

    -- Check to enforce grain
	CONSTRAINT UQ_city_name UNIQUE(city_name)
);
GO