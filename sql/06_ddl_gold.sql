-- ==============================================================
-- GOLD LAYER TABLES
-- STAR schema - 1 fact & 2 dimension tables
-- ==============================================================

USE AUWeatherPipeline;
GO

-- ==============================================================
-- DROP TABLES (in dependacy order)
-- ==============================================================

IF OBJECT_ID('gold.fact_weather', 'U') IS NOT NULL
    DROP TABLE gold.fact_weather;
GO

IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL
    DROP TABLE gold.dim_date;
GO

IF OBJECT_ID('gold.dim_city', 'U') IS NOT NULL
    DROP TABLE gold.dim_city;
GO
-- ==============================================================
-- Date Dimension Table
-- ==============================================================

CREATE TABLE gold.dim_date (
    date_key                 INT NOT NULL PRIMARY KEY, -- YYYYMMDD
    date                     DATE NOT NULL,
	year                     INT NOT NULL,
	month                    INT NOT NULL,
	day                      INT NOT NULL,
	week_of_year             INT NOT NULL,
	month_name               VARCHAR(20) NOT NULL,
    day_name                 VARCHAR(20) NOT NULL,
	quarter                  INT NOT NULL,
    season                   VARCHAR(20) NOT NULL
);

-- ==============================================================
-- City Dimension Table
-- ==============================================================

CREATE TABLE gold.dim_city (
    city_key                 INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate key
    city_id                  INT UNIQUE NOT NULL,        
    city_name                NVARCHAR(50) NOT NULL,       
    state                    NVARCHAR(5) NOT NULL,        
    latitude                 FLOAT,        
    longitude                FLOAT,        
    timezone_iana            NVARCHAR(100),       
    population               INT,        
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
    colloquial_name          NVARCHAR(100)
);

-- ==============================================================
-- Weather Fact Table
-- ==============================================================

CREATE TABLE gold.fact_weather (
    date_key                 INT NOT NULL,
    city_key                 INT NOT NULL,
	temp_max                 FLOAT,
	temp_min                 FLOAT,
	precipitation_sum        FLOAT,
	precipitation_hours      FLOAT,
	windspeed_max            FLOAT,
    sunshine_hours           FLOAT,
	weathercode              INT,
    weather_type             NVARCHAR(50),

    -- Composite primary key
    CONSTRAINT PK_fact_weather PRIMARY KEY (date_key, city_key),

    -- Foreign keys
	CONSTRAINT PK_fact_date FOREIGN KEY (date_key)
        REFERENCES gold.dim_date(date_key),

    CONSTRAINT PK_fact_city FOREIGN KEY (city_key)
        REFERENCES gold.dim_city(city_key),

    -- Unique constraint to enforce the 1 row per city per day grain
    CONSTRAINT UQ_fact_weather UNIQUE (date_key, city_key)
);

-- ==============================================================
-- Indexes
-- ==============================================================

-- Index on date_key to speed up time based queries
CREATE NONCLUSTERED INDEX IX_fact_weather_date
ON gold.fact_weather (date_key);

-- Index on city_key to speed up city based queries
CREATE NONCLUSTERED INDEX IX_fact_weather_city
ON gold.fact_weather (city_key);