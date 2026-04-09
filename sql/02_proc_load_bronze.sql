-- ===============================================================
-- STORED PROCEDURE to POPULATE bronze.city_metadata_raw TABLE 
-- CSV --> #temp_staging_table --> bronze.city_metadata_raw
-- ===============================================================

USE AUWeatherPipeline;
GO

CREATE OR ALTER PROCEDURE bronze.sp_load_city_metadata 
@batch_id UNIQUEIDENTIFIER
AS
BEGIN
	SET NOCOUNT ON;

	BEGIN TRY

	-- ====================================
    -- Create temp staging table
	-- ====================================

	IF OBJECT_ID('tempdb..#city_stage') IS NOT NULL
		DROP TABLE #city_stage

	CREATE TABLE #city_stage (
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

    -- ====================================
    -- Load CSV into staging table
	-- ====================================
    PRINT 'Loading CSV into staging table...';
    PRINT '';

    BULK INSERT #city_stage
    FROM 'C:\Projects\open_meteo\data\city_metadata.csv'
    -- NOTE: file must exist in location accessible to SQL Server 
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );

    -- Row count check (expected result = 8 for 8 cities used)
    DECLARE @row_count INT;
    SELECT @row_count = COUNT(*)
    FROM #city_stage;
    PRINT CONCAT('Rows loaded into staging: ', @row_count);
    PRINT '';

    -- ====================================
    -- Load into bronze layer with batch_id
	-- ====================================
    PRINT 'Inserting into bronze.city_metadata_raw...';
    PRINT '';

    INSERT INTO bronze.city_metadata_raw (
        batch_id,
        city_id,                         
        city_name,                      
        state,                          
        latitude,                      
        longitude,        
        timezone,       
        population_2021,        
        area_km2,        
        avg_house_price_aud,        
        median_weekly_rent_aud,        
        cost_of_living_index,       
        climate_zone,       
        beach_access,
        avg_annual_sunshine_hrs,
        walkability_score,
        public_transport_score,
        avg_annual_rainy_days,
        colloquial_name
    ) 
    SELECT
        @batch_id,
        city_id,                         
        city_name,                      
        state,                          
        latitude,                      
        longitude,        
        timezone,       
        population_2021,        
        area_km2,        
        avg_house_price_aud,        
        median_weekly_rent_aud,        
        cost_of_living_index,       
        climate_zone,       
        beach_access,
        avg_annual_sunshine_hrs,
        walkability_score,
        public_transport_score,
        avg_annual_rainy_days,
        colloquial_name
    FROM #city_stage

    PRINT 'Insert into bronze layer successful.';
	END TRY

    BEGIN CATCH
        DECLARE @error_message NVARCHAR(MAX) = ERROR_MESSAGE();

        RAISERROR(@error_message, 16, 1);
    END CATCH

END;
GO