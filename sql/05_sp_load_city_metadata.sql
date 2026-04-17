-- ===============================================================
-- STORED PROCEDURE TO POPULATE CITY METADATA TABLE IN SILVER LAYER
-- BRONZE --> APPLY TRANSFORMATIONS & TYPE CASTING --> SILVER
-- ===============================================================

USE AUWeatherPipeline;
GO

CREATE OR ALTER PROCEDURE silver.sp_load_city_metadata
AS 
BEGIN
	SET NOCOUNT ON

	DECLARE @row_count INT;
	DECLARE @latest_batch UNIQUEIDENTIFIER;

	BEGIN TRY
    
		-- ====================================
		-- Filter for latest successful batch
		-- ====================================
		SELECT TOP 1 @latest_batch = batch_id 
		FROM bronze.ingestion_log 
		WHERE status = 'SUCCESS' 
		ORDER BY end_time DESC;

		-- ====================================
		-- Ensure idempotency
		-- ====================================
		TRUNCATE TABLE silver.city_metadata;

		-- ====================================
		-- Insert transformed city metadata
		-- ====================================
		INSERT INTO silver.city_metadata (
			city_id,        
			city_name,       
			state,        
			latitude,        
			longitude,        
			timezone_iana,                        
			population_2021,                  
			area_km2,                         
			avg_house_price,              
			median_weekly_rent,           
			cost_of_living_score,            
			climate_zone,                    
			beach_access,             
			avg_annual_sunshine_hrs,  
			walkability_score,        
			public_transport_score,   
			avg_annual_rainy_days,    
			colloquial_name,          
			batch_id
		)
		SELECT 
			TRY_CAST(city_id AS INT) city_id,
			CASE
				WHEN TRIM(city_name) LIKE '%uurne' THEN 'Melbourne'
				ELSE UPPER(LEFT(TRIM(city_name), 1)) + LOWER(SUBSTRING(TRIM(city_name), 2, LEN(city_name)))
			END city_name,
			TRIM(UPPER(REPLACE(state, '.', ''))) state,
			TRY_CAST(latitude AS FLOAT) latitude,
			TRY_CAST(longitude AS FLOAT) longitude,
			TRIM(timezone) timezone_iana,
			TRY_CAST(population_2021 AS INT) population_2021,
			TRY_CAST(area_km2 AS FLOAT) area_km2,
			TRY_CAST(REPLACE(avg_house_price_aud, '$', '') AS FLOAT) avg_house_price,
			TRY_CAST(median_weekly_rent_aud AS FLOAT) median_weekly_rent, 
			TRY_CAST(cost_of_living_index AS INT) cost_of_living_score,
			LOWER(LTRIM(RTRIM(climate_zone))) climate_zone,
			TRY_CAST(CASE
				WHEN LOWER(TRIM(beach_access)) IN ('yes','y','true','1') THEN 1
				ELSE 0
			END AS INT) beach_access, 
			TRY_CAST(avg_annual_sunshine_hrs AS FLOAT) avg_annual_sunshine_hrs,
			TRY_CAST(walkability_score AS INT) walkability_score,
			TRY_CAST(public_transport_score AS INT) public_transport_score,
			TRY_CAST(avg_annual_rainy_days AS FLOAT) avg_annual_rainy_days,
			NULLIF(TRIM(REPLACE(colloquial_name, '_', ' ')), '') colloquial_name,
			batch_id

		FROM bronze.city_metadata_raw
		WHERE 
			batch_id = @latest_batch;

	    -- ====================================
		-- Row count logging
		-- ====================================
		SET @row_count = @@ROWCOUNT;

		PRINT 'Rows inserted into silver.city_metadata: ' + CAST(@row_count AS VARCHAR);

		-- ====================================
		-- Validation check
		-- ====================================
		SELECT COUNT(*) AS total_rows FROM silver.city_metadata;

		PRINT 'Silver city metadata load completed successfully.';

	END TRY

	BEGIN CATCH

		PRINT 'Error in silver.sp_load_city_metadata';
		PRINT ERROR_MESSAGE();

	END CATCH

END;
GO