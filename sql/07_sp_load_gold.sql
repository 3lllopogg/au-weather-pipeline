-- ===============================================================
-- STORED PROCEDURE TO POPULATE STAR SCHEMA IN GOLD LAYER
-- SILVER --> Apply 'business' rules --> GOLD
-- ===============================================================

USE AUWeatherPipeline;
GO

CREATE OR ALTER PROCEDURE gold.sp_load_gold
AS 
BEGIN
	SET NOCOUNT ON

	DECLARE @row_count INT;

	BEGIN TRY
        
		-- =====================================
		-- TRUNCATE TABLES (in dependancy order)
		-- =====================================
		TRUNCATE TABLE gold.fact_weather;
		TRUNCATE TABLE gold.dim_date;
		TRUNCATE TABLE gold.dim_city;

		-- =====================================
		-- DIM DATE
		-- =====================================
		INSERT INTO gold.dim_date(
			date_key,
			date,
			year,
			month,
			day,
			week_of_year,
			month_name,
			day_name,
			quarter,
			season
		)
		SELECT DISTINCT
			(YEAR(date)*10000) + (MONTH(date)*100) + DAY(date) date_key,
			date,
			YEAR(date) year,
			MONTH(date) month,
			DAY(date) day,
			DATEPART(ISO_WEEK, date) week_of_year,
			DATENAME(MONTH, date) month_name,
			DATENAME(WEEKDAY, date) day_name,
			DATEPART(QUARTER, date) quarter,

			-- Australian meteorological seasons
			CASE
				WHEN MONTH(date) IN (12,1,2) THEN 'Summer'
				WHEN MONTH(date) IN (3,4,5) THEN 'Autumn'
				WHEN MONTH(date) IN (6,7,8) THEN 'Winter'
				WHEN MONTH(date) IN (9,10,11) THEN 'Spring'
			END season

		FROM silver.weather_daily

		SET @row_count = @@ROWCOUNT;
		PRINT 'dim_date rows: ' + CAST(@row_count AS VARCHAR);

		-- =====================================
		-- DIM CITY
		-- =====================================
		INSERT INTO gold.dim_city(
			city_id,        
			city_name,       
			state,        
			latitude,        
			longitude,        
			timezone_iana,                        
			population,                  
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
			colloquial_name      			
		)
		SELECT 
			city_id,        
			city_name,       
			state,        
			latitude,        
			longitude,        
			timezone_iana,                        
			population_2021 population,                  
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
			colloquial_name

		FROM silver.city_metadata;

		SET @row_count = @@ROWCOUNT;
		PRINT 'dim_city rows: ' + CAST(@row_count AS VARCHAR);

	    -- =====================================
		-- FACT WEATHER
		-- =====================================
		INSERT INTO gold.fact_weather (
			date_key,                 
			city_key,                
			temp_max,               
			temp_min,                 
			precipitation_sum,     
			precipitation_hours,      
			windspeed_max,            
			sunshine_hours,          
			weathercode,             
			weather_type
		)
		SELECT
			(YEAR(w.date)*10000) + (MONTH(w.date)*100) + DAY(w.date) date_key,
			c.city_key,

			w.temp_max,
			w.temp_min,
			w.precipitation_sum,
			w.precipitation_hours,
			w.windspeed_max,
			w.sunshine_hours,
			w.weathercode,
			w.weather_type

			FROM silver.weather_daily w
			JOIN gold.dim_city c
				ON w.city_name = c.city_name

        SET @row_count = @@ROWCOUNT;
		PRINT 'fact_weather rows: ' + CAST(@row_count AS VARCHAR);

		-- =====================================
		-- Validation check
		-- =====================================
		IF EXISTS(
			SELECT 1 FROM silver.weather_daily w
			LEFT JOIN gold.dim_city c
				ON w.city_name = c.city_name
			WHERE c.city_key IS NULL
		)
		BEGIN
			PRINT 'WARNING: Some rows not matching with dim_city';
		END

		PRINT 'Gold load successfully completed'

	END TRY

	BEGIN CATCH

		PRINT 'Error in gold.sp_load_gold';
		PRINT ERROR_MESSAGE();

	END CATCH

END;
GO