-- ===============================================================
-- STORED PROCEDURE TO POPULATE DAILY WEATHER TABLE IN SILVER LAYER
-- BRONZE --> APPLY TRANSFORMATIONS & TYPE CASTING --> SILVER
-- ===============================================================

USE AUWeatherPipeline;
GO

CREATE OR ALTER PROCEDURE silver.sp_load_weather_daily
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
		TRUNCATE TABLE silver.weather_daily;

		-- ====================================
		-- Insert flattened daily weather data
		-- ====================================
		INSERT INTO silver.weather_daily (
			city_name,
			date,
			temp_max, 
			temp_min, 
			precipitation_sum, 
			precipitation_hours,
			windspeed_max,
			weathercode,
			weather_type,
			sunshine_hours,
			batch_id
		)
		SELECT
			w.city_name,
			CAST(t.value AS DATE) AS date,
			CAST(temp_max.value AS FLOAT) temperature_max,
			CAST(temp_min.value AS FLOAT) temperature_min,
			CAST(precip_sum.value AS FLOAT) precipitation_sum,
			CAST(precip_hours.value AS FLOAT) precipitation_hours,
			CAST(wind.value AS FLOAT) windspeed_max,
			CAST(code.value AS INT) weather_code,

			-- Weather code descriptive mapping
			CASE CAST(code.value AS INT)
				WHEN 0 THEN 'Clear sky'
				WHEN 1 THEN 'Mainly clear'
				WHEN 2 THEN 'Partly cloudy'
				WHEN 3 THEN 'Overcast'
				WHEN 45 THEN 'Fog'
				WHEN 48 THEN 'Depositing rime fog'
				WHEN 51 THEN 'Light drizzle'
				WHEN 53 THEN 'Moderate drizzle'
				WHEN 55 THEN 'Dense drizzle'
				WHEN 56 THEN 'Light freezing drizzle'
				WHEN 57 THEN 'Dense freezing drizzle'
				WHEN 61 THEN 'Slight rain'
				WHEN 63 THEN 'Moderate rain'
				WHEN 65 THEN 'Heavy rain'
				WHEN 66 THEN 'Light freezing rain'
				WHEN 67 THEN 'Heavy freezing rain'
				WHEN 71 THEN 'Slight snow fall'
				WHEN 73 THEN 'Moderate snow fall'
				WHEN 75 THEN 'Heavy snow fall'
				WHEN 77 THEN 'Snow grains'
				WHEN 80 THEN 'Slight rain showers'
				WHEN 81 THEN 'Moderate rain showers'
				WHEN 82 THEN 'Violent rain showers'
				WHEN 85 THEN 'Slight snow showers'
				WHEN 86 THEN 'Heavy snow showers'
				ELSE 'Unknown'
			END AS weather_type,

			-- Convert seconds --> hours
			ROUND(CAST(sun.value AS FLOAT)/3600, 2) sunshine_duration, 
			w.batch_id
		
		FROM bronze.weather_raw w
			CROSS APPLY OPENJSON(w.response_json, '$.daily.time') t
			CROSS APPLY OPENJSON(w.response_json, '$.daily.temperature_2m_max') temp_max
			CROSS APPLY OPENJSON(w.response_json, '$.daily.temperature_2m_min') temp_min
			CROSS APPLY OPENJSON(w.response_json, '$.daily.precipitation_sum') precip_sum
			CROSS APPLY OPENJSON(w.response_json, '$.daily.precipitation_hours') precip_hours
			CROSS APPLY OPENJSON(w.response_json, '$.daily.windspeed_10m_max') wind
			CROSS APPLY OPENJSON(w.response_json, '$.daily.weathercode') code
			CROSS APPLY OPENJSON(w.response_json, '$.daily.sunshine_duration') sun
		WHERE 
			batch_id = @latest_batch

			-- Align arrays by index
			AND t.[key] = temp_max.[key]
			AND t.[key] = temp_min.[key]
			AND t.[key] = precip_sum.[key]
			AND t.[key] = precip_hours.[key]
			AND t.[key] = wind.[key]
			AND t.[key] = code.[key]
			AND t.[key] = sun.[key];

		-- ====================================
		-- Row count logging
		-- ====================================
		SET @row_count = @@ROWCOUNT;

		PRINT 'Rows inserted into silver.weather_daily: ' + CAST(@row_count AS VARCHAR);

		-- ====================================
		-- Validation check
		-- ====================================
		SELECT COUNT(*) AS total_rows FROM silver.weather_daily;

		PRINT 'Silver daily weather load completed successfully.';

	END TRY

	BEGIN CATCH

		PRINT 'Error in silver.sp_load_weather_daily';
		PRINT ERROR_MESSAGE();

	END CATCH

END;
GO