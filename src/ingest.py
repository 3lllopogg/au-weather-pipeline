import requests
import json
import uuid
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from config import get_connection

# ======================================
# Load environment variables & config
# ======================================
load_dotenv()

DAYS_BACK = 365 # Change value to adjust the historical window

# ======================================
# Generate batch ID
# ======================================
batch_id = str(uuid.uuid4())

print(f'\n------ STARTING INGESTION ------')
print(f'Batch ID = {batch_id}')

# ======================================
# Define cities
# ======================================
cities = [
    ("Perth", -31.95, 115.86),
    ("Sydney", -33.86, 151.21),
    ("Melbourne", -37.81, 144.96),
    ("Brisbane", -27.47, 153.03),
    ("Adelaide", -34.93, 138.60),
    ("Canberra", -35.28, 149.13),
    ("Darwin", -12.46, 130.84),
    ("Hobart", -42.88, 147.33)
] # Open-meteo API does not accept city names, latitude and longitude are required

# =====================================
# Define date range (UTC)
# =====================================
end_date = datetime.now(timezone.utc)
start_date = end_date - timedelta(days=DAYS_BACK)

start_date = start_date.strftime("%Y-%m-%d")
end_date = end_date.strftime("%Y-%m-%d")

print(f'\nDate range: {start_date} --> {end_date}')

records_loaded = 0 # Tracks number of API inserts for logging purposes

# =====================================
# DB connection
# =====================================
conn = get_connection()
cursor = conn.cursor()


try:
    # =================================
    # 1. Log STARTED 
    # =================================
    print('\n[INFO] Starting ingestion log...')

    cursor.execute("""
        INSERT INTO bronze.ingestion_log (batch_id, start_time, status, source)
        VALUES (?, GETUTCDATE(), 'STARTED', 'open-meteo')
    """, batch_id)
    conn.commit() 

    # =================================
    # 2. Load API data
    # =================================
    print('\n[INFO] Starting API ingestion...')

    for city, lat, lon in cities:
        url = "https://archive-api.open-meteo.com/v1/archive"
        # Pass query parameters via dict
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,precipitation_hours,windspeed_10m_max,weathercode,uv_index_max,sunshine_duration",
            "timezone": "auto"
        } 

        response = requests.get(url, params=params)

        # Validation check to avoid silent failures 
        if response.status_code != 200:
            raise Exception(f"API call failed for {city}: {response.text}")

        data = response.json() # Store entire JSON response as string - one bronze layer row per city
        raw_json = json.dumps(data)

        cursor.execute("""
            INSERT INTO bronze.weather_raw (batch_id, city_name, latitude, longitude, response_json)
            VALUES (?, ?, ?, ?, ?)
        """, batch_id, city, lat, lon, raw_json)

        records_loaded += 1

    # Commit once after loop
    conn.commit()

    print(f'[INFO] API ingestion completed, rows inserted: {records_loaded}')

    # =================================
    # 3. Load CSV via SQL stored proc
    # =================================
    print('\n[INFO] Loading city metadata via stored procedure...')

    cursor.execute("EXEC bronze.sp_load_city_metadata ?;", batch_id) # File handling logic stays inside SQL
    conn.commit()

    print('[INFO] City metadata load completed.')

    # =================================
    # 4. Log SUCCESS/FAILED
    # Updates existing log row - same batch ID
    # =================================
    print('\n[INFO] Finalising ingestion log...')

    cursor.execute("""
        UPDATE bronze.ingestion_log
        SET end_time = GETUTCDATE(),
            status = 'SUCCESS',
            records_loaded = ?
        WHERE batch_id = ?
    """, records_loaded, batch_id)

    conn.commit()

    print('\n------ INGESTION SUCCESSFUL ------')

except Exception as e:
    error_msg = str(e)

    print('\n[ERROR] Ingestion failed!')
    print(f'[ERROR] {error_msg}')

    cursor.execute("""
        UPDATE bronze.ingestion_log
        SET end_time = GETUTCDATE(),
            status = 'FAILED',
            error_message = ?
        WHERE batch_id = ?
    """, error_msg, batch_id)

    conn.commit()

finally:
    # Close resources regardless of error
    cursor.close()
    conn.close()

    print('\n[INFO] DB connection closed.')