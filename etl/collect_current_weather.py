import requests
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv
import time

load_dotenv()

CURRENT_WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

def get_date_key(dt):
    return int(dt.strftime("%Y%m%d"))

def fetch_current_weather(lat, lon):
    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,relative_humidity_2m,apparent_temperature,weather_code,cloud_cover,pressure_msl,wind_speed_10m",
            "temperature_unit": "fahrenheit",
            "wind_speed_unit": "mph",
            "timezone": "America/New_York"
        }
        
        response = requests.get(CURRENT_WEATHER_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")
        return None

def weather_code_to_description(code):
    codes = {
        0: ("Clear", "Clear sky"),
        1: ("Mainly Clear", "Mainly clear"),
        2: ("Partly Cloudy", "Partly cloudy"),
        3: ("Overcast", "Overcast"),
        45: ("Fog", "Foggy"),
        48: ("Fog", "Depositing rime fog"),
        51: ("Drizzle", "Light drizzle"),
        53: ("Drizzle", "Moderate drizzle"),
        55: ("Drizzle", "Dense drizzle"),
        61: ("Rain", "Slight rain"),
        63: ("Rain", "Moderate rain"),
        65: ("Rain", "Heavy rain"),
        71: ("Snow", "Slight snow"),
        73: ("Snow", "Moderate snow"),
        75: ("Snow", "Heavy snow"),
        77: ("Snow", "Snow grains"),
        80: ("Rain Showers", "Slight rain showers"),
        81: ("Rain Showers", "Moderate rain showers"),
        82: ("Rain Showers", "Violent rain showers"),
        85: ("Snow Showers", "Slight snow showers"),
        86: ("Snow Showers", "Heavy snow showers"),
        95: ("Thunderstorm", "Thunderstorm"),
        96: ("Thunderstorm", "Thunderstorm with slight hail"),
        99: ("Thunderstorm", "Thunderstorm with heavy hail"),
    }
    return codes.get(code, ("Unknown", "Unknown weather condition"))

def insert_weather_record(cursor, location_key, location_name, lat, lon):
    # Fetch weather data
    weather_data = fetch_current_weather(lat, lon)
    
    if not weather_data or 'current' not in weather_data:
        print(f"{location_name}: No data returned")
        return False
    
    try:
        current = weather_data['current']
        
        # Parse observation time from API response
        obs_time_str = current.get('time')  # Format: "2025-01-09T14:00"
        obs_time = datetime.fromisoformat(obs_time_str)
        date_key = get_date_key(obs_time)
        
        # Get weather code and convert to description
        weather_code = current.get('weather_code', 0)
        weather_main, weather_desc = weather_code_to_description(int(weather_code))
        
        # Prepare insert query
        insert_query = """
            INSERT INTO fact_weather (
                date_key, location_key, temperature_f, feels_like_f,
                temp_min_f, temp_max_f, weather_main, weather_description,
                humidity_percent, pressure_hpa, wind_speed_mph,
                cloud_coverage_percent, observation_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_key, location_key, observation_time) DO NOTHING;
        """
        
        # Execute insert
        cursor.execute(insert_query, (
            date_key,
            location_key,
            round(current.get('temperature_2m', 0), 2),
            round(current.get('apparent_temperature', 0), 2),
            None,  # temp_min not available for current weather
            None,  # temp_max not available for current weather
            weather_main,
            weather_desc,
            current.get('relative_humidity_2m'),
            current.get('pressure_msl'),
            round(current.get('wind_speed_10m', 0), 2),
            current.get('cloud_cover'),
            obs_time
        ))
        
        temp = current.get('temperature_2m')
        print(f"{location_name}: {temp}°F, {weather_desc}")
        return True
        
    except Exception as e:
        print(f"{location_name}: Error inserting - {e}")
        return False

def collect_hourly_weather():
    """
    Main function: Collect current weather for all locations
    
    This function is designed to be called by Airflow every hour
    """
    print(f"\n{'='*70}")
    print(f"HOURLY WEATHER COLLECTION")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT location_key, neighborhood_name, latitude, longitude 
            FROM dim_location 
            ORDER BY location_key
        """)
        locations = cursor.fetchall()
        
        print(f"Collecting weather for {len(locations)} locations...\n")
        
        success_count = 0
        fail_count = 0
        
        # Loop through each location
        for location_key, name, lat, lon in locations:
            if insert_weather_record(cursor, location_key, name, lat, lon):
                success_count += 1
            else:
                fail_count += 1
            
            # Small delay to be polite to API rate limit (which is roughly 10,000 requests/day)
            time.sleep(0.5)
        
        # Commit all inserts
        conn.commit()
        
        # Summary
        print(f"\n{'='*70}")
        print(f"COLLECTION COMPLETE")
        print(f"Successful: {success_count}/{len(locations)}")
        print(f"Failed: {fail_count}/{len(locations)}")
        print(f"{'='*70}\n")
        
        # Close connection
        cursor.close()
        conn.close()
        
        # Return success status (this might be useful for Airflow)
        return success_count > 0
        
    except Exception as e:
        print(f"ERROR: {e}")
        raise  # Re-raise so Airflow marks the task as failed

if __name__ == "__main__":
    # This runs when you execute the script directly
    collect_hourly_weather()