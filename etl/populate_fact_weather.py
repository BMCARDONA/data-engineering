import requests
import psycopg2
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time

load_dotenv()

HISTORICAL_URL = "https://archive-api.open-meteo.com/v1/archive"

def get_date_key(date_str):
    """Convert date string to date_key integer"""
    if 'T' in date_str:
        date_obj = datetime.fromisoformat(date_str)
    else:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return int(date_obj.strftime("%Y%m%d"))

def fetch_weather_with_retry(url, params, max_retries=3):
    """Fetch weather data with exponential backoff on rate limits"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 429:  # Too Many Requests
                wait_time = (attempt + 1) * 10  # 10s, 20s, 30s
                print(f"\n  ⚠ Rate limited. Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                print(f"\n  ✗ Failed after {max_retries} attempts: {e}")
                return None
            time.sleep(5)
    
    return None

def weather_code_to_description(code):
    """Convert WMO weather code to readable description"""
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

def insert_hourly_chunk(cursor, location_key, lat, lon, start_date, end_date):
    """Insert hourly weather data for a date range"""
    
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,apparent_temperature,weather_code,relative_humidity_2m,pressure_msl,wind_speed_10m,cloud_cover",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "timezone": "America/New_York"
    }
    
    weather_data = fetch_weather_with_retry(HISTORICAL_URL, params)
    
    if not weather_data or 'hourly' not in weather_data:
        return 0
    
    hourly_data = weather_data['hourly']
    times = hourly_data.get('time', [])
    
    insert_query = """
        INSERT INTO fact_weather (
            date_key, location_key, temperature_f, feels_like_f,
            weather_main, weather_description, humidity_percent, 
            pressure_hpa, wind_speed_mph, cloud_coverage_percent, observation_time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_key, location_key, observation_time) DO NOTHING;
    """
    
    inserted = 0
    for i, time_str in enumerate(times):
        try:
            obs_time = datetime.fromisoformat(time_str)
            date_key = get_date_key(time_str)
            
            weather_code = int(hourly_data['weather_code'][i]) if hourly_data.get('weather_code') else 0
            weather_main, weather_desc = weather_code_to_description(weather_code)
            
            temp = hourly_data['temperature_2m'][i]
            feels_like = hourly_data['apparent_temperature'][i]
            humidity = hourly_data['relative_humidity_2m'][i]
            pressure = hourly_data['pressure_msl'][i]
            wind_speed = hourly_data['wind_speed_10m'][i]
            cloud_cover = hourly_data['cloud_cover'][i]
            
            cursor.execute(insert_query, (
                date_key, location_key,
                round(temp, 2) if temp else None,
                round(feels_like, 2) if feels_like else None,
                weather_main, weather_desc,
                int(humidity) if humidity else None,
                int(pressure) if pressure else None,
                round(wind_speed, 2) if wind_speed else None,
                int(cloud_cover) if cloud_cover else None,
                obs_time
            ))
            inserted += 1
            
        except Exception as e:
            continue
    
    return inserted

def populate_weather_comprehensive():
    """
    Optimized data collection to work within rate limits
    
    Strategy:
    - 50 Manhattan locations
    - 2024 data in monthly chunks (reduces API calls)
    - 50 locations × 12 months = 600 calls
    - ~3 second delay between calls = ~30 minutes total
    - Result: ~440,000 hourly records
    """
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        cursor = conn.cursor()
        
        cursor.execute("SELECT location_key, neighborhood_name, latitude, longitude FROM dim_location ORDER BY location_key")
        locations = cursor.fetchall()
        
        total_locations = len(locations)
        total_records = 0
        total_api_calls = 0
        
        print(f"\n{'='*70}")
        print(f"COMPREHENSIVE WEATHER DATA COLLECTION")
        print(f"{'='*70}")
        print(f"Locations: {total_locations} Manhattan neighborhoods")
        print(f"Data: Hourly weather for all of 2024")
        print(f"Strategy: Monthly chunks to optimize API usage")
        print(f"Estimated time: ~30-40 minutes")
        print(f"{'='*70}\n")
        
        # Define monthly chunks for 2024
        months = []
        for month in range(1, 13):
            start = datetime(2024, month, 1)
            if month == 12:
                end = datetime(2024, 12, 31)
            else:
                end = datetime(2024, month + 1, 1) - timedelta(days=1)
            months.append((start, end))
        
        # Process each location for all months
        for loc_idx, (location_key, name, lat, lon) in enumerate(locations, 1):
            print(f"\n[{loc_idx}/{total_locations}] {name}")
            print("-" * 50)
            
            location_total = 0
            
            for month_idx, (start_date, end_date) in enumerate(months, 1):
                month_name = start_date.strftime("%B")
                print(f"  {month_name}...", end=" ", flush=True)
                
                records = insert_hourly_chunk(
                    cursor, location_key, lat, lon,
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d")
                )
                
                location_total += records
                total_records += records
                total_api_calls += 1
                
                print(f"✓ {records} records")
                
                # Important: Rate limiting delay
                time.sleep(3)
            
            print(f"  → {name} complete: {location_total} total records")
            
            # Commit after each location
            conn.commit()
            
            # Progress update every 10 locations
            if loc_idx % 10 == 0:
                print(f"\n{'='*70}")
                print(f"PROGRESS UPDATE")
                print(f"Locations processed: {loc_idx}/{total_locations}")
                print(f"API calls made: {total_api_calls}")
                print(f"Total records: {total_records:,}")
                print(f"{'='*70}\n")
        
        print(f"\n{'='*70}")
        print(f"COLLECTION COMPLETE!")
        print(f"{'='*70}")
        print(f"Total API calls: {total_api_calls:,}")
        print(f"Total weather records: {total_records:,}")
        print(f"Locations: {total_locations}")
        print(f"Average records per location: {total_records / total_locations:,.1f}")
        print(f"{'='*70}\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\nError: {e}")
        raise

if __name__ == "__main__":
    populate_weather_comprehensive()