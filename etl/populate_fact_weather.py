import requests
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.open-meteo.com/v1/forecast"

def get_date_key(date):
    return int(date.strftime("%Y%m%d"))

def fetch_weather_data(lat, lon):
    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,cloud_cover,pressure_msl,wind_speed_10m",
            "temperature_unit": "fahrenheit",
            "wind_speed_unit": "mph",
            "timezone": "America/New_York"
        }
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching weather data: {e}")
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

def insert_weather_observation(cursor, location_key, lat, lon):
    weather_data = fetch_weather_data(lat, lon)
    
    if not weather_data:
        return False
    
    try:
        current = weather_data.get("current", {})
        
        obs_time = datetime.fromisoformat(current.get("time"))
        date_key = get_date_key(obs_time)
        
        weather_code = current.get("weather_code", 0)
        weather_main, weather_desc = weather_code_to_description(weather_code)
        
        insert_query = """
            INSERT INTO fact_weather (
                date_key, location_key, temperature_f, feels_like_f,
                temp_min_f, temp_max_f, weather_main, weather_description,
                humidity_percent, pressure_hpa, wind_speed_mph,
                cloud_coverage_percent, observation_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_key, location_key, observation_time) DO NOTHING;
        """
        
        cursor.execute(insert_query, (
            date_key,
            location_key,
            round(current.get("temperature_2m", 0), 2),
            round(current.get("apparent_temperature", 0), 2),
            None,  
            None,
            weather_main,
            weather_desc,
            current.get("relative_humidity_2m"),
            current.get("pressure_msl"),
            round(current.get("wind_speed_10m", 0), 2),
            current.get("cloud_cover"),
            obs_time
        ))
        
        return True
        
    except Exception as e:
        print(f"✗ Error inserting weather data: {e}")
        return False

def populate_weather():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        cursor = conn.cursor()
        
        cursor.execute("SELECT location_key, neighborhood_name, latitude, longitude FROM dim_location")
        locations = cursor.fetchall()
        
        success_count = 0
        for location_key, name, lat, lon in locations:
            print(f"Fetching weather for {name}...")
            if insert_weather_observation(cursor, location_key, lat, lon):
                success_count += 1
        
        conn.commit()
        print(f"Successfully inserted weather data for {success_count}/{len(locations)} locations")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error in populate_weather: {e}")
        raise

if __name__ == "__main__":
    populate_weather()