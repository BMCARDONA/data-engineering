import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

MANHATTAN_LOCATIONS = [
    ("Upper West Side", 40.7870, -73.9754, "10024"),
    ("Upper East Side", 40.7736, -73.9566, "10021"),
    ("Midtown", 40.7549, -73.9840, "10018"),
    ("Chelsea", 40.7465, -73.9960, "10001"),
    ("Greenwich Village", 40.7336, -73.9991, "10014"),
    ("East Village", 40.7264, -73.9818, "10003"),
    ("Lower East Side", 40.7153, -73.9877, "10002"),
    ("Financial District", 40.7074, -74.0113, "10004"),
    ("Tribeca", 40.7163, -74.0086, "10013"),
    ("SoHo", 40.7233, -74.0030, "10012"),
    ("Harlem", 40.8116, -73.9465, "10026"),
    ("Washington Heights", 40.8501, -73.9350, "10032"),
]

def populate_locations():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO dim_location (neighborhood_name, latitude, longitude, zip_code)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (neighborhood_name) DO NOTHING;
        """
        
        for location in MANHATTAN_LOCATIONS:
            cursor.execute(insert_query, location)
        
        conn.commit()
        print(f"Successfully inserted {len(MANHATTAN_LOCATIONS)} locations.")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error populating locations: {e}")
        raise

if __name__ == "__main__":
    populate_locations()