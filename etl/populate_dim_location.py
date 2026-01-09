import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# Extended Manhattan neighborhoods - 50 locations
MANHATTAN_LOCATIONS = [
    # Original 12
    ("Upper West Side", 40.7870, -73.9754, "10024", "Manhattan"),
    ("Upper East Side", 40.7736, -73.9566, "10021", "Manhattan"),
    ("Midtown", 40.7549, -73.9840, "10018", "Manhattan"),
    ("Chelsea", 40.7465, -73.9960, "10001", "Manhattan"),
    ("Greenwich Village", 40.7336, -73.9991, "10014", "Manhattan"),
    ("East Village", 40.7264, -73.9818, "10003", "Manhattan"),
    ("Lower East Side", 40.7153, -73.9877, "10002", "Manhattan"),
    ("Financial District", 40.7074, -74.0113, "10004", "Manhattan"),
    ("Tribeca", 40.7163, -74.0086, "10013", "Manhattan"),
    ("SoHo", 40.7233, -74.0030, "10012", "Manhattan"),
    ("Harlem", 40.8116, -73.9465, "10026", "Manhattan"),
    ("Washington Heights", 40.8501, -73.9350, "10032", "Manhattan"),
    
    # Additional 38 Manhattan neighborhoods
    ("Inwood", 40.8677, -73.9212, "10034", "Manhattan"),
    ("Morningside Heights", 40.8101, -73.9626, "10027", "Manhattan"),
    ("Hamilton Heights", 40.8231, -73.9501, "10031", "Manhattan"),
    ("Manhattanville", 40.8157, -73.9575, "10027", "Manhattan"),
    ("West Harlem", 40.8074, -73.9533, "10030", "Manhattan"),
    ("East Harlem", 40.7957, -73.9389, "10029", "Manhattan"),
    ("Spanish Harlem", 40.7900, -73.9424, "10035", "Manhattan"),
    ("Yorkville", 40.7769, -73.9553, "10028", "Manhattan"),
    ("Lenox Hill", 40.7693, -73.9595, "10021", "Manhattan"),
    ("Carnegie Hill", 40.7851, -73.9547, "10128", "Manhattan"),
    ("Lincoln Square", 40.7736, -73.9850, "10023", "Manhattan"),
    ("Columbus Circle", 40.7681, -73.9819, "10019", "Manhattan"),
    ("Hell's Kitchen", 40.7638, -73.9918, "10036", "Manhattan"),
    ("Clinton", 40.7650, -73.9900, "10019", "Manhattan"),
    ("Garment District", 40.7540, -73.9910, "10018", "Manhattan"),
    ("Murray Hill", 40.7486, -73.9754, "10016", "Manhattan"),
    ("Kips Bay", 40.7425, -73.9787, "10010", "Manhattan"),
    ("Rose Hill", 40.7445, -73.9832, "10016", "Manhattan"),
    ("Gramercy Park", 40.7374, -73.9859, "10003", "Manhattan"),
    ("Stuyvesant Town", 40.7310, -73.9760, "10009", "Manhattan"),
    ("Peter Cooper Village", 40.7341, -73.9752, "10010", "Manhattan"),
    ("Flatiron District", 40.7408, -73.9896, "10010", "Manhattan"),
    ("NoMad", 40.7450, -73.9876, "10001", "Manhattan"),
    ("Union Square", 40.7359, -73.9911, "10003", "Manhattan"),
    ("Alphabet City", 40.7241, -73.9782, "10009", "Manhattan"),
    ("NoHo", 40.7270, -73.9938, "10012", "Manhattan"),
    ("Nolita", 40.7227, -73.9950, "10012", "Manhattan"),
    ("Little Italy", 40.7194, -73.9977, "10013", "Manhattan"),
    ("Chinatown", 40.7158, -73.9970, "10013", "Manhattan"),
    ("Civic Center", 40.7142, -74.0033, "10007", "Manhattan"),
    ("Two Bridges", 40.7118, -73.9933, "10002", "Manhattan"),
    ("Battery Park City", 40.7113, -74.0156, "10280", "Manhattan"),
    ("Seaport District", 40.7069, -74.0033, "10038", "Manhattan"),
    ("Stuyvesant Square", 40.7341, -73.9824, "10009", "Manhattan"),
    ("Tudor City", 40.7473, -73.9711, "10017", "Manhattan"),
    ("Sutton Place", 40.7573, -73.9615, "10022", "Manhattan"),
    ("Beekman Place", 40.7541, -73.9665, "10022", "Manhattan"),
    ("Turtle Bay", 40.7520, -73.9682, "10017", "Manhattan"),
    ("Roosevelt Island", 40.7614, -73.9510, "10044", "Manhattan"),
    ("Randalls Island", 40.7954, -73.9249, "10035", "Manhattan"),
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
        
        # Add borough column if it doesn't exist
        cursor.execute("""
            ALTER TABLE dim_location 
            ADD COLUMN IF NOT EXISTS borough VARCHAR(50);
        """)
        
        insert_query = """
            INSERT INTO dim_location (neighborhood_name, latitude, longitude, zip_code, borough)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (neighborhood_name) DO NOTHING;
        """
        
        for location in MANHATTAN_LOCATIONS:
            cursor.execute(insert_query, location)
        
        conn.commit()
        print(f"Successfully inserted {len(MANHATTAN_LOCATIONS)} Manhattan locations")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error populating locations: {e}")
        raise

if __name__ == "__main__":
    populate_locations()