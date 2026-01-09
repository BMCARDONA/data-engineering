import psycopg2
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def populate_dates(start_date, end_date):
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        cursor = conn.cursor()
        
        current_date = start_date
        while current_date <= end_date:
            date_key = int(current_date.strftime("%Y%m%d"))
            year = current_date.year
            quarter = (current_date.month - 1) // 3 + 1
            month = current_date.month
            month_name = current_date.strftime("%B")
            week = current_date.isocalendar()[1]
            day_of_month = current_date.day
            day_of_week = current_date.weekday()
            day_name = current_date.strftime("%A")
            is_weekend = day_of_week >= 5
            
            insert_query = """
                INSERT INTO dim_date (
                    date_key, full_date, year, quarter, month, month_name,
                    week, day_of_month, day_of_week, day_name, is_weekend, is_holiday
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_key) DO NOTHING;
            """
            
            cursor.execute(insert_query, (
                date_key, current_date, year, quarter, month, month_name,
                week, day_of_month, day_of_week, day_name, is_weekend, False
            ))
            
            current_date += timedelta(days=1)
        
        conn.commit()
        print(f"Successfully populated dim_date from {start_date} to {end_date}.")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error populating dates: {e}")
        raise

if __name__ == "__main__":
    # Populate dates for 2024-2026
    start = datetime(2024, 1, 1)
    end = datetime(2026, 12, 31)
    populate_dates(start, end)