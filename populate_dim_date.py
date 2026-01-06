import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        dbname="finance_tracker",
        user="postgres",
        password=os.getenv('DB_PASSWORD'),
        host="localhost"
    )

def main():
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT DISTINCT date FROM fact_stock_prices_daily ORDER BY date")
    dates = cur.fetchall()
    
    print(f"Found {len(dates)} unique dates to process...")
    
    for (date,) in dates:
        dt = datetime.combine(date, datetime.min.time())
        
        year = dt.year
        quarter = (dt.month - 1) // 3 + 1
        month = dt.month
        day_of_week = dt.weekday()  
        week_of_year = dt.isocalendar()[1]
        is_weekend = day_of_week >= 5
        
        next_day = dt.replace(day=28) + timedelta(days=4)
        is_month_end = (next_day.month != dt.month)
        
        is_quarter_end = month in [3, 6, 9, 12] and is_month_end
        
        cur.execute("""
            INSERT INTO dim_date 
            (date, year, quarter, month, day_of_week, week_of_year, 
             is_weekend, is_month_end, is_quarter_end)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING
        """, (
            date, year, quarter, month, day_of_week, week_of_year,
            is_weekend, is_month_end, is_quarter_end
        ))
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Populated dim_date with {len(dates)} dates")

if __name__ == "__main__":
    main()