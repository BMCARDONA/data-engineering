import psycopg2
import yfinance as yf
from dotenv import load_dotenv
import os

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
)

def fetch_historical_data(symbol, time_period='5y'):
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(period=time_period)
        return hist
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

def main():
    symbols = ['NVDA', 'TSM', 'ASML', 'INTC', 'AMD', 
               'QCOM', 'AVGO', 'TXN', 'MU', 'KLAC']
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    total_inserted = 0
    
    for symbol in symbols:
        print(f"Fetching {symbol}...")
        hist = fetch_historical_data(symbol)
        
        if hist is not None and not hist.empty:
            for date, row in hist.iterrows():
                try:
                    cur.execute("""
                        INSERT INTO fact_stock_prices_daily
                        (symbol, date, open, high, low, close, volume, dividends, stock_splits)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, date) DO NOTHING
                    """, (
                        symbol,
                        date.date(),
                        float(row['Open']),
                        float(row['High']),
                        float(row['Low']),
                        float(row['Close']),
                        int(row['Volume']),
                        float(row['Dividends']),
                        float(row['Stock Splits'])
                    ))
                except Exception as e:
                    print(f"Error inserting {symbol} on {date}: {e}")
            
            conn.commit()
            print(f"Inserted {len(hist)} records for {symbol}")
            total_inserted += len(hist)
    
    cur.close()
    conn.close()
    
    print(f"\n✓ Total records inserted: {total_inserted}")

if __name__ == "__main__":
    main()
