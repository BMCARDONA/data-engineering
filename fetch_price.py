import psycopg2
import yfinance as yf
from dotenv import load_dotenv
import os

load_dotenv()

def get_db_connection():
    """Connect to database"""
    return psycopg2.connect(
        dbname="finance_tracker",
        user="postgres",
        password=os.getenv('DB_PASSWORD'),
        host="localhost"
    )

def fetch_stock_price(symbol):
    """Fetch current price for a stock"""
    try:
        stock = yf.Ticker(symbol)
        price = stock.info['currentPrice']
        return price
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

def insert_price(cursor, symbol, price):
    """Insert stock price into database"""
    cursor.execute(
        "INSERT INTO stock_prices (symbol, price) VALUES (%s, %s)",
        (symbol, price)
    )

def main():
    conn = get_db_connection()
    cur = conn.cursor()
    symbols = ['TSLA', 'AAPL', 'MSFT', 'GOOGL', 'NVDA', 'TSM']
    for symbol in symbols:
        price = fetch_stock_price(symbol)
        if price:
            insert_price(cur, symbol, price)
            print(f"Inserted {symbol}: ${price}")
    
    conn.commit()
    cur.close()
    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    main()