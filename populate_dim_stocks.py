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

def fetch_stock_info(symbol):
    try:
        stock = yf.Ticker(symbol)
        info = stock.info
        
        return {
            'symbol': symbol,
            'company_name': info.get('longName', 'N/A'),
            'sector': info.get('sector', 'N/A'),
            'industry': info.get('industry', 'N/A'),
            'market_cap': info.get('marketCap', 0),
            'country': info.get('country', 'N/A'),
            'exchange': info.get('exchange', 'N/A')
        }
    except Exception as e:
        print(f"Error fetching info for {symbol}: {e}")
        return None

def main():
    symbols = ['NVDA', 'TSM', 'ASML', 'INTC', 'AMD', 
               'QCOM', 'AVGO', 'TXN', 'MU', 'KLAC']
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    for symbol in symbols:
        print(f"Fetching info for {symbol}...")
        stock_info = fetch_stock_info(symbol)
        
        if stock_info:
            cur.execute("""
                INSERT INTO dim_stocks 
                (symbol, company_name, sector, industry, market_cap, country, exchange)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry,
                    market_cap = EXCLUDED.market_cap,
                    country = EXCLUDED.country,
                    exchange = EXCLUDED.exchange
            """, (
                stock_info['symbol'],
                stock_info['company_name'],
                stock_info['sector'],
                stock_info['industry'],
                stock_info['market_cap'],
                stock_info['country'],
                stock_info['exchange']
            ))
            print(f"Inserted {stock_info['company_name']}")
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"\n Populated dim_stocks with {len(symbols)} companies")

if __name__ == "__main__":
    main()
    