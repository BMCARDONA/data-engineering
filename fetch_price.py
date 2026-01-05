
import psycopg2
import yfinance as yf
from dotenv import load_dotenv
import os

load_dotenv() 

symbol = 'TSLA'
stock = yf.Ticker(symbol)
price = stock.info['currentPrice']

conn = psycopg2.connect(
    dbname = "finance_tracker",
    user="postgres",
    password=os.getenv("DB_PASSWORD"),
    host="localhost"
)

cur = conn.cursor()
cur.execute(
    "INSERT INTO stock_prices (symbol, price) VALUES (%s, %s)",
    (symbol, price)
)

conn.commit()
cur.close()
conn.close()
print("Successfully inserted AAPL price!")


