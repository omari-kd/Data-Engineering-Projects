# stock_etl.py
import yfinance as yf
import pandas as pd
import sqlite3
from datetime import datetime

def extract_stock_data(ticker="AAPL", period="1mo"):
    stock = yf.Ticker(ticker)
    df = stock.history(period=period)
    return df.reset_index()

def transform_stock_data(df):
    df["daily_return"] = df["Close"].pct_change()
    df["extracted_at"] = datetime.utcnow()
    return df.dropna()

def load_to_sqlite(df, db_name="etl_data.db", table_name="stock_prices"):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists="append", index=False)
    conn.close()
    print(f"Loaded {len(df)} rows into {table_name}")

def run_stock_etl():
    print(f"Running Stock ETL at {datetime.utcnow()}")
    data = extract_stock_data("AAPL", "1mo")
    transformed = transform_stock_data(data)
    load_to_sqlite(transformed)
    print("Stock ETL complete.\n")

if __name__ == "__main__":
    run_stock_etl()
