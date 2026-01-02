import requests
import sqlite3
from datetime import datetime, timezone
import schedule
import time


DB_NAME = 'sales_data.db'

def create_table():
    conn = sqlite3.connect(DB_NAME)
    cursor =  conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
                   id INTEGER PRIMARY KEY,
                   title TEXT,
                   price REAL,
                   discounted_price REAL,
                   stock INTEGER,
                   revenue REAL,
                   timestamp TEXT
                   )
    """)
    conn.commit()
    conn.close()

def extract():
    response = requests.get("https://dummyjson.com/products")
    response.raise_for_status()
    return response.json().get("products", [])

def transform(products):
    rows = []
    ts = datetime.now(timezone.utc).isoformat()
    for p in products:
        discount = p.get("discountedPercentage", 0)
        discounted = p['price'] * (1 - discount / 100)
        revenue = discounted * p['stock']
        rows.append((p['id'], p['title'], p['price'], discounted, p['stock'], revenue, ts))
    return rows


def load(rows):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT OR REPLACE INTO products
        (id, title, price, discounted_price, stock, revenue, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()

def run_etl():
    print(f"Running ETL at {datetime.now(timezone.utc).isoformat()}")
    products = extract()
    rows = transform(products)
    load(rows)
    print("ETL complete.\n")

if __name__ == "__main__":
    create_table()
    run_etl()
    schedule.every().day.at("00:00").do(run_etl)
    print("Scheduler started—running ETL daily.\n")
    while True:
        schedule.run_pending()
        time.sleep(60)