import requests
import sqlite3
import datetime
import schedule
import time
from datetime import datetime, timezone

# Database setup
DB_NAME = "crypto_data.db"

def create_table():
    conn =  sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS crypto_price(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        coin_id TEXT,
        price_usd REAL,
        market_cap REAL,
        timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()



# Extract
def extract_price(coins=['bitcoin', 'ethereum']): 
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd&include_market_cap=true"
    params = {
        'ids': ','.join(coins),
        'vs_currencies':'usd',
        'include_market_cap':'true'
    }
    response = requests.get(url, params=params)
    response.raise_for_status() # Raise an error if API call fails
    return response.json()


# Transform
def transform(data):
    timestamp = datetime.now(timezone.utc).isoformat()
    rows = []
    for coin, info in data.items():
        rows.append((coin, info.get("usd"), info.get("usd_market_cap"), timestamp))
    return rows

# Load
def load(rows):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO crypto_price (coin_id, price_usd, market_cap, timestamp)
        VALUES (?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()

# ETL Pipeline Runner
def run_etl():
    """Run the full ETL pipeline: Extract → Transform → Load"""
    print(f"Running ETL at {datetime.now(timezone.utc).isoformat()}")
    data = extract_price()     # 1. Extract
    rows = transform(data)    # 2. Transform
    load(rows)                # 3. Load
    print("Data loaded successfully\n")


# Main Execution with Scheduler
if __name__ == "__main__":
    # Ensure database and table are created before running ETL
    create_table()
    # Run ETL immediately on startup
    run_etl()
    # Schedule ETL to run every hour
    schedule.every(1).hours.do(run_etl)
    # Keep the script running to allow scheduled jobs to execute
    print("Scheduler started — fetching data every hour...\n")
    while True:
        schedule.run_pending()
        time.sleep(60)