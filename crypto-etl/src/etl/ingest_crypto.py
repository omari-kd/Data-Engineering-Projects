import requests
import logging
import datetime
from .utils import load_config, get_db_connection
from psycopg2.extras import execute_batch
from ..logging_config import setup_logging
from datetime import datetime,timezone

setup_logging()
logger = logging.getLogger(__name__)

logger.info("Starting crypto data ingestion")

config = load_config()
conn =get_db_connection(config)
api_url = config["api_url"]

def fetch_crypto(coins=None):
    if coins is None:
        coins = ["bitcoin", "ethereum"]

    params = {
        "ids": ",".join(coins),
        "vs_currencies": "usd",
        "include_market_cap": "true"
    }

    response = None

    try:
        logger.info(f"Fetching crypto data for: {coins}")

        response = requests.get(api_url,params=params, timeout=10)
        response.raise_for_status() # raises HTTPError for 4xx/5xx

        data = response.json()
        logger.info("Successfully fetched crypto data")

        return data
    
    except requests.exceptions.RequestException as e:
        logger.exception("API request failed", e)
        return None
    
    except ValueError as e:
        logger.exception("Failed to parse JSON response", e)
        return None
    
    finally:
        if response:
            response.close()


def transform(data):
    timestamp = datetime.now(timezone.utc).isoformat()
    rows = []

    for coin, info in data.items():
        price_usd = info.get("usd")
        market_cap = info.get("usd_market_cap")
        rows.append((coin, price_usd, market_cap, timestamp))

    return rows



def load(conn, rows):
    """
    Load transformed data into Postgres.

    rows: list of tuples like (timestamp, coin, price_usd, market_cap)
    """
    sql = """
        INSERT INTO crypto (coin_id, price_usd, market_cap, timestamp)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (coin_id, timestamp)
        DO UPDATE SET
            price_usd = EXCLUDED.price_usd,
            market_cap = EXCLUDED.market_cap;
    """

    cur = conn.cursor()
    execute_batch(cur, sql, rows, page_size=100)
    conn.commit()
    cur.close()


def main():
    
    config=load_config()
    api_url=config["api_url"]

    logger.info("Fetching data from API...")
    data = fetch_crypto()

    logger.info(f"Retrieved {len(data)} records. Transforming data...")
    transformed = transform(data)

    logger.info("Connecting to Neon database...")
    conn = get_db_connection(config)

    logger.info("Inserting data...")
    load(conn, transformed)

    conn.close()
    logger.info("ETL Complete! Data inserted into crypto table")


if __name__ == "__main__":
    main()

