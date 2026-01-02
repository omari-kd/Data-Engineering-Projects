import requests
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime

# -------- CONFIGURATION --------
DB_NAME = "etl_demo"
USER = "postgres"
PASSWORD = "ben/junior"  
HOST = "localhost"
PORT = "5432"
API_URL = "https://fakestoreapi.com/products"

# -------- LOGGING SETUP --------
# Create log file with timestamp
log_filename = f"etl_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("ETL process started...")

try:
    # -------- DATABASE CONNECTION --------
    engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")
    logging.info("Database connection established successfully.")

    # -------- EXTRACT --------
    logging.info("Extracting data from API...")
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()  # raises an exception for HTTP errors

    data = response.json()
    logging.info(f"Successfully extracted {len(data)} records from API.")

    # -------- TRANSFORM --------
    logging.info("Transforming data...")
    df = pd.json_normalize(data)

    df.rename(columns={
        "title": "product_name",
        "price": "unit_price",
        "category": "category_name",
        "rating.rate": "rating_score",
        "rating.count": "rating_count"
    }, inplace=True)

    df = df[["id", "product_name", "unit_price", "category_name", "rating_score", "rating_count"]]

    # Handle missing or invalid values
    df["unit_price"] = df["unit_price"].fillna(0).abs()
    df["rating_score"] = df["rating_score"].fillna(0)
    df["rating_count"] = df["rating_count"].fillna(0)

    logging.info("Data transformation completed successfully.")

    # -------- LOAD --------
    logging.info("Loading data into PostgreSQL...")
    df.to_sql("products", engine, if_exists="replace", index=False)
    logging.info("Data successfully loaded into 'products' table.")

    # -------- VERIFY --------
    result = pd.read_sql("SELECT COUNT(*) FROM products;", engine)
    row_count = result.iloc[0, 0]
    logging.info(f"Verification successful: {row_count} rows in database.")

except requests.exceptions.RequestException as e:
    logging.error(f"API request failed: {e}")

except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")

finally:
    logging.info("ETL process finished.")
    print(f"ETL process complete. Check log file: {log_filename}")
