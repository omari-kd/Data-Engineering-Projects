from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
import os

# -------- CONFIGURATION --------
DB_NAME = "etl_demo"
USER = "postgres"
PASSWORD = "ben/junior"  # replace with your password
HOST = "localhost"
PORT = "5432"
API_URL = "https://fakestoreapi.com/products"

TEMP_FILE = "/tmp/products_raw.csv"        # temporary raw data file
TRANSFORMED_FILE = "/tmp/products_clean.csv"  # temporary transformed data file


# -------- TASK 1: EXTRACT --------
def extract_data():
    print("Extracting data from API...")
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()
    data = response.json()

    df = pd.json_normalize(data)
    df.to_csv(TEMP_FILE, index=False)
    print(f"Extracted {len(df)} records and saved to {TEMP_FILE}")


# -------- TASK 2: TRANSFORM --------
def transform_data():
    print("Transforming data...")
    df = pd.read_csv(TEMP_FILE)

    df.rename(columns={
        "title": "product_name",
        "price": "unit_price",
        "category": "category_name",
        "rating.rate": "rating_score",
        "rating.count": "rating_count"
    }, inplace=True)

    # Keep only relevant columns
    df = df[["id", "product_name", "unit_price", "category_name", "rating_score", "rating_count"]]

    # Clean data
    df["unit_price"] = df["unit_price"].fillna(0).abs()
    df["rating_score"] = df["rating_score"].fillna(0)
    df["rating_count"] = df["rating_count"].fillna(0)

    df.to_csv(TRANSFORMED_FILE, index=False)
    print(f"Transformed data saved to {TRANSFORMED_FILE}")


# -------- TASK 3: LOAD --------
def load_data():
    print("Loading data into PostgreSQL...")
    df = pd.read_csv(TRANSFORMED_FILE)

    engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")
    df.to_sql("products", engine, if_exists="replace", index=False)

    print(f"Loaded {len(df)} records into 'products' table.")


# -------- DAG DEFINITION --------
default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id='api_etl_pipeline_split',
    default_args=default_args,
    description='ETL pipeline split into Extract, Transform, Load tasks',
    start_date=datetime(2025, 10, 21),
    schedule='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    # Define task dependencies
    extract_task >> transform_task >> load_task
