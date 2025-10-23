from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from google.cloud import bigquery
import requests
import pandas as pd
import json

# -------- CONFIGURATION --------
DB_NAME = "etl_demo"
USER = "postgres"
PASSWORD = "ben/junior"  # replace with your password
HOST = "localhost"
PORT = "5432"
API_URL = "https://fakestoreapi.com/products"


# -------- TASK 1: EXTRACT --------
def extract_data(ti):
    """Fetch product data from the API and push it to XCom"""
    print("Extracting data from API...")
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()
    data = response.json()
    print(f"Extracted {len(data)} records.")

    # Push data (as JSON string to keep it serializable)
    ti.xcom_push(key='raw_data', value=json.dumps(data))


# -------- TASK 2: TRANSFORM --------
def transform_data(ti):
    """Pull raw data from XCom, clean it, and push transformed data"""
    print("Transforming data...")
    raw_json = ti.xcom_pull(task_ids='extract_data', key='raw_data')
    data = json.loads(raw_json)

    df = pd.json_normalize(data)
    df.rename(columns={
        "title": "product_name",
        "price": "unit_price",
        "category": "category_name",
        "rating.rate": "rating_score",
        "rating.count": "rating_count"
    }, inplace=True)

    df = df[["id", "product_name", "unit_price", "category_name", "rating_score", "rating_count"]]
    df["unit_price"] = df["unit_price"].fillna(0).abs()
    df["rating_score"] = df["rating_score"].fillna(0)
    df["rating_count"] = df["rating_count"].fillna(0)

    print(f"Transformed {len(df)} records.")
    # Convert DataFrame to JSON string and push to XCom
    ti.xcom_push(key='transformed_data', value=df.to_json(orient='records'))

# -------- TASK 3: VALIDATE --------
def validate_data(ti):
    print("Validating transformed data...")
    transformed_json = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    df = pd.read_json(transformed_json)

    # Basic checks
    if df.empty:
        raise ValueError("Validation failed: DataFrame is empty.")
    if df.isnull().sum().any():
        raise ValueError("Validation failed: Missing values detected.")
    if (df["unit_price"] < 0).any():
        raise ValueError("Validation failed: Negative prices found.")
    if (df["rating_score"] > 5).any():
        raise ValueError("Validation failed: Rating score exceeds 5.")

    print("Data validation passed!")
    ti.xcom_push(key='validated_data', value=df.to_json(orient='records'))


# -------- TASK 4: LOAD --------
def load_data(ti):
    """Pull validated data from XCom and load into PostgreSQL"""
    print("Loading data into PostgreSQL...")
    validated_data = ti.xcom_pull(task_ids='validate_data', key='validated_data')
    df = pd.read_json(validated_data)

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
    dag_id='api_etl_pipeline_xcom_validation',
    default_args=default_args,
    description='ETL pipeline with validation',
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

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    # Define task dependencies
    extract_task >> transform_task >> validate_task >> load_task
