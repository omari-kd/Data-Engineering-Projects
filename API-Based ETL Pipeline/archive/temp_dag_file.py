from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine


# -------- ETL Function ---------
def run_etl():
    DB_NAME = 'etl_demo'
    USER = 'postgres'
    PASSWORD = 'ben/junior'
    HOST = 'localhost'
    PORT = '5432'
    API_URL = 'https://fakestoreapi.com/products'

    engine = create_engine(f"postgresql+pyscopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")

    print("ExtractinG data from API...")
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()
    data = response.json()

    print("Transforming data...")
    df = pd.DataFrame(data)
    df.rename(columns={
        'title': 'product_name',
        'price': 'unit_price',
        'category': 'category_name',
        'rating.rate' : 'rating_rate',
        'rating.count': 'rating_count'
    }, inplace=True)
    df = df[['id', 'product_name', 'unit_price', 'category_name', 'rating_score', 'rating_count']]
    df['unit_price'] = df['unit_price'].fillna(0).abs()

    print('Loading data into PostgreSQL...')
    df.to_sql("Products", engine, if_exists='replace', index=False)
    print('ETL process completed successfully')

    # ---------- DAG DEFINITION ----------
default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id='api_etl_pipeline',
    default_args=default_args,
    description='Automated ETL pipeline with Airflow',
    start_date=datetime(2025, 10, 21),
    schedule='@daily',
    catchup=False
) as dag :
    
    run_etl_task = PythonOperator(
        task_id = 'run_api_etl',
        python_callable=run_etl
    )