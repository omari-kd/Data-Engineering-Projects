import requests
import pandas as pd
from sqlalchemy import create_engine

# ---------Configuration ---------
DB_NAME = 'etl_demo'
USER = 'postgres'
PASSWORD = 'ben/junior'
HOST = 'localhost'
PORT = '5432'
API_URL = 'https://fakestoreapi.com/products'

# Create database connection 
engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")


# ---------- EXTRACT ---------
print('Extracting data from API...')
response = requests.get(API_URL)
if response.status_code == 200:
    data = response.json()
    print(f"Successfully extracted {len(data)} records.")
else: 
    raise Exception(f"Failed to fetch data. Status code: {response.status_code}")


# -------- TRANSFORM --------
print("\nTransforming Data...")
df = pd.json_normalize(data) # Flatten nested  JSON
# df = pd.DataFrame(data) # Convert to a dataframe

# Rename some columns for clarity
df.rename(columns={
    'title': 'product_name',
    'price': "unit_price",
    'category': 'category_name',
    'rating.rate': 'rating_score',
    'rating.count': 'rating_count'
}, inplace=True)

# Keep relevant columns
df = df[['id','product_name', 'unit_price', 'category_name', 'rating_score', 'rating_count']]

# Clean data
df['unit_price'] = df['unit_price'].fillna(0)
print(df.head())


# --------- LOAD ---------
print("\nLoading data into PostgreSQL...")
df.to_sql("products",engine, if_exists='replace', index=False)
print("Data loaded successfully into 'products' table!")


# ---------- VERIFY ----------
print("\nVerifying data in database...")
result = pd.read_sql("SELECT * FROM products LIMIT 5;", engine)
print(result)