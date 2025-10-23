import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# PostgreSQL connection
DB_NAME = "etl_demo"
USER = "postgres"
PASSWORD = "ben/junior"  # update your password if needed
HOST = "localhost"
PORT = "5432"

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")

# Load your transformed/validated data (you can pull from your ETL or local CSV)
df = pd.read_sql("SELECT * FROM products", engine)  # 'products' is from your ETL load step
df['sale_date'] = datetime.now().date()
df['brand_name'] = 'Generic'  # placeholder for now
df['quantity'] = 1
df['total_amount'] = df['unit_price'] * df['quantity']

# --- Load dim_category ---
categories = df[['category_name']].drop_duplicates().reset_index(drop=True)
categories.to_sql('dim_category', engine, schema='warehouse', if_exists='append', index=False)

# --- Load dim_product ---
# Map category IDs
cat_df = pd.read_sql('SELECT * FROM warehouse.dim_category', engine)
products = df[['product_name', 'category_name', 'brand_name']].drop_duplicates()
products = products.merge(cat_df, on='category_name', how='left')
products[['product_name', 'category_id', 'brand_name']].to_sql('dim_product', engine, schema='warehouse', if_exists='append', index=False)

# --- Load dim_date ---
dates = pd.DataFrame({
    'date': df['sale_date'].drop_duplicates(),
})
dates['month'] = dates['date'].apply(lambda x: x.month)
dates['year'] = dates['date'].apply(lambda x: x.year)
dates['weekday'] = dates['date'].apply(lambda x: x.strftime('%A'))
dates.to_sql('dim_date', engine, schema='warehouse', if_exists='append', index=False)

# --- Load fact_sales ---
prod_df = pd.read_sql('SELECT * FROM warehouse.dim_product', engine)
date_df = pd.read_sql('SELECT * FROM warehouse.dim_date', engine)

fact_sales = df.merge(prod_df, on='product_name', how='left') \
               .merge(date_df, left_on='sale_date', right_on='date', how='left')

fact_sales[['product_id', 'quantity', 'total_amount', 'sale_date']] \
    .to_sql('fact_sales', engine, schema='warehouse', if_exists='append', index=False)

print("Data successfully loaded into warehouse schema!")
