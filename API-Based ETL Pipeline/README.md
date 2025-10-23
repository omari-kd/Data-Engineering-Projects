# API-Based ETL Pipeline with PostgreSQL Warehouse and Streamlit Dashboard

## Project Overview

This project demonstrates a complete **data engineering workflow** built from scratch, from raw API extraction to visual analytics, using open-source tools.  
It automates data collection, transformation, validation and loading into a PostgreSQL data warehouse, followed by dashboard visualisation through Streamlit.

---

## ⚙️ Architecture Overview

**Pipeline Flow:**

> API → Airflow (Extract → Transform → Validate → Load) → PostgreSQL Warehouse → Streamlit Dashboard

---

## Core Components

### 1 Data Extraction

- **Source:** [FakeStore API](https://fakestoreapi.com/) (simulated e-commerce dataset).
- Extracted product details such as `id`, `title`, `price`, `category`and ratings.
- Implemented in Airflow’s `PythonOperator` with **XCom** for inter-task communication.

### 2 Data Transformation

- Cleaned column names and standardised field formats.
- Normalised data using `pandas.json_normalize()`.
- Handled missing values and negative prices.

### 3 Data Validation

- Ensured no empty or missing values.
- Checked valid price ranges and rating constraints (0–5).
- Logged validation outcomes for monitoring.

### 4 Data Loading

- Loaded validated data into PostgreSQL using SQLAlchemy.
- Warehouse schema:

```sql
CREATE TABLE products (
    id INT PRIMARY KEY,
    product_name TEXT,
    unit_price NUMERIC,
    category_name TEXT,
    rating_score NUMERIC,
    rating_count NUMERIC
);
```

### 5 Dashboard Visualisation

- Built an interactive Streamlit app connected directly to PostgreSQL.
- Displayed:

  - Total products and categories

  - Average price and rating

  - Bar chart: price by category

  - Line chart: product ratings

  - Table of top-rated products
