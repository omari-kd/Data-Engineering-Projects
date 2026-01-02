# API-Based ETL Pipeline with PostgreSQL Warehouse and Streamlit Dashboard

App is available at https://omari-apibasedetlpipeline.streamlit.app/

<img width="1920" height="823" alt="Image" src="https://github.com/user-attachments/assets/2a855306-79a9-4c3d-91c6-f91300c76dfc" />

<img width="1920" height="809" alt="Image" src="https://github.com/user-attachments/assets/1df18005-0ba3-40f8-9ff7-016be2cb336b" />

## Project Overview

This project demonstrates a complete **data engineering workflow** built from scratch, from raw API extraction to visual analytics, using open-source tools.  
It automates data collection, transformation, validation and loading into a PostgreSQL data warehouse, followed by dashboard visualisation through Streamlit.

---

## Architecture Overview

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

### 6 Tools & Technologies

| Layer           | Technology         |
| --------------- | ------------------ |
| Orchestration   | Apache Airflow     |
| Programming     | Python             |
| Data Storage    | PostgreSQL         |
| Data Processing | Pandas, SQLAlchemy |
| Dashboarding    | Streamlit          |
| Data Source     | REST API           |

### 7 Key Learning Outcomes

- Designed and automated ETL pipelines with Airflow.

- Implemented robust data validation and logging.

- Created a clean data-warehouse schema for analytics.

- Built and connected a live dashboard to a relational data source.

### How to Run Locally

#### 1. Clone the repository

```bash
git clone https://github.com/yourusername/api-etl-pipeline.git
cd api-etl-pipeline
```

#### 2. Start PostgreSQL and create the warehouse

```sql
CREATE DATABASE data_warehouse;
```

#### 3. Run Airflow DAG

Place your DAG in the dags/ folder and start the Airflow webserver & scheduler:

```bash
airflow api-server
```

#### 4. Launch Streamtlit Dashbaord

```bash
streamlit run dashboard.py
```

Then open http://localhost:8501
