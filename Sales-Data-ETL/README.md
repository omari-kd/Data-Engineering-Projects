# Sales Data ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline that ingests sales transaction data from a public API, cleans and enriches it and loads it into a relational database for analysis.

## Workflow

1. Extract

- Fetches mock sales transaction data from a public API.

2. Transform

- Cleans missing values.

- Normalises columns (e.g., standardising text case, parsing dates).

- Adds derived metrics such as profit margin.

3. Load

- Stores the processed data into a SQLite database (sales_data.db).

Schema:

```
CREATE TABLE sales (
  id INTEGER PRIMARY KEY,
  product_id TEXT,
  quantity INTEGER,
  price REAL,
  revenue REAL,
  cost REAL,
  profit REAL,
  profit_margin REAL,
  timestamp TEXT
);
```

4. Automation

- A scheduler runs the ETL pipeline daily, simulating continuous ingestion of sales data.
