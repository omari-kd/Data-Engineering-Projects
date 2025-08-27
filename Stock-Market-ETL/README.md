# Stock Market ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for stock market data.
It fetches stock price data using the Yahoo Finance API (yfinance), processes it and loads it into a database for further analysis.

The pipeline is automated to run daily after market close, ensuring fresh and up-to-date data is always available.

# Features

- Extract: Pulls daily stock prices from Yahoo Finance.

- Transform:

  - Cleans raw stock data (removes missing values, standardises column names).

  - Computes additional metrics like daily returns and volatility.

- Load: Saves processed data into a SQLite/Postgres database.

- Automation: Runs daily via a scheduler (e.g., APScheduler).
