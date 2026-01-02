# crypto-etl-pipeline

A data engineering project for extracting, transforming and loading cryptocurrency data from a public API into a PostgreSQL database.

## Project Structure

```
crypto-etl/
├── .venv/                          # Virtual environment
├── logs/                           # Application logs
├── scripts/
│   └── run_daily.sh                # Shell script to run the daily ETL pipeline
├── src/
│   ├── __init__.py
│   ├── logging_config.py           # Logging configuration
│   ├── config/
│   │   └── config.yaml             # Database and API configuration
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── ingest_crypto.py        # Main ETL script
│   │   └── utils.py                # Utility functions for config and DB connection
│   └── sql/
│       └── create_table.sql        # SQL script to create the crypto table
├── .gitignore                      # Git ignore file
├── Dockerfile                      # Docker configuration
├── Makefile                        # Build automation
├── README.md                       # This file
├── docker-compose.yml              # Docker Compose configuration
├── pyproject.toml                  # Python project configuration
├── requirements.txt                # Python dependencies
└── setup_db.py                     # Database setup script
```

## Prerequisites

- Python 3.8+
- PostgreSQL database (or Neon database for cloud hosting)
- Virtual environment (recommended)

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/omari-kd/crypto-etl-pipeline.git
cd crypto-etl-pipeline
```

### 2. Create and Activate Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

Or using Makefile:

```bash
make install
```

### 4. Configure Database Connection

Update `src/config/config.yaml` with your database credentials:

```yaml
database:
  host: your-host
  port: 5432
  name: your-database
  user: your-username
  password: your-password

api_url: https://api.coingecko.com/api/v3/simple/price
```

### 5. Set Up Database Table

Run the database setup script:

```bash
python setup_db.py
```

## Running the Pipeline

### Using Makefile

```bash
make run
```

### Using Shell Script

```bash
./scripts/run_daily.sh
```

### Manual Execution

```bash
python -m src.etl.ingest_crypto
```

## Docker Support

Build and run with Docker:

```bash
docker build -t crypto-etl .
docker run crypto-etl
```

## Logging

Logs are stored in the `logs/` directory. The application uses Python's logging module with custom configuration in `src/logging_config.py`.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request
