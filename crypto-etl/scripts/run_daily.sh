#!/bin/bash
# Run full crypto ETL: Ingest

set -euo pipefail # safer script execution

# Absolute project directory
PROJECT_DIR="/home/kojo/Projects/Data-Engineering-Projects/crypto-etl"

# Activate virtual environment
source "$PROJECT_DIR/.venv/bin/activate"

# Move to project directory
cd "$PROJECT_DIR" || exit

echo "Starting CRYPTO ETL pipeline..."

echo "Step 1: Ingesting raw data from API..."
python3 -m src.etl.ingest_crypto
echo "Ingestion complete"

echo "ETL pipeline finished successfully"

# deactivate venv
deactivate