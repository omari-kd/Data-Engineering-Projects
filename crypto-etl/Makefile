.PHONY: installl run ingest lint clean

install:
	pip install -r requirements.txt

ingest: 
	python -m src.etl.ingest_crypto

run: 
	python -m src.etl.ingest_crypto

clean: 
	find . -type d -name "__pycache__" -exec rm -rf {} +