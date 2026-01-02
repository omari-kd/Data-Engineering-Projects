import yaml
import psycopg2
from pathlib import Path

# Read configuration values from config.yaml
def load_config(path=None):
    if path is None:
        # Make path relative to this file
        base_dir = Path(__file__).resolve().parents[2]  # goes up 2 levels to project root
        path = base_dir /"src" / "config" / "config.yaml"

    with open(path, "r") as f:
        return yaml.safe_load(f)
    

def get_db_connection(config):
    db = config["database"]
    conn = psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["dbname"],
        user=db["user"],
        password=db["password"],
        sslmode=db["sslmode"]
    )

    return conn