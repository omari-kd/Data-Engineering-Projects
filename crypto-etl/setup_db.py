from src.etl.utils import load_config, get_db_connection;
import logging
from src.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

logger.info("Connecting to database...")

conn = None
cur = None

try:
    config = load_config("src/config/config.yaml")
    conn = get_db_connection(config)
    cur = conn.cursor()

    with open("src/sql/create_table.sql") as f:
        sql = f.read()
        cur.execute(sql)

    conn.commit()
    print("Database table created successfully")
    logger.info("Database table created successfully!")

except Exception as e:
    logger.exception("Error occurred while creating database table")
    if conn:
        conn.rollback()

finally:
    if cur:
        cur.close()
    if conn:
        conn.close()

print("Database setup script finished")

