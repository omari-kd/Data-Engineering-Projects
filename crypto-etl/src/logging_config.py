import logging
import sys
import os

def setup_logging():
    project_root=os.path.dirname(os.path.dirname(__file__))
    log_file=os.path.join(project_root, "logs", "etl.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file)
        ]
    )