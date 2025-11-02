# logger_config.py
import logging
import os

def setup_logging(log_dir: str = "logs", log_file: str = "python-app.log"):
    # Make sure logs/ directory exists
    os.makedirs(log_dir, exist_ok=True)

    # Configure Python logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(log_dir, log_file)),
            logging.StreamHandler()
        ]
    )

def get_logger(name: str):
    # log my spark application logs at INFO level to both console and file
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger