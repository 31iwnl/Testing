import logging
import os
from datetime import datetime

def setup_logger(log_type):
    logs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
    os.makedirs(logs_dir, exist_ok=True)

    log_date = datetime.now().strftime('%Y-%m-%d')
    log_filename = f"{log_type}_{log_date}.log"
    log_path = os.path.join(logs_dir, log_filename)

    logger = logging.getLogger(log_type)
    logger.setLevel(logging.WARNING)  # Записываем WARNING и ERROR

    if not logger.hasHandlers():
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(logging.WARNING)
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
