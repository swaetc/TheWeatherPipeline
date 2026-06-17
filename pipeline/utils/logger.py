# pipeline/utils/logger.py
import logging
import os
from datetime import datetime
from config.settings import LOG_LEVEL, LOG_DIR
 
def get_logger(name: str) -> logging.Logger:
    """Return a configured logger writing to console and a daily log file."""
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
 
    if not logger.handlers:
        fmt = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)
        log_file = os.path.join(LOG_DIR, f"pipeline_{datetime.now():%Y-%m-%d}.log")
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
 
    return logger
