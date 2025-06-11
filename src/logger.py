import logging
import os


def setup_logger(name: str, log_file: str):
    """Setup a logger for each ETL step"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # ensure log directory exists
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        # create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # add handler to logger
        logger.addHandler(file_handler)

    return logger


# Dynamic extract logger (will be set later)
extract_logger = None

# Create loggers for transform and load steps
transform_logger = setup_logger('transform_logger', 'logs/etl/transform_to_silver.log')
load_logger = setup_logger('load_logger', 'logs/etl/load.log')


def log_extract_start(extraction_type):
    global extract_logger
    log_path = f'logs/etl/extract_{extraction_type}.log'
    extract_logger = setup_logger(f'extract_logger_{extraction_type}', log_path)
    extract_logger.info("Starting extraction task...")


def log_extract_end():
    global extract_logger
    if extract_logger is not None:
        extract_logger.info("Extraction task completed.")
    else:
        logging.warning("extract_logger not initialized. Call log_extract_start first.")


def log_transform_start():
    transform_logger.info("Starting transformation task...")

def log_transform_end():
    transform_logger.info("Transformation task completed.")

def log_load_start():
    load_logger.info("Starting load task...")

def log_load_end():
    load_logger.info("Load task completed.")

def log_error(stage: str, message: str):
    global extract_logger
    """Log an error message to the appropriate stage logger."""
    if stage == 'extract':
        if extract_logger is not None:
            extract_logger.error(message)
        else:
            logging.error(f"[extract] Logger not initialized: {message}")
    elif stage == 'transform':
        transform_logger.error(message)
    elif stage == 'load':
        load_logger.error(message)
    else:
        logging.error(f"[Unknown Stage] {message}")

def log_info(stage: str, message: str):
    global extract_logger
    """Log an info-level message to the appropriate stage logger."""
    if stage == 'extract':
        if extract_logger is not None:
            extract_logger.info(message)
        else:
            logging.info(f"[extract] Logger not initialized: {message}")
    elif stage == 'transform':
        transform_logger.info(message)
    elif stage == 'load':
        load_logger.info(message)
    else:
        logging.info(f"[Unknown Stage] {message}")
