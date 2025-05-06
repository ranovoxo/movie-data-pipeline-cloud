import logging

def setup_logger(name: str, log_file: str):
    """Setup a logger for each ETL step"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Create file handler to log to specific log file
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # Add the file handler to the logger
    logger.addHandler(file_handler)
    
    return logger

# Create loggers for extract, transform, and load steps
extract_logger = setup_logger('extract_logger', 'logs/etl/extract.log')
transform_logger = setup_logger('transform_logger', 'logs/etl/transform.log')
load_logger = setup_logger('load_logger', 'logs/etl/load.log')

# Usage Example
def log_extract_start():
    extract_logger.info("Starting extraction task...")

def log_extract_end():
    extract_logger.info("Extraction task completed.")

def log_transform_start():
    transform_logger.info("Starting transformation task...")

def log_transform_end():
    transform_logger.info("Transformation task completed.")

def log_load_start():
    load_logger.info("Starting load task...")

def log_load_end():
    load_logger.info("Load task completed.")

def log_error(stage: str, message: str):
    """Log an error message to the appropriate stage logger."""
    if stage == 'extract':
        extract_logger.error(message)
    elif stage == 'transform':
        transform_logger.error(message)
    elif stage == 'load':
        load_logger.error(message)
    else:
        logging.error(f"[Unknown Stage] {message}")

def log_info(stage: str, message: str):
    """Log an info-level message to the appropriate stage logger."""
    if stage == 'extract':
        extract_logger.info(message)
    elif stage == 'transform':
        transform_logger.info(message)
    elif stage == 'load':
        load_logger.info(message)
    else:
        logging.info(f"[Unknown Stage] {message}")