"""
Centralized logging configuration for spark Jobs Pipeline
"""
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path

def setup_pipeline_logging(log_dir="logs", log_level=logging.INFO):
    """
    Configure production-grade logging for PySpark pipeline
    
    Args:
        log_dir: Directory to save log files
        log_level: Logging level (INFO, DEBUG, ERROR)
    """
    # Create logs directory
    Path(log_dir).mkdir(exist_ok=True)
    # Log file name with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"{log_dir}/nyc_jobs_pipeline_{timestamp}.log"
    # Create formatter with structured output
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
    # Root logger configuration
    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.handlers.clear()
    # Clear any existing handlers
    # Console handler (for Spark driver)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    # File handler with rotation (max 10MB, keep 5 backups)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    # Disable Spark noisy logs
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)
    return logger

def get_logger(name=__name__, log_dir="logs"):
    """Get configured logger for specific module"""
    logger = logging.getLogger(name)
    if not logger.handlers:  # Only configure if not already done
        setup_pipeline_logging(log_dir)
    return logger
