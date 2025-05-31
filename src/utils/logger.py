"""
Logging setup and management utilities.

This module provides centralized logging configuration for the
CSV to JSON conversion project.
"""

import logging
import logging.handlers
from pathlib import Path
from typing import Optional
from src.utils.config import config_manager


def setup_logger(
    name: str = __name__, 
    level: Optional[str] = None,
    log_file: Optional[str] = None,
    log_format: Optional[str] = None
) -> logging.Logger:
    """
    Set up and configure a logger instance.
    
    Args:
        name: Logger name (typically __name__)
        level: Logging level override
        log_file: Log file path override
        log_format: Log format override
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Load configuration
    try:
        settings = config_manager.load_settings()
        log_config = settings.get('logging', {})
    except Exception:
        # Fallback to defaults if config loading fails
        log_config = {}
    
    # Set logging level
    log_level = level or log_config.get('level', 'INFO')
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Set log format
    log_format = log_format or log_config.get(
        'format', 
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    formatter = logging.Formatter(log_format)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    log_file_path = log_file or log_config.get('file', 'logs/conversion.log')
    if log_file_path:
        # Ensure log directory exists
        log_path = Path(log_file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file_path,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str = __name__) -> logging.Logger:
    """
    Get a logger instance. Creates one if it doesn't exist.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    
    # Set up logger if not already configured
    if not logger.handlers:
        return setup_logger(name)
    
    return logger


# Set up root application logger
app_logger = setup_logger('csv_to_json_converter') 