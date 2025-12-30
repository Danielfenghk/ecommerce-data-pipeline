"""
Logging utilities for the E-Commerce Data Pipeline.
Uses loguru for structured logging with file rotation.
"""

import sys
from pathlib import Path
from datetime import datetime
from loguru import logger
from functools import wraps
import time
from typing import Callable, Any

from src.config import settings


def setup_logging(
    log_level: str = "INFO",
    log_to_file: bool = True,
    log_to_console: bool = True
) -> None:
    """
    Configure logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Whether to log to file
        log_to_console: Whether to log to console
    """
    # Remove default logger
    logger.remove()
    
    # Log format
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )
    
    # Console logging
    if log_to_console:
        logger.add(
            sys.stdout,
            format=log_format,
            level=log_level,
            colorize=True
        )
    
    # File logging
    if log_to_file:
        log_file = settings.logs_path / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
        logger.add(
            str(log_file),
            format=log_format,
            level=log_level,
            rotation="100 MB",
            retention="30 days",
            compression="gz"
        )
        
        # Error log file
        error_log_file = settings.logs_path / "errors.log"
        logger.add(
            str(error_log_file),
            format=log_format,
            level="ERROR",
            rotation="50 MB",
            retention="90 days",
            compression="gz"
        )


def log_execution_time(func: Callable) -> Callable:
    """
    Decorator to log function execution time.
    
    Args:
        func: Function to wrap
        
    Returns:
        Wrapped function
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        logger.info(f"Starting execution of {func.__name__}")
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(
                f"Completed {func.__name__} in {execution_time:.2f} seconds"
            )
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(
                f"Error in {func.__name__} after {execution_time:.2f} seconds: {str(e)}"
            )
            raise
    
    return wrapper


def log_execution_time_async(func: Callable) -> Callable:
    """
    Async decorator to log function execution time.
    
    Args:
        func: Async function to wrap
        
    Returns:
        Wrapped async function
    """
    @wraps(func)
    async def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        logger.info(f"Starting async execution of {func.__name__}")
        
        try:
            result = await func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(
                f"Completed {func.__name__} in {execution_time:.2f} seconds"
            )
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(
                f"Error in {func.__name__} after {execution_time:.2f} seconds: {str(e)}"
            )
            raise
    
    return wrapper


class PipelineLogger:
    """Logger class for pipeline operations with context."""
    
    def __init__(self, pipeline_name: str, run_id: str = None):
        self.pipeline_name = pipeline_name
        self.run_id = run_id or datetime.now().strftime('%Y%m%d_%H%M%S')
        self.context = {
            'pipeline': pipeline_name,
            'run_id': self.run_id
        }
    
    def _format_message(self, message: str) -> str:
        """Format message with context."""
        return f"[{self.pipeline_name}:{self.run_id}] {message}"
    
    def info(self, message: str, **kwargs):
        """Log info message."""
        logger.info(self._format_message(message), **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message."""
        logger.debug(self._format_message(message), **kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message."""
        logger.warning(self._format_message(message), **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message."""
        logger.error(self._format_message(message), **kwargs)
    
    def exception(self, message: str, **kwargs):
        """Log exception with traceback."""
        logger.exception(self._format_message(message), **kwargs)
    
    def log_metrics(self, metrics: dict):
        """Log pipeline metrics."""
        metrics_str = ", ".join(f"{k}={v}" for k, v in metrics.items())
        self.info(f"Metrics: {metrics_str}")


# Initialize logging on module import
setup_logging()

# Export logger
__all__ = ['logger', 'setup_logging', 'log_execution_time', 
           'log_execution_time_async', 'PipelineLogger']