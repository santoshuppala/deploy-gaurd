"""
Logging utilities for the validation framework.
Provides centralized logging configuration with file and console handlers.
"""
import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


class ValidationLogger:
    """Centralized logging manager for the validation framework."""

    _loggers = {}

    @classmethod
    def get_logger(cls, name: str, log_level: str = "INFO",
                   log_file: Optional[str] = None,
                   console_output: bool = True) -> logging.Logger:
        """
        Get or create a logger instance.

        Args:
            name: Logger name (typically module name)
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: Path to log file (optional)
            console_output: Whether to output to console

        Returns:
            Configured logger instance
        """
        if name in cls._loggers:
            return cls._loggers[name]

        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, log_level.upper()))
        logger.handlers.clear()

        # Create formatter
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Console handler
        if console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        # File handler
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(log_file, mode='a')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        cls._loggers[name] = logger
        return logger

    @classmethod
    def setup_default_logger(cls, output_dir: str = "output/logs",
                            log_level: str = "INFO") -> logging.Logger:
        """
        Setup default logger for the framework.

        Args:
            output_dir: Directory for log files
            log_level: Logging level

        Returns:
            Configured logger instance
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"{output_dir}/validation_{timestamp}.log"

        return cls.get_logger(
            name="validation_framework",
            log_level=log_level,
            log_file=log_file,
            console_output=True
        )

    @classmethod
    def get_module_logger(cls, module_name: str) -> logging.Logger:
        """Get logger for a specific module."""
        return cls.get_logger(module_name)


def get_logger(name: str = "validation_framework") -> logging.Logger:
    """
    Convenience function to get a logger.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return ValidationLogger.get_logger(name)
