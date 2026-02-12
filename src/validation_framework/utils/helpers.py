"""
Utility helper functions for the validation framework.
"""
import os
import re
from typing import Any, Dict
from datetime import datetime


def substitute_env_variables(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively substitute environment variables in configuration.

    Supports ${VAR_NAME} and ${VAR_NAME:default_value} syntax.

    Args:
        config: Configuration dictionary

    Returns:
        Configuration with substituted values
    """
    if isinstance(config, dict):
        return {k: substitute_env_variables(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [substitute_env_variables(item) for item in config]
    elif isinstance(config, str):
        return _substitute_string(config)
    return config


def _substitute_string(value: str) -> str:
    """Substitute environment variables in a string."""
    pattern = r'\$\{([^:}]+)(?::([^}]*))?\}'

    def replacer(match):
        var_name = match.group(1)
        default_value = match.group(2) if match.group(2) is not None else ''
        return os.environ.get(var_name, default_value)

    return re.sub(pattern, replacer, value)


def format_number(value: int) -> str:
    """Format number with thousand separators."""
    return f"{value:,}"


def format_percentage(value: float, decimals: int = 2) -> str:
    """Format float as percentage."""
    return f"{value:.{decimals}f}%"


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.2f}h"


def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename by removing invalid characters.

    Args:
        filename: Original filename

    Returns:
        Sanitized filename
    """
    # Remove or replace invalid characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove leading/trailing spaces and dots
    sanitized = sanitized.strip('. ')
    return sanitized


def generate_timestamp_filename(base_name: str, extension: str) -> str:
    """
    Generate filename with timestamp.

    Args:
        base_name: Base name of file
        extension: File extension (with or without dot)

    Returns:
        Filename with timestamp
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ext = extension if extension.startswith('.') else f'.{extension}'
    return f"{base_name}_{timestamp}{ext}"


def deep_merge(dict1: Dict, dict2: Dict) -> Dict:
    """
    Deep merge two dictionaries.

    Args:
        dict1: First dictionary
        dict2: Second dictionary (takes precedence)

    Returns:
        Merged dictionary
    """
    result = dict1.copy()
    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero.

    Args:
        numerator: Numerator
        denominator: Denominator
        default: Default value if division by zero

    Returns:
        Result of division or default
    """
    return numerator / denominator if denominator != 0 else default


def truncate_string(text: str, max_length: int, suffix: str = "...") -> str:
    """
    Truncate string to maximum length.

    Args:
        text: Text to truncate
        max_length: Maximum length
        suffix: Suffix to add if truncated

    Returns:
        Truncated string
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix
