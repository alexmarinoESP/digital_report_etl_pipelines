"""
Environment variable utilities.
Provides functions for accessing environment variables with proper error handling.
"""

import os
from typing import Optional


def get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Get environment variable with optional default.

    Args:
        key: Environment variable name
        default: Default value if not set

    Returns:
        Environment variable value or default
    """
    return os.environ.get(key, default)


def get_env_or_raise(key: str) -> str:
    """
    Get environment variable or raise error if not set.

    Args:
        key: Environment variable name

    Returns:
        Environment variable value

    Raises:
        ValueError: If environment variable is not set
    """
    value = os.environ.get(key)
    if value is None:
        raise ValueError(
            f"Environment variable '{key}' is not set. "
            f"Please set it in your .env file or environment."
        )
    return value


def get_env_bool(key: str, default: bool = False) -> bool:
    """
    Get environment variable as boolean.

    Args:
        key: Environment variable name
        default: Default value if not set

    Returns:
        Boolean value
    """
    value = os.environ.get(key, "").lower()
    if value in ("true", "1", "yes", "on"):
        return True
    if value in ("false", "0", "no", "off"):
        return False
    return default


def get_env_int(key: str, default: int = 0) -> int:
    """
    Get environment variable as integer.

    Args:
        key: Environment variable name
        default: Default value if not set

    Returns:
        Integer value
    """
    value = os.environ.get(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def get_env_list(key: str, separator: str = ",") -> list:
    """
    Get environment variable as list.

    Args:
        key: Environment variable name
        separator: List separator

    Returns:
        List of values
    """
    value = os.environ.get(key, "")
    if not value:
        return []
    return [item.strip() for item in value.split(separator)]
