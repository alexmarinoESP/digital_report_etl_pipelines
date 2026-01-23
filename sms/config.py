"""
Configuration module for SMS pipeline.

Loads configuration from environment variables with sensible defaults.
Uses python-dotenv to load from .env file if present.
"""

import os
from pathlib import Path
from typing import Optional

# Try to load .env file from project root if exists
try:
    from dotenv import load_dotenv
    # Go up two levels: sms/config.py -> sms/ -> root/
    project_root = Path(__file__).parent.parent
    env_file = project_root / '.env'
    if env_file.exists():
        load_dotenv(env_file)
except ImportError:
    pass  # python-dotenv not installed, use system env vars only


def get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Get environment variable with optional default.

    Args:
        key: Environment variable name
        default: Default value if not set

    Returns:
        Environment variable value or default
    """
    return os.getenv(key, default)


def get_env_required(key: str) -> str:
    """
    Get required environment variable.

    Args:
        key: Environment variable name

    Returns:
        Environment variable value

    Raises:
        ValueError: If environment variable not set
    """
    value = os.getenv(key)
    if value is None:
        raise ValueError(
            f"Required environment variable '{key}' not set. "
            f"Please set it in .env file or environment."
        )
    return value


# =============================================================================
# MAPP API Configuration
# =============================================================================

MAPP_USERNAME = get_env("MAPP_USERNAME", "datascience@esprinet.com")
"""MAPP API username (default: datascience@esprinet.com)"""

MAPP_PASSWORD = get_env_required("MAPP_PASSWORD")
"""MAPP API password (REQUIRED - set in environment)"""


# =============================================================================
# Bitly API Configuration
# =============================================================================

BITLY_TOKEN = get_env_required("BITLY_TOKEN")
"""Bitly API access token (REQUIRED - set in environment)"""
