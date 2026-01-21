"""
Social module for digital report ETL pipelines.
ETL pipeline for social media ads data (Facebook, Google, LinkedIn).
"""

import os
import sys
import re
import json
from pathlib import Path
from typing import Dict, Any

import yaml
from dotenv import load_dotenv

# Load .env file if it exists (important for local development)
load_dotenv()

from shared.utils.env import get_env

# Module root
_ROOT = Path(os.path.dirname(__file__)).absolute()

# Constants
DAY_BEHIND = 7
SCHEMA = "GoogleAnalytics"
sender = "fremont.esprinet.com"
receiver = "DataScience@esprinet.com"


def read_json(file: str) -> dict:
    """Read JSON file."""
    with open(file, "r") as jsfile:
        return json.load(jsfile)


def read_config(file: str) -> dict:
    """Read YAML configuration file."""
    with open(file, "r") as ymlfile:
        return yaml.safe_load(ymlfile)


def get_credentials() -> Dict[str, Any]:
    """
    Get social credentials from environment variables or file.

    Returns:
        Dictionary with credentials for all platforms
    """
    # Try to load from credentials.yml if exists
    cred_path = os.path.join(_ROOT, "credentials.yml")
    if os.path.exists(cred_path):
        return read_config(cred_path)

    # Otherwise use environment variables
    return {
        "facebook": {
            "access_token": get_env("FACEBOOK_ACCESS_TOKEN", ""),
            "id_account": get_env("FACEBOOK_ACCOUNT_IDS", "").split(","),
            "id_business": get_env("FACEBOOK_BUSINESS_ID", ""),
            "app_secret": get_env("FACEBOOK_APP_SECRET", ""),
            "app_id": get_env("FACEBOOK_APP_ID", ""),
        },
        "linkedin": {
            "client_id": get_env("LINKEDIN_CLIENT_ID", ""),
            "client_secret": get_env("LINKEDIN_CLIENT_SECRET", ""),
            "access_token": get_env("LINKEDIN_ACCESS_TOKEN", ""),
            "refresh_token": get_env("LINKEDIN_REFRESH_TOKEN", ""),
        },
        "google": {
            "manager_id": get_env("GOOGLE_ADS_MANAGER_IDS", "").split(","),
            "version": get_env("GOOGLE_ADS_API_VERSION", "v19"),
        },
    }


# Data paths
platform = sys.platform
if not re.search(r"win", platform):
    TMPDATA_PATH = os.path.join(
        os.environ.get("AIRFLOW_HOME", ""), "tmpdata", "social_posts"
    )
else:
    TMPDATA_PATH = os.path.join(_ROOT, "data")

# Load credentials
cfg_credentials = get_credentials()
