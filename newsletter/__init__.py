"""
Newsletter module for digital report ETL pipelines.
Renders newsletter HTML to images for digital reports.
"""

import os
import sys
import re
from pathlib import Path
from enum import Enum
from typing import Dict, Any

import yaml

from shared.utils.env import get_env


# Company mapping
COMPANY_DICT = {"it": 1, "es": 2, "pt": 3, "vvit": 32}
COMP_PREVIEW = ["it", "es", "vvit"]

# Module root
_ROOT = Path(os.path.dirname(__file__)).absolute()


class Endpoint(Enum):
    """Mapp API endpoints."""
    preview = "message/getHistorical"


class Company(Enum):
    """Company enumeration with (code, id) pairs."""
    IT = ("it", 1)
    ES = ("es", 2)
    PT = ("pt", 3)
    VVIT = ("vvit", 32)

    def __init__(self, code: str, company_id: int):
        self.code = code
        self.company_id = company_id


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def get_config() -> Dict[str, Any]:
    """
    Get newsletter configuration.
    Loads from config.yml or environment variables.
    """
    config_path = os.path.join(_ROOT, "config.yml")
    if os.path.exists(config_path):
        return load_config(config_path)

    # Fallback to environment variables
    return {
        "credentials_zapier": {
            "HCTI_API_ENDPOINT": get_env("HCTI_API_ENDPOINT", "https://hcti.io/v1/image"),
            "HCTI_API_USER_ID": get_env("HCTI_API_USER_ID", ""),
            "HCTI_API_KEY": get_env("HCTI_API_KEY", ""),
        },
        "postprocessing": {
            "uniformity_threshold": float(get_env("CROP_UNIFORMITY_THRESHOLD", "0.95")),
            "margin_ratio": float(get_env("CROP_MARGIN_RATIO", "0.05")),
            "additional_margin_ratio": float(get_env("CROP_ADDITIONAL_MARGIN_RATIO", "0.02")),
            "extra_crop_ratio": float(get_env("CROP_EXTRA_CROP_RATIO", "0.02")),
        },
    }


# Data paths
platform = sys.platform
if not re.search(r"win", platform):
    TMPDATA_PATH = os.path.join(
        os.environ.get("AIRFLOW_HOME", ""), "tmpdata", "newsletter"
    )
else:
    TMPDATA_PATH = os.path.join(_ROOT, "data")

# Bucket name for S3/Minio
BUCKET_NAME = get_env("S3_BUCKET_NAME", "report-digital-preview")

# Load config
config = get_config()
