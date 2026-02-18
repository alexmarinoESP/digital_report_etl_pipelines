"""
Newsletter module for digital report ETL pipelines.
Renders newsletter HTML to images for digital reports.

Architecture:
    newsletter/
    ├── domain/          # Models and interfaces (no dependencies)
    │   ├── models.py    # Newsletter, NewsletterImage, Company, PipelineStats
    │   └── interfaces.py # Abstract interfaces for adapters
    ├── adapters/        # External service implementations
    │   ├── mapp_adapter.py      # Mapp Newsletter API
    │   ├── hcti_adapter.py      # HTML to image rendering
    │   ├── azure_blob_adapter.py # Azure Blob Storage
    │   ├── local_storage_adapter.py  # Local filesystem
    │   └── repository_adapter.py     # Vertica database
    ├── services/        # Business logic
    │   ├── html_cleaner.py      # HTML sanitization
    │   ├── image_cropper.py     # Smart image cropping
    │   ├── extraction_service.py
    │   ├── rendering_service.py
    │   └── upload_service.py
    ├── pipeline.py      # Main orchestrator
    └── scripts/         # CLI entry points
        └── test_newsletter.py

Usage:
    from newsletter.pipeline import PipelineFactory

    # Production (Azure Blob storage)
    pipeline = PipelineFactory.create_default()
    result = pipeline.run()

    # Testing (local storage)
    pipeline = PipelineFactory.create_with_local_storage("./output")
    result = pipeline.run(companies=[Company.IT], sources=["dynamics"])
"""

import os
import sys
import re
from pathlib import Path
from typing import Dict, Any

import yaml

from shared.utils.env import get_env


# =============================================================================
# Configuration
# =============================================================================

_ROOT = Path(os.path.dirname(__file__)).absolute()


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

# Azure Blob container name
CONTAINER_NAME = get_env("AZURE_STORAGE_CONTAINER", "newsletter-images")

# Load config
config = get_config()


# =============================================================================
# Public API
# =============================================================================

from newsletter.domain.models import Company, Newsletter, NewsletterImage, PipelineStats
from newsletter.pipeline import NewsletterPipeline, PipelineFactory, PipelineResult

__all__ = [
    # Configuration
    "config",
    "TMPDATA_PATH",
    "CONTAINER_NAME",
    # Domain models
    "Company",
    "Newsletter",
    "NewsletterImage",
    "PipelineStats",
    # Pipeline
    "NewsletterPipeline",
    "PipelineFactory",
    "PipelineResult",
]
