"""Google Ads Platform Package.

This package provides a complete, independent implementation of the Google Ads
ETL pipeline following SOLID principles.

Key Components:
- constants: GAQL queries, account mappings, and configuration constants
- http_client: gRPC/Protobuf client for Google Ads API
- adapter: Data extraction from Google Ads API
- processor: Chainable data transformation pipeline
- pipeline: Main ETL orchestrator
- run_google: Container entry point

Architecture:
- NO base class inheritance (completely independent)
- Protocol-based contracts for flexibility
- Type-safe with 100% type hints
- Production-ready error handling

Example:
    from social.platforms.google import GooglePipeline, load_config
    from social.infrastructure.file_token_provider import FileBasedTokenProvider

    # Load configuration
    config = load_config("social/platforms/google/googleads_config.yml")

    # Initialize pipeline
    pipeline = GooglePipeline(
        config=config,
        token_provider=FileBasedTokenProvider("tokens.json"),
        google_config_file="social/platforms/google/google-ads-9474097201.yml",
        manager_customer_id="9474097201",
    )

    # Run all tables
    results = pipeline.run_all_tables()
"""

import os

from social import read_config
from social.platforms.google.fields import *

# Backward compatibility - legacy config loading
cfg_google_ads = read_config(
    os.path.join(os.path.dirname(__file__), "googleads_config.yml")
)

cfg_config_google_ads_key_9474097201 = os.path.join(
    os.path.dirname(__file__), "google-ads-9474097201.yml"
)
cfg_config_google_ads_key_4619434319 = os.path.join(
    os.path.dirname(__file__), "google-ads-4619434319.yml"
)

cfg_config_google_ads_key_d = {
    "9474097201": cfg_config_google_ads_key_9474097201,
    "4619434319": cfg_config_google_ads_key_4619434319,
}

# Legacy query dispatcher
dispatcher = {
    "query_ads_ad_creatives": query_ads_ad_creatives,
    "query_by_device": query_by_device,
    "query_by_device_2": query_by_device_2,
    "query_campaign": query_campaign,
    "query_placement": query_placement,
    "query_placement_2": query_placement_2,
    "query_audience": query_audience,
    "query_audience_2": query_audience_2,
    "query_ad_report": query_ad_report,
    "query_violation_policy": query_violation_policy,
}

# Legacy manager to company mapping
manager_to_company = {
    "9474097201": 1,
    "4619434319": 1,
}

# New independent implementation
from social.platforms.google.adapter import GoogleAdapter
from social.platforms.google.constants import (
    API_VERSION,
    COMPANY_ACCOUNT_MAP,
    DEFAULT_LOOKBACK_DAYS,
    GAQL_QUERIES,
    MICROS_DIVISOR,
)
from social.platforms.google.http_client import GoogleHTTPClient
from social.platforms.google.pipeline import GooglePipeline, load_config
from social.platforms.google.processor import GoogleProcessor

__all__ = [
    # Main pipeline
    "GooglePipeline",
    "load_config",
    # Core components
    "GoogleAdapter",
    "GoogleHTTPClient",
    "GoogleProcessor",
    # Constants
    "API_VERSION",
    "COMPANY_ACCOUNT_MAP",
    "DEFAULT_LOOKBACK_DAYS",
    "GAQL_QUERIES",
    "MICROS_DIVISOR",
    # Legacy
    "cfg_google_ads",
    "cfg_config_google_ads_key_d",
    "dispatcher",
    "manager_to_company",
]

__version__ = "1.0.0"
