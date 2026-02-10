"""Constants and enumerations for the social module.

This module centralizes all magic strings, numbers, and enumerations
to improve maintainability and avoid duplication.
"""

from enum import Enum
from typing import Final


# Database constants
DATABASE_SCHEMA: Final[str] = "GoogleAnalytics"
DATABASE_TEST_SUFFIX: Final[str] = "_test"

# Date formats
DATE_FORMAT_ISO: Final[str] = "%Y-%m-%d"
DATE_FORMAT_DISPLAY: Final[str] = "%d/%m/%Y"
DATETIME_FORMAT_ISO: Final[str] = "%Y-%m-%d %H:%M:%S"

# API constants
DEFAULT_PAGE_SIZE: Final[int] = 100
MAX_RETRIES: Final[int] = 3
RETRY_BACKOFF_FACTOR: Final[float] = 2.0
REQUEST_TIMEOUT_SECONDS: Final[int] = 30
RATE_LIMIT_DELAY_SECONDS: Final[int] = 60

# LinkedIn specific constants
LINKEDIN_API_VERSION: Final[str] = "202509"  # Must match old project version
LINKEDIN_MAX_PAGE_SIZE: Final[int] = 10000
LINKEDIN_DEFAULT_TIME_GRANULARITY: Final[str] = "DAILY"

# Google Ads specific constants
GOOGLE_ADS_API_VERSION: Final[str] = "v19"
GOOGLE_ADS_MAX_PAGE_SIZE: Final[int] = 10000

# Data processing constants
INSIGHTS_LOOKBACK_DAYS: Final[int] = 150
COPY_CHUNK_SIZE: Final[int] = 1000
PIPE_DELIMITER: Final[str] = "|"

# Special characters that need escaping
ESCAPE_CHARS: Final[dict] = {
    "\\": "\\\\",  # Backslash must be escaped first
    "|": "\\|",    # Pipe character used as delimiter
}


class Platform(Enum):
    """Supported advertising platforms."""

    LINKEDIN = "linkedin"
    GOOGLE = "google"
    FACEBOOK = "facebook"
    MICROSOFT = "microsoft"
    TWITTER = "twitter"    # Future support


class LoadMode(Enum):
    """Data loading modes for the database sink."""

    APPEND = "append"      # Add new rows without checking for duplicates
    REPLACE = "replace"    # Truncate table and insert all rows
    UPSERT = "upsert"      # Insert or update based on primary key
    MERGE = "merge"        # Custom merge logic (anti-join pattern)


class LinkedInTable(Enum):
    """LinkedIn Ads table definitions."""

    ACCOUNT = "linkedin_ads_account"
    CAMPAIGN = "linkedin_ads_campaign"
    AUDIENCE = "linkedin_ads_audience"
    CAMPAIGN_AUDIENCE = "linkedin_ads_campaign_audience"
    INSIGHTS = "linkedin_ads_insights"
    CREATIVE = "linkedin_ads_creative"


class GoogleAdsTable(Enum):
    """Google Ads table definitions."""

    AD_CREATIVES = "google_ads_ad_creatives"
    DEVICE = "google_ads_by_device"
    DEVICE_2 = "google_ads_by_device_2"
    CAMPAIGN = "google_ads_campaign"
    PLACEMENT = "google_ads_placement"
    PLACEMENT_2 = "google_ads_placement_2"
    AUDIENCE = "google_ads_audience"
    AUDIENCE_2 = "google_ads_audience_2"
    AD_REPORT = "google_ads_ad_report"
    VIOLATION_POLICY = "google_ads_violation_policy"


class MicrosoftAdsTable(Enum):
    """Microsoft Ads table definitions."""

    AD_REPORT = "microsoft_ads_report"
    CAMPAIGN_REPORT = "microsoft_ads_campaign_report"


class HTTPMethod(Enum):
    """HTTP request methods."""

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"


class ProcessingStep(Enum):
    """Data processing step identifiers.

    These correspond to methods in the processor classes.
    """

    # Common steps
    ADD_COMPANY = "add_company"
    ADD_ROW_LOADED_DATE = "add_row_loaded_date"
    RENAME_COLUMN = "rename_column"
    CONVERT_STRING = "convert_string"

    # Date/time steps
    BUILD_DATE_FIELD = "build_date_field"
    CONVERT_UNIX_TIMESTAMP_TO_DATE = "convert_unix_timestamp_to_date"
    CONVERT_NAT_TO_NAN = "convert_nat_to_nan"

    # URN processing
    EXTRACT_ID_FROM_URN = "extract_id_from_urn"
    RESPONSE_DECORATION = "response_decoration"
    MODIFY_URN_LI_SPONSORED_ACCOUNT = "modify_urn_li_sponsoredAccount"

    # Data cleaning
    MODIFY_NAME = "modify_name"
    REPLACE_NAN_WITH_ZERO = "replace_nan_with_zero"


# Table dependencies (tables that must be processed before others)
TABLE_DEPENDENCIES: Final[dict] = {
    LinkedInTable.INSIGHTS.value: [LinkedInTable.CAMPAIGN.value],
    LinkedInTable.CREATIVE.value: [LinkedInTable.CAMPAIGN.value],
    LinkedInTable.CAMPAIGN_AUDIENCE.value: [
        LinkedInTable.CAMPAIGN.value,
        LinkedInTable.AUDIENCE.value,
    ],
}

# Default company ID for accounts without mapping
DEFAULT_COMPANY_ID: Final[int] = 1

# Environment variable names
ENV_TEST_MODE: Final[str] = "TEST_MODE"
ENV_DRY_RUN: Final[str] = "DRY_RUN"
ENV_DATABASE_HOST: Final[str] = "VERTICA_HOST"
ENV_DATABASE_PORT: Final[str] = "VERTICA_PORT"
ENV_DATABASE_NAME: Final[str] = "VERTICA_DATABASE"
ENV_DATABASE_USER: Final[str] = "VERTICA_USER"
ENV_DATABASE_PASSWORD: Final[str] = "VERTICA_PASSWORD"

# Logging configuration
LOG_FORMAT: Final[str] = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<level>{message}</level>"
)
LOG_LEVEL_DEFAULT: Final[str] = "INFO"
LOG_LEVEL_DEBUG: Final[str] = "DEBUG"
