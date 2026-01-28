"""LinkedIn Organic Posts Platform Constants.

This module defines all constants specific to the LinkedIn Community Management API,
including API configuration and organization mappings.

Constants:
- API_BASE_URL: Base URL for LinkedIn REST API
- API_VERSION: LinkedIn API version (YYYYMM format)
- ORGANIZATION_MAP: Mapping of LinkedIn organization IDs to company details
- POSTS_LOOKBACK_DAYS: Number of days to look back for posts
"""

from typing import Dict, Final, NamedTuple


# API Configuration
API_BASE_URL: Final[str] = "https://api.linkedin.com/rest"
API_VERSION: Final[str] = "202509"

# Data extraction configuration
POSTS_LOOKBACK_DAYS: Final[int] = 365  # 1 year of posts
FOLLOWER_STATS_LOOKBACK_DAYS: Final[int] = 365  # 1 year of follower stats


class OrganizationInfo(NamedTuple):
    """Organization information."""
    name: str
    companyid: int


# Organization-to-Company Mapping
# Maps LinkedIn organization IDs to internal company details
ORGANIZATION_MAP: Final[Dict[str, OrganizationInfo]] = {
    "17857": OrganizationInfo(name="Esprinet", companyid=1),
    "1788340": OrganizationInfo(name="V-Valley the value of Esprinet", companyid=1),
}

# List of organization IDs for iteration
ORGANIZATION_IDS: Final[list] = list(ORGANIZATION_MAP.keys())
