"""
LinkedIn API endpoints.
"""

from enum import Enum


class LinkedinEndPoint(Enum):
    """LinkedIn API endpoint definitions."""

    API_BASE_PATH = "https://api.linkedin.com/rest"
    VERSION = "202509"  # LinkedIn API version (September 2025)

    # Campaign endpoints
    CAMPAIGNS = "{}/adAccounts/{}/adCampaigns"
    CAMPAIGN = "{}/adAccounts/{}/adCampaigns"  # Alias for compatibility
    CAMPAIGN_GROUPS = "{}/adAccounts/{}/adCampaignGroups"

    # Account endpoints
    ACCOUNTS = "{}/adAccounts"
    ACCOUNT = "{}/adAccounts"  # Alias for compatibility

    # Analytics endpoints
    INSIGHTS = "{}/adAnalytics"
    ANALYTICS = "{}/adAnalytics"  # Alias for compatibility

    # Creative endpoints
    CREATIVES = "{}/adAccounts/{}/creatives/{}"

    # Audience endpoints
    AUDIENCES = "{}/adSegments"
    AUDIENCE = "{}/adSegments"  # Alias for compatibility
