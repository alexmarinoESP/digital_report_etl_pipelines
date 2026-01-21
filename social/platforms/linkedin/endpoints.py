"""
LinkedIn API endpoints.
"""

from enum import Enum


class LinkedinEndPoint(Enum):
    """LinkedIn API endpoint definitions."""

    API_BASE_PATH = "https://api.linkedin.com/rest"

    # Campaign endpoints
    CAMPAIGNS = "{}/adAccounts/{}/adCampaigns"
    CAMPAIGN_GROUPS = "{}/adAccounts/{}/adCampaignGroups"

    # Account endpoints
    ACCOUNTS = "{}/adAccounts"

    # Analytics endpoints
    INSIGHTS = "{}/adAnalytics"

    # Creative endpoints
    CREATIVES = "{}/adAccounts/{}/creatives/{}"

    # Audience endpoints
    AUDIENCES = "{}/adSegments"
