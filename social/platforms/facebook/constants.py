"""Facebook Ads Platform Constants.

This module defines constants specific to the Facebook Ads platform,
including API versions, account mappings, and field definitions.

Constants:
- API_VERSION: Facebook Graph API version
- DEFAULT_DATE_PRESET: Default time range for API requests
- INSIGHTS_BREAKDOWNS: Available breakdown dimensions for insights
- COMPANY_ACCOUNT_MAP: Mapping of Facebook account IDs to company IDs
- FIELD_DEFINITIONS: Field lists for different API endpoints
"""

from typing import Dict, List

# Facebook Graph API Version
API_VERSION = "v24.0"

# Default date preset for API requests
# Options: "today", "yesterday", "last_7d", "last_14d", "last_30d", "last_90d", "maximum"
DEFAULT_DATE_PRESET = "last_7d"

# Insights API breakdowns
INSIGHTS_BREAKDOWNS = {
    "action_type": [
        "link_click",
        "offsite_conversion",
        "omni_purchase",
        "onsite_conversion.post_save",
        "video_view",
        "post_engagement",
        "page_engagement",
        "post_reaction",
        "comment",
        "post",
    ],
    "action_attribution_windows": [
        "1d_click",
        "7d_click",
        "28d_click",
        "1d_view",
        "7d_view",
        "28d_view",
    ],
}

# Company to Account Mapping
# Maps Facebook Ad Account IDs to internal company IDs
# Format: "act_123456789" -> company_id
COMPANY_ACCOUNT_MAP: Dict[str, int] = {
    # Add your account mappings here
    # Example:
    # "act_123456789": 1,
    # "act_987654321": 2,
}

# Import Facebook SDK field definitions for type-safety
# This allows using both string fields and SDK Field objects
try:
    from facebook_business.adobjects.adaccount import AdAccount
    from facebook_business.adobjects.adset import AdSet
    from facebook_business.adobjects.adsinsights import AdsInsights
    from facebook_business.adobjects.campaign import Campaign
    from facebook_business.adobjects.customconversion import CustomConversion

    # Facebook Ads API Field Definitions using SDK Field objects
    # These provide type-safety and are compatible with the old implementation
    FIELD_DEFINITIONS: Dict[str, List] = {
        # Account fields
        "fields_account_info": [
            AdAccount.Field.id,
            AdAccount.Field.account_id,
            AdAccount.Field.name,
            AdAccount.Field.account_status,
            AdAccount.Field.age,
            AdAccount.Field.currency,
            AdAccount.Field.timezone_name,
            AdAccount.Field.created_time,
        ],
        # Campaign fields
        "fields_ads_campaign": [
            Campaign.Field.id,
            Campaign.Field.status,
            Campaign.Field.configured_status,
            Campaign.Field.effective_status,
            Campaign.Field.created_time,
            Campaign.Field.objective,
        ],
        # Ad Set fields
        "fields_ads_adset": [
            AdSet.Field.id,
            AdSet.Field.campaign_id,
            AdSet.Field.start_time,
            AdSet.Field.end_time,
            AdSet.Field.destination_type,
        ],
        # Ad Set targeting fields (for audience extraction)
        "fields_ads_audience_adset": [
            AdSet.Field.id,
            AdSet.Field.campaign_id,
            AdSet.Field.targeting,
        ],
        # Custom Conversion fields
        "fields_custom_convers": [
            CustomConversion.Field.id,
            CustomConversion.Field.custom_event_type,
            CustomConversion.Field.rule,
        ],
        # Insights fields (performance metrics)
        "fields_ads_insight": [
            AdsInsights.Field.account_id,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.adset_id,
            AdsInsights.Field.ad_id,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.spend,
            AdsInsights.Field.impressions,
            AdsInsights.Field.reach,
            AdsInsights.Field.inline_link_clicks,
            AdsInsights.Field.inline_link_click_ctr,
            AdsInsights.Field.clicks,
            AdsInsights.Field.ctr,
            AdsInsights.Field.cpc,
            AdsInsights.Field.cpm,
        ],
        # Insights actions fields (conversion metrics)
        "fields_ads_insight_actions": [
            AdsInsights.Field.ad_id,
            AdsInsights.Field.actions,
        ],
        # Ad Creative fields - using string fallback as AdCreative fields vary
        "fields_ads_creative": [
            "id",
            "name",
            "title",
            "body",
            "object_story_id",
            "object_type",
            "image_url",
            "video_id",
            "thumbnail_url",
            "effective_object_story_id",
        ],
    }
except ImportError:
    # Fallback to string-based fields if SDK not available
    FIELD_DEFINITIONS: Dict[str, List[str]] = {
        "fields_account_info": ["id", "account_id", "name", "account_status", "age", "currency", "timezone_name", "created_time"],
        "fields_ads_campaign": ["id", "status", "configured_status", "effective_status", "created_time", "objective"],
        "fields_ads_adset": ["id", "campaign_id", "start_time", "end_time", "destination_type"],
        "fields_ads_audience_adset": ["id", "campaign_id", "targeting"],
        "fields_custom_convers": ["id", "custom_event_type", "rule"],
        "fields_ads_insight": ["account_id", "campaign_id", "adset_id", "ad_id", "ad_name", "spend", "impressions", "reach", "inline_link_clicks", "inline_link_click_ctr", "clicks", "ctr", "cpc", "cpm"],
        "fields_ads_insight_actions": ["ad_id", "actions"],
        "fields_ads_creative": ["id", "name", "title", "body", "object_story_id", "object_type", "image_url", "video_id", "thumbnail_url", "effective_object_story_id"],
    }

# Rate limiting configuration
RATE_LIMIT_DELAY_SECONDS = 15  # Delay between API calls to avoid rate limits
MAX_RETRIES = 3  # Maximum number of retries for failed requests
BACKOFF_FACTOR = 2  # Exponential backoff factor (15s, 30s, 60s)

# Date chunking configuration
# Facebook API has data size limits, so large date ranges need chunking
DATE_CHUNK_DAYS = 120  # Split large date ranges into 120-day chunks (balance between speed and rate limits)
MAX_DATE_RANGE_DAYS = 730  # Maximum 2 years of historical data

# Default pagination settings
DEFAULT_PAGE_SIZE = 1000  # Default number of items per page
MAX_PAGE_SIZE = 10000  # Maximum allowed page size
