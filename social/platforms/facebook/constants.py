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
API_VERSION = "v19.0"

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
            Campaign.Field.name,
            Campaign.Field.status,
            Campaign.Field.configured_status,
            Campaign.Field.effective_status,
            Campaign.Field.created_time,
            Campaign.Field.updated_time,
            Campaign.Field.objective,
            Campaign.Field.lifetime_budget,
            Campaign.Field.daily_budget,
            Campaign.Field.budget_remaining,
        ],
        # Ad Set fields
        "fields_ads_adset": [
            AdSet.Field.id,
            AdSet.Field.name,
            AdSet.Field.campaign_id,
            AdSet.Field.status,
            AdSet.Field.configured_status,
            AdSet.Field.effective_status,
            AdSet.Field.start_time,
            AdSet.Field.end_time,
            AdSet.Field.created_time,
            AdSet.Field.updated_time,
            AdSet.Field.destination_type,
            AdSet.Field.optimization_goal,
            AdSet.Field.billing_event,
            AdSet.Field.bid_amount,
            AdSet.Field.daily_budget,
            AdSet.Field.lifetime_budget,
        ],
        # Ad Set targeting fields (for audience extraction)
        "fields_ads_audience_adset": [
            AdSet.Field.id,
            AdSet.Field.name,
            AdSet.Field.campaign_id,
            AdSet.Field.targeting,
        ],
        # Custom Conversion fields
        "fields_custom_convers": [
            CustomConversion.Field.id,
            CustomConversion.Field.name,
            CustomConversion.Field.custom_event_type,
            CustomConversion.Field.rule,
            CustomConversion.Field.default_conversion_value,
        ],
        # Insights fields (performance metrics)
        "fields_ads_insight": [
            AdsInsights.Field.account_id,
            AdsInsights.Field.account_name,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.adset_id,
            AdsInsights.Field.adset_name,
            AdsInsights.Field.ad_id,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.date_start,
            AdsInsights.Field.date_stop,
            AdsInsights.Field.spend,
            AdsInsights.Field.impressions,
            AdsInsights.Field.reach,
            AdsInsights.Field.frequency,
            AdsInsights.Field.clicks,
            AdsInsights.Field.ctr,
            AdsInsights.Field.cpc,
            AdsInsights.Field.cpm,
            AdsInsights.Field.cpp,
            AdsInsights.Field.inline_link_clicks,
            AdsInsights.Field.inline_link_click_ctr,
            AdsInsights.Field.cost_per_inline_link_click,
            AdsInsights.Field.unique_clicks,
            AdsInsights.Field.unique_ctr,
            AdsInsights.Field.unique_inline_link_clicks,
            AdsInsights.Field.unique_inline_link_click_ctr,
        ],
        # Insights actions fields (conversion metrics)
        "fields_ads_insight_actions": [
            AdsInsights.Field.ad_id,
            AdsInsights.Field.ad_name,
            AdsInsights.Field.date_start,
            AdsInsights.Field.date_stop,
            AdsInsights.Field.actions,
            AdsInsights.Field.action_values,
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
        "fields_ads_campaign": ["id", "name", "status", "configured_status", "effective_status", "created_time", "updated_time", "objective", "lifetime_budget", "daily_budget", "budget_remaining"],
        "fields_ads_adset": ["id", "name", "campaign_id", "status", "configured_status", "effective_status", "start_time", "end_time", "created_time", "updated_time", "destination_type", "optimization_goal", "billing_event", "bid_amount", "daily_budget", "lifetime_budget"],
        "fields_ads_audience_adset": ["id", "name", "campaign_id", "targeting"],
        "fields_custom_convers": ["id", "name", "custom_event_type", "rule", "default_conversion_value"],
        "fields_ads_insight": ["account_id", "account_name", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name", "date_start", "date_stop", "spend", "impressions", "reach", "frequency", "clicks", "ctr", "cpc", "cpm", "cpp", "inline_link_clicks", "inline_link_click_ctr", "cost_per_inline_link_click", "unique_clicks", "unique_ctr", "unique_inline_link_clicks", "unique_inline_link_click_ctr"],
        "fields_ads_insight_actions": ["ad_id", "ad_name", "date_start", "date_stop", "actions", "action_values"],
        "fields_ads_creative": ["id", "name", "title", "body", "object_story_id", "object_type", "image_url", "video_id", "thumbnail_url", "effective_object_story_id"],
    }

# Rate limiting configuration
RATE_LIMIT_DELAY_SECONDS = 60  # Delay between API calls to avoid rate limits
MAX_RETRIES = 3  # Maximum number of retries for failed requests
BACKOFF_FACTOR = 2  # Exponential backoff factor

# Date chunking configuration
# Facebook API has data size limits, so large date ranges need chunking
DATE_CHUNK_DAYS = 90  # Split large date ranges into 90-day chunks
MAX_DATE_RANGE_DAYS = 730  # Maximum 2 years of historical data

# Default pagination settings
DEFAULT_PAGE_SIZE = 1000  # Default number of items per page
MAX_PAGE_SIZE = 10000  # Maximum allowed page size
