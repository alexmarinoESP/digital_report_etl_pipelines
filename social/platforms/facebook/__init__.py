"""
Facebook Ads platform implementation.

This module provides a complete independent implementation for Facebook Marketing API
data extraction, following the platform-independent architecture.

Key Components:
- http_client: FacebookHttpClient for Graph API v19.0 integration
- adapter: Facebook API integration using Facebook Business SDK
- processor: Data transformations specific to Facebook (nested breakdowns, actions arrays)
- pipeline: Complete ETL pipeline orchestration

Facebook API Specifics:
- Graph API Version: v19.0
- SDK-based approach using facebook-business library
- Object-oriented response handling (Campaign, AdSet, Ad objects)
- Nested breakdown dimensions (age, gender, placement, device)
- Actions/action_values arrays requiring flattening
- Account-level iteration for multi-account support

Architecture Principles:
- Completely independent from other platforms (no shared base classes)
- Protocol-based contracts for type safety
- Dependency injection for token providers and data sinks
- SOLID principles throughout
"""

__version__ = "1.0.0"
__author__ = "Data Science Team"

import os

from social import read_config
from social.platforms.facebook.fields import *
from social.platforms.facebook.pipeline import FacebookPipeline

# Backwards compatibility exports
__all__ = ["FacebookPipeline"]

SCHEMA = "GoogleAnalytics"

# Account to Company mapping
accounts_company = {
    "388306055080640": 1,
    "272865923626084": 1,
    "312917396248502": 1,
    "1505529823270238": 2,
    "2521097554864020": 20,
}

# Fields dispatcher
dispatcher = {
    "fields_account_info": fields_account_info,
    "fields_custom_convers": fields_custom_convers,
    "fields_ads_insight": fields_ads_insight,
    "fields_ads_adset": fields_ads_adset,
    "fields_ads_campaign": fields_ads_campaign,
    "fields_ads_insight_actions": fields_ads_insight_actions,
    "fields_ads_creative": fields_ads_creative,
    "fields_ads_images": fields_ads_images,
    "fields_ads_audience_adset": fields_ads_audience_adset,
}

# Load Facebook Ads configuration
cfg_fb_ads = read_config(
    os.path.join(os.path.dirname(__file__), "config_fb_ads.yml")
)
