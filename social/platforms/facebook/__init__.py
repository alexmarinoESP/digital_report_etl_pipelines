"""Facebook Ads platform module."""

import os

from social import read_config
from social.platforms.facebook.fields import *

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
