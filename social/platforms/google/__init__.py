"""Google Ads platform module."""

import os

from social import read_config
from social.platforms.google.fields import *

# Google Ads config files
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

# Query dispatcher
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

# Manager to company mapping
manager_to_company = {
    "9474097201": 1,
    "4619434319": 1,
}
