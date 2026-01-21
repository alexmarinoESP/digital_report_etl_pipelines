"""LinkedIn Ads platform module."""

import os

from social import read_config

# Creatives type mapping
creatives_type = {
    "SPONSORED_UPDATE_CAROUSEL": "SponsoredUpdateCarouselCreativeVariables",
    "TEXT_AD": "TextAdCreativeVariables",
    "SPONSORED_STATUS_UPDATE": "SponsoredUpdateCreativeVariables",
    "SPONSORED_VIDEO": "SponsoredVideoCreativeVariables",
}

# Company to account mapping
company_account = {
    "503427986": 1,
    "510686676": 1,
    "512866551": 30,  # Zeliatech
    "512065861": 23,  # V-valley PT
    "506509802": 32,  # V-Valley IT
    "506522380": 19,  # DACOM
    "511420282": 2,
    "511422249": 20,  # V-valley ES
}

# Default headers
headers = {
    "urlencoded": {"Content-Type": "application/x-www-form-urlencoded"},
    "json": {"x-li-format": "json", "Content-Type": "application/json"},
}

# Load LinkedIn Ads configuration
cfg_linkedin_ads = read_config(
    os.path.join(os.path.dirname(__file__), "config_linkedin_ads.yml")
)
