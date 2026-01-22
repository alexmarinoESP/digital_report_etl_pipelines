"""Platform adapters for different advertising platforms.

Adapters implement the Adapter pattern to provide a consistent interface
for interacting with different advertising platforms (LinkedIn, Google Ads, Facebook Ads).
"""

from social.adapters.base import BaseAdsPlatformAdapter
from social.adapters.linkedin_adapter import LinkedInAdsAdapter
from social.adapters.google_adapter import GoogleAdsAdapter
from social.adapters.facebook_adapter import FacebookAdsAdapter

__all__ = [
    "BaseAdsPlatformAdapter",
    "LinkedInAdsAdapter",
    "GoogleAdsAdapter",
    "FacebookAdsAdapter",
]
