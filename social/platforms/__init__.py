"""Social platforms module."""

from social.platforms.facebook.ads_client import FBAdsServiceBuilderAccount, FBAdsServiceAccount
from social.platforms.facebook.processor import FBAdsProcess
from social.platforms.google.ads_client import GoogleAdsServiceBuilder, GoogleAdsService
from social.platforms.google.processor import GoogleAdsProcess
from social.platforms.linkedin.ads_client import LinkedinAdsBuilder, LinkedinAdsService
from social.platforms.linkedin.processor import LinkedinProcess

__all__ = [
    "FBAdsServiceBuilderAccount",
    "FBAdsServiceAccount",
    "FBAdsProcess",
    "GoogleAdsServiceBuilder",
    "GoogleAdsService",
    "GoogleAdsProcess",
    "LinkedinAdsBuilder",
    "LinkedinAdsService",
    "LinkedinProcess",
]
