"""Social exceptions module."""

from social.exceptions.platform_exceptions import (
    PlatformError,
    FacebookError,
    LinkedinError,
    GoogleAdsError,
    RateLimitError,
)

__all__ = [
    "PlatformError",
    "FacebookError",
    "LinkedinError",
    "GoogleAdsError",
    "RateLimitError",
]
