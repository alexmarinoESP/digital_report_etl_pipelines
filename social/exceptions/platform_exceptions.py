"""
Platform-specific exceptions.
Provides typed exceptions for better error handling.
"""

from typing import Optional


class PlatformError(Exception):
    """Base exception for platform errors."""

    def __init__(
        self,
        message: str,
        platform: str = "unknown",
        error_code: Optional[str] = None,
    ):
        self.message = message
        self.platform = platform
        self.error_code = error_code
        super().__init__(f"[{platform}] {message}")


class FacebookError(PlatformError):
    """Exception for Facebook API errors."""

    def __init__(self, message: str, error_code: Optional[str] = None):
        super().__init__(message, platform="facebook", error_code=error_code)


class GoogleAdsError(PlatformError):
    """Exception for Google Ads API errors."""

    def __init__(self, message: str, error_code: Optional[str] = None):
        super().__init__(message, platform="google_ads", error_code=error_code)


class LinkedinError(PlatformError):
    """Exception for LinkedIn API errors."""

    def __init__(self, message: str, error_code: Optional[str] = None):
        super().__init__(message, platform="linkedin", error_code=error_code)


class RateLimitError(PlatformError):
    """Exception for rate limit errors."""

    def __init__(
        self,
        message: str,
        platform: str,
        retry_after: Optional[int] = None,
    ):
        super().__init__(message, platform=platform)
        self.retry_after = retry_after
