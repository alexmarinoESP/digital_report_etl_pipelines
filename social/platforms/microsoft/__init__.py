"""
Microsoft Ads platform implementation.

This module provides a complete independent implementation for Microsoft Advertising
(formerly Bing Ads) data extraction, following the platform-independent architecture.

Key Components:
- authenticator: OAuth2 + Service Principal authentication with 3-way fallback
- client: BingAds SDK wrapper for report generation and CSV processing
- processor: Data cleaning and transformation specific to Microsoft Ads
- pipeline: Complete ETL pipeline orchestration

Architecture Principles:
- Completely independent from other platforms (no shared base classes)
- Protocol-based contracts for type safety
- Dependency injection for token providers and data sinks
- SOLID principles throughout
"""

__version__ = "1.0.0"
__author__ = "Data Science Team"

from social.platforms.microsoft.pipeline import MicrosoftAdsPipeline

__all__ = ["MicrosoftAdsPipeline"]
