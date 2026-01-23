"""
LinkedIn Ads platform implementation.

This module provides a complete independent implementation for LinkedIn Marketing API
data extraction, following the platform-independent architecture.

Key Components:
- http_client: Custom NoQuotedCommasSession for LinkedIn's special parameter encoding
- adapter: LinkedIn API integration for campaigns, insights, creatives, audiences
- processor: Data transformations specific to LinkedIn (URN extraction, date building)
- pipeline: Complete ETL pipeline orchestration

LinkedIn API Specifics:
- API Version: 202601 (current as of 2026-01)
- REST API with special parameter encoding (NoQuotedCommasSession)
- URN-based resource identification
- 150-day lookback for insights
- Page size: 10000 (vs old 1000)

Architecture Principles:
- Completely independent from other platforms (no shared base classes)
- Protocol-based contracts for type safety
- Dependency injection for token providers and data sinks
- SOLID principles throughout
"""

__version__ = "1.0.0"
__author__ = "Data Science Team"

from social.platforms.linkedin.pipeline import LinkedInPipeline

__all__ = ["LinkedInPipeline"]
