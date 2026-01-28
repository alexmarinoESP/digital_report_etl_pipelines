"""LinkedIn Organic Posts Platform Module.

This module provides ETL pipelines for LinkedIn organic posts data using
the Community Management API.

Components:
- LinkedInPostsHTTPClient: HTTP client for Community Management API
- LinkedInPostsAdapter: API call implementations
- LinkedInPostsProcessor: Data transformation processor
- LinkedInPostsPipeline: ETL orchestration

Tables populated:
- linkedin_organic_pages: Organization lookup
- linkedin_organic_posts: Post content and metadata
- linkedin_organic_posts_insights: Post engagement metrics
- linkedin_organic_page_stats: Page view statistics
- linkedin_organic_followers: Follower growth tracking
- linkedin_organic_follower_demographics: Follower demographics
"""

from social.platforms.linkedin_posts.constants import (
    API_BASE_URL,
    ORGANIZATION_MAP,
)
from social.platforms.linkedin_posts.http_client import LinkedInPostsHTTPClient
from social.platforms.linkedin_posts.adapter import LinkedInPostsAdapter
from social.platforms.linkedin_posts.processor import LinkedInPostsProcessor
from social.platforms.linkedin_posts.pipeline import LinkedInPostsPipeline

__all__ = [
    "API_BASE_URL",
    "ORGANIZATION_MAP",
    "LinkedInPostsHTTPClient",
    "LinkedInPostsAdapter",
    "LinkedInPostsProcessor",
    "LinkedInPostsPipeline",
]
