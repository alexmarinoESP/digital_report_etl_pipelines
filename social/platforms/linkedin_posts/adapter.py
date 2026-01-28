"""LinkedIn Organic Posts API Adapter.

This module provides the adapter layer for LinkedIn Community Management API,
implementing all data retrieval methods for organic posts.

API Endpoints:
- Posts API: GET /rest/posts
- Share Statistics: GET /rest/organizationalEntityShareStatistics
- Page Statistics: GET /rest/organizationPageStatistics
- Follower Statistics: GET /rest/organizationalEntityFollowerStatistics

Each method returns raw API data which is then processed by the LinkedInPostsProcessor.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from urllib.parse import quote
from loguru import logger

from social.platforms.linkedin_posts.http_client import LinkedInPostsHTTPClient
from social.platforms.linkedin_posts.constants import (
    ORGANIZATION_IDS,
    FOLLOWER_STATS_LOOKBACK_DAYS,
)


class LinkedInPostsAdapter:
    """Adapter for LinkedIn Community Management API.

    Implements all API calls for organic posts data retrieval.
    Each method returns raw data from the API.

    Attributes:
        http_client: HTTP client instance
    """

    def __init__(self, http_client: LinkedInPostsHTTPClient):
        """Initialize adapter with HTTP client.

        Args:
            http_client: Configured HTTP client
        """
        self.http_client = http_client
        logger.debug("LinkedInPostsAdapter initialized")

    # ============================================================================
    # Posts API
    # ============================================================================

    def get_posts(
        self,
        organization_id: str,
        count: int = 100,
        max_results: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch posts for an organization.

        Uses the Posts API finder by author.

        Args:
            organization_id: LinkedIn organization ID
            count: Results per page (max 100)
            max_results: Maximum total results (None for all)

        Returns:
            List of post dictionaries
        """
        logger.info(f"Fetching posts for organization {organization_id}")

        org_urn = self.http_client.format_organization_urn(organization_id)

        params = {
            "q": "author",
            "author": org_urn,
            "sortBy": "LAST_MODIFIED",
        }

        headers = {"X-RestLi-Method": "FINDER"}

        posts = list(self.http_client.get_paginated(
            endpoint="posts",
            params=params,
            headers=headers,
            max_results=max_results,
            page_size=count,
        ))

        logger.info(f"Fetched {len(posts)} posts for organization {organization_id}")
        return posts

    def get_all_posts(
        self,
        organization_ids: Optional[List[str]] = None,
        max_results_per_org: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch posts for all organizations.

        Args:
            organization_ids: List of org IDs (default: all configured)
            max_results_per_org: Max results per organization

        Returns:
            List of all posts with organization_id added
        """
        org_ids = organization_ids or ORGANIZATION_IDS
        all_posts = []

        for org_id in org_ids:
            try:
                posts = self.get_posts(org_id, max_results=max_results_per_org)
                # Add organization_id to each post for reference
                for post in posts:
                    post["_organization_id"] = org_id
                all_posts.extend(posts)
            except Exception as e:
                logger.error(f"Failed to fetch posts for org {org_id}: {e}")

        return all_posts

    # ============================================================================
    # Share Statistics API
    # ============================================================================

    def get_share_statistics_aggregate(
        self,
        organization_id: str,
    ) -> Dict[str, Any]:
        """Fetch aggregate share statistics for an organization.

        Returns lifetime totals for all shares.

        Args:
            organization_id: LinkedIn organization ID

        Returns:
            Statistics dictionary with totalShareStatistics
        """
        logger.info(f"Fetching aggregate share statistics for org {organization_id}")

        org_urn = self.http_client.format_organization_urn(organization_id)

        params = {
            "q": "organizationalEntity",
            "organizationalEntity": org_urn,
        }

        response = self.http_client.get("organizationalEntityShareStatistics", params)
        elements = response.get("elements", [])

        if elements:
            return elements[0]
        return {}

    def get_share_statistics_by_posts(
        self,
        organization_id: str,
        post_urns: List[str],
    ) -> List[Dict[str, Any]]:
        """Fetch share statistics for specific posts.

        Uses LinkedIn List() format for batch requests.

        Args:
            organization_id: LinkedIn organization ID
            post_urns: List of post URNs (max ~20 per request)

        Returns:
            List of statistics per post
        """
        if not post_urns:
            return []

        logger.info(f"Fetching share statistics for {len(post_urns)} posts")

        org_urn = self.http_client.format_organization_urn(organization_id)
        all_stats = []

        # Process in batches of 20 to avoid URL length limits
        batch_size = 20
        for i in range(0, len(post_urns), batch_size):
            batch = post_urns[i:i + batch_size]

            # Separate ugcPosts and shares
            ugc_posts = [urn for urn in batch if "ugcPost" in urn]
            shares = [urn for urn in batch if "share" in urn]

            # Build raw query string with List() format
            # URNs must be fully URL encoded (including colons)
            query_parts = [
                f"q=organizationalEntity",
                f"organizationalEntity={quote(org_urn, safe='')}",
            ]

            # Add ugcPosts parameter using List() format
            if ugc_posts:
                list_param = self.http_client.build_list_param("ugcPosts", ugc_posts)
                if list_param:
                    query_parts.append(list_param)

            # Add shares parameter using List() format
            if shares:
                list_param = self.http_client.build_list_param("shares", shares)
                if list_param:
                    query_parts.append(list_param)

            raw_query = "&".join(query_parts)

            try:
                response = self.http_client.get(
                    "organizationalEntityShareStatistics",
                    raw_query_string=raw_query,
                )
                elements = response.get("elements", [])
                all_stats.extend(elements)
            except Exception as e:
                logger.error(f"Failed to fetch stats for batch: {e}")
                # Fallback: try individual requests
                all_stats.extend(self._get_stats_individually(organization_id, batch))

        return all_stats

    def _get_stats_individually(
        self,
        organization_id: str,
        post_urns: List[str],
    ) -> List[Dict[str, Any]]:
        """Fallback method to fetch statistics one post at a time.

        Args:
            organization_id: LinkedIn organization ID
            post_urns: List of post URNs

        Returns:
            List of statistics per post
        """
        logger.info(f"Falling back to individual stats requests for {len(post_urns)} posts")
        stats = []

        for urn in post_urns:
            try:
                stat = self.get_share_statistics_single(organization_id, urn)
                if stat:
                    stats.append(stat)
            except Exception as e:
                logger.warning(f"Failed to fetch stats for {urn}: {e}")

        return stats

    def get_share_statistics_single(
        self,
        organization_id: str,
        post_urn: str,
    ) -> Optional[Dict[str, Any]]:
        """Fetch share statistics for a single post.

        Args:
            organization_id: LinkedIn organization ID
            post_urn: Single post URN

        Returns:
            Statistics dictionary or None
        """
        org_urn = self.http_client.format_organization_urn(organization_id)

        # Determine param name based on URN type
        if "ugcPost" in post_urn:
            param_name = "ugcPosts"
        elif "share" in post_urn:
            param_name = "shares"
        else:
            return None

        # Build query with single post using List() format
        # URNs must be fully URL encoded (including colons)
        raw_query = (
            f"q=organizationalEntity"
            f"&organizationalEntity={quote(org_urn, safe='')}"
            f"&{param_name}=List({quote(post_urn, safe='')})"
        )

        try:
            response = self.http_client.get(
                "organizationalEntityShareStatistics",
                raw_query_string=raw_query,
            )
            elements = response.get("elements", [])
            return elements[0] if elements else None
        except Exception:
            return None

    # ============================================================================
    # Page Statistics API
    # ============================================================================

    def get_page_statistics(
        self,
        organization_id: str,
        time_bound: bool = False,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        granularity: str = "DAY",
    ) -> List[Dict[str, Any]]:
        """Fetch page statistics for an organization.

        Uses RestLi tuple format for timeIntervals when time_bound is True.

        Args:
            organization_id: LinkedIn organization ID
            time_bound: If True, fetch time-series data
            start_date: Start date for time-bound query
            end_date: End date for time-bound query
            granularity: Time granularity (DAY or MONTH)

        Returns:
            List of statistics entries
        """
        logger.info(f"Fetching page statistics for org {organization_id}")

        org_urn = self.http_client.format_organization_urn(organization_id)

        if time_bound:
            if not start_date:
                start_date = datetime.now() - timedelta(days=FOLLOWER_STATS_LOOKBACK_DAYS)
            if not end_date:
                end_date = datetime.now()

            start_ts = int(start_date.timestamp() * 1000)
            end_ts = int(end_date.timestamp() * 1000)

            # LinkedIn requires RestLi tuple format for timeIntervals
            raw_query = (
                f"q=organization"
                f"&organization={quote(org_urn, safe='')}"
                f"&timeIntervals=(timeRange:(start:{start_ts},end:{end_ts}),timeGranularityType:{granularity})"
            )

            response = self.http_client.get(
                "organizationPageStatistics",
                raw_query_string=raw_query,
            )
        else:
            params = {
                "q": "organization",
                "organization": org_urn,
            }
            response = self.http_client.get("organizationPageStatistics", params)

        elements = response.get("elements", [])

        # Add organization_id to each element
        for elem in elements:
            elem["_organization_id"] = organization_id

        return elements

    def get_all_page_statistics(
        self,
        organization_ids: Optional[List[str]] = None,
        time_bound: bool = False,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """Fetch page statistics for all organizations.

        Args:
            organization_ids: List of org IDs (default: all configured)
            time_bound: If True, fetch time-series data
            **kwargs: Additional arguments for get_page_statistics

        Returns:
            List of all page statistics
        """
        org_ids = organization_ids or ORGANIZATION_IDS
        all_stats = []

        for org_id in org_ids:
            try:
                stats = self.get_page_statistics(org_id, time_bound, **kwargs)
                all_stats.extend(stats)
            except Exception as e:
                logger.error(f"Failed to fetch page stats for org {org_id}: {e}")

        return all_stats

    # ============================================================================
    # Follower Statistics API
    # ============================================================================

    def get_follower_statistics(
        self,
        organization_id: str,
    ) -> Dict[str, Any]:
        """Fetch lifetime follower statistics for an organization.

        Returns demographics breakdown (by function, industry, seniority, etc.)

        Args:
            organization_id: LinkedIn organization ID

        Returns:
            Follower statistics dictionary
        """
        logger.info(f"Fetching follower statistics for org {organization_id}")

        org_urn = self.http_client.format_organization_urn(organization_id)

        params = {
            "q": "organizationalEntity",
            "organizationalEntity": org_urn,
        }

        response = self.http_client.get("organizationalEntityFollowerStatistics", params)
        elements = response.get("elements", [])

        if elements:
            elem = elements[0]
            elem["_organization_id"] = organization_id
            return elem
        return {}

    def get_follower_statistics_timebound(
        self,
        organization_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        granularity: str = "DAY",
    ) -> List[Dict[str, Any]]:
        """Fetch time-bound follower statistics (follower gains).

        Uses RestLi tuple format for timeIntervals parameter.

        Args:
            organization_id: LinkedIn organization ID
            start_date: Start date (default: FOLLOWER_STATS_LOOKBACK_DAYS ago)
            end_date: End date (default: now)
            granularity: Time granularity (DAY, WEEK, or MONTH)

        Returns:
            List of follower gain entries per time period
        """
        logger.info(f"Fetching time-bound follower statistics for org {organization_id}")

        if not start_date:
            start_date = datetime.now() - timedelta(days=FOLLOWER_STATS_LOOKBACK_DAYS)
        if not end_date:
            end_date = datetime.now()

        org_urn = self.http_client.format_organization_urn(organization_id)
        start_ts = int(start_date.timestamp() * 1000)
        end_ts = int(end_date.timestamp() * 1000)

        # LinkedIn requires RestLi tuple format for timeIntervals
        raw_query = (
            f"q=organizationalEntity"
            f"&organizationalEntity={quote(org_urn, safe='')}"
            f"&timeIntervals=(timeRange:(start:{start_ts},end:{end_ts}),timeGranularityType:{granularity})"
        )

        response = self.http_client.get(
            "organizationalEntityFollowerStatistics",
            raw_query_string=raw_query,
        )
        elements = response.get("elements", [])

        # Add organization_id to each element
        for elem in elements:
            elem["_organization_id"] = organization_id

        return elements

    def get_all_follower_statistics(
        self,
        organization_ids: Optional[List[str]] = None,
        include_timebound: bool = True,
        **kwargs,
    ) -> Dict[str, Any]:
        """Fetch follower statistics for all organizations.

        Args:
            organization_ids: List of org IDs (default: all configured)
            include_timebound: If True, also fetch time-bound data
            **kwargs: Additional arguments for time-bound query

        Returns:
            Dictionary with 'demographics' and optionally 'timebound' data
        """
        org_ids = organization_ids or ORGANIZATION_IDS

        demographics = []
        timebound = []

        for org_id in org_ids:
            try:
                # Lifetime demographics
                demo = self.get_follower_statistics(org_id)
                if demo:
                    demographics.append(demo)

                # Time-bound gains
                if include_timebound:
                    tb = self.get_follower_statistics_timebound(org_id, **kwargs)
                    timebound.extend(tb)

            except Exception as e:
                logger.error(f"Failed to fetch follower stats for org {org_id}: {e}")

        result = {"demographics": demographics}
        if include_timebound:
            result["timebound"] = timebound

        return result
