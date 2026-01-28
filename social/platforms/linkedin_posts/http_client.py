"""LinkedIn Community Management API HTTP Client.

This module provides the HTTP client for LinkedIn Community Management API,
handling organic posts, page statistics, and follower data.

Key Features:
- Standard requests library (no special encoding needed)
- Version header injection (LinkedIn-Version)
- Pagination support
- Rate limiting awareness

Note: Unlike the LinkedIn Ads API, the Community Management API
uses standard URL encoding for all parameters.
"""

from typing import Dict, Any, Optional, List, Generator
from urllib.parse import quote
import requests
from loguru import logger

from social.platforms.linkedin_posts.constants import API_BASE_URL, API_VERSION


class LinkedInPostsHTTPClient:
    """HTTP client for LinkedIn Community Management API.

    This client handles organic posts and page statistics endpoints.
    Uses standard requests library without special parameter encoding.

    Attributes:
        access_token: OAuth2 access token
        timeout: Request timeout in seconds
    """

    def __init__(self, access_token: str, timeout: int = 60):
        """Initialize LinkedIn Posts HTTP client.

        Args:
            access_token: OAuth2 access token for authentication
            timeout: Request timeout in seconds (default: 60)
        """
        self.access_token = access_token
        self.timeout = timeout
        self._session = requests.Session()

        logger.debug(f"LinkedInPostsHTTPClient initialized with API version {API_VERSION}")

    def _build_headers(self, additional_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Build headers with LinkedIn version requirement.

        Args:
            additional_headers: Optional headers to merge

        Returns:
            Complete headers dictionary
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "LinkedIn-Version": API_VERSION,
            "X-Restli-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
        }

        if additional_headers:
            headers.update(additional_headers)

        return headers

    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        raw_query_string: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute GET request.

        Args:
            endpoint: API endpoint (will be appended to base URL)
            params: URL parameters
            headers: Additional headers
            raw_query_string: Raw query string to append (not URL encoded)

        Returns:
            Response data as dictionary

        Raises:
            requests.HTTPError: If request fails
        """
        url = f"{API_BASE_URL}/{endpoint}" if not endpoint.startswith("http") else endpoint
        complete_headers = self._build_headers(headers)

        # If raw_query_string is provided, append it directly to URL
        if raw_query_string:
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}{raw_query_string}"
            params = None  # Don't use params when using raw query

        logger.debug(f"GET {url}")
        if params:
            logger.debug(f"Params: {params}")

        try:
            response = self._session.get(
                url=url,
                params=params,
                headers=complete_headers,
                timeout=self.timeout,
            )

            response.raise_for_status()

            if response.content:
                return response.json()
            else:
                return {}

        except requests.HTTPError as e:
            logger.error(f"LinkedIn API request failed: {e}")
            # Try to log response body for debugging
            if e.response is not None:
                try:
                    logger.error(f"Response body: {e.response.text[:500]}")
                except Exception:
                    pass
            raise
        except Exception as e:
            logger.error(f"LinkedIn API request failed: {e}")
            raise

    def get_paginated(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        max_results: Optional[int] = None,
        page_size: int = 100,
    ) -> Generator[Dict[str, Any], None, None]:
        """Execute paginated GET requests.

        Yields elements from all pages until no more results or max_results reached.

        Args:
            endpoint: API endpoint
            params: URL parameters (start and count will be managed)
            headers: Additional headers
            max_results: Maximum total results to fetch (None for all)
            page_size: Number of results per page (default: 100)

        Yields:
            Individual elements from response
        """
        params = params or {}
        params["count"] = page_size
        start = 0
        total_fetched = 0

        while True:
            params["start"] = start

            response = self.get(endpoint, params, headers)
            elements = response.get("elements", [])

            if not elements:
                break

            for element in elements:
                yield element
                total_fetched += 1

                if max_results and total_fetched >= max_results:
                    return

            # Check if there are more results
            paging = response.get("paging", {})
            total = paging.get("total")

            if total and start + len(elements) >= total:
                break

            # Check for next link
            links = paging.get("links", [])
            has_next = any(link.get("rel") == "next" for link in links)

            if not has_next:
                break

            start += page_size

    def close(self):
        """Close the HTTP session."""
        if self._session:
            self._session.close()

    # ============================================================================
    # URN Formatting Utilities
    # ============================================================================

    @staticmethod
    def build_list_param(param_name: str, values: List[str]) -> str:
        """Build LinkedIn List() parameter format.

        LinkedIn REST API requires List() format for array parameters:
        ugcPosts=List(urn%3Ali%3AugcPost%3A123,urn%3Ali%3AugcPost%3A456)

        IMPORTANT: URN values must be fully URL encoded (including colons).

        Args:
            param_name: Parameter name (e.g., 'ugcPosts', 'shares')
            values: List of values to include

        Returns:
            Formatted query string segment
        """
        if not values:
            return ""
        # URL encode each URN value fully (including colons)
        encoded_values = ",".join(quote(v, safe='') for v in values)
        return f"{param_name}=List({encoded_values})"

    @staticmethod
    def format_organization_urn(org_id: str) -> str:
        """Format organization ID into URN.

        Args:
            org_id: Organization ID

        Returns:
            Formatted URN: urn:li:organization:123
        """
        return f"urn:li:organization:{org_id}"

    @staticmethod
    def extract_id_from_urn(urn: str) -> str:
        """Extract numeric ID from URN.

        Args:
            urn: Full URN (e.g., urn:li:ugcPost:123456)

        Returns:
            Numeric ID as string
        """
        if not urn:
            return ""
        parts = urn.split(":")
        return parts[-1] if parts else ""

    @staticmethod
    def get_post_type_from_urn(urn: str) -> str:
        """Determine post type from URN.

        Args:
            urn: Post URN

        Returns:
            Post type: 'ugcPost' or 'share'
        """
        if not urn:
            return "unknown"
        if "ugcPost" in urn:
            return "ugcPost"
        elif "share" in urn:
            return "share"
        return "unknown"
