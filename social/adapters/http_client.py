"""HTTP client for making API requests with authentication.

This module provides a clean separation between HTTP communication
and business logic, following the Single Responsibility Principle.
"""

from typing import Dict, Any, Optional, List
from abc import ABC, abstractmethod
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from loguru import logger

from social.core.exceptions import APIError, AuthenticationError, RetryableError
from social.core.protocols import TokenProvider
from social.core.constants import (
    MAX_RETRIES,
    RETRY_BACKOFF_FACTOR,
    REQUEST_TIMEOUT_SECONDS,
    HTTPMethod,
)


class HTTPClient(ABC):
    """Abstract base class for HTTP clients."""

    @abstractmethod
    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Execute a GET request."""
        pass

    @abstractmethod
    def post(
        self,
        url: str,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Execute a POST request."""
        pass


class AuthenticatedHTTPClient(HTTPClient):
    """HTTP client with OAuth 2.0 authentication and retry logic.

    This client handles:
    - Automatic token injection into headers
    - Token refresh on 401 errors
    - Automatic retries with exponential backoff
    - Request/response logging
    """

    def __init__(
        self,
        token_provider: TokenProvider,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
        max_retries: int = MAX_RETRIES,
    ):
        """Initialize the HTTP client.

        Args:
            token_provider: Provider for authentication tokens
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
        """
        self.token_provider = token_provider
        self.timeout = timeout
        self.max_retries = max_retries
        self._session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry configuration.

        Returns:
            Configured requests Session
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=RETRY_BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _build_headers(self, additional_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Build request headers including authentication.

        Args:
            additional_headers: Optional headers to merge

        Returns:
            Complete headers dictionary
        """
        headers = {
            "Authorization": f"Bearer {self.token_provider.get_access_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        if additional_headers:
            headers.update(additional_headers)

        return headers

    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Execute a GET request.

        Args:
            url: Request URL
            params: Query parameters
            headers: Additional headers

        Returns:
            Response data as dictionary

        Raises:
            APIError: If request fails
            AuthenticationError: If authentication fails
        """
        return self._request(HTTPMethod.GET.value, url, params=params, headers=headers)

    def post(
        self,
        url: str,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Execute a POST request.

        Args:
            url: Request URL
            data: Request body data
            headers: Additional headers

        Returns:
            Response data as dictionary

        Raises:
            APIError: If request fails
            AuthenticationError: If authentication fails
        """
        return self._request(HTTPMethod.POST.value, url, json=data, headers=headers)

    def _request(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        retry_count: int = 0,
    ) -> Dict[str, Any]:
        """Execute an HTTP request with error handling and retries.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            params: Query parameters
            json: JSON body data
            headers: Additional headers
            retry_count: Current retry attempt

        Returns:
            Response data as dictionary

        Raises:
            APIError: If request fails after all retries
            AuthenticationError: If token refresh fails
        """
        # Build complete headers
        complete_headers = self._build_headers(headers)

        # Log request (without sensitive data)
        logger.debug(f"{method} {url}")
        if params:
            logger.debug(f"Params: {self._sanitize_log_data(params)}")

        try:
            response = self._session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                headers=complete_headers,
                timeout=self.timeout,
            )

            # Handle 401 - token expired
            if response.status_code == 401:
                if retry_count < 1:  # Only retry once for auth errors
                    logger.info("Access token expired, refreshing...")
                    try:
                        self.token_provider.refresh_access_token()
                        return self._request(method, url, params, json, headers, retry_count + 1)
                    except Exception as e:
                        raise AuthenticationError(
                            "Failed to refresh access token",
                            details={"error": str(e)}
                        )
                else:
                    raise AuthenticationError("Authentication failed after token refresh")

            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                raise RetryableError(
                    "Rate limit exceeded",
                    retry_after=retry_after,
                    details={"url": url, "method": method}
                )

            # Check for other errors
            response.raise_for_status()

            # Parse response
            if response.content:
                try:
                    return response.json()
                except ValueError as e:
                    raise APIError(
                        "Invalid JSON in response",
                        status_code=response.status_code,
                        response_body=response.text[:500],
                        details={"error": str(e)}
                    )
            else:
                return {}

        except requests.exceptions.Timeout as e:
            raise APIError(
                f"Request timeout after {self.timeout}s",
                details={"url": url, "error": str(e)}
            )
        except requests.exceptions.ConnectionError as e:
            raise APIError(
                "Connection error",
                details={"url": url, "error": str(e)}
            )
        except requests.exceptions.HTTPError as e:
            # Parse error response if available
            error_message = "HTTP error"
            error_details = {}

            try:
                if e.response.content:
                    error_data = e.response.json()
                    error_message = error_data.get("message", error_message)
                    error_details = error_data
            except:
                pass

            raise APIError(
                error_message,
                status_code=e.response.status_code,
                response_body=e.response.text[:500] if hasattr(e.response, 'text') else None,
                details=error_details
            )

    def get_paginated(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch all pages from a paginated endpoint.

        This is a generic implementation that can be overridden for
        platform-specific pagination logic.

        Args:
            url: Request URL
            params: Query parameters
            headers: Additional headers
            page_size: Items per page
            max_pages: Maximum pages to fetch (None = all)

        Returns:
            List of all items from all pages

        Raises:
            APIError: If any request fails
        """
        all_items = []
        page = 0
        params = params or {}
        params["count"] = page_size

        while True:
            params["start"] = page * page_size

            response = self.get(url, params=params, headers=headers)

            # Extract items (platform-specific field names)
            items = response.get("elements", response.get("data", []))
            if not items:
                break

            all_items.extend(items)

            # Check if more pages available
            paging = response.get("paging", {})
            if not paging.get("links", {}).get("next"):
                break

            page += 1
            if max_pages and page >= max_pages:
                break

        return all_items

    @staticmethod
    def _sanitize_log_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Remove sensitive data from logs.

        Args:
            data: Data to sanitize

        Returns:
            Sanitized copy of data
        """
        sensitive_keys = {"access_token", "refresh_token", "password", "secret", "api_key"}
        sanitized = {}

        for key, value in data.items():
            if any(sk in key.lower() for sk in sensitive_keys):
                sanitized[key] = "***REDACTED***"
            else:
                sanitized[key] = value

        return sanitized
