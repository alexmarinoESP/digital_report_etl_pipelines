"""Facebook Ads HTTP Client Module.

This module provides a completely independent HTTP client for Facebook Marketing API
using the official Facebook Business SDK. It handles authentication, API requests,
rate limiting, and response processing.

Key Features:
- Independent implementation (no base classes)
- Facebook Business SDK integration
- Exponential backoff for rate limiting
- Date chunking for large date ranges
- Object-to-dict conversion utilities

Architecture:
- FacebookHTTPClient: Main HTTP client class
- No inheritance, protocol-based contracts only
"""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from loguru import logger

from social.core.exceptions import APIError, AuthenticationError
from social.core.protocols import TokenProvider
from social.platforms.facebook.constants import (
    API_VERSION,
    BACKOFF_FACTOR,
    DATE_CHUNK_DAYS,
    MAX_RETRIES,
    RATE_LIMIT_DELAY_SECONDS,
)


class FacebookHTTPClient:
    """HTTP client for Facebook Marketing API using Facebook Business SDK.

    This client provides a complete interface to the Facebook Ads API,
    handling authentication, requests, rate limiting, and response processing.

    Unlike other platform clients, this uses the official Facebook Business SDK
    rather than direct REST API calls.

    Attributes:
        token_provider: Provider for Facebook access tokens
        app_id: Facebook App ID
        app_secret: Facebook App Secret
        api: Facebook Ads API instance
    """

    def __init__(
        self,
        token_provider: TokenProvider,
        app_id: str,
        app_secret: str,
    ):
        """Initialize Facebook HTTP client.

        Args:
            token_provider: Provider for Facebook access tokens
            app_id: Facebook App ID
            app_secret: Facebook App Secret

        Raises:
            AuthenticationError: If API initialization fails
        """
        self.token_provider = token_provider
        self.app_id = app_id
        self.app_secret = app_secret

        # Initialize Facebook Ads API
        try:
            access_token = token_provider.get_access_token()
            self.api = FacebookAdsApi.init(
                app_id=app_id,
                app_secret=app_secret,
                access_token=access_token,
            )
            logger.info(f"Facebook Ads API initialized (version: {API_VERSION})")
        except Exception as e:
            raise AuthenticationError(
                f"Failed to initialize Facebook Ads API: {str(e)}",
                details={"app_id": app_id, "error": str(e)},
            )

    def get_ad_account(self, account_id: str) -> AdAccount:
        """Get AdAccount object for a specific account.

        Args:
            account_id: Ad Account ID (format: "act_123456789")

        Returns:
            AdAccount object from Facebook SDK

        Raises:
            APIError: If account retrieval fails
        """
        try:
            # Ensure account ID has "act_" prefix
            if not account_id.startswith("act_"):
                account_id = f"act_{account_id}"

            return AdAccount(account_id, api=self.api)
        except Exception as e:
            raise APIError(
                f"Failed to get AdAccount: {str(e)}",
                details={"account_id": account_id, "error": str(e)},
            )

    def get_campaigns(
        self,
        account_id: str,
        fields: List[str],
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Get campaigns for a specific account.

        Args:
            account_id: Ad Account ID
            fields: List of fields to retrieve (strings or Field objects)
            params: Optional parameters (date_preset, filtering, etc.)

        Returns:
            List of campaign dictionaries

        Raises:
            APIError: If API request fails
        """
        logger.debug(f"Fetching campaigns for account {account_id}")

        try:
            account = self.get_ad_account(account_id)
            params = params or {}

            # Normalize fields (convert Field objects to strings)
            normalized_fields = self._normalize_fields(fields)

            # Execute API request with retry logic
            campaigns = self._execute_with_retry(
                lambda: account.get_campaigns(fields=normalized_fields, params=params)
            )

            # Convert SDK objects to dictionaries
            campaign_list = self._convert_to_dict_list(campaigns)

            logger.success(f"Retrieved {len(campaign_list)} campaigns")
            return campaign_list

        except Exception as e:
            logger.error(f"Failed to fetch campaigns: {e}")
            raise APIError(
                f"Failed to fetch campaigns for account {account_id}",
                details={"account_id": account_id, "error": str(e)},
            )

    def get_ad_sets(
        self,
        account_id: str,
        fields: List[str],
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Get ad sets for a specific account.

        Args:
            account_id: Ad Account ID
            fields: List of fields to retrieve (strings or Field objects)
            params: Optional parameters

        Returns:
            List of ad set dictionaries

        Raises:
            APIError: If API request fails
        """
        logger.debug(f"Fetching ad sets for account {account_id}")

        try:
            account = self.get_ad_account(account_id)
            params = params or {}

            # Normalize fields
            normalized_fields = self._normalize_fields(fields)

            # Execute API request with retry logic
            ad_sets = self._execute_with_retry(
                lambda: account.get_ad_sets(fields=normalized_fields, params=params)
            )

            # Convert SDK objects to dictionaries
            ad_set_list = self._convert_to_dict_list(ad_sets)

            logger.success(f"Retrieved {len(ad_set_list)} ad sets")
            return ad_set_list

        except Exception as e:
            logger.error(f"Failed to fetch ad sets: {e}")
            raise APIError(
                f"Failed to fetch ad sets for account {account_id}",
                details={"account_id": account_id, "error": str(e)},
            )

    def get_ads(
        self,
        account_id: str,
        fields: List[str],
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Get ads for a specific account.

        Args:
            account_id: Ad Account ID
            fields: List of fields to retrieve (strings or Field objects)
            params: Optional parameters

        Returns:
            List of ad dictionaries

        Raises:
            APIError: If API request fails
        """
        logger.debug(f"Fetching ads for account {account_id}")

        try:
            account = self.get_ad_account(account_id)
            params = params or {}

            # Normalize fields
            normalized_fields = self._normalize_fields(fields)

            # Execute API request with retry logic
            ads = self._execute_with_retry(
                lambda: account.get_ads(fields=normalized_fields, params=params)
            )

            # Convert SDK objects to dictionaries
            ad_list = self._convert_to_dict_list(ads)

            logger.success(f"Retrieved {len(ad_list)} ads")
            return ad_list

        except Exception as e:
            logger.error(f"Failed to fetch ads: {e}")
            raise APIError(
                f"Failed to fetch ads for account {account_id}",
                details={"account_id": account_id, "error": str(e)},
            )

    def get_insights(
        self,
        account_id: str,
        fields: List[str],
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Get insights (performance metrics) for a specific account.

        Args:
            account_id: Ad Account ID
            fields: List of fields to retrieve (strings or Field objects)
            params: Optional parameters (time_range, level, breakdowns, etc.)

        Returns:
            List of insight dictionaries with performance metrics

        Raises:
            APIError: If API request fails
        """
        logger.debug(f"Fetching insights for account {account_id}")

        try:
            account = self.get_ad_account(account_id)
            params = params or {}

            # Normalize fields
            normalized_fields = self._normalize_fields(fields)

            # Set default parameters for insights
            if "level" not in params:
                params["level"] = "ad"
            if "action_attribution_windows" not in params:
                params["action_attribution_windows"] = ["7d_click", "1d_view"]

            # Execute API request with retry logic
            insights = self._execute_with_retry(
                lambda: account.get_insights(fields=normalized_fields, params=params)
            )

            # Convert SDK objects to dictionaries
            insight_list = self._convert_to_dict_list(insights)

            logger.success(f"Retrieved {len(insight_list)} insight records")
            return insight_list

        except Exception as e:
            logger.error(f"Failed to fetch insights: {e}")
            raise APIError(
                f"Failed to fetch insights for account {account_id}",
                details={"account_id": account_id, "error": str(e)},
            )

    def get_insights_chunked(
        self,
        account_id: str,
        fields: List[str],
        start_date: datetime,
        end_date: datetime,
        chunk_days: int = DATE_CHUNK_DAYS,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Get insights with date chunking for large date ranges.

        Facebook API has data size limits, so large date ranges need to be
        split into smaller chunks.

        Args:
            account_id: Ad Account ID
            fields: List of fields to retrieve
            start_date: Start date for data
            end_date: End date for data
            chunk_days: Days per chunk (default: 90)
            params: Optional base parameters

        Returns:
            List of insight dictionaries from all chunks

        Raises:
            APIError: If any chunk request fails
        """
        logger.info(
            f"Fetching insights with chunking: {start_date.date()} to {end_date.date()}"
        )

        chunks = self._generate_date_chunks(start_date, end_date, chunk_days)
        all_insights = []

        for idx, chunk in enumerate(chunks):
            logger.info(f"Requesting chunk {idx + 1}/{len(chunks)}: {chunk['since']} to {chunk['until']}")

            # Build parameters for this chunk
            chunk_params = params.copy() if params else {}
            chunk_params["time_range"] = chunk
            chunk_params.setdefault("level", "ad")
            chunk_params.setdefault("action_attribution_windows", ["7d_click", "1d_view"])

            # Retry logic for rate-limited chunks
            retry_count = 0
            chunk_success = False

            while retry_count < MAX_RETRIES and not chunk_success:
                try:
                    insights = self.get_insights(account_id, fields, chunk_params)
                    all_insights.extend(insights)
                    logger.info(f"Chunk received with {len(insights)} records")
                    chunk_success = True

                    # Add rate limit delay between chunks (not after the last one)
                    if idx < len(chunks) - 1:
                        logger.debug(f"Waiting {RATE_LIMIT_DELAY_SECONDS}s before next chunk...")
                        time.sleep(RATE_LIMIT_DELAY_SECONDS)

                except APIError as e:
                    retry_count += 1

                    # Check if it's a transient rate limit error
                    error_msg = str(e)
                    is_rate_limit = "rate limit" in error_msg.lower() or "too many" in error_msg.lower() or "error_subcode" in error_msg.lower()

                    if is_rate_limit and retry_count < MAX_RETRIES:
                        # Exponential backoff: 10s, 20s, 40s
                        backoff_delay = RATE_LIMIT_DELAY_SECONDS * (BACKOFF_FACTOR ** (retry_count - 1))
                        logger.warning(
                            f"Rate limit hit for chunk {chunk['since']}-{chunk['until']}. "
                            f"Retry {retry_count}/{MAX_RETRIES} after {backoff_delay}s backoff..."
                        )
                        time.sleep(backoff_delay)
                    else:
                        logger.error(f"Failed to fetch chunk {chunk['since']}-{chunk['until']} after {retry_count} retries: {e}")
                        break  # Exit retry loop, continue to next chunk

        logger.success(f"Retrieved {len(all_insights)} total insight records from {len(chunks)} chunks")
        return all_insights

    def get_custom_conversions(
        self,
        account_id: str,
        fields: List[str],
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Get custom conversion events for a specific account.

        Args:
            account_id: Ad Account ID
            fields: List of fields to retrieve (strings or Field objects)
            params: Optional parameters

        Returns:
            List of custom conversion dictionaries

        Raises:
            APIError: If API request fails
        """
        logger.debug(f"Fetching custom conversions for account {account_id}")

        try:
            account = self.get_ad_account(account_id)
            params = params or {}

            # Normalize fields
            normalized_fields = self._normalize_fields(fields)

            # Execute API request with retry logic
            conversions = self._execute_with_retry(
                lambda: account.get_custom_conversions(fields=normalized_fields, params=params)
            )

            # Convert SDK objects to dictionaries
            conversion_list = self._convert_to_dict_list(conversions)

            logger.success(f"Retrieved {len(conversion_list)} custom conversions")
            return conversion_list

        except Exception as e:
            logger.error(f"Failed to fetch custom conversions: {e}")
            raise APIError(
                f"Failed to fetch custom conversions for account {account_id}",
                details={"account_id": account_id, "error": str(e)},
            )

    def _execute_with_retry(self, func: callable, max_retries: int = MAX_RETRIES) -> Any:
        """Execute API call with exponential backoff retry logic.

        Args:
            func: Function to execute (API call)
            max_retries: Maximum number of retry attempts

        Returns:
            Result from successful API call

        Raises:
            APIError: If all retries are exhausted
        """
        for attempt in range(max_retries):
            try:
                result = func()

                # Add rate limit delay after successful call
                time.sleep(RATE_LIMIT_DELAY_SECONDS)

                return result

            except Exception as e:
                if attempt < max_retries - 1:
                    # Calculate backoff delay
                    delay = RATE_LIMIT_DELAY_SECONDS * (BACKOFF_FACTOR ** attempt)
                    logger.warning(
                        f"API call failed (attempt {attempt + 1}/{max_retries}), "
                        f"retrying in {delay}s: {e}"
                    )
                    time.sleep(delay)
                else:
                    # Last attempt failed
                    raise APIError(
                        f"API call failed after {max_retries} attempts",
                        details={"error": str(e), "attempts": max_retries},
                    )

    def _generate_date_chunks(
        self,
        start_date: datetime,
        end_date: datetime,
        chunk_days: int = DATE_CHUNK_DAYS,
    ) -> List[Dict[str, str]]:
        """Generate date chunks for large date ranges.

        Args:
            start_date: Start date
            end_date: End date
            chunk_days: Days per chunk

        Returns:
            List of time_range dictionaries with 'since' and 'until' keys
        """
        chunks = []
        current_date = start_date

        while current_date < end_date:
            chunk_end = min(current_date + timedelta(days=chunk_days), end_date)
            chunks.append({
                "since": current_date.strftime("%Y-%m-%d"),
                "until": chunk_end.strftime("%Y-%m-%d"),
            })
            current_date = chunk_end + timedelta(days=1)

        logger.debug(f"Generated {len(chunks)} date chunks")
        return chunks

    def _normalize_fields(self, fields: List[Any]) -> List[str]:
        """Normalize field list to strings.

        Converts Facebook SDK Field objects to strings for API compatibility.
        Supports both string fields and SDK Field objects.

        Args:
            fields: List of field names (strings or Field objects)

        Returns:
            List of field names as strings
        """
        normalized = []
        for field in fields:
            if isinstance(field, str):
                normalized.append(field)
            elif hasattr(field, "name"):
                # SDK Field object with name attribute
                normalized.append(field.name)
            else:
                # Try to convert to string
                normalized.append(str(field))
        return normalized

    def _convert_to_dict_list(self, sdk_objects: Any) -> List[Dict[str, Any]]:
        """Convert Facebook SDK objects to list of dictionaries.

        Facebook Business SDK returns objects that need to be converted
        to dictionaries for DataFrame processing.

        Args:
            sdk_objects: FacebookResponse or iterable of SDK objects

        Returns:
            List of dictionaries with exported data
        """
        if not sdk_objects:
            return []

        result = []

        # Handle different response types
        if hasattr(sdk_objects, "__iter__"):
            for obj in sdk_objects:
                if hasattr(obj, "export_all_data"):
                    # SDK object with export method
                    result.append(obj.export_all_data())
                elif isinstance(obj, dict):
                    # Already a dictionary
                    result.append(obj)
                else:
                    # Try to convert to dict
                    result.append(dict(obj))
        elif hasattr(sdk_objects, "export_all_data"):
            # Single SDK object
            result.append(sdk_objects.export_all_data())
        elif isinstance(sdk_objects, dict):
            # Single dictionary
            result.append(sdk_objects)

        return result

    def close(self) -> None:
        """Close the HTTP client and release resources.

        Note: Facebook SDK doesn't require explicit cleanup,
        but this method is provided for consistency with other clients.
        """
        logger.debug("FacebookHTTPClient closed")
