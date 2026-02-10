"""Facebook Ads Adapter Module.

This module provides a completely independent adapter for Facebook Marketing API.
It follows SOLID principles with no base class inheritance, using only protocol contracts.

Key Features:
- Independent implementation (no base classes)
- Protocol compliance (TokenProvider, DataSink)
- Complete type hints and docstrings
- Multiple ad account iteration
- Nested breakdown handling (actions, action_values)
- Targeting field extraction

Architecture:
- FacebookAdapter: Main adapter class
- FacebookHTTPClient: HTTP communication layer
- Protocol-based dependency injection
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from social.core.exceptions import APIError, ConfigurationError
from social.core.protocols import DataSink, TokenProvider
from social.platforms.facebook.constants import (
    DEFAULT_DATE_PRESET,
    FIELD_DEFINITIONS,
    MAX_DATE_RANGE_DAYS,
)
from social.platforms.facebook.http_client import FacebookHTTPClient


class FacebookAdapter:
    """Independent adapter for Facebook Marketing API.

    This adapter provides methods for extracting data from Facebook Ads API
    without inheriting from any base class. It uses protocol-based contracts
    for flexibility and testability.

    Attributes:
        token_provider: Provider for OAuth2 access tokens
        app_id: Facebook App ID
        app_secret: Facebook App Secret
        ad_account_ids: List of Facebook Ad Account IDs
        http_client: Facebook-specific HTTP client
        data_sink: Optional data sink for database queries
    """

    def __init__(
        self,
        token_provider: TokenProvider,
        app_id: str,
        app_secret: str,
        ad_account_ids: List[str],
        data_sink: Optional[DataSink] = None,
    ):
        """Initialize Facebook adapter.

        Args:
            token_provider: Provider for authentication tokens
            app_id: Facebook App ID
            app_secret: Facebook App Secret
            ad_account_ids: List of Facebook Ad Account IDs (e.g., ["act_123", "act_456"])
            data_sink: Optional data sink for database queries

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if not app_id or not app_secret:
            raise ConfigurationError(
                "Facebook Ads requires app_id and app_secret",
                details={"app_id": bool(app_id), "app_secret": bool(app_secret)},
            )

        if not ad_account_ids:
            raise ConfigurationError(
                "Facebook Ads requires at least one ad_account_id",
                details={"ad_account_ids": ad_account_ids},
            )

        self.token_provider = token_provider
        self.app_id = app_id
        self.app_secret = app_secret
        self.ad_account_ids = ad_account_ids
        self.data_sink = data_sink

        # Initialize HTTP client
        self.http_client = FacebookHTTPClient(
            token_provider=token_provider,
            app_id=app_id,
            app_secret=app_secret,
        )

        logger.info(f"FacebookAdapter initialized with {len(ad_account_ids)} accounts")

    def get_campaigns(self, account_id: str) -> List[Dict[str, Any]]:
        """Get campaigns for a specific account.

        Args:
            account_id: Facebook Ad Account ID

        Returns:
            List of campaign dictionaries with metadata

        Raises:
            APIError: If API request fails
        """
        logger.info(f"Fetching campaigns for account {account_id}")

        try:
            fields = FIELD_DEFINITIONS.get("fields_ads_campaign", [])
            params = {"date_preset": DEFAULT_DATE_PRESET}

            campaigns = self.http_client.get_campaigns(
                account_id=account_id,
                fields=fields,
                params=params,
            )

            logger.success(f"Retrieved {len(campaigns)} campaigns")
            return campaigns

        except Exception as e:
            logger.error(f"Failed to fetch campaigns for account {account_id}: {e}")
            raise APIError(
                f"Failed to fetch campaigns for account {account_id}",
                details={"account_id": account_id, "error": str(e)},
            )

    def get_ad_sets(self, account_id: str) -> List[Dict[str, Any]]:
        """Get ad sets for a specific account.

        Args:
            account_id: Facebook Ad Account ID

        Returns:
            List of ad set dictionaries with metadata

        Raises:
            APIError: If API request fails
        """
        logger.info(f"Fetching ad sets for account {account_id}")

        try:
            fields = FIELD_DEFINITIONS.get("fields_ads_adset", [])
            params = {"date_preset": DEFAULT_DATE_PRESET}

            ad_sets = self.http_client.get_ad_sets(
                account_id=account_id,
                fields=fields,
                params=params,
            )

            logger.success(f"Retrieved {len(ad_sets)} ad sets")
            return ad_sets

        except Exception as e:
            logger.error(f"Failed to fetch ad sets for account {account_id}: {e}")
            raise APIError(
                f"Failed to fetch ad sets for account {account_id}",
                details={"account_id": account_id, "error": str(e)},
            )

    def get_insights(
        self,
        account_id: str,
        date_range: Optional[str] = None,
        level: str = "ad",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Get insights (performance metrics) for a specific account.

        Args:
            account_id: Facebook Ad Account ID
            date_range: Date preset (e.g., "last_7d", "last_30d", "maximum")
            level: Aggregation level ("account", "campaign", "adset", "ad")
            start_date: Optional start date (for custom date ranges)
            end_date: Optional end date (for custom date ranges)

        Returns:
            List of insight dictionaries with performance metrics

        Raises:
            APIError: If API request fails
        """
        logger.info(f"Fetching insights for account {account_id} (level: {level})")

        try:
            fields = FIELD_DEFINITIONS.get("fields_ads_insight", [])

            # Handle "maximum" date preset with chunking
            if date_range == "maximum" or (start_date and end_date):
                # Calculate date range
                if not start_date:
                    end_date = end_date or datetime.now()
                    start_date = end_date - timedelta(days=MAX_DATE_RANGE_DAYS)

                logger.info(f"Using chunked date range: {start_date.date()} to {end_date.date()}")

                insights = self.http_client.get_insights_chunked(
                    account_id=account_id,
                    fields=fields,
                    start_date=start_date,
                    end_date=end_date,
                    params={
                        "level": level,
                        "action_attribution_windows": ["7d_click", "1d_view"],
                    },
                )
            else:
                # Use date preset
                date_preset = date_range or DEFAULT_DATE_PRESET
                params = {
                    "date_preset": date_preset,
                    "level": level,
                    "action_attribution_windows": ["7d_click", "1d_view"],
                }

                insights = self.http_client.get_insights(
                    account_id=account_id,
                    fields=fields,
                    params=params,
                )

            logger.success(f"Retrieved {len(insights)} insight records")
            return insights

        except Exception as e:
            logger.error(f"Failed to fetch insights for account {account_id}: {e}")
            raise APIError(
                f"Failed to fetch insights for account {account_id}",
                details={"account_id": account_id, "error": str(e)},
            )

    def get_insights_with_actions(
        self,
        account_id: str,
        date_range: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get insights with actions breakdown for conversion tracking.

        This method retrieves insights with the 'actions' and 'action_values'
        fields, which contain arrays of conversion events.

        Args:
            account_id: Facebook Ad Account ID
            date_range: Date preset (e.g., "last_7d", "last_14d")

        Returns:
            List of insight dictionaries with actions data

        Raises:
            APIError: If API request fails
        """
        logger.info(f"Fetching insights with actions for account {account_id}")

        try:
            # Fields specific to actions tracking
            fields = FIELD_DEFINITIONS.get("fields_ads_insight_actions", [])

            date_preset = date_range or DEFAULT_DATE_PRESET
            params = {
                "date_preset": date_preset,
                "level": "ad",
                "action_attribution_windows": ["7d_click", "1d_view"],
                "action_breakdowns": ["action_type"],
            }

            insights = self.http_client.get_insights(
                account_id=account_id,
                fields=fields,
                params=params,
            )

            logger.success(f"Retrieved {len(insights)} insight records with actions")
            return insights

        except Exception as e:
            logger.error(f"Failed to fetch insights with actions: {e}")
            raise APIError(
                f"Failed to fetch insights with actions for account {account_id}",
                details={"account_id": account_id, "error": str(e)},
            )

    def get_custom_conversions(self, account_id: str) -> List[Dict[str, Any]]:
        """Get custom conversion events for a specific account.

        Args:
            account_id: Facebook Ad Account ID

        Returns:
            List of custom conversion dictionaries

        Raises:
            APIError: If API request fails
        """
        logger.info(f"Fetching custom conversions for account {account_id}")

        try:
            fields = FIELD_DEFINITIONS.get("fields_custom_convers", [])

            conversions = self.http_client.get_custom_conversions(
                account_id=account_id,
                fields=fields,
            )

            logger.success(f"Retrieved {len(conversions)} custom conversions")
            return conversions

        except Exception as e:
            logger.error(f"Failed to fetch custom conversions: {e}")
            raise APIError(
                f"Failed to fetch custom conversions for account {account_id}",
                details={"account_id": account_id, "error": str(e)},
            )

    def get_audience_targeting(self, account_id: str) -> List[Dict[str, Any]]:
        """Get audience targeting information from ad sets.

        This method retrieves ad sets with the 'targeting' field and extracts
        the custom_audiences information.

        Args:
            account_id: Facebook Ad Account ID

        Returns:
            List of dictionaries with audience targeting data

        Raises:
            APIError: If API request fails
        """
        logger.info(f"Fetching audience targeting for account {account_id}")

        try:
            fields = FIELD_DEFINITIONS.get("fields_ads_audience_adset", [])
            params = {"date_preset": DEFAULT_DATE_PRESET}

            ad_sets = self.http_client.get_ad_sets(
                account_id=account_id,
                fields=fields,
                params=params,
            )

            # Extract audience targeting from ad sets
            audience_data = []
            for ad_set in ad_sets:
                # Skip ad sets without required fields
                adset_id = ad_set.get("id")
                if not adset_id:
                    logger.warning(f"Skipping ad set without id: {ad_set}")
                    continue

                targeting = ad_set.get("targeting", {})

                # Extract custom audiences if present
                custom_audiences = targeting.get("custom_audiences", [])

                for audience in custom_audiences:
                    audience_id = audience.get("id")
                    if not audience_id:
                        logger.warning(f"Skipping audience without id in adset {adset_id}")
                        continue

                    audience_data.append({
                        "campaign_id": ad_set.get("campaign_id"),
                        "adset_id": adset_id,
                        "audience_id": audience_id,
                        "name": audience.get("name"),
                    })

            logger.success(f"Retrieved {len(audience_data)} audience targeting records")
            return audience_data

        except Exception as e:
            logger.error(f"Failed to fetch audience targeting: {e}")
            raise APIError(
                f"Failed to fetch audience targeting for account {account_id}",
                details={"account_id": account_id, "error": str(e)},
            )

    def get_all_campaigns(
        self,
        date_preset: Optional[str] = None,
        fields: Optional[List[str]] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Get campaigns for all configured ad accounts.

        Args:
            date_preset: Date preset filter (e.g., "last_7d", "last_30d") - optional
            fields: List of fields to retrieve - optional
            **kwargs: Additional parameters (ignored for compatibility)

        Returns:
            List of campaign dictionaries from all accounts

        Raises:
            APIError: If any account fails
        """
        logger.info(f"Fetching campaigns for {len(self.ad_account_ids)} accounts (date_preset={date_preset})")

        all_campaigns = []
        failed_accounts = []

        for account_id in self.ad_account_ids:
            try:
                campaigns = self.get_campaigns(account_id)
                all_campaigns.extend(campaigns)
            except APIError as e:
                logger.error(f"Failed to fetch campaigns for account {account_id}: {e}")
                failed_accounts.append(account_id)
                continue

        if failed_accounts:
            logger.warning(f"Failed accounts: {failed_accounts}")

        logger.success(f"Retrieved {len(all_campaigns)} total campaigns from {len(self.ad_account_ids) - len(failed_accounts)} accounts")

        # Convert to DataFrame
        return pd.DataFrame(all_campaigns)

    def get_all_ad_sets(
        self,
        date_preset: Optional[str] = None,
        fields: Optional[List[str]] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Get ad sets for all configured ad accounts.

        Args:
            date_preset: Date preset filter (e.g., "last_7d", "last_30d") - optional
            fields: List of fields to retrieve - optional
            **kwargs: Additional parameters (ignored for compatibility)

        Returns:
            List of ad set dictionaries from all accounts

        Raises:
            APIError: If any account fails
        """
        logger.info(f"Fetching ad sets for {len(self.ad_account_ids)} accounts (date_preset={date_preset})")

        all_ad_sets = []
        failed_accounts = []

        for account_id in self.ad_account_ids:
            try:
                ad_sets = self.get_ad_sets(account_id)
                all_ad_sets.extend(ad_sets)
            except APIError as e:
                logger.error(f"Failed to fetch ad sets for account {account_id}: {e}")
                failed_accounts.append(account_id)
                continue

        if failed_accounts:
            logger.warning(f"Failed accounts: {failed_accounts}")

        logger.success(f"Retrieved {len(all_ad_sets)} total ad sets from {len(self.ad_account_ids) - len(failed_accounts)} accounts")

        # Convert to DataFrame
        return pd.DataFrame(all_ad_sets)

    def get_all_insights(
        self,
        date_range: Optional[str] = None,
        date_preset: Optional[str] = None,
        level: str = "ad",
        fields: Optional[List[str]] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Get insights for all configured ad accounts.

        Args:
            date_range: Date preset (e.g., "last_7d", "maximum")
            date_preset: Alternative param name for date_range (compatibility)
            level: Aggregation level
            fields: List of fields to retrieve - optional
            **kwargs: Additional parameters (ignored for compatibility)

        Returns:
            List of insight dictionaries from all accounts

        Raises:
            APIError: If any account fails
        """
        # Support both date_range and date_preset parameter names
        effective_date = date_preset or date_range

        logger.info(f"Fetching insights for {len(self.ad_account_ids)} accounts (date_preset={effective_date})")

        all_insights = []
        failed_accounts = []

        for account_id in self.ad_account_ids:
            try:
                insights = self.get_insights(account_id, effective_date, level)
                all_insights.extend(insights)
            except APIError as e:
                logger.error(f"Failed to fetch insights for account {account_id}: {e}")
                failed_accounts.append(account_id)
                continue

        if failed_accounts:
            logger.warning(f"Failed accounts: {failed_accounts}")

        logger.success(f"Retrieved {len(all_insights)} total insights from {len(self.ad_account_ids) - len(failed_accounts)} accounts")

        # Convert to DataFrame
        return pd.DataFrame(all_insights)

    def get_all_insights_with_actions(
        self,
        date_preset: Optional[str] = None,
        fields: Optional[List[str]] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Get insights with actions for all configured ad accounts.

        This is an alias for get_all_insights with actions fields included.

        Args:
            date_preset: Date preset filter (e.g., "last_7d", "last_14d")
            fields: List of fields to retrieve - optional
            **kwargs: Additional parameters

        Returns:
            List of insight dictionaries with actions from all accounts
        """
        logger.info(f"Fetching insights with actions (date_preset={date_preset})")
        return self.get_all_insights(date_preset=date_preset, fields=fields, **kwargs)

    def get_all_custom_conversions(
        self,
        date_preset: Optional[str] = None,
        fields: Optional[List[str]] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Get custom conversions for all configured ad accounts.

        Args:
            date_preset: Date preset filter - optional
            fields: List of fields to retrieve - optional
            **kwargs: Additional parameters (ignored for compatibility)

        Returns:
            List of custom conversion dictionaries from all accounts
        """
        logger.info(f"Fetching custom conversions for {len(self.ad_account_ids)} accounts")

        all_conversions = []
        failed_accounts = []

        for account_id in self.ad_account_ids:
            try:
                conversions = self.get_custom_conversions(account_id)
                all_conversions.extend(conversions)
            except APIError as e:
                logger.error(f"Failed to fetch custom conversions for account {account_id}: {e}")
                failed_accounts.append(account_id)
                continue

        if failed_accounts:
            logger.warning(f"Failed accounts: {failed_accounts}")

        logger.success(f"Retrieved {len(all_conversions)} total custom conversions from {len(self.ad_account_ids) - len(failed_accounts)} accounts")

        # Convert to DataFrame
        return pd.DataFrame(all_conversions)

    def get_all_audience_targeting(
        self,
        date_preset: Optional[str] = None,
        fields: Optional[List[str]] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Get audience targeting for all configured ad accounts.

        Args:
            date_preset: Date preset filter - optional
            fields: List of fields to retrieve - optional
            **kwargs: Additional parameters (ignored for compatibility)

        Returns:
            List of audience targeting dictionaries from all accounts
        """
        logger.info(f"Fetching audience targeting for {len(self.ad_account_ids)} accounts")

        all_audiences = []
        failed_accounts = []

        for account_id in self.ad_account_ids:
            try:
                audiences = self.get_audience_targeting(account_id)
                all_audiences.extend(audiences)
            except APIError as e:
                logger.error(f"Failed to fetch audience targeting for account {account_id}: {e}")
                failed_accounts.append(account_id)
                continue

        if failed_accounts:
            logger.warning(f"Failed accounts: {failed_accounts}")

        logger.success(f"Retrieved {len(all_audiences)} total audience targeting records from {len(self.ad_account_ids) - len(failed_accounts)} accounts")

        # Convert to DataFrame
        return pd.DataFrame(all_audiences)

    def close(self) -> None:
        """Close the HTTP client and release resources."""
        if self.http_client:
            self.http_client.close()
            logger.debug("FacebookAdapter closed")
