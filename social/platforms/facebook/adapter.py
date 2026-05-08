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

import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

from social.core.exceptions import APIError, ConfigurationError
from social.core.protocols import DataSink, TokenProvider
from social.platforms.facebook.constants import (
    DEFAULT_DATE_PRESET,
    DEFERRED_RETRY_BACKOFF_FACTOR,
    DEFERRED_RETRY_INITIAL_WAIT,
    DEFERRED_RETRY_PASSES,
    FIELD_DEFINITIONS,
    MAX_DATE_RANGE_DAYS,
    RATE_LIMIT_DELAY_SECONDS,
)
from social.platforms.facebook.http_client import FacebookHTTPClient


def _is_rate_limit_error(exc: Exception) -> bool:
    """Heuristic: does this error look like a transient rate-limit?

    We catch on the message because the SDK doesn't expose error_subcode
    in a stable place. Errors that match are eligible for deferred retry;
    everything else is propagated immediately.
    """
    s = str(exc).lower()
    return (
        "rate limit" in s
        or "too many" in s
        or "request limit reached" in s
        or "error_subcode" in s
        or "1504022" in s        # Application request limit reached
        or "is_transient" in s   # Meta marks transient failures explicitly
    )


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
        breakdowns: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Get insights (performance metrics) for a specific account.

        Args:
            account_id: Facebook Ad Account ID
            date_range: Date preset (e.g., "last_7d", "last_30d", "maximum")
            level: Aggregation level ("account", "campaign", "adset", "ad")
            breakdowns: Optional list of breakdown dimensions (e.g., ["age", "gender"], ["publisher_platform"])
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

                base_params = {
                    "level": level,
                    "action_attribution_windows": ["7d_click", "1d_view"],
                }
                if breakdowns:
                    base_params["breakdowns"] = breakdowns

                insights = self.http_client.get_insights_chunked(
                    account_id=account_id,
                    fields=fields,
                    start_date=start_date,
                    end_date=end_date,
                    params=base_params,
                )
            else:
                # Use date preset
                date_preset = date_range or DEFAULT_DATE_PRESET
                params = {
                    "date_preset": date_preset,
                    "level": level,
                    "action_attribution_windows": ["7d_click", "1d_view"],
                }
                if breakdowns:
                    params["breakdowns"] = breakdowns

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

    def _fetch_per_account_with_deferred_retry(
        self,
        fetch_fn: Callable[[str], List[Dict[str, Any]]],
        op_label: str,
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Iterate self.ad_account_ids calling fetch_fn(account_id), parking
        rate-limited accounts in a deferred queue and retrying them later.

        Pattern (mirrors http_client.get_insights_chunked):
          1. Pass 1: try every account once; rate-limit failures go to a queue.
          2. Pass 2..N: wait for the quota to refill (30s, 60s, 120s) and
             retry only the parked accounts.
          3. Anything still failing after N+1 passes is logged as ``ERROR``
             and propagated as "definitively failed" to the caller (which
             may then mark the table as partial in the run summary).

        Non-rate-limit errors propagate up immediately — only transient
        rate-limit hits are eligible for deferred retry.

        Args:
            fetch_fn: callable that takes an ad_account_id (with "act_"
                prefix) and returns a list of dicts.
            op_label: short tag for the log lines (e.g. "insights",
                "insights_with_actions"); helps when scanning logs.

        Returns:
            (all_rows, failed_accounts) — failed_accounts is the list of
            account ids that didn't recover even after all deferred passes.
        """
        all_rows: List[Dict[str, Any]] = []
        deferred: List[str] = []
        failed: List[str] = []

        # Pass 1: try every account
        for account_id in self.ad_account_ids:
            try:
                rows = fetch_fn(account_id)
                all_rows.extend(rows)
            except APIError as e:
                if _is_rate_limit_error(e):
                    deferred.append(account_id)
                    logger.warning(
                        f"[{op_label}] account {account_id} parked in deferred queue "
                        f"(rate limit, {len(deferred)} pending)"
                    )
                else:
                    logger.error(f"[{op_label}] account {account_id} non-transient failure: {e}")
                    failed.append(account_id)

        # Pass 2..N: deferred retries with progressively longer waits.
        # Same constants we use for chunked retries (30s, 60s, 120s).
        for pass_idx in range(DEFERRED_RETRY_PASSES):
            if not deferred:
                break
            wait = DEFERRED_RETRY_INITIAL_WAIT * (DEFERRED_RETRY_BACKOFF_FACTOR ** pass_idx)
            logger.warning(
                f"[{op_label}] deferred pass {pass_idx + 1}/{DEFERRED_RETRY_PASSES}: "
                f"{len(deferred)} account(s) to retry. Waiting {wait}s..."
            )
            time.sleep(wait)

            still_pending: List[str] = []
            for account_id in deferred:
                try:
                    rows = fetch_fn(account_id)
                    all_rows.extend(rows)
                    logger.success(
                        f"[{op_label}] deferred retry recovered account {account_id} "
                        f"({len(rows)} rows)"
                    )
                except APIError as e:
                    if _is_rate_limit_error(e):
                        still_pending.append(account_id)
                    else:
                        logger.error(
                            f"[{op_label}] account {account_id} hit a non-transient "
                            f"error during deferred retry, giving up: {e}"
                        )
                        failed.append(account_id)
                # tiny pause between accounts even in deferred passes
                time.sleep(RATE_LIMIT_DELAY_SECONDS)
            deferred = still_pending

        # Whatever is still in deferred after all passes is unrecoverable now.
        if deferred:
            logger.error(
                f"[{op_label}] {len(deferred)} account(s) still rate-limited after "
                f"{DEFERRED_RETRY_PASSES + 1} pass(es): {deferred}. "
                f"Their data will be picked up by the next scheduled run."
            )
            failed.extend(deferred)

        return all_rows, failed

    def get_all_insights(
        self,
        date_range: Optional[str] = None,
        date_preset: Optional[str] = None,
        level: str = "ad",
        breakdowns: Optional[List[str]] = None,
        fields: Optional[List[str]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Get insights for all configured ad accounts.

        Per-account rate-limit failures are parked in a deferred queue and
        retried with longer waits — see ``_fetch_per_account_with_deferred_retry``.

        Args:
            date_range: Date preset (e.g., "last_7d", "maximum")
            date_preset: Alternative param name for date_range (compatibility)
            level: Aggregation level
            breakdowns: Optional list of breakdown dimensions
            fields: Ignored (taken from FIELD_DEFINITIONS by adapter.get_insights)
            **kwargs: Additional parameters (ignored for compatibility)

        Returns:
            DataFrame with insight rows from all accessible accounts.
        """
        effective_date = date_preset or date_range
        logger.info(
            f"Fetching insights for {len(self.ad_account_ids)} accounts "
            f"(date_preset={effective_date}, breakdowns={breakdowns})"
        )

        def _fetch(account_id: str) -> List[Dict[str, Any]]:
            return self.get_insights(account_id, effective_date, level, breakdowns=breakdowns)

        all_insights, failed = self._fetch_per_account_with_deferred_retry(
            _fetch, op_label=f"insights[{breakdowns or 'no-breakdown'}]"
        )

        if failed:
            logger.warning(f"Failed accounts (definitive): {failed}")

        ok_count = len(self.ad_account_ids) - len(failed)
        logger.success(
            f"Retrieved {len(all_insights)} total insights from {ok_count} accounts"
        )
        return pd.DataFrame(all_insights)

    def get_all_insights_with_actions(
        self,
        date_preset: Optional[str] = None,
        fields: Optional[List[str]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Get insights with actions for all configured ad accounts.

        Calls ``get_insights_with_actions`` (which uses ``fields_ads_insight_actions``
        and adds the ``actions`` field with proper ``action_breakdowns``) once per
        account, accumulates the rows and returns a DataFrame. Previously this
        was an alias of ``get_all_insights`` and consequently never asked for the
        ``actions`` field, leaving ``fb_ads_insight_actions`` empty.

        Args:
            date_preset: Date preset filter (e.g., "last_7d", "last_14d").
            fields: Ignored — fields are taken from ``fields_ads_insight_actions``.
            **kwargs: Additional parameters (ignored, kept for compatibility).

        Returns:
            DataFrame with insight rows including the ``actions`` field.
        """
        logger.info(
            f"Fetching insights+actions for {len(self.ad_account_ids)} accounts "
            f"(date_preset={date_preset})"
        )

        def _fetch(account_id: str) -> List[Dict[str, Any]]:
            return self.get_insights_with_actions(account_id, date_preset)

        all_rows, failed = self._fetch_per_account_with_deferred_retry(
            _fetch, op_label="insights_with_actions"
        )

        if failed:
            logger.warning(f"Failed accounts (definitive): {failed}")

        ok_count = len(self.ad_account_ids) - len(failed)
        logger.success(
            f"Retrieved {len(all_rows)} total insights+actions rows from {ok_count} accounts"
        )
        return pd.DataFrame(all_rows)

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
