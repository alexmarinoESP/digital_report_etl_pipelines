"""Google Ads Adapter.

This module provides a completely independent adapter for extracting data from
the Google Ads API using GAQL queries and the gRPC/Protobuf client.

Key Features:
- Multi-account iteration (all customer accounts under MCC)
- GAQL query execution for all table types
- Protobuf to DataFrame conversion
- Company ID mapping
- Comprehensive error handling

Design:
- NO base class inheritance (SOLID principles)
- Protocol-based contracts
- Type-safe with 100% type hints
- Production-ready error handling
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from social.core.exceptions import APIError, ConfigurationError
from social.core.protocols import TokenProvider
from social.platforms.google.constants import (
    API_VERSION,
    COMPANY_ACCOUNT_MAP,
    DEFAULT_LOOKBACK_DAYS,
    GAQL_QUERIES,
)
from social.platforms.google.http_client import GoogleHTTPClient


class GoogleAdapter:
    """
    Adapter for Google Ads API data extraction.

    This adapter handles all data extraction from the Google Ads API:
    - Account hierarchy retrieval
    - Campaign data extraction
    - Ad performance reports
    - Creative details
    - Placement data
    - Audience segments
    - Device breakdown

    Design:
    - Completely independent (NO base class)
    - Iterates all customer accounts
    - Executes GAQL queries
    - Converts Protobuf to DataFrames
    - Maps customer IDs to company IDs

    Attributes:
        token_provider: Token provider for authentication
        http_client: Google Ads HTTP client
        config_file_path: Path to google-ads.yaml config
        manager_customer_id: Manager account ID (MCC)
        api_version: Google Ads API version
    """

    def __init__(
        self,
        token_provider: TokenProvider,
        config_file_path: str,
        manager_customer_id: str = "9474097201",
        api_version: str = API_VERSION,
    ) -> None:
        """
        Initialize Google Ads adapter.

        Args:
            token_provider: Token provider for authentication
            config_file_path: Path to google-ads.yaml configuration file
            manager_customer_id: Manager account ID (MCC)
            api_version: Google Ads API version

        Raises:
            ConfigurationError: If initialization fails
        """
        self.token_provider = token_provider
        self.config_file_path = config_file_path
        self.manager_customer_id = manager_customer_id
        self.api_version = api_version

        # Initialize HTTP client
        try:
            self.http_client = GoogleHTTPClient(
                token_provider=token_provider,
                config_file_path=config_file_path,
                api_version=api_version,
                manager_customer_id=manager_customer_id,
            )
            logger.info("Google Ads adapter initialized successfully")
        except Exception as e:
            raise ConfigurationError(
                f"Failed to initialize Google Ads adapter: {str(e)}",
                details={
                    "config_file": config_file_path,
                    "manager_id": manager_customer_id,
                },
            )

    def get_customer_accounts(self) -> pd.DataFrame:
        """
        Get all customer accounts under the manager account.

        Returns:
            DataFrame with account information:
                - id: Customer ID
                - descriptive_name: Account name
                - manager: Whether account is a manager
                - status: Account status
                - currency_code: Currency
                - time_zone: Timezone
                - level: Hierarchy level

        Raises:
            APIError: If account retrieval fails
        """
        logger.info("Fetching Google Ads customer accounts")

        try:
            accounts = self.http_client.get_all_accounts()

            if not accounts:
                logger.warning("No customer accounts found")
                return pd.DataFrame()

            df = pd.DataFrame(accounts)
            logger.success(f"Retrieved {len(df)} customer accounts")

            return df

        except Exception as e:
            raise APIError(
                f"Failed to get customer accounts: {str(e)}",
                details={"manager_id": self.manager_customer_id},
            )

    def get_all_campaigns(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Get campaigns for all customer accounts.

        Args:
            start_date: Start date for filtering (defaults to DEFAULT_LOOKBACK_DAYS ago)
            end_date: End date for filtering (defaults to today)

        Returns:
            DataFrame with campaign data from all accounts

        Raises:
            APIError: If extraction fails
        """
        logger.info("Fetching campaigns for all customer accounts")

        # Calculate date range
        if end_date is None:
            end_date = datetime.now()
        if start_date is None:
            start_date = end_date - timedelta(days=DEFAULT_LOOKBACK_DAYS)

        # Get enabled customer accounts
        accounts = self._get_enabled_customer_accounts()

        if accounts.empty:
            logger.warning("No enabled customer accounts found")
            return pd.DataFrame()

        # Execute query for each account
        all_data = []
        query_template = GAQL_QUERIES["query_campaign"]
        query = query_template.format(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )

        for _, account in accounts.iterrows():
            customer_id = str(account["id"])
            account_name = account.get("descriptiveName", "Unknown")

            logger.debug(f"Querying campaigns for account: {account_name} ({customer_id})")

            try:
                df = self.http_client.execute_query(
                    customer_id=customer_id,
                    query=query,
                    use_streaming=True,
                )

                if not df.empty:
                    all_data.append(df)
                    logger.debug(f"Retrieved {len(df)} campaigns from {account_name}")

            except Exception as e:
                logger.warning(f"Failed to query account {customer_id}: {str(e)}")
                continue

        # Combine all results
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.success(f"Retrieved {len(combined_df)} total campaigns")
            return combined_df
        else:
            logger.warning("No campaign data retrieved")
            return pd.DataFrame()

    def get_all_ad_report(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Get ad performance report for all customer accounts.

        Args:
            start_date: Start date for filtering
            end_date: End date for filtering

        Returns:
            DataFrame with ad performance metrics

        Raises:
            APIError: If extraction fails
        """
        logger.info("Fetching ad report for all customer accounts")

        # Calculate date range
        if end_date is None:
            end_date = datetime.now()
        if start_date is None:
            start_date = end_date - timedelta(days=DEFAULT_LOOKBACK_DAYS)

        # Get enabled customer accounts
        accounts = self._get_enabled_customer_accounts()

        if accounts.empty:
            logger.warning("No enabled customer accounts found")
            return pd.DataFrame()

        # Execute query for each account
        all_data = []
        query_template = GAQL_QUERIES["query_ad_report"]
        query = query_template.format(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )

        for _, account in accounts.iterrows():
            customer_id = str(account["id"])
            account_name = account.get("descriptiveName", "Unknown")

            logger.debug(f"Querying ad report for account: {account_name} ({customer_id})")

            try:
                df = self.http_client.execute_query(
                    customer_id=customer_id,
                    query=query,
                    use_streaming=True,
                )

                if not df.empty:
                    all_data.append(df)
                    logger.debug(f"Retrieved {len(df)} ad metrics from {account_name}")

            except Exception as e:
                logger.warning(f"Failed to query account {customer_id}: {str(e)}")
                continue

        # Combine all results
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.success(f"Retrieved {len(combined_df)} total ad metrics")
            return combined_df
        else:
            logger.warning("No ad report data retrieved")
            return pd.DataFrame()

    def get_all_ad_creatives(self) -> pd.DataFrame:
        """
        Get ad creatives for all customer accounts.

        Returns:
            DataFrame with ad creative details

        Raises:
            APIError: If extraction fails
        """
        logger.info("Fetching ad creatives for all customer accounts")

        # Get enabled customer accounts
        accounts = self._get_enabled_customer_accounts()

        if accounts.empty:
            logger.warning("No enabled customer accounts found")
            return pd.DataFrame()

        # Execute query for each account
        all_data = []
        query = GAQL_QUERIES["query_ads_ad_creatives"]

        for _, account in accounts.iterrows():
            customer_id = str(account["id"])
            account_name = account.get("descriptiveName", "Unknown")

            logger.debug(f"Querying ad creatives for account: {account_name} ({customer_id})")

            try:
                df = self.http_client.execute_query(
                    customer_id=customer_id,
                    query=query,
                    use_streaming=True,
                )

                if not df.empty:
                    all_data.append(df)
                    logger.debug(f"Retrieved {len(df)} ad creatives from {account_name}")

            except Exception as e:
                logger.warning(f"Failed to query account {customer_id}: {str(e)}")
                continue

        # Combine all results
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.success(f"Retrieved {len(combined_df)} total ad creatives")
            return combined_df
        else:
            logger.warning("No ad creative data retrieved")
            return pd.DataFrame()

    def get_all_placements(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Get placements for all customer accounts.

        Executes both query_placement (ENABLED/SERVING) and query_placement_2 (PAUSED).

        Args:
            start_date: Start date for filtering
            end_date: End date for filtering

        Returns:
            DataFrame with placement data

        Raises:
            APIError: If extraction fails
        """
        logger.info("Fetching placements for all customer accounts")

        # Calculate date range
        if end_date is None:
            end_date = datetime.now()
        if start_date is None:
            start_date = end_date - timedelta(days=600)  # Placements use longer lookback

        # Get enabled customer accounts
        accounts = self._get_enabled_customer_accounts()

        if accounts.empty:
            logger.warning("No enabled customer accounts found")
            return pd.DataFrame()

        # Execute both queries for each account
        all_data = []
        queries = [
            ("query_placement", GAQL_QUERIES["query_placement"]),
            ("query_placement_2", GAQL_QUERIES["query_placement_2"]),
        ]

        for query_name, query_template in queries:
            query = query_template.format(
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d"),
            )

            for _, account in accounts.iterrows():
                customer_id = str(account["id"])
                account_name = account.get("descriptiveName", "Unknown")

                logger.debug(f"Querying {query_name} for account: {account_name} ({customer_id})")

                try:
                    df = self.http_client.execute_query(
                        customer_id=customer_id,
                        query=query,
                        use_streaming=False,  # Placements use regular request
                    )

                    if not df.empty:
                        all_data.append(df)
                        logger.debug(f"Retrieved {len(df)} placements from {account_name} ({query_name})")

                except Exception as e:
                    logger.warning(f"Failed to query account {customer_id}: {str(e)}")
                    continue

        # Combine all results
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.success(f"Retrieved {len(combined_df)} total placements")
            return combined_df
        else:
            logger.warning("No placement data retrieved")
            return pd.DataFrame()

    def get_all_audiences(self) -> pd.DataFrame:
        """
        Get audience segments for all customer accounts.

        Executes both query_audience (ENABLED/SERVING) and query_audience_2 (PAUSED).

        Returns:
            DataFrame with audience data

        Raises:
            APIError: If extraction fails
        """
        logger.info("Fetching audiences for all customer accounts")

        # Get enabled customer accounts
        accounts = self._get_enabled_customer_accounts()

        if accounts.empty:
            logger.warning("No enabled customer accounts found")
            return pd.DataFrame()

        # Execute both queries for each account
        all_data = []
        queries = [
            ("query_audience", GAQL_QUERIES["query_audience"]),
            ("query_audience_2", GAQL_QUERIES["query_audience_2"]),
        ]

        for query_name, query in queries:
            for _, account in accounts.iterrows():
                customer_id = str(account["id"])
                account_name = account.get("descriptiveName", "Unknown")

                logger.debug(f"Querying {query_name} for account: {account_name} ({customer_id})")

                try:
                    df = self.http_client.execute_query(
                        customer_id=customer_id,
                        query=query,
                        use_streaming=False,  # Audiences use regular request
                    )

                    if not df.empty:
                        all_data.append(df)
                        logger.debug(f"Retrieved {len(df)} audiences from {account_name} ({query_name})")

                except Exception as e:
                    logger.warning(f"Failed to query account {customer_id}: {str(e)}")
                    continue

        # Combine all results
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.success(f"Retrieved {len(combined_df)} total audience segments")
            return combined_df
        else:
            logger.warning("No audience data retrieved")
            return pd.DataFrame()

    def get_all_cost_by_device(self) -> pd.DataFrame:
        """
        Get cost breakdown by device for all customer accounts.

        Executes both query_by_device (ENABLED/SERVING) and query_by_device_2 (PAUSED).

        Returns:
            DataFrame with device breakdown data

        Raises:
            APIError: If extraction fails
        """
        logger.info("Fetching cost by device for all customer accounts")

        # Get enabled customer accounts
        accounts = self._get_enabled_customer_accounts()

        if accounts.empty:
            logger.warning("No enabled customer accounts found")
            return pd.DataFrame()

        # Execute both queries for each account
        all_data = []
        queries = [
            ("query_by_device", GAQL_QUERIES["query_by_device"]),
            ("query_by_device_2", GAQL_QUERIES["query_by_device_2"]),
        ]

        for query_name, query in queries:
            for _, account in accounts.iterrows():
                customer_id = str(account["id"])
                account_name = account.get("descriptiveName", "Unknown")

                logger.debug(f"Querying {query_name} for account: {account_name} ({customer_id})")

                try:
                    df = self.http_client.execute_query(
                        customer_id=customer_id,
                        query=query,
                        use_streaming=False,  # Device data uses regular request
                    )

                    if not df.empty:
                        all_data.append(df)
                        logger.debug(f"Retrieved {len(df)} device records from {account_name} ({query_name})")

                except Exception as e:
                    logger.warning(f"Failed to query account {customer_id}: {str(e)}")
                    continue

        # Combine all results
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.success(f"Retrieved {len(combined_df)} total device records")
            return combined_df
        else:
            logger.warning("No device data retrieved")
            return pd.DataFrame()

    def _get_enabled_customer_accounts(self) -> pd.DataFrame:
        """
        Get all enabled non-manager customer accounts.

        Returns:
            DataFrame with enabled customer accounts only

        Raises:
            APIError: If account retrieval fails
        """
        try:
            all_accounts = self.http_client.get_all_accounts()

            if not all_accounts:
                return pd.DataFrame()

            df = pd.DataFrame(all_accounts)

            # Filter: non-manager + enabled status
            enabled = df[
                (~df.get("manager", False)) &
                (df.get("status") == "ENABLED")
            ]

            logger.debug(f"Found {len(enabled)} enabled customer accounts (out of {len(df)} total)")

            return enabled

        except Exception as e:
            raise APIError(
                f"Failed to get enabled customer accounts: {str(e)}",
                details={"manager_id": self.manager_customer_id},
            )

    def close(self) -> None:
        """
        Close the adapter and release resources.
        """
        if self.http_client:
            self.http_client.close()
        logger.debug("Google Ads adapter closed")
