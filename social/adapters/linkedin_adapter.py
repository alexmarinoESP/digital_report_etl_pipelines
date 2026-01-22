"""LinkedIn Ads platform adapter implementation.

This module implements the LinkedIn-specific adapter for extracting
advertising data using the LinkedIn Marketing API v202509.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import pandas as pd
from loguru import logger

from social.adapters.base import BaseAdsPlatformAdapter
from social.adapters.linkedin_http_client import LinkedInHTTPClient
from social.core.protocols import TokenProvider, DataSink
from social.core.config import PlatformConfig, TableConfig
from social.core.exceptions import APIError, ConfigurationError
from social.core.constants import TABLE_DEPENDENCIES, INSIGHTS_LOOKBACK_DAYS
from social.domain.models import DateRange
from social.domain.services import CompanyMappingService, DateRangeCalculator, URNExtractor
from social.processing.factory import ProcessingStrategyFactory
from social.processing.pipeline import DataProcessingPipeline


class LinkedInAdsAdapter(BaseAdsPlatformAdapter):
    """Adapter for LinkedIn Marketing API v202509.

    This adapter handles:
    - LinkedIn-specific API authentication and requests
    - Table dependency management (insights need campaigns, etc.)
    - Data transformation using processing pipeline
    - URN-based data dependencies (insights/creatives need campaign URNs)
    """

    def __init__(
        self,
        config: PlatformConfig,
        token_provider: TokenProvider,
        data_sink: Optional[DataSink] = None,
    ):
        """Initialize LinkedIn adapter.

        Args:
            config: Platform configuration
            token_provider: Provider for authentication tokens
            data_sink: Optional data sink for database queries
        """
        super().__init__(config, token_provider, data_sink)

        # Initialize HTTP client
        self.http_client = LinkedInHTTPClient(token_provider)

        # Initialize domain services
        self.company_mapping = CompanyMappingService(
            account_to_company=config.account_to_company_mapping,
            default_company_id=config.default_company_id
        )
        self.urn_extractor = URNExtractor()
        self.date_calculator = DateRangeCalculator()

        # Initialize processing factory and pipeline
        self.processing_factory = ProcessingStrategyFactory(
            company_mapping_service=self.company_mapping,
            urn_extractor=self.urn_extractor
        )

    def extract_table(
        self,
        table_name: str,
        date_range: Optional[DateRange] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Extract data for a specific LinkedIn Ads table.

        Args:
            table_name: Name of the table to extract
            date_range: Optional date range for time-series data
            **kwargs: Additional parameters

        Returns:
            DataFrame with extracted and processed data

        Raises:
            APIError: If API request fails
            ConfigurationError: If table not configured
        """
        logger.info(f"Extracting LinkedIn table: {table_name}")

        # Get table configuration
        table_config = self.get_table_config(table_name)

        # Handle tables with URN dependencies
        if table_name == "linkedin_ads_insights":
            return self._extract_insights(table_config, date_range)
        elif table_name == "linkedin_ads_creative":
            return self._extract_creatives(table_config)
        else:
            # Standard extraction
            return self._extract_standard_table(table_config)

    def get_table_dependencies(self, table_name: str) -> List[str]:
        """Get list of tables that must be extracted before this one.

        Args:
            table_name: Name of the table

        Returns:
            List of dependency table names
        """
        # Check predefined dependencies
        return TABLE_DEPENDENCIES.get(table_name, [])

    def _extract_standard_table(self, table_config: TableConfig) -> pd.DataFrame:
        """Extract a standard table (account, campaign, audience, etc.).

        Args:
            table_config: Table configuration

        Returns:
            Processed DataFrame

        Raises:
            APIError: If extraction fails
        """
        all_data = []

        # Determine if we need to query per account
        needs_account_iteration = self._table_needs_account_iteration(table_config.name)

        if needs_account_iteration:
            # Query each account separately
            for account_id in self.company_mapping.get_all_mappings().keys():
                try:
                    data = self._fetch_table_for_account(table_config, account_id)
                    if data:
                        all_data.extend(data)
                except Exception as e:
                    logger.error(f"Failed to fetch {table_config.name} for account {account_id}: {e}")
                    continue
        else:
            # Single query for all accounts
            data = self._fetch_table_data(table_config)
            if data:
                all_data.extend(data)

        if not all_data:
            logger.warning(f"No data retrieved for {table_config.name}")
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(all_data)

        # Apply processing pipeline
        df = self._process_data(df, table_config)

        return df

    def _fetch_table_data(self, table_config: TableConfig) -> List[Dict[str, Any]]:
        """Fetch data for a table using configured endpoint.

        Args:
            table_config: Table configuration

        Returns:
            List of records

        Raises:
            APIError: If request fails
        """
        # Build request components
        url = self._build_request_url(table_config)
        headers = self._build_request_headers(table_config)
        params = self._build_request_params(table_config)

        # Execute request
        try:
            if table_config.request_type == "GET":
                response = self.http_client.get(url, params=params, headers=headers)
            else:
                raise ConfigurationError(f"Unsupported request type: {table_config.request_type}")

            # Parse response
            return self._parse_response(response, table_config.name)

        except Exception as e:
            logger.error(f"Failed to fetch {table_config.name}: {e}")
            raise APIError(
                f"Failed to fetch {table_config.name}",
                details={"error": str(e), "url": url}
            )

    def _fetch_table_for_account(
        self,
        table_config: TableConfig,
        account_id: str
    ) -> List[Dict[str, Any]]:
        """Fetch data for a specific account.

        Args:
            table_config: Table configuration
            account_id: Account ID to query

        Returns:
            List of records for this account
        """
        # Build URL with account ID
        url = self._build_request_url(table_config, account_id=account_id)
        headers = self._build_request_headers(table_config)
        params = self._build_request_params(table_config)

        # For audience, add account URN parameter
        if table_config.name == "linkedin_ads_audience":
            account_urn = self.http_client.format_account_urn(account_id)
            params["accounts"] = account_urn

        # Execute request
        response = self.http_client.get(url, params=params, headers=headers)
        return self._parse_response(response, table_config.name)

    def _extract_insights(
        self,
        table_config: TableConfig,
        date_range: Optional[DateRange] = None
    ) -> pd.DataFrame:
        """Extract insights data (requires campaign URNs from database).

        Args:
            table_config: Table configuration
            date_range: Optional date range (uses default lookback if None)

        Returns:
            Processed DataFrame with insights

        Raises:
            APIError: If extraction fails
        """
        if not self.data_sink:
            raise ConfigurationError(
                "Data sink required to fetch campaign URNs for insights extraction"
            )

        # Get campaign URNs from database
        campaign_urns = self._get_campaign_urns_from_db()

        if campaign_urns.empty:
            logger.warning("No campaigns found in database for insights extraction")
            return pd.DataFrame()

        logger.info(f"Fetching insights for {len(campaign_urns)} campaigns")

        # Calculate date range
        if date_range is None:
            date_range = self.date_calculator.get_insights_date_range(INSIGHTS_LOOKBACK_DAYS)

        all_insights = []

        # Fetch insights for each campaign
        for campaign_id in campaign_urns["id"]:
            try:
                insights = self._fetch_insights_for_campaign(
                    table_config,
                    campaign_id,
                    date_range
                )
                if insights:
                    all_insights.extend(insights)
            except Exception as e:
                logger.error(f"Failed to fetch insights for campaign {campaign_id}: {e}")
                continue

        if not all_insights:
            logger.warning("No insights data retrieved")
            return pd.DataFrame()

        df = pd.DataFrame(all_insights)
        df = self._process_data(df, table_config)

        return df

    def _fetch_insights_for_campaign(
        self,
        table_config: TableConfig,
        campaign_id: str,
        date_range: DateRange
    ) -> List[Dict[str, Any]]:
        """Fetch insights for a single campaign.

        Args:
            table_config: Table configuration
            campaign_id: Campaign ID
            date_range: Date range for insights

        Returns:
            List of insight records
        """
        url = self._build_request_url(table_config)
        headers = self._build_request_headers(table_config)

        # Build regular (URL-encoded) parameters
        params = {
            "q": "analytics",
            "pivot": "CREATIVE",
            "timeGranularity": "DAILY",
        }

        # Build non-encoded parameters (LinkedIn special format)
        campaign_urn_param = self.http_client.format_campaign_urns_for_insights([campaign_id])
        date_param = self.http_client.format_date_range(
            date_range.start_date.year,
            date_range.start_date.month,
            date_range.start_date.day
        )

        no_encoded_params = {
            "campaigns": campaign_urn_param,
            "dateRange": date_param,
        }

        # Add fields if configured
        if table_config.fields:
            fields_str = self.http_client.format_fields(table_config.fields)
            no_encoded_params["fields"] = fields_str

        # Execute request with special parameter handling
        response = self.http_client.get_with_special_params(
            url=url,
            params=params,
            no_encoded_params=no_encoded_params,
            headers=headers
        )

        return self._parse_response(response, table_config.name)

    def _extract_creatives(self, table_config: TableConfig) -> pd.DataFrame:
        """Extract creative data (requires creative URNs from insights).

        Args:
            table_config: Table configuration

        Returns:
            Processed DataFrame with creatives
        """
        if not self.data_sink:
            raise ConfigurationError(
                "Data sink required to fetch creative URNs for creatives extraction"
            )

        # Get creative URNs from insights table
        creative_urns = self._get_creative_urns_from_db()

        if creative_urns.empty:
            logger.warning("No creatives found in insights table")
            return pd.DataFrame()

        logger.info(f"Fetching data for {len(creative_urns)} creatives")

        all_creatives = []

        # Fetch each creative
        for account_id in self.company_mapping.get_all_mappings().keys():
            for creative_id in creative_urns["id"]:
                try:
                    creative_data = self._fetch_creative(
                        table_config,
                        account_id,
                        creative_id
                    )
                    if creative_data:
                        all_creatives.append(creative_data)
                except Exception as e:
                    logger.debug(f"Creative {creative_id} not found in account {account_id}: {e}")
                    continue

        if not all_creatives:
            logger.warning("No creative data retrieved")
            return pd.DataFrame()

        df = pd.DataFrame(all_creatives)
        df = self._process_data(df, table_config)

        return df

    def _fetch_creative(
        self,
        table_config: TableConfig,
        account_id: str,
        creative_id: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single creative.

        Args:
            table_config: Table configuration
            account_id: Account ID
            creative_id: Creative ID

        Returns:
            Creative data or None if not found
        """
        # Build URL with creative URN in path
        creative_urn = self.http_client.format_creative_urn_encoded(creative_id)
        url = self._build_request_url(
            table_config,
            account_id=account_id,
            creative_id=creative_urn
        )

        headers = self._build_request_headers(table_config)

        try:
            response = self.http_client.get(url, params={}, headers=headers)
            # Response is a single object, not a list
            return response if response else None
        except APIError as e:
            if hasattr(e, 'status_code') and e.status_code == 404:
                return None
            raise

    def _get_campaign_urns_from_db(self) -> pd.DataFrame:
        """Query database for campaign URNs for insights.

        Returns:
            DataFrame with campaign IDs
        """
        table_suffix = "_TEST" if hasattr(self.data_sink, 'test_mode') and self.data_sink.test_mode else ""

        query = f"""
            SELECT DISTINCT id
            FROM esp_digital_report.linkedin_ads_campaign{table_suffix}
            WHERE ROW_LOADED_DATE >= CURRENT_DATE - 150
        """

        return self.data_sink.query(query)

    def _get_creative_urns_from_db(self) -> pd.DataFrame:
        """Query database for creative URNs from insights.

        Returns:
            DataFrame with creative IDs
        """
        table_suffix = "_TEST" if hasattr(self.data_sink, 'test_mode') and self.data_sink.test_mode else ""

        query = f"""
            SELECT DISTINCT creative_id AS id
            FROM esp_digital_report.linkedin_ads_insights{table_suffix}
            WHERE ROW_LOADED_DATE >= CURRENT_DATE - 150
            AND creative_id IS NOT NULL
        """

        return self.data_sink.query(query)

    def _table_needs_account_iteration(self, table_name: str) -> bool:
        """Check if table needs to be queried per account.

        Args:
            table_name: Table name

        Returns:
            True if needs per-account iteration
        """
        # Tables that need per-account queries
        per_account_tables = [
            "linkedin_ads_campaign",
            "linkedin_ads_audience",
        ]

        return table_name in per_account_tables

    def _build_request_url(
        self,
        table_config: TableConfig,
        account_id: Optional[str] = None,
        creative_id: Optional[str] = None
    ) -> str:
        """Build the complete API request URL.

        Args:
            table_config: Table configuration
            account_id: Optional account ID for URL placeholder
            creative_id: Optional creative ID for URL placeholder

        Returns:
            Complete URL string
        """
        base_url = self.config.api_base_url
        endpoint = table_config.endpoint

        # Format placeholders in endpoint
        if "{}" in endpoint:
            # Count placeholders
            placeholder_count = endpoint.count("{}")

            if placeholder_count == 1:
                # Base URL only
                return endpoint.format(base_url)
            elif placeholder_count == 2:
                # Base URL + account ID
                if account_id is None:
                    raise ConfigurationError(f"Account ID required for endpoint: {endpoint}")
                return endpoint.format(base_url, account_id)
            elif placeholder_count == 3:
                # Base URL + account ID + creative ID
                if account_id is None or creative_id is None:
                    raise ConfigurationError(f"Account ID and creative ID required for endpoint: {endpoint}")
                return endpoint.format(base_url, account_id, creative_id)

        return f"{base_url}{endpoint}"

    def _build_request_headers(self, table_config: TableConfig) -> Dict[str, str]:
        """Build HTTP headers for the request.

        Args:
            table_config: Table configuration

        Returns:
            Dictionary of HTTP headers
        """
        # LinkedIn HTTP client adds required headers automatically
        return {}

    def _build_request_params(self, table_config: TableConfig, **kwargs) -> Dict[str, Any]:
        """Build query parameters for the request.

        Args:
            table_config: Table configuration
            **kwargs: Additional parameters

        Returns:
            Dictionary of query parameters
        """
        params = {}

        # Add configured parameters
        for key, value in table_config.additional_params.items():
            # Skip internal parameters
            skip_params = ['request', 'type', 'processing', 'query', 'update',
                          'merge', 'avoid_request', 'pagination', 'nested_element']
            if key not in skip_params:
                params[key] = value

        # Add fields if configured
        if table_config.fields:
            params["fields"] = ",".join(table_config.fields)

        # Add search parameter if needed
        if "q" in table_config.additional_params:
            params["q"] = table_config.additional_params["q"]

        return params

    def _parse_response(self, response: Dict[str, Any], table_name: str) -> List[Dict[str, Any]]:
        """Parse API response into list of records.

        Args:
            response: Raw API response
            table_name: Name of the table being extracted

        Returns:
            List of record dictionaries
        """
        # LinkedIn API wraps results in 'elements' key
        if "elements" in response:
            return response["elements"]

        # Some responses are already lists
        if isinstance(response, list):
            return response

        # Single object response
        if isinstance(response, dict) and response:
            return [response]

        return []

    def _process_data(
        self,
        df: pd.DataFrame,
        table_config: TableConfig
    ) -> pd.DataFrame:
        """Process raw data into clean DataFrame using processing pipeline.

        Args:
            df: Raw DataFrame
            table_config: Table configuration with processing steps

        Returns:
            Cleaned and transformed DataFrame
        """
        if df.empty:
            return df

        # Create processing pipeline
        pipeline = DataProcessingPipeline(self.processing_factory)

        # Add steps from configuration
        if table_config.processing_steps:
            pipeline.add_steps_from_config(table_config.processing_steps)

        # Apply processing
        try:
            return pipeline.process(df)
        except Exception as e:
            logger.error(f"Failed to process data for {table_config.name}: {e}")
            raise
