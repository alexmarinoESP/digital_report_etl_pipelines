"""Google Ads platform adapter implementation.

This module implements the Google Ads-specific adapter for extracting
advertising data using the Google Ads API.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import pandas as pd
from loguru import logger

from social.adapters.base import BaseAdsPlatformAdapter
from social.adapters.google_http_client import GoogleAdsHTTPClient
from social.core.protocols import TokenProvider, DataSink
from social.core.config import PlatformConfig, TableConfig
from social.core.exceptions import APIError, ConfigurationError
from social.domain.models import DateRange
from social.domain.services import CompanyMappingService, DateRangeCalculator
from social.processing.factory import ProcessingStrategyFactory
from social.processing.pipeline import DataProcessingPipeline


# Google Ads GAQL Queries (from social_posts/googleads/fields.py)
GAQL_QUERIES = {
    "query_customer_hierarchy": """
        SELECT
          customer_client.client_customer,
          customer_client.level,
          customer_client.manager,
          customer_client.descriptive_name,
          customer_client.currency_code,
          customer_client.time_zone,
          customer_client.id,
          customer_client.status
        FROM customer_client
    """,
    "query_campaign": """
        SELECT campaign.start_date,
        campaign.end_date, campaign.name,
        campaign.id, campaign.serving_status,
        customer.id, campaign.status
        FROM campaign
        WHERE segments.date BETWEEN '{}' AND '{}'
    """,
    "query_ads_ad_creatives": """
        SELECT ad_group_ad.ad.type,
        ad_group_ad.ad.name,
        ad_group_ad.ad.id,
        ad_group.id,
        customer.id FROM ad_group_ad
    """,
    "query_by_device": """
        SELECT ad_group_ad.ad.id, metrics.cost_micros,
        metrics.clicks, segments.device,
        customer.id FROM ad_group_ad
        WHERE campaign.serving_status IN ('ENDED','SERVING')
    """,
    "query_by_device_2": """
        SELECT ad_group_ad.ad.id, metrics.cost_micros,
        metrics.clicks, segments.device,
        customer.id FROM ad_group_ad
        WHERE campaign.status IN ('PAUSED')
    """,
    "query_placement": """
        SELECT group_placement_view.placement,
        group_placement_view.placement_type,
        group_placement_view.display_name,
        group_placement_view.target_url,
        ad_group.id,
        metrics.impressions,
        metrics.active_view_ctr,
        customer.id
        FROM group_placement_view
        WHERE campaign.serving_status IN ('ENDED','SERVING')
        AND segments.date BETWEEN '{}' AND '{}'
        ORDER BY metrics.impressions DESC
    """,
    "query_placement_2": """
        SELECT group_placement_view.placement,
        group_placement_view.placement_type,
        group_placement_view.display_name,
        group_placement_view.target_url,
        ad_group.id,
        metrics.impressions,
        metrics.active_view_ctr,
        customer.id
        FROM group_placement_view
        WHERE campaign.status IN ('PAUSED')
        AND segments.date BETWEEN '{}' AND '{}'
        ORDER BY metrics.impressions DESC
    """,
    "query_audience": """
        SELECT
          ad_group.id,
          ad_group_criterion.display_name,
          customer.id
        FROM ad_group_audience_view
        WHERE campaign.serving_status IN ('ENDED','SERVING')
    """,
    "query_audience_2": """
        SELECT
          ad_group.id,
          ad_group_criterion.display_name,
          customer.id
        FROM ad_group_audience_view
        WHERE campaign.status IN ('PAUSED')
    """,
    "query_ad_report": """
        SELECT metrics.clicks, metrics.conversions,
        metrics.average_cpc, metrics.average_cost,
        metrics.average_cpm, metrics.impressions, metrics.cost_micros,
        ad_group_ad.ad.id, ad_group.id, campaign.id,
        metrics.ctr, segments.date, customer.id
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{}' AND '{}'
    """,
}


class GoogleAdsAdapter(BaseAdsPlatformAdapter):
    """Adapter for Google Ads API.

    This adapter handles:
    - Google Ads API authentication via google-ads.yaml config file
    - GAQL query execution (Google Ads Query Language)
    - Account hierarchy traversal (MCC → customer accounts)
    - Data transformation using processing pipeline
    - Multiple request types (SearchGoogleAdsRequest, SearchGoogleAdsStreamRequest)
    """

    def __init__(
        self,
        config: PlatformConfig,
        token_provider: TokenProvider,
        data_sink: Optional[DataSink] = None,
    ):
        """Initialize Google Ads adapter.

        Args:
            config: Platform configuration
            token_provider: Provider for authentication tokens
            data_sink: Optional data sink for database queries
        """
        super().__init__(config, token_provider, data_sink)

        # Get Google Ads specific config
        google_config_file = config.additional_config.get(
            "google_ads_config_file",
            "social/platforms/google/google-ads-9474097201.yml"
        )
        api_version = config.additional_config.get("api_version", "v18")
        manager_id = config.additional_config.get("manager_customer_id", "9474097201")

        # Initialize HTTP client
        self.http_client = GoogleAdsHTTPClient(
            token_provider=token_provider,
            config_file_path=google_config_file,
            api_version=api_version,
            manager_customer_id=manager_id
        )

        # Initialize domain services
        self.company_mapping = CompanyMappingService(
            account_to_company=config.account_to_company_mapping,
            default_company_id=config.default_company_id
        )
        self.date_calculator = DateRangeCalculator()

        # Initialize processing factory
        self.processing_factory = ProcessingStrategyFactory(
            company_mapping_service=self.company_mapping
        )

        logger.info("Google Ads adapter initialized successfully")

    def extract_table(
        self,
        table_name: str,
        date_range: Optional[DateRange] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Extract data for a specific Google Ads table.

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
        logger.info(f"Extracting Google Ads table: {table_name}")

        # Get table configuration
        table_config = self.get_table_config(table_name)

        # Handle special case: google_ads_account
        if table_name == "google_ads_account":
            return self._extract_accounts(table_config)

        # Standard extraction for other tables
        return self._extract_standard_table(table_config, date_range)

    def _extract_accounts(self, table_config: TableConfig) -> pd.DataFrame:
        """Extract Google Ads account hierarchy.

        Args:
            table_config: Configuration for accounts table

        Returns:
            DataFrame with account data
        """
        logger.info("Extracting Google Ads account hierarchy")

        # Get all accounts
        accounts = self.http_client.get_all_accounts()

        # Convert to DataFrame
        df = pd.DataFrame(accounts)

        logger.info(f"Retrieved {len(df)} Google Ads accounts")

        # Apply processing steps
        df = self._process_data(df, table_config)

        return df

    def _extract_standard_table(
        self,
        table_config: TableConfig,
        date_range: Optional[DateRange] = None
    ) -> pd.DataFrame:
        """Extract standard Google Ads table by querying all customer accounts.

        Args:
            table_config: Table configuration
            date_range: Optional date range for filtering

        Returns:
            DataFrame with extracted data
        """
        # Get query names from config
        query_names = table_config.additional_params.get("queryget", [])
        if not query_names:
            raise ConfigurationError(
                f"No queries configured for table {table_config.name}",
                details={"table": table_config.name}
            )

        # Get request type (SearchGoogleAdsRequest or SearchGoogleAdsStreamRequest)
        request_type = table_config.request_type
        use_streaming = (request_type == "SearchGoogleAdsStreamRequest")

        # Calculate date range if needed
        if date_range is None and "day" in table_config.additional_params:
            lookback_days = table_config.additional_params["day"]
            end_date = datetime.now()
            start_date = end_date - timedelta(days=lookback_days)
            date_range = DateRange(
                start_date=start_date.date(),
                end_date=end_date.date()
            )

        # Get all enabled customer accounts
        accounts = self.http_client.get_all_accounts()
        enabled_accounts = [
            acc for acc in accounts
            if not acc.get("manager", False) and acc.get("status") == "ENABLED"
        ]

        logger.info(f"Querying {len(enabled_accounts)} enabled customer accounts")

        # Execute queries for each account
        all_results = []
        for account in enabled_accounts:
            customer_id = str(account["id"])
            account_name = account.get("descriptiveName", "Unknown")

            logger.info(f"Querying account: {account_name} (ID: {customer_id})")

            for query_name in query_names:
                # Get query template
                query_template = GAQL_QUERIES.get(query_name)
                if not query_template:
                    logger.warning(f"Query '{query_name}' not found, skipping")
                    continue

                # Format query with date range if needed
                if date_range and "{}" in query_template:
                    query = query_template.format(
                        date_range.start_date.strftime("%Y-%m-%d"),
                        date_range.end_date.strftime("%Y-%m-%d")
                    )
                else:
                    query = query_template

                # Execute query
                try:
                    df = self.http_client.execute_query(
                        customer_id=customer_id,
                        query=query,
                        use_streaming=use_streaming
                    )

                    if not df.empty:
                        all_results.append(df)
                        logger.debug(f"Retrieved {len(df)} rows for {query_name}")
                    else:
                        logger.debug(f"No data for {query_name}")

                except APIError as e:
                    logger.warning(f"Query failed for account {customer_id}: {str(e)}")
                    continue

        # Combine all results
        if all_results:
            combined_df = pd.concat(all_results, ignore_index=True)
            logger.info(f"Combined {len(combined_df)} total rows from all accounts")
        else:
            logger.warning(f"No data retrieved for table {table_config.name}")
            combined_df = pd.DataFrame()

        # Apply processing steps
        combined_df = self._process_data(combined_df, table_config)

        return combined_df

    def _process_data(
        self,
        raw_df: pd.DataFrame,
        table_config: TableConfig
    ) -> pd.DataFrame:
        """Process raw data using configured processing pipeline.

        Args:
            raw_df: Raw DataFrame from API
            table_config: Table configuration with processing steps

        Returns:
            Processed DataFrame
        """
        if raw_df.empty:
            logger.warning(f"Empty DataFrame for table {table_config.name}")
            return raw_df

        # Create processing pipeline
        pipeline = DataProcessingPipeline(factory=self.processing_factory)

        # Add processing steps from config
        if table_config.processing_steps:
            for step in table_config.processing_steps:
                # Parse step format: "step_name:param1=value1,param2=value2"
                if ":" in step:
                    step_name, params_str = step.split(":", 1)
                    params = dict(p.split("=") for p in params_str.split(","))
                else:
                    step_name = step
                    params = {}

                pipeline.add_step(step_name, params)

        # Execute pipeline
        processed_df = pipeline.process(raw_df)

        logger.info(f"Processed {len(processed_df)} rows for {table_config.name}")

        return processed_df

    def get_table_dependencies(self, table_name: str) -> List[str]:
        """Get list of tables that must be extracted before this one.

        Google Ads tables are mostly independent, but some may depend on account.

        Args:
            table_name: Name of the table

        Returns:
            List of dependency table names
        """
        # Most tables depend on account being extracted first
        if table_name == "google_ads_account":
            return []
        else:
            return ["google_ads_account"]
