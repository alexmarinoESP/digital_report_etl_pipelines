"""Facebook Ads platform adapter implementation.

This module implements the Facebook Ads-specific adapter for extracting
advertising data using the Facebook Marketing API.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import pandas as pd
from loguru import logger

from social.adapters.base import BaseAdsPlatformAdapter
from social.adapters.facebook_http_client import FacebookAdsHTTPClient
from social.core.protocols import TokenProvider, DataSink
from social.core.config import PlatformConfig, TableConfig
from social.core.exceptions import APIError, ConfigurationError
from social.domain.models import DateRange
from social.domain.services import CompanyMappingService, DateRangeCalculator
from social.processing.factory import ProcessingStrategyFactory
from social.processing.pipeline import DataProcessingPipeline


# Facebook Ads field definitions (from social_posts/fb_ads/fields.py)
FB_ADS_FIELDS = {
    "fields_ads_campaign": [
        "id", "status", "configured_status", "effective_status", "created_time", "objective"
    ],
    "fields_ads_adset": [
        "id", "campaign_id", "start_time", "end_time", "destination_type"
    ],
    "fields_ads_audience_adset": [
        "id", "campaign_id", "targeting"
    ],
    "fields_custom_convers": [
        "id", "custom_event_type", "rule"
    ],
    "fields_ads_insight": [
        "account_id", "campaign_id", "adset_id", "ad_id", "ad_name",
        "spend", "impressions", "reach", "inline_link_clicks",
        "inline_link_click_ctr", "clicks", "ctr", "cpc", "cpm"
    ],
    "fields_ads_insight_actions": [
        "ad_id", "actions"
    ],
}


class FacebookAdsAdapter(BaseAdsPlatformAdapter):
    """Adapter for Facebook Marketing API.

    This adapter handles:
    - Facebook Ads API authentication (OAuth 2.0)
    - Facebook Graph API requests
    - Data transformation specific to Facebook Ads
    - Insights and creative data extraction
    - Account, Campaign, AdSet, and Ad hierarchy
    - Rate limit handling
    """

    def __init__(
        self,
        config: PlatformConfig,
        token_provider: TokenProvider,
        data_sink: Optional[DataSink] = None,
    ):
        """Initialize Facebook Ads adapter.

        Args:
            config: Platform configuration
            token_provider: Provider for authentication tokens
            data_sink: Optional data sink for database queries
        """
        super().__init__(config, token_provider, data_sink)

        # Get Facebook Ads specific config
        app_id = config.additional_config.get("app_id")
        app_secret = config.additional_config.get("app_secret")
        ad_account_ids = config.additional_config.get("ad_account_ids", [])

        if not app_id or not app_secret:
            raise ConfigurationError(
                "Facebook Ads requires app_id and app_secret in configuration",
                details={"app_id": app_id is not None, "app_secret": app_secret is not None}
            )

        if not ad_account_ids:
            raise ConfigurationError(
                "Facebook Ads requires at least one ad_account_id",
                details={"ad_account_ids": ad_account_ids}
            )

        # Initialize HTTP client
        self.http_client = FacebookAdsHTTPClient(
            token_provider=token_provider,
            app_id=app_id,
            app_secret=app_secret,
            ad_account_ids=ad_account_ids
        )

        # Store account IDs for iteration
        self.ad_account_ids = ad_account_ids

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

        logger.info(f"Facebook Ads adapter initialized with {len(ad_account_ids)} accounts")

    def extract_table(
        self,
        table_name: str,
        date_range: Optional[DateRange] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Extract data for a specific Facebook Ads table.

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
        logger.info(f"Extracting Facebook Ads table: {table_name}")

        # Get table configuration
        table_config = self.get_table_config(table_name)

        # Handle special case: targeting field extraction
        if table_name == "fb_ads_audience_adset":
            return self._extract_audience_adset(table_config)

        # Standard extraction
        return self._extract_standard_table(table_config, date_range)

    def _extract_standard_table(
        self,
        table_config: TableConfig,
        date_range: Optional[DateRange] = None
    ) -> pd.DataFrame:
        """Extract standard Facebook Ads table by querying all ad accounts.

        Args:
            table_config: Table configuration
            date_range: Optional date range for filtering

        Returns:
            DataFrame with extracted data
        """
        # Get API method name (e.g., "get_campaigns", "get_insights")
        method_name = table_config.additional_params.get("type")
        if not method_name:
            raise ConfigurationError(
                f"No API method configured for table {table_config.name}",
                details={"table": table_config.name}
            )

        # Get fields to request
        # Check both table_config.fields and additional_params for backward compatibility
        fields_key = table_config.fields or table_config.additional_params.get("fields")
        fields = FB_ADS_FIELDS.get(fields_key, []) if isinstance(fields_key, str) else fields_key or []

        if not fields:
            logger.warning(f"No fields configured for {fields_key}, using defaults")
            fields = []

        # Get date preset (e.g., "last_7d", "maximum")
        date_preset = table_config.additional_params.get("date_preset", "last_7d")

        # Execute requests for all ad accounts
        all_results = []

        for account_id in self.ad_account_ids:
            logger.info(f"Extracting {table_config.name} for account {account_id}")

            try:
                # Handle "maximum" date preset with chunking
                if date_preset == "maximum":
                    logger.info("Using chunked date ranges for 'maximum' preset")

                    # Calculate date range (last 2 years)
                    end_date = datetime.now()
                    start_date = end_date - timedelta(days=730)

                    # Use chunked request
                    responses = self.http_client.request_data_chunked(
                        account_id=account_id,
                        method_name=method_name,
                        fields=fields,
                        start_date=start_date,
                        end_date=end_date,
                        chunk_days=90
                    )

                    # Convert all responses to DataFrame
                    if responses:
                        df = self.http_client.convert_to_df(responses)
                        if not df.empty:
                            all_results.append(df)
                    else:
                        logger.warning(f"No data for {table_config.name} from account {account_id}")

                else:
                    # Use regular request with date_preset
                    params = {
                        "date_preset": date_preset,
                        "level": "ad",
                        "action_attribution_windows": ["7d_click", "1d_view"]
                    }

                    response = self.http_client.request_data(
                        account_id=account_id,
                        method_name=method_name,
                        fields=fields,
                        params=params
                    )

                    # Convert to DataFrame
                    df = self.http_client.convert_to_df(response)

                    if not df.empty:
                        all_results.append(df)
                    else:
                        logger.warning(f"No data for {table_config.name} from account {account_id}")

            except APIError as e:
                logger.warning(f"Failed to extract {table_config.name} for account {account_id}: {str(e)}")
                continue

        # Combine all results
        if all_results:
            combined_df = pd.concat(all_results, ignore_index=True)
            logger.info(f"Combined {len(combined_df)} total rows from {len(all_results)} accounts")
        else:
            logger.warning(f"No data retrieved for table {table_config.name}")
            combined_df = pd.DataFrame()

        # Apply processing steps
        combined_df = self._process_data(combined_df, table_config)

        return combined_df

    def _extract_audience_adset(self, table_config: TableConfig) -> pd.DataFrame:
        """Extract audience targeting information from AdSets.

        This is a special extraction that needs to parse the targeting field
        and extract custom_audiences.

        Args:
            table_config: Table configuration

        Returns:
            DataFrame with audience data
        """
        logger.info("Extracting Facebook Ads audience targeting from AdSets")

        # First, get all AdSets with targeting field
        method_name = table_config.additional_params.get("type", "get_ad_sets")
        fields_key = table_config.fields or table_config.additional_params.get("fields")
        fields = FB_ADS_FIELDS.get(fields_key, ["id", "campaign_id", "targeting"]) if isinstance(fields_key, str) else fields_key or ["id", "campaign_id", "targeting"]

        date_preset = table_config.additional_params.get("date_preset", "last_7d")

        all_results = []

        for account_id in self.ad_account_ids:
            logger.info(f"Extracting targeting for account {account_id}")

            try:
                params = {
                    "date_preset": date_preset,
                    "level": "ad"
                }

                response = self.http_client.request_data(
                    account_id=account_id,
                    method_name=method_name,
                    fields=fields,
                    params=params
                )

                # Convert to DataFrame
                df = self.http_client.convert_to_df(response)

                if not df.empty:
                    # Convert targeting field to audience DataFrame
                    audience_df = self.http_client.convert_targeting_field(df)

                    if not audience_df.empty:
                        all_results.append(audience_df)
                    else:
                        logger.warning(f"No targeting data for account {account_id}")

            except APIError as e:
                logger.warning(f"Failed to extract targeting for account {account_id}: {str(e)}")
                continue

        # Combine all results
        if all_results:
            combined_df = pd.concat(all_results, ignore_index=True)
            logger.info(f"Extracted {len(combined_df)} audience targeting rows")
        else:
            logger.warning("No audience targeting data retrieved")
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
        pipeline = DataProcessingPipeline(strategy_factory=self.processing_factory)

        # Add processing steps from config
        if table_config.processing_steps:
            # Handle both dict format (from YAML) and list format (legacy)
            if isinstance(table_config.processing_steps, dict):
                # New format: {step_name: {param1: value1, param2: value2}}
                for step_name, step_params in table_config.processing_steps.items():
                    # Handle various parameter formats
                    if isinstance(step_params, dict):
                        params = step_params
                    elif step_params is None or step_params == "None":
                        params = {}
                    elif isinstance(step_params, str):
                        # Legacy string format - skip for now
                        logger.warning(f"Skipping step {step_name} with string params: {step_params}")
                        continue
                    else:
                        params = {}
                    pipeline.add_step(step_name, params)
            else:
                # Legacy format: ["step_name:param1=value1,param2=value2"]
                for step in table_config.processing_steps:
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

        Facebook Ads hierarchy: Account → Campaign → AdSet → Ad
        Insights can be fetched at any level.

        Args:
            table_name: Name of the table

        Returns:
            List of dependency table names
        """
        dependencies = {
            "fb_ads_ad_set": ["fb_ads_campaign"],
            "fb_ads_insight": ["fb_ads_campaign"],
            "fb_ads_audience_adset": ["fb_ads_ad_set"],
        }

        return dependencies.get(table_name, [])

    # Abstract method implementations (not used - Facebook uses its own SDK)
    def _build_request_url(self, table_config: TableConfig, **kwargs) -> str:
        """Build request URL (not used - Facebook uses SDK)."""
        raise NotImplementedError("Facebook adapter uses SDK, not direct HTTP requests")

    def _build_request_headers(self, **kwargs) -> Dict[str, str]:
        """Build request headers (not used - Facebook uses SDK)."""
        raise NotImplementedError("Facebook adapter uses SDK, not direct HTTP requests")

    def _build_request_params(self, table_config: TableConfig, **kwargs) -> Dict[str, Any]:
        """Build request parameters (not used - Facebook uses SDK)."""
        raise NotImplementedError("Facebook adapter uses SDK, not direct HTTP requests")

    def _parse_response(self, response: Any, table_config: TableConfig) -> pd.DataFrame:
        """Parse API response (not used - Facebook uses SDK)."""
        raise NotImplementedError("Facebook adapter uses SDK, not direct HTTP requests")
