"""LinkedIn Ads ETL Pipeline Module.

This module provides the main pipeline orchestrator for LinkedIn Ads data extraction,
transformation, and loading (ETL). It coordinates the adapter, processor, and data sink
to provide a complete end-to-end workflow.

Key Features:
- Multi-table support with configuration-driven table definitions
- Dependency injection for TokenProvider and DataSink
- Proper error handling and recovery
- Detailed logging and progress tracking
- Support for both single table and batch processing
- Table dependency management (insights needs campaigns first)

Architecture:
- SOLID principles with dependency injection
- Protocol-based contracts for flexibility
- Clear separation of concerns (adapter/processor/sink)
- Container-ready (no browser interactions)
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml
from loguru import logger

from social.core.exceptions import ConfigurationError, PipelineError
from social.core.protocols import DataSink, TokenProvider
from social.platforms.linkedin.adapter import LinkedInAdapter
from social.platforms.linkedin.constants import COMPANY_ACCOUNT_MAP, INSIGHTS_LOOKBACK_DAYS
from social.platforms.linkedin.processor import LinkedInProcessor


class LinkedInPipeline:
    """
    Main ETL pipeline for LinkedIn Ads platform.

    This pipeline orchestrates the complete data extraction process:
    1. Authentication (via TokenProvider)
    2. Data extraction (via LinkedInAdapter)
    3. Data processing (via LinkedInProcessor)
    4. Data loading (via DataSink)

    Design:
    - Configuration-driven table definitions
    - Dependency injection for flexibility
    - Protocol-based contracts
    - Comprehensive error handling

    Attributes:
        config (Dict): Platform configuration with table definitions
        token_provider (TokenProvider): Authentication token provider
        adapter (LinkedInAdapter): LinkedIn Ads API adapter
        data_sink (Optional[DataSink]): Data sink for loading processed data
    """

    def __init__(
        self,
        config: Dict[str, Any],
        token_provider: TokenProvider,
        data_sink: Optional[DataSink] = None,
    ):
        """
        Initialize the LinkedIn Ads pipeline.

        Args:
            config: Platform configuration dictionary (from YAML)
            token_provider: Token provider for authentication
            data_sink: Optional data sink for loading data (e.g., VerticaDBManager)

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if config is None:
            raise ConfigurationError("Configuration cannot be None")

        self.config = config
        self.token_provider = token_provider
        self.data_sink = data_sink

        # Initialize adapter
        self.adapter = LinkedInAdapter(
            token_provider=token_provider,
            data_sink=data_sink
        )

        # Extract table names from config
        self.table_names = [
            key for key in config.keys()
            if key != "platform" and key.startswith("linkedin_ads_")
        ]

        logger.info(f"LinkedInPipeline initialized with {len(self.table_names)} tables")
        logger.debug(f"Tables: {self.table_names}")

    def run(
        self,
        table_name: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        load_to_sink: bool = True,
    ) -> pd.DataFrame:
        """
        Run the pipeline for a single table.

        This method executes the complete ETL workflow:
        1. Retrieve table configuration
        2. Extract data from LinkedIn Ads API
        3. Process the data
        4. Load to data sink (if configured)

        Args:
            table_name: Name of the table to process
            start_date: Optional start date for time-series data (insights)
            end_date: Optional end date for time-series data (insights)
            load_to_sink: If True, load data to sink after processing

        Returns:
            Processed DataFrame

        Raises:
            ConfigurationError: If table configuration not found
            PipelineError: If any step fails
        """
        logger.info(f"Starting pipeline for table: {table_name}")
        start_time = datetime.now()

        try:
            # Get table configuration
            table_config = self.config.get(table_name)
            if not table_config:
                raise ConfigurationError(f"Table '{table_name}' not found in configuration")

            # Extract data
            logger.info(f"Extracting data for {table_name}")
            df = self._extract_table(table_name, table_config, start_date, end_date)

            if df.empty:
                logger.warning(f"No data extracted for {table_name}")
                return df

            logger.success(f"Extracted {len(df)} rows for {table_name}")

            # Process the data
            logger.info(f"Processing data for {table_name}")
            processed_df = self._process_table(df, table_name, table_config)

            logger.success(f"Processing complete: {len(processed_df)} rows, {len(processed_df.columns)} columns")

            # Load to sink if configured
            if load_to_sink and self.data_sink is not None:
                logger.info(f"Loading data to sink: {table_name}")
                rows_loaded = self._load_to_sink(processed_df, table_name)
                logger.success(f"Loaded {rows_loaded} rows to {table_name}")

            # Calculate duration
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Pipeline completed for {table_name} in {duration:.2f}s")

            return processed_df

        except Exception as e:
            logger.error(f"Pipeline failed for table {table_name}: {e}")
            raise PipelineError(f"Pipeline failed for table {table_name}") from e

    def run_all_tables(
        self,
        load_to_sink: bool = True,
    ) -> Dict[str, pd.DataFrame]:
        """
        Run the pipeline for all configured tables.

        Tables are processed in dependency order:
        1. linkedin_ads_account
        2. linkedin_ads_campaign
        3. linkedin_ads_audience
        4. linkedin_ads_campaign_audience
        5. linkedin_ads_insights (needs campaigns in DB)
        6. linkedin_ads_creative (needs insights in DB)

        Args:
            load_to_sink: If True, load data to sink after processing

        Returns:
            Dictionary mapping table names to processed DataFrames

        Raises:
            PipelineError: If any critical table fails
        """
        logger.info(f"Starting pipeline for all tables: {self.table_names}")
        start_time = datetime.now()

        # Define processing order (respecting dependencies)
        processing_order = [
            "linkedin_ads_account",
            "linkedin_ads_campaign",
            "linkedin_ads_audience",
            "linkedin_ads_campaign_audience",
            "linkedin_ads_insights",
            "linkedin_ads_creative",
        ]

        results = {}
        failed_tables = []

        for table_name in processing_order:
            if table_name not in self.table_names:
                logger.debug(f"Skipping {table_name} (not in config)")
                continue

            try:
                logger.info(f"Processing table: {table_name}")
                df = self.run(
                    table_name=table_name,
                    load_to_sink=load_to_sink,
                )
                results[table_name] = df
                logger.success(f"Table {table_name} completed successfully")

            except Exception as e:
                logger.error(f"Table {table_name} failed: {e}")
                failed_tables.append(table_name)
                # Continue with other tables (non-critical failure)

        # Calculate duration
        duration = (datetime.now() - start_time).total_seconds()

        # Summary
        logger.info(
            f"Pipeline batch complete: "
            f"{len(results)} succeeded, {len(failed_tables)} failed, "
            f"duration: {duration:.2f}s"
        )

        if failed_tables:
            logger.warning(f"Failed tables: {failed_tables}")

        return results

    def _extract_table(
        self,
        table_name: str,
        table_config: Dict[str, Any],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Extract data for a table using the adapter.

        Args:
            table_name: Name of the table
            table_config: Table configuration from YAML
            start_date: Optional start date for insights
            end_date: Optional end date for insights

        Returns:
            Raw DataFrame from API

        Raises:
            PipelineError: If extraction fails
        """
        all_data = []

        try:
            if table_name == "linkedin_ads_account":
                # Extract accounts
                for account_id in COMPANY_ACCOUNT_MAP.keys():
                    account_data = self.adapter.get_account(account_id)
                    if account_data:
                        all_data.append(account_data)

            elif table_name == "linkedin_ads_campaign":
                # Extract campaigns per account
                for account_id in COMPANY_ACCOUNT_MAP.keys():
                    campaigns = self.adapter.get_campaigns(account_id)
                    all_data.extend(campaigns)

            elif table_name == "linkedin_ads_audience":
                # Extract audiences per account
                for account_id in COMPANY_ACCOUNT_MAP.keys():
                    audiences = self.adapter.get_audiences(account_id)
                    all_data.extend(audiences)

            elif table_name == "linkedin_ads_campaign_audience":
                # Same as campaigns but will extract targeting criteria
                for account_id in COMPANY_ACCOUNT_MAP.keys():
                    campaigns = self.adapter.get_campaigns(account_id)
                    all_data.extend(campaigns)

            elif table_name == "linkedin_ads_insights":
                # Extract insights (requires campaign URNs from DB)
                all_data = self._extract_insights(start_date, end_date)

            elif table_name == "linkedin_ads_creative":
                # Extract creatives (requires creative IDs from insights DB)
                all_data = self._extract_creatives()

            else:
                raise ConfigurationError(f"Unknown table: {table_name}")

            # Convert to DataFrame
            if all_data:
                df = pd.DataFrame(all_data)
                return df
            else:
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Failed to extract {table_name}: {e}")
            raise PipelineError(f"Failed to extract {table_name}") from e

    def _extract_insights(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Extract insights data (requires campaign URNs from database).

        Args:
            start_date: Start date for insights (defaults to INSIGHTS_LOOKBACK_DAYS ago)
            end_date: End date for insights (defaults to today)

        Returns:
            List of insight dictionaries

        Raises:
            PipelineError: If data sink not configured or query fails
        """
        if not self.data_sink:
            raise PipelineError(
                "Data sink required to fetch campaign URNs for insights extraction"
            )

        # Calculate date range
        if end_date is None:
            end_date = datetime.now()
        if start_date is None:
            start_date = end_date - timedelta(days=INSIGHTS_LOOKBACK_DAYS)

        # Get campaign URNs from database
        campaign_urns = self._get_campaign_urns_from_db()

        if campaign_urns.empty:
            logger.warning("No campaigns found in database for insights extraction")
            return []

        logger.info(f"Fetching insights for {len(campaign_urns)} campaigns from {start_date.date()} to {end_date.date()}")

        all_insights = []

        # Fetch insights for each campaign
        for campaign_id in campaign_urns["id"]:
            try:
                insights = self.adapter.get_insights(
                    campaign_id=str(campaign_id),
                    start_date=start_date,
                    end_date=end_date
                )
                if insights:
                    all_insights.extend(insights)
            except Exception as e:
                logger.error(f"Failed to fetch insights for campaign {campaign_id}: {e}")
                continue

        return all_insights

    def _extract_creatives(self) -> List[Dict[str, Any]]:
        """
        Extract creative data (requires creative URNs from insights table).

        Returns:
            List of creative dictionaries

        Raises:
            PipelineError: If data sink not configured or query fails
        """
        if not self.data_sink:
            raise PipelineError(
                "Data sink required to fetch creative URNs for creatives extraction"
            )

        # Get creative URNs from insights table
        creative_urns = self._get_creative_urns_from_db()

        if creative_urns.empty:
            logger.warning("No creatives found in insights table")
            return []

        logger.info(f"Fetching data for {len(creative_urns)} creatives")

        all_creatives = []

        # Fetch each creative (try all accounts until found)
        for creative_id in creative_urns["id"]:
            found = False
            for account_id in COMPANY_ACCOUNT_MAP.keys():
                try:
                    creative_data = self.adapter.get_creatives(
                        account_id=account_id,
                        creative_id=str(creative_id)
                    )
                    if creative_data:
                        all_creatives.append(creative_data)
                        found = True
                        break
                except Exception:
                    continue

            if not found:
                logger.debug(f"Creative {creative_id} not found in any account")

        return all_creatives

    def _process_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        table_config: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Process raw data into clean DataFrame using processor.

        Args:
            df: Raw DataFrame
            table_name: Name of the table
            table_config: Table configuration with processing steps

        Returns:
            Cleaned and transformed DataFrame
        """
        if df.empty:
            return df

        # Create processor
        processor = LinkedInProcessor(df)

        # Apply processing steps from configuration
        processing_config = table_config.get("processing", {})

        for step_name, step_params in processing_config.items():
            try:
                # Get the processing method
                method = getattr(processor, step_name, None)

                if method is None:
                    logger.warning(f"Unknown processing step: {step_name}")
                    continue

                # Call the method with parameters
                if step_params is None or step_params == "None":
                    # No parameters needed
                    processor = method()
                elif isinstance(step_params, dict):
                    # Parameters as dictionary
                    processor = method(**step_params)
                else:
                    logger.warning(f"Invalid parameters for {step_name}: {step_params}")
                    continue

            except Exception as e:
                logger.error(f"Failed to apply processing step '{step_name}': {e}")
                # Continue with other steps

        return processor.get_df()

    def _get_campaign_urns_from_db(self) -> pd.DataFrame:
        """Query database for campaign URNs for insights.

        Returns:
            DataFrame with campaign IDs
        """
        if not self.data_sink:
            return pd.DataFrame()

        # Determine test mode suffix
        table_suffix = "_TEST" if hasattr(self.data_sink, 'test_mode') and self.data_sink.test_mode else ""

        query = f"""
            SELECT DISTINCT id
            FROM GoogleAnalytics.linkedin_ads_campaign{table_suffix}
            WHERE row_loaded_date >= CURRENT_DATE - {INSIGHTS_LOOKBACK_DAYS}
        """

        try:
            return self.data_sink.query(query)
        except Exception as e:
            logger.error(f"Failed to query campaign URNs: {e}")
            return pd.DataFrame()

    def _get_creative_urns_from_db(self) -> pd.DataFrame:
        """Query database for creative URNs from insights.

        Returns:
            DataFrame with creative IDs
        """
        if not self.data_sink:
            return pd.DataFrame()

        # Determine test mode suffix
        table_suffix = "_TEST" if hasattr(self.data_sink, 'test_mode') and self.data_sink.test_mode else ""

        query = f"""
            SELECT DISTINCT creative_id AS id
            FROM GoogleAnalytics.linkedin_ads_insights{table_suffix}
            WHERE row_loaded_date >= CURRENT_DATE - {INSIGHTS_LOOKBACK_DAYS}
            AND creative_id IS NOT NULL
        """

        try:
            return self.data_sink.query(query)
        except Exception as e:
            logger.error(f"Failed to query creative URNs: {e}")
            return pd.DataFrame()

    def _load_to_sink(
        self,
        df: pd.DataFrame,
        table_name: str,
    ) -> int:
        """
        Load DataFrame to data sink.

        Args:
            df: DataFrame to load
            table_name: Target table name

        Returns:
            Number of rows loaded

        Raises:
            PipelineError: If data sink not configured or load fails
        """
        if self.data_sink is None:
            raise PipelineError("Data sink not configured")

        try:
            # Check if sink has write_dataframe method (Vertica style)
            if hasattr(self.data_sink, "write_dataframe"):
                rows_loaded = self.data_sink.write_dataframe(
                    df=df,
                    table_name=table_name,
                    schema_name="GoogleAnalytics",
                    if_exists="append",
                )
            # Check if sink has load method (Protocol style)
            elif hasattr(self.data_sink, "load"):
                rows_loaded = self.data_sink.load(
                    df=df,
                    table_name=table_name,
                    mode="append",
                )
            else:
                raise PipelineError("Data sink does not have a compatible load method")

            return rows_loaded

        except Exception as e:
            logger.error(f"Failed to load data to sink: {e}")
            raise PipelineError(f"Failed to load data to sink") from e

    def get_table_names(self) -> List[str]:
        """
        Get list of configured table names.

        Returns:
            List of table names
        """
        return self.table_names

    def get_all_tables(self) -> List[str]:
        """
        Get list of all configured table names (alias for compatibility).

        Returns:
            List of table names
        """
        return self.get_table_names()

    def extract_all_tables(self, tables: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """
        Extract data for all tables (or specified subset).

        This method is for compatibility with the main pipeline orchestrator.
        It runs the pipeline for each table and returns the DataFrames.

        Args:
            tables: Optional list of specific tables to extract (None = all)

        Returns:
            Dictionary mapping table names to DataFrames

        Raises:
            PipelineError: If extraction fails
        """
        tables_to_process = tables if tables else self.table_names
        results = {}

        for table_name in tables_to_process:
            if table_name not in self.table_names:
                logger.warning(f"Table {table_name} not in configuration, skipping")
                continue

            try:
                # Run pipeline for this table (without loading to sink)
                df = self.run(table_name=table_name, load_to_sink=False)
                results[table_name] = df
            except Exception as e:
                logger.error(f"Failed to extract {table_name}: {e}")
                # Continue with other tables
                continue

        return results

    def close(self) -> None:
        """Close the adapter and release resources."""
        if self.adapter:
            self.adapter.close()
            logger.debug("LinkedInPipeline closed")
