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
from social.utils.commons import handle_nested_response, extract_targeting_criteria


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
    ) -> tuple[pd.DataFrame, Optional[Dict[str, int]]]:
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
            Tuple of (DataFrame, LoadStats dict) where LoadStats is None if not loaded

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
                return df, None

            logger.success(f"Extracted {len(df)} rows for {table_name}")

            # Process the data
            logger.info(f"Processing data for {table_name}")
            processed_df = self._process_table(df, table_name, table_config)

            logger.success(f"Processing complete: {len(processed_df)} rows, {len(processed_df.columns)} columns")

            # Load to sink if configured
            stats = None
            if load_to_sink and self.data_sink is not None:
                logger.info(f"Loading data to sink: {table_name}")
                stats = self._load_to_sink(processed_df, table_name)
                logger.success(
                    f"Loaded {stats['rows_written']} rows to {table_name} "
                    f"({stats['rows_inserted']} new + {stats['rows_updated']} updated)"
                )

            # Calculate duration
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Pipeline completed for {table_name} in {duration:.2f}s")

            return processed_df, stats

        except Exception as e:
            logger.error(f"Pipeline failed for table {table_name}: {e}")
            raise PipelineError(f"Pipeline failed for table {table_name}") from e

    def run_all_tables(
        self,
        load_to_sink: bool = True,
    ) -> tuple[Dict[str, Dict[str, int]], Dict[str, str]]:
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
            Tuple of (results_stats, errors) where:
            - results_stats: Dict mapping table_name to LoadStats dict
            - errors: Dict mapping table_name to error message (only for tables that raised exceptions)

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
            "linkedin_ads_demographics_company",
            "linkedin_ads_demographics_job_title",
        ]

        results_stats = {}
        errors = {}

        for table_name in processing_order:
            if table_name not in self.table_names:
                logger.debug(f"Skipping {table_name} (not in config)")
                continue

            try:
                logger.info(f"Processing table: {table_name}")
                df, stats = self.run(
                    table_name=table_name,
                    load_to_sink=load_to_sink,
                )
                results_stats[table_name] = stats
                logger.success(f"Table {table_name} completed successfully")

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Table {table_name} failed: {error_msg}")
                errors[table_name] = error_msg
                # Continue with other tables (non-critical failure)

        # Calculate duration
        duration = (datetime.now() - start_time).total_seconds()

        # Summary
        successful = len([name for name in results_stats if name not in errors])
        total_written = sum(stats.get("rows_written", 0) for stats in results_stats.values())
        total_from_api = sum(stats.get("rows_from_api", 0) for stats in results_stats.values())

        logger.info(
            f"Pipeline batch complete: "
            f"{successful} succeeded, {len(errors)} failed, "
            f"{total_written} rows written from {total_from_api} API rows, "
            f"duration: {duration:.2f}s"
        )

        if errors:
            logger.warning(f"Failed tables: {list(errors.keys())}")

        return results_stats, errors

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
                # Extract campaigns and parse targeting criteria
                for account_id in COMPANY_ACCOUNT_MAP.keys():
                    campaigns = self.adapter.get_campaigns(account_id)
                    all_data.extend(campaigns)

                # Use special extraction for targeting criteria
                if all_data:
                    df = extract_targeting_criteria(all_data)
                    return df
                else:
                    return pd.DataFrame()

            elif table_name == "linkedin_ads_insights":
                # Extract insights (requires campaign URNs from DB)
                all_data = self._extract_insights(start_date, end_date)

            elif table_name == "linkedin_ads_creative":
                # Extract creatives (requires creative IDs from insights DB)
                all_data = self._extract_creatives()

            elif table_name.startswith("linkedin_ads_demographics_"):
                # Extract demographics (requires campaigns from DB)
                all_data = self._extract_demographics(table_name, table_config)

            else:
                raise ConfigurationError(f"Unknown table: {table_name}")

            # Convert to DataFrame
            if all_data:
                # Check if table has nested_element config - use special flattening
                nested_element = table_config.get("nested_element", None)

                if nested_element:
                    logger.debug(f"Using nested response handler for {table_name} with elements: {nested_element}")
                    df = handle_nested_response(all_data, nested_element)
                    logger.debug(f"DataFrame columns after flatten: {list(df.columns)}")
                else:
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

    def _extract_demographics(
        self,
        table_name: str,
        table_config: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Extract demographics data per campaign.

        Demographics data is queried per campaign (like Google placements).
        This method:
        1. Gets active campaigns from DB
        2. For each campaign, queries demographics with specified pivot
        3. Enriches data with campaign info (id, name)

        Args:
            table_name: Name of the demographics table
            table_config: Table configuration from YAML

        Returns:
            List of demographic dictionaries with campaign info

        Raises:
            PipelineError: If data sink not configured or query fails
        """
        if not self.data_sink:
            raise PipelineError(
                "Data sink required to fetch campaigns for demographics extraction"
            )

        # Get configuration
        pivot = table_config.get("pivot")
        time_granularity = table_config.get("time_granularity", "ALL")
        lookback_days = table_config.get("lookback_days", 90)

        if not pivot:
            raise ConfigurationError(f"Missing 'pivot' in config for {table_name}")

        logger.info(f"Extracting demographics with pivot {pivot} for active campaigns")

        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)

        # Get campaigns from database
        campaign_urns = self._get_campaign_urns_from_db()

        if campaign_urns.empty:
            logger.warning("No campaigns found in database for demographics extraction")
            return []

        logger.info(f"Fetching demographics for {len(campaign_urns)} campaigns")

        all_demographics = []

        # Fetch demographics for each campaign
        for idx, row in campaign_urns.iterrows():
            campaign_id = str(int(float(row["id"])))  # Remove .0 decimal
            campaign_name = row.get("name", f"Campaign {campaign_id}")

            try:
                logger.debug(f"Fetching {pivot} for campaign {campaign_id}")

                demographics = self.adapter.get_demographics_insights(
                    pivot=pivot,
                    campaign_ids=[campaign_id],
                    start_date=start_date,
                    end_date=end_date,
                    time_granularity=time_granularity
                )

                # Enrich each record with campaign info
                for record in demographics:
                    record["campaign_id"] = campaign_id
                    record["campaign_name"] = campaign_name
                    all_demographics.append(record)

                logger.debug(f"Retrieved {len(demographics)} records for campaign {campaign_id}")

            except Exception as e:
                logger.error(f"Failed to fetch demographics for campaign {campaign_id}: {e}")
                continue

        logger.success(f"Retrieved {len(all_demographics)} total demographic records")
        return all_demographics

    def _get_account_for_campaign(self, campaign_id: str) -> Optional[str]:
        """Get account_id for a campaign (reverse lookup from campaigns in DB).

        Args:
            campaign_id: Campaign ID

        Returns:
            Account ID or None
        """
        # Simple approach: Query campaign table for account_id
        # If not available, we can leave it None (will be filled later if needed)
        if not self.data_sink:
            return None

        table_suffix = "_TEST" if hasattr(self.data_sink, 'test_mode') and self.data_sink.test_mode else ""

        query = f"""
            SELECT account_id
            FROM GoogleAnalytics.linkedin_ads_campaign{table_suffix}
            WHERE id = '{campaign_id}'
            LIMIT 1
        """

        try:
            result = self.data_sink.query(query)
            if not result.empty:
                return str(result.iloc[0]["account_id"])
        except Exception as e:
            logger.debug(f"Could not fetch account_id for campaign {campaign_id}: {e}")

        return None

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

        # Create processor with adapter for lookups
        processor = LinkedInProcessor(df, adapter=self.adapter)

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
                    logger.debug(f"Calling {step_name}() with no params")
                    processor = method()
                elif isinstance(step_params, dict):
                    # Parameters as dictionary
                    logger.debug(f"Calling {step_name}() with params: {step_params}")
                    processor = method(**step_params)
                else:
                    logger.warning(f"Invalid parameters for {step_name}: {step_params}")
                    continue

            except Exception as e:
                logger.error(f"Failed to apply processing step '{step_name}': {e}")
                # Continue with other steps

        # Log final DataFrame columns
        final_df = processor.get_df()
        logger.debug(f"Final DataFrame columns after processing: {list(final_df.columns)}")
        return final_df

    def _get_campaign_urns_from_db(self) -> pd.DataFrame:
        """Query database for campaign URNs for insights.

        Uses business logic based on campaign dates and status:
        - Active/Paused campaigns: always included
        - Completed campaigns: only if ended in last 90 days
        - Archived campaigns: excluded (no active data)

        Returns:
            DataFrame with campaign IDs and names
        """
        if not self.data_sink:
            return pd.DataFrame()

        # Determine test mode suffix
        table_suffix = "_TEST" if hasattr(self.data_sink, 'test_mode') and self.data_sink.test_mode else ""

        query = f"""
            SELECT DISTINCT id, name
            FROM GoogleAnalytics.linkedin_ads_campaign{table_suffix}
            WHERE (
                -- Always include active/paused campaigns
                status IN ('ACTIVE', 'PAUSED', 'DRAFT')
                -- Include completed campaigns if ended recently
                OR (status = 'COMPLETED' AND (end_date IS NULL OR end_date >= CURRENT_DATE - 90))
                -- Exclude archived campaigns (old, no active data)
            )
        """

        try:
            return self.data_sink.query(query)
        except Exception as e:
            logger.error(f"Failed to query campaign URNs: {e}")
            return pd.DataFrame()

    def _get_creative_urns_from_db(self) -> pd.DataFrame:
        """Query database for creative URNs from insights.

        Gets creatives that have recent activity (based on insights date dimension).
        Uses the 'date' column (business date) instead of load_date (ETL metadata).

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
            WHERE date >= CURRENT_DATE - 90  -- Creatives with activity in last 90 days
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
    ) -> Dict[str, int]:
        """
        Load DataFrame to data sink.

        Determines load mode from table configuration:
        - If 'increment' config exists → use increment mode
        - Otherwise → use append mode (default)

        Args:
            df: DataFrame to load
            table_name: Target table name

        Returns:
            LoadStats dict with rows_from_api, rows_inserted, rows_updated, etc.

        Raises:
            PipelineError: If data sink not configured or load fails
        """
        if self.data_sink is None:
            raise PipelineError("Data sink not configured")

        try:
            # Get table configuration to determine load mode
            table_config = self.config.get(table_name, {})

            # Determine load mode from configuration
            pk_columns = None
            increment_columns = None

            if "increment" in table_config:
                # INCREMENT: Insert new + Increment metrics for existing
                load_mode = "increment"
                increment_config = table_config["increment"]
                pk_columns = increment_config.get("pk_columns")
                increment_columns = increment_config.get("increment_columns")
                logger.debug(f"Using increment mode for {table_name}: PK={pk_columns}, metrics={increment_columns}")

            elif "upsert" in table_config:
                # UPSERT: Insert new + Update all fields for existing
                load_mode = "upsert"
                upsert_config = table_config["upsert"]
                pk_columns = upsert_config.get("pk_columns")
                logger.debug(f"Using upsert mode for {table_name}: PK={pk_columns}")

            else:
                # Default: APPEND (insert only new rows, skip duplicates)
                load_mode = "append"
                logger.debug(f"Using append mode for {table_name}")

            # Check if sink has write_dataframe method (Vertica style - legacy)
            if hasattr(self.data_sink, "write_dataframe"):
                rows_loaded = self.data_sink.write_dataframe(
                    df=df,
                    table_name=table_name,
                    schema_name="GoogleAnalytics",
                    if_exists=load_mode,
                )
                # Legacy sink returns int, convert to LoadStats dict
                return {
                    "rows_from_api": len(df),
                    "rows_inserted": rows_loaded,
                    "rows_updated": 0,
                    "rows_skipped": 0,
                    "rows_filtered": 0,
                    "rows_written": rows_loaded,
                }
            # Check if sink has load method (Protocol style - VerticaDataSink)
            elif hasattr(self.data_sink, "load"):
                stats = self.data_sink.load(
                    df=df,
                    table_name=table_name,
                    mode=load_mode,
                    dedupe_columns=pk_columns,
                    increment_columns=increment_columns,
                )
                # LoadStats object, convert to dict
                return stats.to_dict()
            else:
                raise PipelineError("Data sink does not have a compatible load method")

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
