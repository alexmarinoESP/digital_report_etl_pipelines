"""Google Ads ETL Pipeline Module.

This module provides the main pipeline orchestrator for Google Ads data extraction,
transformation, and loading (ETL). It coordinates the adapter, processor, and data sink
to provide a complete end-to-end workflow.

Key Features:
- Multi-table support with configuration-driven table definitions
- Multi-account iteration (all customer accounts under MCC)
- Dependency injection for TokenProvider and DataSink
- Proper error handling and recovery
- Detailed logging and progress tracking
- Support for both single table and batch processing
- Handles both SearchGoogleAdsRequest and SearchGoogleAdsStreamRequest

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
from social.platforms.google.adapter import GoogleAdapter
from social.platforms.google.constants import API_VERSION, DEFAULT_LOOKBACK_DAYS
from social.platforms.google.processor import GoogleProcessor


class GooglePipeline:
    """
    Main ETL pipeline for Google Ads platform.

    This pipeline orchestrates the complete data extraction process:
    1. Authentication (via google-ads.yaml config file)
    2. Data extraction (via GoogleAdapter)
    3. Data processing (via GoogleProcessor)
    4. Data loading (via DataSink)

    Design:
    - Configuration-driven table definitions
    - Dependency injection for flexibility
    - Protocol-based contracts
    - Comprehensive error handling

    Attributes:
        config: Platform configuration dictionary (from YAML)
        token_provider: Token provider for authentication
        google_config_file: Path to google-ads.yaml config
        manager_customer_id: Manager account ID (MCC)
        api_version: Google Ads API version
        adapter: Google Ads API adapter
        data_sink: Optional data sink for loading processed data
    """

    def __init__(
        self,
        config: Dict[str, Any],
        token_provider: TokenProvider,
        google_config_file: str,
        manager_customer_id: str = "9474097201",
        api_version: str = API_VERSION,
        data_sink: Optional[DataSink] = None,
    ):
        """
        Initialize the Google Ads pipeline.

        Args:
            config: Platform configuration dictionary (from googleads_config.yml)
            token_provider: Token provider for authentication
            google_config_file: Path to google-ads.yaml configuration file
            manager_customer_id: Manager account ID (MCC)
            api_version: Google Ads API version
            data_sink: Optional data sink for loading data (e.g., VerticaDBManager)

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if config is None:
            raise ConfigurationError("Configuration cannot be None")

        self.config = config
        self.token_provider = token_provider
        self.google_config_file = google_config_file
        self.manager_customer_id = manager_customer_id
        self.api_version = api_version
        self.data_sink = data_sink

        # Initialize adapter
        try:
            self.adapter = GoogleAdapter(
                token_provider=token_provider,
                config_file_path=google_config_file,
                manager_customer_id=manager_customer_id,
                api_version=api_version,
            )
        except Exception as e:
            raise ConfigurationError(f"Failed to initialize Google Ads adapter: {str(e)}")

        # Extract table names from config
        self.table_names = [
            key for key in config.keys()
            if key != "platform" and key.startswith("google_ads_")
        ]

        logger.info(f"GooglePipeline initialized with {len(self.table_names)} tables")
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
        2. Extract data from Google Ads API
        3. Process the data
        4. Load to data sink (if configured)

        Args:
            table_name: Name of the table to process
            start_date: Optional start date for time-series data
            end_date: Optional end date for time-series data
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
                rows_loaded = self._load_to_sink(processed_df, table_name, table_config)
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
        tables: Optional[List[str]] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        Run the pipeline for all configured tables.

        Tables are processed in order:
        1. google_ads_account
        2. google_ads_campaign
        3. google_ads_report
        4. google_ads_ad_creatives
        5. google_ads_placement
        6. google_ads_audience
        7. google_ads_cost_by_device

        Args:
            load_to_sink: If True, load data to sink after processing
            tables: Optional list of specific tables to process (default: all)

        Returns:
            Dictionary mapping table names to processed DataFrames

        Raises:
            PipelineError: If any critical table fails
        """
        # Filter tables if specific ones requested
        tables_to_process = self.table_names if tables is None else [t for t in self.table_names if t in tables]
        logger.info(f"Starting pipeline for {len(tables_to_process)} table(s): {', '.join(tables_to_process)}")
        start_time = datetime.now()

        # Define processing order
        processing_order = [
            "google_ads_account",
            "google_ads_campaign",
            "google_ads_report",
            "google_ads_ad_creatives",
            "google_ads_placement",
            "google_ads_audience",
            "google_ads_cost_by_device",
        ]

        results = {}
        failed_tables = []

        for table_name in processing_order:
            if table_name not in self.table_names:
                logger.debug(f"Skipping {table_name} (not in config)")
                continue

            # Skip if not in requested tables filter
            if tables and table_name not in tables:
                logger.debug(f"Skipping {table_name} (not in requested tables)")
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
            start_date: Optional start date for time-series data
            end_date: Optional end date for time-series data

        Returns:
            Raw DataFrame from API

        Raises:
            PipelineError: If extraction fails
        """
        try:
            # Calculate date range if needed
            if end_date is None:
                end_date = datetime.now()
            if start_date is None:
                # Get lookback days from config or use default
                lookback_days = table_config.get("day", DEFAULT_LOOKBACK_DAYS)
                start_date = end_date - timedelta(days=lookback_days)

            # Extract based on table type
            if table_name == "google_ads_account":
                return self.adapter.get_customer_accounts()

            elif table_name == "google_ads_campaign":
                return self.adapter.get_all_campaigns(start_date, end_date)

            elif table_name == "google_ads_report":
                return self.adapter.get_all_ad_report(start_date, end_date)

            elif table_name == "google_ads_ad_creatives":
                return self.adapter.get_all_ad_creatives()

            elif table_name == "google_ads_placement":
                return self.adapter.get_all_placements(start_date, end_date)

            elif table_name == "google_ads_audience":
                return self.adapter.get_all_audiences()

            elif table_name == "google_ads_cost_by_device":
                return self.adapter.get_all_cost_by_device()

            else:
                raise ConfigurationError(f"Unknown table: {table_name}")

        except Exception as e:
            logger.error(f"Failed to extract {table_name}: {e}")
            raise PipelineError(f"Failed to extract {table_name}") from e

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
        processor = GoogleProcessor(df)

        # Apply processing steps from configuration
        processing_config = table_config.get("processing", {})

        for step_name, step_params in processing_config.items():
            try:
                # Get the processing method
                method = getattr(processor, step_name, None)

                if method is None:
                    logger.warning(f"Unknown processing step: {step_name}")
                    continue

                # Parse parameters
                if step_params is None or step_params == "None":
                    # No parameters needed
                    processor = method()
                elif isinstance(step_params, dict):
                    # Handle both 'params' and 'cols'/'col'/'columns' keys
                    params = step_params.get("params")
                    cols = step_params.get("cols") or step_params.get("col") or step_params.get("columns")

                    if params is None or params == "None":
                        # Use cols if available
                        if cols:
                            processor = method(cols)
                        else:
                            processor = method()
                    else:
                        # Use params directly
                        processor = method(**params)
                else:
                    logger.warning(f"Invalid parameters for {step_name}: {step_params}")
                    continue

            except Exception as e:
                logger.error(f"Failed to apply processing step '{step_name}': {e}")
                # Continue with other steps

        return processor.get_df()

    def _load_to_sink(
        self,
        df: pd.DataFrame,
        table_name: str,
        table_config: Dict[str, Any],
    ) -> int:
        """
        Load DataFrame to data sink.

        Args:
            df: DataFrame to load
            table_name: Target table name
            table_config: Table configuration

        Returns:
            Number of rows loaded

        Raises:
            PipelineError: If data sink not configured or load fails
        """
        if self.data_sink is None:
            raise PipelineError("Data sink not configured")

        try:
            # Determine load mode and parameters
            pk_columns = None
            increment_columns = None

            # DEBUG: Log table_config to verify truncate field
            logger.warning(f"DEBUG table_config for {table_name}: truncate={table_config.get('truncate')}, upsert={table_config.get('upsert')}, increment={table_config.get('increment')}")

            if table_config.get("increment"):
                # INCREMENT mode: Insert new + Increment metrics for existing
                load_mode = "increment"
                increment_config = table_config["increment"]
                pk_columns = increment_config.get("pk_columns")
                increment_columns = increment_config.get("increment_columns")
                logger.debug(f"Using increment mode for {table_name}: PK={pk_columns}, metrics={increment_columns}")

            elif table_config.get("upsert"):
                # UPSERT mode: Insert new + Update all fields for existing
                load_mode = "upsert"
                upsert_config = table_config["upsert"]
                pk_columns = upsert_config.get("pk_columns")
                logger.debug(f"Using upsert mode for {table_name}: PK={pk_columns}")

            elif table_config.get("truncate", False):
                load_mode = "replace"  # Truncate and insert
                logger.debug(f"Using replace mode for {table_name}")

            elif table_config.get("update"):
                # Legacy: 'update' config → map to 'upsert'
                load_mode = "upsert"
                logger.debug(f"Using upsert mode for {table_name} (legacy 'update' config)")

            elif table_config.get("merge"):
                # Legacy: 'merge' config → map to 'upsert'
                load_mode = "upsert"
                logger.debug(f"Using upsert mode for {table_name} (legacy 'merge' config)")

            elif table_config.get("append"):
                # APPEND mode with explicit PK columns
                load_mode = "append"
                append_config = table_config["append"]
                pk_columns = append_config.get("pk_columns")
                logger.debug(f"Using append mode for {table_name} with PK={pk_columns}")

            else:
                # Default: APPEND (insert only new rows, skip duplicates)
                load_mode = "append"
                logger.debug(f"Using append mode for {table_name} (no PK specified)")

            # Check if sink has write_dataframe method (Vertica style)
            if hasattr(self.data_sink, "write_dataframe"):
                rows_loaded = self.data_sink.write_dataframe(
                    df=df,
                    table_name=table_name,
                    schema_name="GoogleAnalytics",
                    if_exists=load_mode,
                )
            # Check if sink has load method (Protocol style - preferred)
            elif hasattr(self.data_sink, "load"):
                rows_loaded = self.data_sink.load(
                    df=df,
                    table_name=table_name,
                    mode=load_mode,
                    dedupe_columns=pk_columns,
                    increment_columns=increment_columns,
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

    def close(self) -> None:
        """Close the pipeline and release resources."""
        if self.adapter:
            self.adapter.close()
            logger.debug("GooglePipeline closed")


def load_config(config_path: Path) -> Dict[str, Any]:
    """
    Load pipeline configuration from YAML file.

    Args:
        config_path: Path to configuration YAML file

    Returns:
        Configuration dictionary

    Raises:
        ConfigurationError: If config loading fails
    """
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except Exception as e:
        raise ConfigurationError(f"Failed to load config from {config_path}: {str(e)}") from e
