"""Facebook Ads ETL Pipeline Module.

This module provides the main pipeline orchestrator for Facebook Ads data extraction,
transformation, and loading (ETL). It coordinates the adapter, processor, and data sink
to provide a complete end-to-end workflow.

Key Features:
- Multi-table support with configuration-driven table definitions
- Multi-account iteration (Facebook ad accounts)
- Dependency injection for TokenProvider and DataSink
- Proper error handling and recovery
- Detailed logging and progress tracking
- Support for both single table and batch processing
- Nested breakdowns handling (actions, action_values)

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
from social.platforms.facebook.adapter import FacebookAdapter
from social.platforms.facebook.processor import FacebookProcessor


class FacebookPipeline:
    """Main ETL pipeline for Facebook Ads platform."""

    def __init__(
        self,
        config: Dict[str, Any],
        token_provider: TokenProvider,
        ad_account_ids: List[str],
        app_id: str,
        app_secret: str,
        data_sink: Optional[DataSink] = None,
    ):
        """Initialize the Facebook Ads pipeline."""
        if config is None:
            raise ConfigurationError("Configuration cannot be None")

        self.config = config
        self.token_provider = token_provider
        self.data_sink = data_sink
        self.ad_account_ids = ad_account_ids

        # Initialize adapter
        self.adapter = FacebookAdapter(
            token_provider=token_provider,
            app_id=app_id,
            app_secret=app_secret,
            ad_account_ids=ad_account_ids,
        )

        # Extract table names
        self.table_names = [
            key for key in config.keys()
            if key != "platform" and key.startswith("fb_ads_")
        ]

        logger.info(f"FacebookPipeline initialized with {len(self.table_names)} tables and {len(ad_account_ids)} accounts")

    def run(
        self,
        table_name: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        load_to_sink: bool = True,
    ) -> pd.DataFrame:
        """Run the pipeline for a single table."""
        logger.info(f"Starting pipeline for table: {table_name}")
        start_time = datetime.now()

        try:
            table_config = self.config.get(table_name)
            if not table_config:
                raise ConfigurationError(f"Table '{table_name}' not found in configuration")

            # Extract
            df = self._extract_table(table_name, table_config, start_date, end_date)
            if df.empty:
                logger.warning(f"No data extracted for {table_name}")
                return df

            logger.success(f"Extracted {len(df)} rows for {table_name}")

            # Process
            processed_df = self._process_table(df, table_name, table_config)
            if processed_df.empty:
                logger.warning(f"No data after processing for {table_name}")
                return processed_df

            logger.success(f"Processed {len(processed_df)} rows for {table_name}")

            # Load
            if load_to_sink and self.data_sink:
                self._load_to_sink(processed_df, table_name)

            duration = (datetime.now() - start_time).total_seconds()
            logger.success(f"Pipeline completed for {table_name} in {duration:.2f}s")

            return processed_df

        except Exception as e:
            logger.error(f"Pipeline failed for {table_name}: {str(e)}")
            raise PipelineError(f"Pipeline failed for {table_name}: {str(e)}") from e

    def run_all_tables(self) -> Dict[str, pd.DataFrame]:
        """Run the pipeline for all configured tables."""
        logger.info(f"Running pipeline for all {len(self.table_names)} tables")
        results = {}

        for table_name in self.table_names:
            try:
                df = self.run(table_name, load_to_sink=True)
                results[table_name] = df
            except Exception as e:
                logger.error(f"Failed to process table {table_name}: {str(e)}")
                results[table_name] = pd.DataFrame()

        successful = sum(1 for df in results.values() if not df.empty)
        logger.info(f"Pipeline completed: {successful}/{len(self.table_names)} tables successful")
        return results

    def _extract_table(
        self,
        table_name: str,
        table_config: Dict[str, Any],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """Extract data for a specific table."""
        try:
            table_type = table_config.get("type")
            date_preset = table_config.get("date_preset", "last_7d")

            if table_type == "get_campaigns":
                df = self.adapter.get_all_campaigns(date_preset=date_preset)
            elif table_type == "get_ad_sets":
                df = self.adapter.get_all_ad_sets(date_preset=date_preset)
            elif table_type == "get_insights":
                if table_name == "fb_ads_insight_actions":
                    df = self.adapter.get_all_insights_with_actions(date_preset=date_preset)
                else:
                    df = self.adapter.get_all_insights(date_preset=date_preset)
            elif table_type == "get_custom_conversions":
                df = self.adapter.get_all_custom_conversions()
            else:
                if table_name == "fb_ads_audience_adset":
                    df = self.adapter.get_all_audience_targeting(date_preset=date_preset)
                else:
                    raise PipelineError(f"Unknown table type: {table_type}")
            return df
        except Exception as e:
            logger.error(f"Extraction failed for {table_name}: {str(e)}")
            raise PipelineError(f"Extraction failed for {table_name}: {str(e)}") from e

    def _process_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        table_config: Dict[str, Any],
    ) -> pd.DataFrame:
        """Process extracted data."""
        if df.empty:
            return df

        processor = FacebookProcessor(df)
        processing_steps = table_config.get("processing", {})

        for step_name, step_params in processing_steps.items():
            try:
                # Handle None or "None" string
                if step_params is None or step_params == "None":
                    step_params = {}

                if isinstance(step_params, dict):
                    # Extract params from nested structure
                    if "params" in step_params:
                        raw_params = step_params["params"]
                        # Handle None, "None", or empty params
                        if raw_params is None or raw_params == "None" or raw_params == "":
                            params = {}
                        elif isinstance(raw_params, str):
                            # String params should be empty dict
                            params = {}
                        else:
                            params = raw_params
                    else:
                        params = step_params
                else:
                    params = {}

                # Ensure params is always a dict
                if not isinstance(params, dict):
                    params = {}

                if hasattr(processor, step_name):
                    method = getattr(processor, step_name)
                    processor = method(**params) if params else method()
                else:
                    logger.warning(f"Unknown processing step: {step_name}")
            except Exception as e:
                logger.error(f"Processing step '{step_name}' failed: {str(e)}")

        return processor.get_df()

    def _load_to_sink(self, df: pd.DataFrame, table_name: str) -> None:
        """Load processed data to the configured data sink.

        Determines load mode from table configuration:
        - If 'increment' config exists → use increment mode
        - Otherwise → use append mode (default)
        """
        if not self.data_sink:
            logger.warning("No data sink configured, skipping load")
            return

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

            # Write to sink with appropriate method
            if hasattr(self.data_sink, "load"):
                # VerticaDataSink has load() with all modes
                rows_written = self.data_sink.load(
                    df=df,
                    table_name=table_name,
                    mode=load_mode,
                    dedupe_columns=pk_columns,
                    increment_columns=increment_columns,
                )
            elif hasattr(self.data_sink, "write"):
                # Fallback to write() method (older sinks - limited support)
                if load_mode in ["increment", "upsert"]:
                    logger.warning(f"Data sink does not support {load_mode} mode, falling back to append")
                    load_mode = "append"
                rows_written = self.data_sink.write(df=df, table_name=table_name, if_exists=load_mode)
            else:
                raise PipelineError("Data sink has no compatible write/load method")

            logger.success(f"Loaded {rows_written} rows to {table_name}")
        except Exception as e:
            logger.error(f"Failed to load data to sink: {str(e)}")
            raise PipelineError(f"Failed to load data to sink: {str(e)}") from e

    def get_all_tables(self) -> List[str]:
        """Get list of all configured table names.

        Returns:
            List of table names
        """
        return self.table_names

    def extract_all_tables(self, tables: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """Extract data for all tables (or specified subset).

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
                results[table_name] = pd.DataFrame()

        return results

    def close(self) -> None:
        """Close pipeline resources and cleanup."""
        if hasattr(self, "adapter") and self.adapter:
            self.adapter.close()
            logger.debug("Pipeline resources closed")


def load_config(config_path: Path) -> Dict[str, Any]:
    """Load pipeline configuration from YAML file."""
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except Exception as e:
        raise ConfigurationError(f"Failed to load config from {config_path}: {str(e)}") from e
