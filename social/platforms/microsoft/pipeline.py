"""
Microsoft Ads ETL Pipeline Module.

This module provides the main pipeline orchestrator for Microsoft Ads data extraction,
transformation, and loading (ETL). It coordinates the client, processor, and data sink
to provide a complete end-to-end workflow.

Key Features:
- Multi-table support with configuration-driven table definitions
- Dependency injection for TokenProvider and DataSink
- Proper error handling and recovery
- Detailed logging and progress tracking
- Support for both single table and batch processing

Architecture:
- SOLID principles with dependency injection
- Protocol-based contracts for flexibility
- Clear separation of concerns (client/processor/sink)
- Container-ready (no browser interactions)
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from bingads.authorization import AuthorizationData
from loguru import logger

from social.core.config import PlatformConfig, TableConfig
from social.core.exceptions import ConfigurationError, PipelineError
from social.platforms.microsoft.authenticator import MicrosoftAdsAuthenticator
from social.platforms.microsoft.client import MicrosoftAdsClient
from social.platforms.microsoft.processor import MicrosoftAdsProcessor


class MicrosoftAdsPipeline:
    """
    Main ETL pipeline for Microsoft Ads platform.

    This pipeline orchestrates the complete data extraction process:
    1. Authentication (via MicrosoftAdsAuthenticator)
    2. Report generation (via MicrosoftAdsClient)
    3. Data processing (via MicrosoftAdsProcessor)
    4. Data loading (via DataSink)

    Design:
    - Configuration-driven table definitions
    - Dependency injection for flexibility
    - Protocol-based contracts
    - Comprehensive error handling

    Attributes:
        config (PlatformConfig): Platform configuration with table definitions
        authenticator (MicrosoftAdsAuthenticator): Authentication manager
        client (MicrosoftAdsClient): Microsoft Ads API client
        data_sink (Optional): Data sink for loading processed data
    """

    def __init__(
        self,
        config: PlatformConfig,
        authenticator: Optional[MicrosoftAdsAuthenticator] = None,
        data_sink: Optional[Any] = None,
    ):
        """
        Initialize the Microsoft Ads pipeline.

        Args:
            config: Platform configuration with table definitions
            authenticator: Optional authenticator (will be created from config if not provided)
            data_sink: Optional data sink for loading data (e.g., VerticaDBManager)

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if config is None:
            raise ConfigurationError("Configuration cannot be None")

        self.config = config
        self.authenticator = authenticator
        self.data_sink = data_sink
        self.client: Optional[MicrosoftAdsClient] = None

        logger.info("MicrosoftAdsPipeline initialized")
        logger.debug(f"Platform: {config.name}, Tables: {len(config.tables)}")

    def _ensure_authenticated(self) -> None:
        """
        Ensure authentication is complete and client is initialized.

        Raises:
            PipelineError: If authentication fails or authenticator not provided
        """
        if self.authenticator is None:
            raise PipelineError(
                "No authenticator provided. Cannot initialize client without authentication."
            )

        if self.client is None:
            logger.info("Initializing Microsoft Ads client with authentication")

            # Get authorization data from authenticator
            authorization_data = self.authenticator.get_authorization_data()

            # Initialize client
            api_version = int(self.config.api_version.replace("v", ""))
            self.client = MicrosoftAdsClient(
                authorization_data=authorization_data,
                api_version=api_version
            )

            logger.success("Microsoft Ads client initialized successfully")

    def run(
        self,
        table_name: str,
        account_ids: Optional[List[str]] = None,
        report_params: Optional[Dict[str, Any]] = None,
        load_to_sink: bool = True,
    ) -> pd.DataFrame:
        """
        Run the pipeline for a single table.

        This method executes the complete ETL workflow:
        1. Retrieve table configuration
        2. Generate report from Microsoft Ads API
        3. Process the data
        4. Load to data sink (if configured)

        Args:
            table_name: Name of the table to process
            account_ids: Optional list of account IDs (fetches all if not provided)
            report_params: Optional parameters to override config (e.g., time_period, aggregation)
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
            # Ensure authentication
            self._ensure_authenticated()
            assert self.client is not None, "Client should be initialized"

            # Get table configuration
            table_config = self.config.get_table_config(table_name)
            logger.debug(f"Table config loaded: {table_config.name}")

            # Get account IDs if not provided
            if account_ids is None:
                logger.info("No account IDs provided, fetching all accounts")
                account_ids = self.client.get_account_ids()
                logger.info(f"Found {len(account_ids)} account(s)")

            # Prepare report parameters
            params = self._prepare_report_params(table_config, report_params)
            logger.debug(f"Report parameters: {params}")

            # Generate and process report
            logger.info(f"Generating report: {params['report_name']}")
            _, df = self.client.generate_and_process_report(
                account_ids=account_ids,
                report_name=params["report_name"],
                time_period=params["time_period"],
                aggregation=params["aggregation"],
                columns=params.get("columns"),
                header_identifier=params["header_identifier"],
                use_temp_dir=True,
                delete_after_processing=True,
            )

            logger.success(f"Report generated successfully: {len(df)} rows")

            # Process the data
            logger.info("Processing data")
            processor = MicrosoftAdsProcessor(df)

            # Apply processing steps from config
            if table_config.processing_steps:
                logger.debug(f"Applying processing steps: {table_config.processing_steps}")
                processor = self._apply_processing_steps(processor, table_config.processing_steps)

            processed_df = processor.get_df()
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
        account_ids: Optional[List[str]] = None,
        load_to_sink: bool = True,
    ) -> Dict[str, pd.DataFrame]:
        """
        Run the pipeline for all configured tables.

        Args:
            account_ids: Optional list of account IDs (fetches all if not provided)
            load_to_sink: If True, load data to sink after processing

        Returns:
            Dictionary mapping table names to processed DataFrames

        Raises:
            PipelineError: If any table fails
        """
        logger.info(f"Starting pipeline for all tables: {list(self.config.tables.keys())}")
        start_time = datetime.now()

        results = {}
        failed_tables = []

        for table_name in self.config.tables.keys():
            try:
                logger.info(f"Processing table {table_name}")
                df = self.run(
                    table_name=table_name,
                    account_ids=account_ids,
                    load_to_sink=load_to_sink,
                )
                results[table_name] = df
                logger.success(f"Table {table_name} completed successfully")

            except Exception as e:
                logger.error(f"Table {table_name} failed: {e}")
                failed_tables.append(table_name)
                # Continue with other tables

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

    def _prepare_report_params(
        self,
        table_config: TableConfig,
        override_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Prepare report parameters from table config and overrides.

        Args:
            table_config: Table configuration
            override_params: Optional parameters to override config values

        Returns:
            Dictionary of report parameters
        """
        # Start with defaults
        params = {
            "report_name": table_config.additional_params.get("report_name", "Ad"),
            "time_period": table_config.additional_params.get("time_period", "LastSixMonths"),
            "aggregation": table_config.additional_params.get("aggregation", "Summary"),
            "header_identifier": table_config.additional_params.get("header_identifier", "Conversions"),
            "columns": table_config.fields,
        }

        # Apply overrides
        if override_params:
            params.update(override_params)

        return params

    def _apply_processing_steps(
        self,
        processor: MicrosoftAdsProcessor,
        processing_steps: Union[List, Dict],
    ) -> MicrosoftAdsProcessor:
        """
        Apply custom processing steps from configuration.

        Args:
            processor: Processor instance
            processing_steps: List or dict of processing steps

        Returns:
            Processor with applied steps
        """
        # Convert dict format to list format if needed
        if isinstance(processing_steps, dict):
            steps_list = []
            for step_name, step_params in processing_steps.items():
                if isinstance(step_params, dict) and step_params:
                    steps_list.append({"name": step_name, **step_params})
                else:
                    steps_list.append(step_name)
            processing_steps = steps_list

        # Apply each step
        for step in processing_steps:
            if isinstance(step, str):
                step_name = step
                step_params = {}
            elif isinstance(step, dict):
                step_name = step.get("name", list(step.keys())[0])
                step_params = {k: v for k, v in step.items() if k != "name"}
            else:
                logger.warning(f"Unknown step format: {step}")
                continue

            # Apply known processing methods
            if step_name == "convert_id_types":
                id_columns = step_params.get("id_columns")
                processor.convert_id_types(id_columns)
            elif step_name == "add_row_loaded_date":
                column_name = step_params.get("column_name", "row_loaded_date")
                processor.add_row_loaded_date(column_name)
            elif step_name == "add_ingestion_timestamp":
                column_name = step_params.get("column_name", "IngestionTimestamp")
                processor.add_ingestion_timestamp(column_name)
            elif step_name == "replace_nan_with_zero":
                columns = step_params.get("columns")
                processor.replace_nan_with_zero(columns)
            elif step_name == "drop_columns":
                columns = step_params.get("columns", [])
                processor.drop_columns(columns)
            elif step_name == "rename_columns":
                mapping = step_params.get("mapping", {})
                processor.rename_columns(mapping)
            elif step_name == "deduplicate":
                subset = step_params.get("subset")
                keep = step_params.get("keep", "first")
                processor.deduplicate(subset, keep)
            else:
                logger.warning(f"Unknown processing step: {step_name}")

        return processor

    def _load_to_sink(
        self,
        df: pd.DataFrame,
        table_name: str,
        table_config: TableConfig,
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
            # Determine load mode
            load_mode = table_config.additional_params.get("load_mode", "append")

            # Check if sink has write_dataframe method (Vertica style)
            if hasattr(self.data_sink, "write_dataframe"):
                rows_loaded = self.data_sink.write_dataframe(
                    df=df,
                    table_name=table_name,
                    schema_name=table_config.additional_params.get("schema", "GoogleAnalytics"),
                    if_exists=load_mode,
                )
            # Check if sink has load method (Protocol style)
            elif hasattr(self.data_sink, "load"):
                rows_loaded = self.data_sink.load(
                    df=df,
                    table_name=table_name,
                    mode=load_mode,
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
        return list(self.config.tables.keys())

    def get_table_config(self, table_name: str) -> TableConfig:
        """
        Get configuration for a specific table.

        Args:
            table_name: Name of the table

        Returns:
            TableConfig instance

        Raises:
            ConfigurationError: If table not found
        """
        return self.config.get_table_config(table_name)
