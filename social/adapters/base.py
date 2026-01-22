"""Base adapter for advertising platforms.

This module defines the abstract base class that all platform-specific
adapters must implement, ensuring consistent behavior across platforms.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import pandas as pd
from loguru import logger

from social.core.protocols import TokenProvider, DataSink
from social.core.config import PlatformConfig, TableConfig
from social.core.exceptions import APIError, ConfigurationError
from social.domain.models import DateRange


class BaseAdsPlatformAdapter(ABC):
    """Abstract base class for advertising platform adapters.

    Each platform (LinkedIn, Google Ads, Facebook, etc.) must implement
    this interface to provide data extraction capabilities.

    This follows the Adapter pattern to provide a uniform interface
    to different advertising platform APIs.
    """

    def __init__(
        self,
        config: PlatformConfig,
        token_provider: TokenProvider,
        data_sink: Optional[DataSink] = None,
    ):
        """Initialize the platform adapter.

        Args:
            config: Platform configuration
            token_provider: Provider for authentication tokens
            data_sink: Optional data sink for database queries (e.g., for URN dependencies)
        """
        self.config = config
        self.token_provider = token_provider
        self.data_sink = data_sink
        self._validate_configuration()

    def _validate_configuration(self) -> None:
        """Validate that the configuration is complete.

        Raises:
            ConfigurationError: If required configuration is missing
        """
        if not self.config.name:
            raise ConfigurationError("Platform name is required")
        if not self.config.api_base_url:
            raise ConfigurationError(f"API base URL is required for {self.config.name}")
        if not self.config.tables:
            raise ConfigurationError(f"No tables configured for {self.config.name}")

        logger.debug(
            f"Initialized {self.config.name} adapter with {len(self.config.tables)} tables"
        )

    @abstractmethod
    def extract_table(
        self,
        table_name: str,
        date_range: Optional[DateRange] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Extract data for a specific table.

        Args:
            table_name: Name of the table to extract
            date_range: Optional date range for time-series data
            **kwargs: Additional platform-specific parameters

        Returns:
            DataFrame with extracted and processed data

        Raises:
            APIError: If API request fails
            ConfigurationError: If table not configured
        """
        pass

    @abstractmethod
    def get_table_dependencies(self, table_name: str) -> List[str]:
        """Get list of tables that must be extracted before this one.

        Some tables require data from other tables (e.g., insights need campaigns).
        This method defines the dependency graph.

        Args:
            table_name: Name of the table

        Returns:
            List of table names that are dependencies
        """
        pass

    def get_table_config(self, table_name: str) -> TableConfig:
        """Get configuration for a specific table.

        Args:
            table_name: Name of the table

        Returns:
            TableConfig instance

        Raises:
            ConfigurationError: If table not configured
        """
        return self.config.get_table_config(table_name)

    def get_all_tables(self) -> List[str]:
        """Get list of all configured tables for this platform.

        Returns:
            List of table names
        """
        return list(self.config.tables.keys())

    def extract_all_tables(
        self,
        date_range: Optional[DateRange] = None,
        tables: Optional[List[str]] = None,
    ) -> Dict[str, pd.DataFrame]:
        """Extract data for all tables (or specified subset).

        This method handles dependency resolution to ensure tables are
        extracted in the correct order.

        Args:
            date_range: Optional date range for time-series data
            tables: Optional list of specific tables to extract (None = all)

        Returns:
            Dictionary mapping table names to DataFrames

        Raises:
            APIError: If any extraction fails
        """
        tables_to_extract = tables if tables else self.get_all_tables()
        sorted_tables = self._topological_sort(tables_to_extract)

        results = {}
        for table_name in sorted_tables:
            logger.info(f"Extracting {self.config.name} table: {table_name}")
            try:
                df = self.extract_table(table_name, date_range)
                results[table_name] = df
                logger.info(f"Extracted {len(df)} rows from {table_name}")
            except Exception as e:
                logger.error(f"Failed to extract {table_name}: {e}")
                raise

        return results

    def _topological_sort(self, tables: List[str]) -> List[str]:
        """Sort tables by dependencies using topological sort.

        Tables with no dependencies come first, tables with dependencies come after.

        Args:
            tables: List of table names to sort

        Returns:
            Sorted list of table names

        Raises:
            ConfigurationError: If circular dependencies detected
        """
        # Build dependency graph
        graph = {table: self.get_table_dependencies(table) for table in tables}

        # Kahn's algorithm for topological sort
        sorted_tables = []
        no_deps = [table for table in tables if not graph[table]]
        remaining = {table: deps[:] for table, deps in graph.items() if deps}

        while no_deps:
            current = no_deps.pop(0)
            sorted_tables.append(current)

            # Remove current from dependencies
            for table in list(remaining.keys()):
                if current in remaining[table]:
                    remaining[table].remove(current)
                    if not remaining[table]:
                        no_deps.append(table)
                        del remaining[table]

        if remaining:
            raise ConfigurationError(
                f"Circular dependencies detected in tables: {list(remaining.keys())}"
            )

        return sorted_tables

    @abstractmethod
    def _build_request_url(self, table_config: TableConfig, **params) -> str:
        """Build the complete API request URL.

        Args:
            table_config: Configuration for the table
            **params: Parameters to inject into the URL

        Returns:
            Complete URL string
        """
        pass

    @abstractmethod
    def _build_request_headers(self, table_config: TableConfig) -> Dict[str, str]:
        """Build HTTP headers for the request.

        Args:
            table_config: Configuration for the table

        Returns:
            Dictionary of HTTP headers
        """
        pass

    @abstractmethod
    def _build_request_params(self, table_config: TableConfig, **kwargs) -> Dict[str, Any]:
        """Build query parameters for the request.

        Args:
            table_config: Configuration for the table
            **kwargs: Additional parameters

        Returns:
            Dictionary of query parameters
        """
        pass

    @abstractmethod
    def _parse_response(self, response: Dict[str, Any], table_name: str) -> List[Dict[str, Any]]:
        """Parse API response into list of records.

        Different platforms have different response structures.
        This method normalizes them into a consistent format.

        Args:
            response: Raw API response
            table_name: Name of the table being extracted

        Returns:
            List of record dictionaries
        """
        pass

    @abstractmethod
    def _process_data(
        self,
        raw_data: List[Dict[str, Any]],
        table_config: TableConfig
    ) -> pd.DataFrame:
        """Process raw data into clean DataFrame.

        This applies all transformation steps defined in the table configuration.

        Args:
            raw_data: Raw records from API
            table_config: Configuration including processing steps

        Returns:
            Cleaned and transformed DataFrame
        """
        pass
