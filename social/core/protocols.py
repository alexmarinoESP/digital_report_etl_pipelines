"""Protocol definitions (interfaces) for the social module.

This module defines all the abstract interfaces using Python's Protocol
to support dependency inversion and enable proper testing with mocks.
"""

from typing import Protocol, Dict, Any, List, Optional
from datetime import datetime
import pandas as pd


class TokenProvider(Protocol):
    """Interface for providing and refreshing authentication tokens."""

    def get_access_token(self) -> str:
        """Retrieve the current access token.

        Returns:
            str: Valid access token

        Raises:
            AuthenticationError: If token retrieval fails
        """
        ...

    def get_refresh_token(self) -> str:
        """Retrieve the refresh token.

        Returns:
            str: Refresh token

        Raises:
            AuthenticationError: If token retrieval fails
        """
        ...

    def refresh_access_token(self) -> str:
        """Refresh the access token using the refresh token.

        Returns:
            str: New access token

        Raises:
            AuthenticationError: If token refresh fails
        """
        ...

    def get_token_expiry(self) -> datetime:
        """Get the token expiration datetime.

        Returns:
            datetime: When the token expires
        """
        ...


class AdsPlatformClient(Protocol):
    """Interface for ads platform API clients (LinkedIn, Google, Facebook, etc.)."""

    def get(self, endpoint: str, params: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Execute a GET request to the platform API.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            headers: Optional HTTP headers

        Returns:
            Dict containing the API response

        Raises:
            APIError: If the request fails
        """
        ...

    def post(self, endpoint: str, data: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Execute a POST request to the platform API.

        Args:
            endpoint: API endpoint path
            data: Request body data
            headers: Optional HTTP headers

        Returns:
            Dict containing the API response

        Raises:
            APIError: If the request fails
        """
        ...

    def get_paginated(self, endpoint: str, params: Dict[str, Any], page_size: int = 100) -> List[Dict[str, Any]]:
        """Fetch all pages of data from a paginated endpoint.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            page_size: Number of items per page

        Returns:
            List of all items from all pages

        Raises:
            APIError: If any request fails
        """
        ...


class DataProcessor(Protocol):
    """Interface for processing raw API data into clean DataFrames."""

    def process(self, raw_data: List[Dict[str, Any]], config: Dict[str, Any]) -> pd.DataFrame:
        """Process raw API data into a clean DataFrame.

        Args:
            raw_data: Raw data from API
            config: Processing configuration

        Returns:
            Cleaned and transformed DataFrame

        Raises:
            DataValidationError: If data validation fails
        """
        ...

    def validate(self, df: pd.DataFrame, schema: Dict[str, Any]) -> bool:
        """Validate DataFrame against expected schema.

        Args:
            df: DataFrame to validate
            schema: Expected schema definition

        Returns:
            True if valid, False otherwise

        Raises:
            DataValidationError: If validation fails critically
        """
        ...


class DataSource(Protocol):
    """Interface for data sources (APIs, databases, files, etc.)."""

    def extract(self, table_name: str, config: Dict[str, Any]) -> pd.DataFrame:
        """Extract data for a specific table.

        Args:
            table_name: Name of the table/entity to extract
            config: Extraction configuration

        Returns:
            DataFrame with extracted data

        Raises:
            APIError: If extraction fails
        """
        ...

    def get_dependencies(self, table_name: str) -> List[str]:
        """Get list of tables that must be extracted before this one.

        Args:
            table_name: Name of the table

        Returns:
            List of dependency table names
        """
        ...


class DataSink(Protocol):
    """Interface for data sinks (databases, files, cloud storage, etc.)."""

    def load(self, df: pd.DataFrame, table_name: str, mode: str = "append") -> int:
        """Load DataFrame into the sink.

        Args:
            df: DataFrame to load
            table_name: Target table name
            mode: Load mode - 'append', 'replace', 'upsert'

        Returns:
            Number of rows loaded

        Raises:
            DatabaseError: If load fails
        """
        ...

    def query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results.

        Args:
            sql: SQL query string

        Returns:
            DataFrame with query results

        Raises:
            DatabaseError: If query fails
        """
        ...

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the sink.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
        ...


class ConfigProvider(Protocol):
    """Interface for configuration providers."""

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value by key.

        Args:
            key: Configuration key (supports dot notation for nested keys)
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        ...

    def get_table_config(self, platform: str, table_name: str) -> Dict[str, Any]:
        """Get configuration for a specific table.

        Args:
            platform: Platform name (linkedin, google, facebook)
            table_name: Table name

        Returns:
            Table configuration dictionary

        Raises:
            ConfigurationError: If configuration not found
        """
        ...

    def get_platform_config(self, platform: str) -> Dict[str, Any]:
        """Get configuration for a platform.

        Args:
            platform: Platform name

        Returns:
            Platform configuration dictionary

        Raises:
            ConfigurationError: If configuration not found
        """
        ...
