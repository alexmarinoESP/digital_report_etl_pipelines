"""
Microsoft Ads Data Processor Module.

This module provides data processing functionality specific to Microsoft Ads reports.
Unlike other platforms, Microsoft Ads reports come pre-formatted from the API,
requiring minimal processing beyond adding metadata columns.

Key Operations:
- Add row_loaded_date (current date for tracking)
- Add IngestionTimestamp (precise timestamp for data lineage)
- Convert ID columns to strings (to prevent precision loss)
- Minimal transformations (data is already clean from API)

Architecture:
- Immutable operations (returns new DataFrame)
- Chainable method design
- Type-safe with pandas type hints
- Comprehensive logging
"""

from datetime import datetime
from typing import List, Optional

import pandas as pd
from loguru import logger


class MicrosoftAdsProcessor:
    """
    Processor for Microsoft Ads report data.

    This processor handles minimal transformations as Microsoft Ads reports
    are already well-formatted from the API. Primary focus is on adding
    metadata columns and ensuring proper data types.

    Design Notes:
    - Microsoft Ads reports come pre-cleaned from the API (unlike LinkedIn/Facebook)
    - Processing is minimal: metadata addition + type conversions
    - Methods are chainable for fluent API style
    - All operations return new DataFrames (immutable pattern)

    Attributes:
        df (pd.DataFrame): The DataFrame being processed
    """

    def __init__(self, df: pd.DataFrame):
        """
        Initialize the processor with a DataFrame.

        Args:
            df: DataFrame to process (typically from MicrosoftAdsClient)

        Raises:
            ValueError: If df is None or empty
        """
        if df is None:
            raise ValueError("DataFrame cannot be None")

        if df.empty:
            logger.warning("Initializing processor with empty DataFrame")

        self.df = df.copy()  # Work with a copy to avoid side effects
        logger.debug(f"MicrosoftAdsProcessor initialized with {len(self.df)} rows")

    def add_row_loaded_date(self, column_name: str = "row_loaded_date") -> "MicrosoftAdsProcessor":
        """
        Add a column with the current date (for tracking when data was loaded).

        This column uses date format (YYYY-MM-DD) and is used to track
        which day the data was extracted from the API.

        Args:
            column_name: Name of the column to add (default: 'row_loaded_date')

        Returns:
            Self for method chaining
        """
        current_date = datetime.now().strftime("%Y-%m-%d")
        self.df[column_name] = current_date

        logger.debug(f"Added '{column_name}' column with value: {current_date}")
        return self

    def add_ingestion_timestamp(self, column_name: str = "IngestionTimestamp") -> "MicrosoftAdsProcessor":
        """
        Add a column with the current timestamp (for data lineage tracking).

        This column uses ISO 8601 format with microsecond precision,
        providing precise tracking of when data entered the pipeline.

        Args:
            column_name: Name of the column to add (default: 'IngestionTimestamp')

        Returns:
            Self for method chaining
        """
        current_timestamp = datetime.now().isoformat()
        self.df[column_name] = current_timestamp

        logger.debug(f"Added '{column_name}' column with value: {current_timestamp}")
        return self

    def convert_id_types(
        self,
        id_columns: Optional[List[str]] = None
    ) -> "MicrosoftAdsProcessor":
        """
        Convert ID columns to string type to prevent precision loss.

        Microsoft Ads uses large integer IDs that can lose precision
        when stored as int64. Converting to string preserves exact values.

        Args:
            id_columns: List of column names to convert. If None, uses default set:
                       ['AccountId', 'CampaignId', 'AdId', 'CustomerId']

        Returns:
            Self for method chaining
        """
        if id_columns is None:
            id_columns = ["AccountId", "CampaignId", "AdId", "CustomerId"]

        converted_count = 0
        for col in id_columns:
            if col in self.df.columns:
                try:
                    # Convert via int first to handle any float representations
                    self.df[col] = self.df[col].astype(int).astype(str)
                    converted_count += 1
                    logger.debug(f"Converted column '{col}' to string type")
                except Exception as e:
                    logger.warning(f"Could not convert column '{col}' to string: {e}")

        logger.info(f"Converted {converted_count} ID column(s) to string type")
        return self

    def replace_nan_with_zero(
        self,
        columns: Optional[List[str]] = None
    ) -> "MicrosoftAdsProcessor":
        """
        Replace NaN values with 0 in numeric columns.

        Useful for metric columns where NaN should be interpreted as zero
        (no clicks, no impressions, etc.).

        Args:
            columns: List of column names to process. If None, applies to all numeric columns.

        Returns:
            Self for method chaining
        """
        if columns is None:
            # Apply to all numeric columns
            numeric_columns = self.df.select_dtypes(include=["number"]).columns.tolist()
            columns = numeric_columns

        filled_count = 0
        for col in columns:
            if col in self.df.columns:
                nan_count = self.df[col].isna().sum()
                if nan_count > 0:
                    self.df[col] = self.df[col].fillna(0)
                    filled_count += 1
                    logger.debug(f"Filled {nan_count} NaN values in column '{col}'")

        logger.info(f"Replaced NaN with 0 in {filled_count} column(s)")
        return self

    def drop_columns(self, columns: List[str]) -> "MicrosoftAdsProcessor":
        """
        Drop specified columns from the DataFrame.

        Args:
            columns: List of column names to drop

        Returns:
            Self for method chaining
        """
        existing_cols = [col for col in columns if col in self.df.columns]

        if existing_cols:
            self.df = self.df.drop(columns=existing_cols)
            logger.debug(f"Dropped columns: {existing_cols}")
        else:
            logger.warning(f"None of the specified columns exist: {columns}")

        return self

    def rename_columns(self, column_mapping: dict) -> "MicrosoftAdsProcessor":
        """
        Rename columns according to the provided mapping.

        Args:
            column_mapping: Dictionary mapping old column names to new names

        Returns:
            Self for method chaining
        """
        # Only rename columns that exist
        valid_mapping = {
            old: new for old, new in column_mapping.items() if old in self.df.columns
        }

        if valid_mapping:
            self.df = self.df.rename(columns=valid_mapping)
            logger.debug(f"Renamed columns: {valid_mapping}")
        else:
            logger.warning("No valid columns found to rename")

        return self

    def filter_rows(self, condition: pd.Series) -> "MicrosoftAdsProcessor":
        """
        Filter rows based on a boolean condition.

        Args:
            condition: Boolean Series indicating which rows to keep

        Returns:
            Self for method chaining
        """
        rows_before = len(self.df)
        self.df = self.df[condition]
        rows_after = len(self.df)

        logger.debug(f"Filtered rows: {rows_before} -> {rows_after} ({rows_before - rows_after} removed)")
        return self

    def deduplicate(
        self,
        subset: Optional[List[str]] = None,
        keep: str = "first"
    ) -> "MicrosoftAdsProcessor":
        """
        Remove duplicate rows from the DataFrame.

        Args:
            subset: Column names to consider for identifying duplicates.
                   If None, uses all columns.
            keep: Which duplicate to keep ('first', 'last', or False to drop all)

        Returns:
            Self for method chaining
        """
        rows_before = len(self.df)
        self.df = self.df.drop_duplicates(subset=subset, keep=keep)
        rows_after = len(self.df)

        duplicates_removed = rows_before - rows_after
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate row(s)")
        else:
            logger.debug("No duplicate rows found")

        return self

    def sort_by(self, columns: List[str], ascending: bool = True) -> "MicrosoftAdsProcessor":
        """
        Sort DataFrame by specified columns.

        Args:
            columns: Column names to sort by
            ascending: Sort order (True for ascending, False for descending)

        Returns:
            Self for method chaining
        """
        existing_cols = [col for col in columns if col in self.df.columns]

        if existing_cols:
            self.df = self.df.sort_values(by=existing_cols, ascending=ascending)
            logger.debug(f"Sorted DataFrame by columns: {existing_cols}")
        else:
            logger.warning(f"None of the specified sort columns exist: {columns}")

        return self

    def get_df(self) -> pd.DataFrame:
        """
        Get the processed DataFrame.

        Returns:
            Processed DataFrame

        Note:
            Returns a copy to prevent external modifications
        """
        logger.debug(f"Returning processed DataFrame with {len(self.df)} rows and {len(self.df.columns)} columns")
        return self.df.copy()

    def get_row_count(self) -> int:
        """
        Get the number of rows in the DataFrame.

        Returns:
            Number of rows
        """
        return len(self.df)

    def get_column_names(self) -> List[str]:
        """
        Get list of column names in the DataFrame.

        Returns:
            List of column names
        """
        return self.df.columns.tolist()

    def __len__(self) -> int:
        """
        Get the number of rows in the DataFrame.

        Returns:
            Number of rows
        """
        return len(self.df)

    def __repr__(self) -> str:
        """
        String representation of the processor.

        Returns:
            String describing the processor state
        """
        return f"MicrosoftAdsProcessor(rows={len(self.df)}, columns={len(self.df.columns)})"
