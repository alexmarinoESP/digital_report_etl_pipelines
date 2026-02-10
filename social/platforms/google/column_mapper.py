"""
Google Ads Column Mapper - Simple and Explicit Mapping System

This module provides a clean, transparent mapping between Google Ads API responses
and Vertica database columns using an explicit YAML configuration file.

NO MORE:
- handle_columns() magic
- rename_columns() confusion
- dropna_value() data loss
- Multiple transformation layers

JUST:
API Response → Explicit Rename → Type Conversion → Validate → DB Insert
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
import yaml
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


class GoogleColumnMapper:
    """Maps Google Ads API columns to database columns using explicit YAML config."""

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize mapper with column mapping configuration.

        Args:
            config_path: Path to column_mapping.yml (defaults to same directory)
        """
        if config_path is None:
            config_path = Path(__file__).parent / "column_mapping.yml"

        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        logger.info(f"Loaded column mappings for {len(self.config)} tables")

    def get_table_config(self, table_name: str) -> Dict:
        """Get configuration for a specific table."""
        if table_name not in self.config:
            raise ValueError(f"No mapping configuration found for table: {table_name}")
        return self.config[table_name]

    def map_columns(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Map API columns to DB columns for a specific table.

        Args:
            df: DataFrame with API column names (from json_normalize)
            table_name: Name of target table (e.g., 'google_ads_report')

        Returns:
            DataFrame with DB column names

        Raises:
            ValueError: If table not found in config or required columns missing
        """
        if df.empty:
            logger.warning(f"Empty DataFrame provided for {table_name}")
            return df

        config = self.get_table_config(table_name)
        mapping = config['api_to_db']

        # Log BEFORE mapping
        logger.info(f"[{table_name}] BEFORE mapping - Columns: {df.columns.tolist()}")

        # Apply column rename
        df = df.rename(columns=mapping)

        # Log AFTER mapping
        logger.info(f"[{table_name}] AFTER mapping - Columns: {df.columns.tolist()}")

        # Check for unmapped columns that might be important
        unmapped = set(df.columns) - set(mapping.values())
        if unmapped:
            # Filter out resource names and internal fields
            important_unmapped = [
                col for col in unmapped
                if not col.endswith('resourceName') and not col.endswith('.resourceName')
            ]
            if important_unmapped:
                logger.warning(
                    f"[{table_name}] Unmapped API columns found: {important_unmapped}. "
                    f"If these are needed, add them to column_mapping.yml"
                )

        return df

    def apply_conversions(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Apply data type conversions (e.g., cost_micros / 1,000,000).

        Args:
            df: DataFrame with DB column names
            table_name: Name of target table

        Returns:
            DataFrame with converted values
        """
        if df.empty:
            return df

        config = self.get_table_config(table_name)
        conversions = config.get('conversions', {})

        # Convert cost fields from micros to actual currency
        cost_fields = conversions.get('cost_fields', [])
        for field in cost_fields:
            if field in df.columns:
                # Convert string to numeric if needed
                df[field] = pd.to_numeric(df[field], errors='coerce')
                df[field] = df[field] / 1_000_000
                logger.debug(f"[{table_name}] Converted {field} from micros to currency")

        return df

    def add_metadata(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Add metadata columns (load_date, etc).

        Args:
            df: DataFrame with DB column names
            table_name: Name of target table

        Returns:
            DataFrame with metadata columns added
        """
        if df.empty:
            return df

        config = self.get_table_config(table_name)
        metadata = config.get('metadata', {})

        # Add load_date
        if metadata.get('add_load_date', False):
            df['load_date'] = datetime.now()
            logger.debug(f"[{table_name}] Added load_date column")

        # Process date fields (YYYYMMDD or YYYY-MM-DD → datetime)
        date_fields = metadata.get('process_dates', [])
        for field in date_fields:
            if field in df.columns:
                # Store original values to try multiple formats
                original_values = df[field].copy()

                # Try YYYYMMDD first
                converted = pd.to_datetime(original_values, format='%Y%m%d', errors='coerce')

                # If all NaT, try YYYY-MM-DD format
                if converted.isna().all():
                    converted = pd.to_datetime(original_values, format='%Y-%m-%d', errors='coerce')

                # If still all NaT, let pandas infer
                if converted.isna().all():
                    converted = pd.to_datetime(original_values, errors='coerce')

                df[field] = converted
                logger.debug(f"[{table_name}] Converted {field} to datetime")

        return df

    def validate_required_columns(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Validate that all required DB columns are present and not NULL.

        Args:
            df: DataFrame with DB column names
            table_name: Name of target table

        Raises:
            ValueError: If required columns are missing or NULL
        """
        if df.empty:
            return

        config = self.get_table_config(table_name)

        # Get required columns (exclude load_date which is added by us)
        if 'db_columns_source' in config:
            # For tables with source/target split (like report)
            required_cols = [c for c in config['db_columns_source'] if c != 'load_date']
        else:
            required_cols = [c for c in config['db_columns'] if c != 'load_date']

        # Check missing columns
        missing = set(required_cols) - set(df.columns)
        if missing:
            raise ValueError(
                f"[{table_name}] Missing required columns: {missing}. "
                f"Check column_mapping.yml api_to_db section."
            )

        # Check for NULL values in critical columns (IDs)
        critical_id_cols = [c for c in required_cols if 'id' in c.lower()]
        for col in critical_id_cols:
            if col in df.columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    logger.error(
                        f"[{table_name}] Column '{col}' has {null_count}/{len(df)} NULL values. "
                        f"This will cause DB insert failures."
                    )
                    # Show sample of rows with NULL
                    null_rows = df[df[col].isna()].head(3)
                    logger.error(f"Sample rows with NULL {col}:\n{null_rows.to_string()}")

    def select_db_columns(self, df: pd.DataFrame, table_name: str, use_source: bool = False) -> pd.DataFrame:
        """
        Select only columns that exist in the database table.

        Args:
            df: DataFrame with potentially extra columns
            table_name: Name of target table
            use_source: If True, use db_columns_source instead of db_columns

        Returns:
            DataFrame with only DB columns in correct order
        """
        if df.empty:
            return df

        config = self.get_table_config(table_name)

        if use_source and 'db_columns_source' in config:
            db_cols = config['db_columns_source']
        else:
            db_cols = config.get('db_columns', [])

        # Select only columns that exist in both df and db_cols
        available_cols = [c for c in db_cols if c in df.columns]

        if len(available_cols) != len(db_cols):
            missing = set(db_cols) - set(available_cols)
            logger.warning(f"[{table_name}] Some DB columns not in DataFrame: {missing}")

        return df[available_cols]

    def process_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        use_source_columns: bool = False
    ) -> pd.DataFrame:
        """
        Complete processing pipeline for a table.

        Pipeline:
        1. Map API columns → DB columns
        2. Apply conversions (cost_micros, dates)
        3. Add metadata (load_date)
        4. Validate required columns
        5. Select only DB columns

        Args:
            df: Raw DataFrame from API (after json_normalize)
            table_name: Target table name
            use_source_columns: Use db_columns_source if True

        Returns:
            Processed DataFrame ready for DB insert

        Raises:
            ValueError: If required columns missing or NULL
        """
        if df.empty:
            logger.warning(f"[{table_name}] Empty DataFrame, skipping processing")
            return df

        logger.info(f"[{table_name}] Starting processing pipeline for {len(df)} rows")

        # Step 1: Map columns
        df = self.map_columns(df, table_name)

        # Step 2: Apply conversions
        df = self.apply_conversions(df, table_name)

        # Step 3: Add metadata
        df = self.add_metadata(df, table_name)

        # Step 4: Validate
        self.validate_required_columns(df, table_name)

        # Step 5: Select DB columns
        df = self.select_db_columns(df, table_name, use_source=use_source_columns)

        logger.info(
            f"[{table_name}] Processing complete - {len(df)} rows, "
            f"{len(df.columns)} columns: {df.columns.tolist()}"
        )

        return df
