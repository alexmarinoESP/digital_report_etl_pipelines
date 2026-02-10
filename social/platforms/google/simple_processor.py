"""
Simple Google Ads Processor - Clean Room Implementation

This is a NEW, simplified processor that uses explicit column mapping from YAML.
It replaces the complex handle_columns/rename_columns approach with a single,
transparent processing pipeline.

Key Differences from old processor:
- NO handle_columns() magic
- NO rename_columns() confusion
- NO dropna_value() data loss
- Uses explicit column_mapping.yml for ALL transformations
- Single linear pipeline: Map → Convert → Validate → Select

This processor is ONLY for Google Ads and does not affect other platforms.
"""

import logging
import re
from typing import List, Optional
import pandas as pd

from social.platforms.google.column_mapper import GoogleColumnMapper

logger = logging.getLogger(__name__)


class SimpleGoogleProcessor:
    """
    Simplified Google Ads data processor using explicit column mapping.

    This processor wraps GoogleColumnMapper and adds Google-specific
    data cleaning operations (emoji removal, string cleaning, etc).
    """

    def __init__(self, table_name: str, mapper: Optional[GoogleColumnMapper] = None):
        """
        Initialize processor for a specific table.

        Args:
            table_name: Name of target table (e.g., 'google_ads_report')
            mapper: Column mapper instance (creates new one if None)
        """
        self.table_name = table_name
        self.mapper = mapper or GoogleColumnMapper()

    def process(
        self,
        df: pd.DataFrame,
        clean_placement: bool = False,
        clean_audience: bool = False,
        use_source_columns: bool = False
    ) -> pd.DataFrame:
        """
        Process DataFrame through complete pipeline.

        Args:
            df: Raw DataFrame from Google Ads API (after json_normalize)
            clean_placement: Apply placement-specific cleaning
            clean_audience: Apply audience-specific cleaning
            use_source_columns: Use db_columns_source for report table

        Returns:
            Processed DataFrame ready for database insert
        """
        if df.empty:
            logger.warning(f"[{self.table_name}] Empty DataFrame provided")
            return df

        logger.info(f"[{self.table_name}] Processing {len(df)} rows")

        # Core processing pipeline (from mapper)
        df = self.mapper.process_dataframe(
            df,
            self.table_name,
            use_source_columns=use_source_columns
        )

        # Apply table-specific cleaning
        if clean_placement and self.table_name == 'google_ads_placement':
            df = self._clean_placement_fields(df)

        if clean_audience and self.table_name == 'google_ads_audience':
            df = self._clean_audience_fields(df)

        # Clean campaign/ad names if present
        if 'name' in df.columns:
            df = self._clean_name_field(df, 'name')
        if 'ad_name' in df.columns:
            df = self._clean_name_field(df, 'ad_name')

        logger.info(f"[{self.table_name}] Processing complete: {len(df)} rows")
        return df

    def _clean_placement_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean placement-specific fields (display_name)."""
        if 'display_name' in df.columns:
            logger.debug(f"[{self.table_name}] Cleaning placement display_name")
            df['display_name'] = (
                df['display_name']
                .apply(self._remove_emoji)
                .apply(self._remove_non_latin)
                .apply(self._remove_piping)
            )
        return df

    def _clean_audience_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean audience-specific fields (display_name)."""
        if 'display_name' in df.columns:
            logger.debug(f"[{self.table_name}] Cleaning audience display_name")
            # Remove 'uservertical::' prefix if present
            df['display_name'] = df['display_name'].str.replace(
                r'^uservertical::', '', regex=True
            )
        return df

    def _clean_name_field(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """Clean name fields (campaign names, ad names)."""
        if column in df.columns:
            logger.debug(f"[{self.table_name}] Cleaning {column}")
            # Remove special characters but keep alphanumeric, spaces, and common separators
            df[column] = df[column].str.replace(r'[^\w\s\-|]', '', regex=True)
        return df

    @staticmethod
    def _remove_emoji(text: str) -> str:
        """Remove emoji characters from text."""
        if pd.isna(text):
            return text
        # Remove emoji using unicode ranges
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F1E0-\U0001F1FF"  # flags (iOS)
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "]+",
            flags=re.UNICODE,
        )
        return emoji_pattern.sub('', text)

    @staticmethod
    def _remove_non_latin(text: str) -> str:
        """Remove non-Latin characters from text."""
        if pd.isna(text):
            return text
        # Keep only Latin characters, numbers, spaces, and common punctuation
        return re.sub(r'[^\x00-\x7F]+', '', text)

    @staticmethod
    def _remove_piping(text: str) -> str:
        """Remove pipe characters from text."""
        if pd.isna(text):
            return text
        return text.replace('|', '')

    def limit_placement(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limit placement to top 25 by impressions per ad group (matches old logic).

        This replicates the exact behavior from social_posts/googleads/process.py:
        - Groups by 'id' (ad group id)
        - Sorts by impressions descending
        - Keeps top 25 per group

        Args:
            df: DataFrame with placement data

        Returns:
            DataFrame with top 25 placements per ad group
        """
        if df.empty:
            return df

        if 'id' not in df.columns or 'impressions' not in df.columns:
            logger.warning(f"[{self.table_name}] Missing 'id' or 'impressions' columns, skipping limit_placement")
            return df

        try:
            # Convert impressions to int (matches old logic)
            df['impressions'] = df['impressions'].astype(int)

            # Sort by id and impressions, then keep top 25 per id
            # This matches: df.groupby(['id']).apply(lambda x: x.sort_values(by='impressions', ascending=False).head(25))
            df = df.sort_values(by=['id', 'impressions'], ascending=[True, False])
            df = df.groupby('id').head(25).reset_index(drop=True)

            logger.info(f"[{self.table_name}] Limited to top 25 placements per ad group")
            return df

        except Exception as e:
            logger.error(f"[{self.table_name}] Failed to limit placements: {e}")
            return df

    def drop_duplicates(self, df: pd.DataFrame, subset: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Drop duplicate rows.

        Args:
            df: DataFrame
            subset: Columns to consider for duplicates (None = all columns)

        Returns:
            DataFrame without duplicates
        """
        initial_rows = len(df)
        df = df.drop_duplicates(subset=subset, keep='first')
        removed = initial_rows - len(df)
        if removed > 0:
            logger.info(f"[{self.table_name}] Removed {removed} duplicate rows")
        return df

    def dropna(self, df: pd.DataFrame, subset: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Drop rows with NA values in specified columns.

        Args:
            df: DataFrame
            subset: Columns to check for NA (None = all columns)

        Returns:
            DataFrame without NA rows
        """
        initial_rows = len(df)
        df = df.dropna(subset=subset)
        removed = initial_rows - len(df)
        if removed > 0:
            logger.info(f"[{self.table_name}] Removed {removed} rows with NA values")
        return df

    def fill_na(self, df: pd.DataFrame, columns: List[str], value: float = 0.0) -> pd.DataFrame:
        """
        Fill NA values in specified columns.

        Args:
            df: DataFrame
            columns: Columns to fill
            value: Value to use for filling

        Returns:
            DataFrame with filled values
        """
        for col in columns:
            if col in df.columns:
                df[col] = df[col].fillna(value)
        return df

    def aggregate_by_device(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate cost_by_device data to avoid duplicates (matches old logic).

        This replicates social_posts/googleads/process.py aggregate_by_keys():
        - Removes duplicates BEFORE aggregation (campaigns both SERVING and PAUSED)
        - Groups by (ad_id, device)
        - Sums cost_micros and clicks
        - Keeps customer_id_google as first

        Args:
            df: DataFrame with device cost data

        Returns:
            Aggregated DataFrame
        """
        if df.empty:
            return df

        required_cols = ['ad_id', 'device']
        if not all(col in df.columns for col in required_cols):
            logger.warning(f"[{self.table_name}] Missing required columns for aggregation, skipping")
            return df

        try:
            # CRITICAL FIX: Remove duplicates BEFORE aggregation
            initial_rows = len(df)
            df = df.drop_duplicates(subset=['ad_id', 'device'], keep='first')
            removed = initial_rows - len(df)

            if removed > 0:
                logger.warning(f"[{self.table_name}] Removed {removed} duplicate ad_id/device combinations before aggregation")

            # Build aggregation dict
            agg_dict = {}

            # Metrics to sum
            if 'cost_micros' in df.columns:
                df['cost_micros'] = pd.to_numeric(df['cost_micros'], errors='coerce')
                agg_dict['cost_micros'] = 'sum'

            if 'clicks' in df.columns:
                df['clicks'] = pd.to_numeric(df['clicks'], errors='coerce')
                agg_dict['clicks'] = 'sum'

            # Keep customer_id
            if 'customer_id_google' in df.columns:
                agg_dict['customer_id_google'] = 'first'
            elif 'customer_id' in df.columns:
                agg_dict['customer_id'] = 'first'

            if agg_dict:
                df = df.groupby(['ad_id', 'device'], as_index=False).agg(agg_dict)
                logger.info(f"[{self.table_name}] Aggregated to {len(df)} unique ad_id/device combinations")

            return df

        except Exception as e:
            logger.error(f"[{self.table_name}] Failed to aggregate by device: {e}")
            return df
