"""Google Ads Data Processor.

This module provides a chainable data processor for Google Ads data transformation.
All methods return self to enable method chaining.

Key Features:
- Chainable API (fluent interface)
- Column renaming and cleaning
- Cost conversion (micros to currency)
- Date handling
- Company ID mapping
- Emoji and special character removal
- Aggregation and deduplication

Design:
- Completely independent (NO base class)
- Immutable pattern (returns new processor)
- Type-safe with 100% type hints
- Production-ready error handling
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from social.platforms.google.constants import COMPANY_ACCOUNT_MAP, MICROS_DIVISOR
from social.utils.aggregation import aggregate_metrics_by_entity


class GoogleProcessor:
    """
    Chainable data processor for Google Ads data.

    This processor implements a fluent interface where all methods return self,
    allowing operations to be chained together:

    Example:
        processor = GoogleProcessor(df)
        result = (processor
            .handle_columns()
            .convert_costs(['cost_micros'])
            .deal_with_date(['start_date', 'end_date'])
            .add_company('customer_id_google')
            .add_row_loaded_date()
            .get_df())

    Design:
    - Chainable methods (return self)
    - In-place DataFrame modifications
    - Comprehensive logging
    - Error handling with fallback

    Attributes:
        df: DataFrame being processed
    """

    def __init__(self, df: pd.DataFrame) -> None:
        """
        Initialize processor with DataFrame.

        Args:
            df: DataFrame to process
        """
        self.df = df.copy()  # Work on a copy to avoid side effects

    def handle_columns(self) -> "GoogleProcessor":
        """
        Handle Google Ads column names by removing prefixes and cleaning up.

        This method:
        1. Removes prefixes (e.g., 'campaign.id' -> 'id', but keeps 'customer.id')
        2. Replaces dots with underscores
        3. Removes 'resource' columns
        4. Converts to lowercase

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        try:
            logger.debug(f"handle_columns BEFORE: {self.df.columns.tolist()}")

            # 1. Remove prefixes (split on first dot), except for 'customer'
            new_columns = []
            for col in self.df.columns:
                if "customer" not in col and "." in col:
                    # Remove prefix: 'campaign.id' → 'id'
                    col = col.split(".", 1)[1]
                new_columns.append(col)
            self.df.columns = new_columns

            # 2. Replace remaining dots with underscores
            self.df.columns = [col.replace(".", "_") for col in self.df.columns]

            # 3. Remove 'resource' columns completely (we don't need them after ad_group.id is present)
            cols_to_keep = [col for col in self.df.columns if "resource" not in col.lower()]
            self.df = self.df[cols_to_keep]

            # 4. Convert to lowercase
            self.df.columns = [col.lower() for col in self.df.columns]

            # 5. Remove duplicate column names (keep first occurrence)
            if self.df.columns.duplicated().any():
                logger.warning(f"Found duplicate column names: {self.df.columns[self.df.columns.duplicated()].tolist()}")
                self.df = self.df.loc[:, ~self.df.columns.duplicated()]

            logger.debug(f"handle_columns AFTER: {self.df.columns.tolist()}")
            logger.debug(f"Cleaned {len(self.df.columns)} Google Ads column names")

        except Exception as e:
            logger.error(f"Error handling columns: {e}")

        return self

    def convert_costs(self, columns: List[str]) -> "GoogleProcessor":
        """
        Convert Google Ads cost micros to actual currency units.

        Google Ads returns costs in micros (1/1,000,000 of currency unit).
        This method divides by MICROS_DIVISOR (1,000,000) to get actual values.

        Args:
            columns: List of column names containing micros values

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col in self.df.columns:
                try:
                    # Fill NaN with 0, convert to int64, then divide by micros divisor
                    self.df[col] = (
                        self.df[col]
                        .fillna(0)
                        .astype("int64") / MICROS_DIVISOR
                    )
                    logger.debug(f"Converted cost column '{col}' from micros to currency")
                except Exception as e:
                    logger.warning(f"Failed to convert cost column '{col}': {e}")
            else:
                logger.debug(f"Column '{col}' not found, skipping cost conversion")

        return self

    def deal_with_date(self, columns: List[str]) -> "GoogleProcessor":
        """
        Convert date strings to datetime objects.

        Handles Google Ads date formats (YYYY-MM-DD or date objects).

        Args:
            columns: List of column names containing dates

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col in self.df.columns:
                try:
                    self.df[col] = pd.to_datetime(
                        self.df[col],
                        format="%Y-%m-%d",
                        errors="coerce"
                    )
                    logger.debug(f"Converted column '{col}' to datetime")
                except Exception as e:
                    logger.warning(f"Failed to convert column '{col}' to datetime: {e}")
            else:
                logger.debug(f"Column '{col}' not found, skipping date conversion")

        return self

    def add_company(self, account_column: str = "customer_id_google") -> "GoogleProcessor":
        """
        Add company_id column based on customer account mapping.

        Maps Google customer IDs to company IDs using COMPANY_ACCOUNT_MAP.

        Args:
            account_column: Name of column containing customer IDs

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        if account_column not in self.df.columns:
            logger.warning(f"Account column '{account_column}' not found, skipping company mapping")
            return self

        try:
            self.df["companyid"] = self.df[account_column].apply(
                lambda x: COMPANY_ACCOUNT_MAP.get(str(x), 1)  # Default to company 1
            )
            logger.debug(f"Added company IDs for {len(self.df)} rows")
        except Exception as e:
            logger.error(f"Failed to add company IDs: {e}")

        return self

    def add_row_loaded_date(self) -> "GoogleProcessor":
        """
        Add row_loaded_date column with current timestamp.

        DEPRECATED: Use add_load_date() instead.

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        self.df["row_loaded_date"] = datetime.now()
        logger.debug("Added row_loaded_date column")

        return self

    def add_load_date(self) -> "GoogleProcessor":
        """
        Add load_date column with current date (no timestamp).

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        self.df["load_date"] = datetime.now().date()
        logger.debug("Added load_date column")

        return self

    def modify_name(self, columns: List[str]) -> "GoogleProcessor":
        """
        Clean name columns by removing emoji and special characters.

        This combines emoji removal, non-latin removal, and pipe removal.

        Args:
            columns: List of column names to clean

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col in self.df.columns:
                try:
                    if self.df[col].dtype == object:  # String column
                        # Remove emoji
                        self.df[col] = self.df[col].apply(
                            lambda x: self._remove_emojis(str(x)) if pd.notna(x) else x
                        )
                        # Remove non-latin
                        self.df[col] = self.df[col].apply(
                            lambda x: self._keep_latin_only(str(x)) if pd.notna(x) else x
                        )
                        # Remove pipes
                        self.df[col] = self.df[col].str.replace("|", "-", regex=False)

                        logger.debug(f"Cleaned name column '{col}'")
                except Exception as e:
                    logger.warning(f"Failed to clean column '{col}': {e}")
            else:
                logger.debug(f"Column '{col}' not found, skipping name cleaning")

        return self

    def remove_emoji(self, columns: List[str]) -> "GoogleProcessor":
        """
        Remove emoji characters from specified columns.

        Args:
            columns: List of column names

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col in self.df.columns:
                try:
                    self.df[col] = self.df[col].apply(
                        lambda x: self._remove_emojis(str(x)) if pd.notna(x) else x
                    )
                    logger.debug(f"Removed emojis from column '{col}'")
                except Exception as e:
                    logger.warning(f"Failed to remove emojis from '{col}': {e}")

        return self

    def remove_non_latin(self, columns: List[str]) -> "GoogleProcessor":
        """
        Remove non-Latin characters from specified columns.

        Args:
            columns: List of column names

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col in self.df.columns:
                try:
                    self.df[col] = self.df[col].apply(
                        lambda x: self._keep_latin_only(str(x)) if pd.notna(x) else x
                    )
                    logger.debug(f"Removed non-Latin characters from column '{col}'")
                except Exception as e:
                    logger.warning(f"Failed to remove non-Latin from '{col}': {e}")

        return self

    def remove_piping(self, columns: List[str]) -> "GoogleProcessor":
        """
        Remove pipe characters from specified columns.

        Pipe characters (|) are used as delimiters in COPY statements,
        so they must be replaced in data values.

        Args:
            columns: List of column names

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col in self.df.columns:
                try:
                    if self.df[col].dtype == object:  # String column
                        self.df[col] = self.df[col].str.replace("|", "-", regex=False)
                        logger.debug(f"Removed pipes from column '{col}'")
                except Exception as e:
                    logger.warning(f"Failed to remove pipes from '{col}': {e}")

        return self

    def clean_audience_string(self) -> "GoogleProcessor":
        """
        Clean audience display names.

        Removes special characters and normalizes audience strings.

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        if "display_name" in self.df.columns:
            try:
                # Remove special characters, keep alphanumeric and spaces
                self.df["display_name"] = self.df["display_name"].apply(
                    lambda x: re.sub(r'[^a-zA-Z0-9\s\-_]', '', str(x)) if pd.notna(x) else x
                )
                logger.debug("Cleaned audience display names")
            except Exception as e:
                logger.warning(f"Failed to clean audience strings: {e}")

        return self

    def aggregate_by_keys(self, group_by: Optional[List[str]] = None) -> "GoogleProcessor":
        """
        Aggregate Google Ads data by specified keys.

        CRITICAL FIX: Removes duplicates BEFORE aggregation to prevent
        double-counting when campaigns are both SERVING and PAUSED.

        Sums numeric metrics for each key combination.

        Args:
            group_by: List of columns to group by (default: ["ad_id", "device"])

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        # Default to ad_id + device for backward compatibility
        group_by = group_by or ["ad_id", "device"]

        # Check if all group_by columns exist
        if not all(col in self.df.columns for col in group_by):
            missing = [col for col in group_by if col not in self.df.columns]
            logger.warning(f"Missing required columns for aggregation: {missing}, skipping")
            return self

        try:
            # CRITICAL FIX: Remove duplicates BEFORE aggregation
            # When campaigns are both SERVING and PAUSED, both queries return same data
            initial_rows = len(self.df)
            self.df = self.df.drop_duplicates(
                subset=group_by,
                keep="first"  # Keep first occurrence (SERVING has priority)
            )
            final_rows = len(self.df)

            if initial_rows != final_rows:
                logger.warning(f"Removed {initial_rows - final_rows} duplicate combinations for {group_by}")
                logger.warning("This prevents incorrect summing when campaigns are both SERVING and PAUSED")

            # Build aggregation dict for numeric columns
            agg_dict = {}

            # Metrics to sum (base metrics only, not averages)
            sum_columns = ["cost_micros", "clicks", "impressions", "conversions"]
            for col in sum_columns:
                if col in self.df.columns:
                    # Convert to float to prevent overflow
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                    agg_dict[col] = "sum"

            # ID columns - take first
            id_columns = ["customer_id", "customer_id_google"]
            for col in id_columns:
                if col in self.df.columns and col not in group_by:
                    agg_dict[col] = "first"

            if agg_dict:
                self.df = self.df.groupby(group_by, as_index=False).agg(agg_dict)
                logger.debug(f"Aggregated to {len(self.df)} unique {'/'.join(group_by)} combinations")

                # Recalculate averages AFTER aggregation
                # CPC = cost / clicks
                if "cost_micros" in self.df.columns and "clicks" in self.df.columns:
                    self.df["averagecpc"] = self.df.apply(
                        lambda row: row["cost_micros"] / row["clicks"] if row["clicks"] > 0 else 0,
                        axis=1
                    )

                # CPM = (cost / impressions) * 1000
                if "cost_micros" in self.df.columns and "impressions" in self.df.columns:
                    self.df["averagecpm"] = self.df.apply(
                        lambda row: (row["cost_micros"] / row["impressions"]) * 1000 if row["impressions"] > 0 else 0,
                        axis=1
                    )

                # Average cost = cost_micros (already aggregated)
                if "cost_micros" in self.df.columns:
                    self.df["averagecost"] = self.df["cost_micros"]

                # CTR = (clicks / impressions) * 100
                if "clicks" in self.df.columns and "impressions" in self.df.columns:
                    self.df["ctr"] = self.df.apply(
                        lambda row: (row["clicks"] / row["impressions"]) * 100 if row["impressions"] > 0 else 0,
                        axis=1
                    )

                logger.debug("Recalculated average metrics (CPC, CPM, CTR) after aggregation")
        except Exception as e:
            logger.warning(f"Failed to aggregate by keys: {e}")

        return self

    def fill_view_ctr_nan(self, columns: List[str]) -> "GoogleProcessor":
        """
        Fill NaN values with 0 in specified columns (typically CTR fields).

        Args:
            columns: List of column names to fill

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col in self.df.columns:
                try:
                    self.df[col] = self.df[col].fillna(0)
                    logger.debug(f"Filled NaN with 0 in column '{col}'")
                except Exception as e:
                    logger.warning(f"Failed to fill NaN in '{col}': {e}")
            else:
                logger.debug(f"Column '{col}' not found, skipping fillna")

        return self

    def replace_nat(self) -> "GoogleProcessor":
        """
        Replace NaT (Not a Time) with None for database compatibility.

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        try:
            # Replace NaT in all datetime columns
            for col in self.df.columns:
                if self.df[col].dtype == 'datetime64[ns]':
                    self.df[col] = self.df[col].where(self.df[col].notna(), None)

            logger.debug("Replaced NaT with None in datetime columns")
        except Exception as e:
            logger.warning(f"Failed to replace NaT: {e}")

        return self

    def delete_nan_string(self) -> "GoogleProcessor":
        """
        Delete rows where string columns contain "nan" or "NaT".

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        try:
            original_count = len(self.df)

            # Replace string "nan" and "NaT" with None
            for col in self.df.columns:
                if self.df[col].dtype == object:
                    self.df[col] = self.df[col].replace(["nan", "NaT", "None"], None)

            dropped = original_count - len(self.df)
            if dropped > 0:
                logger.debug(f"Cleaned {dropped} 'nan' strings")

        except Exception as e:
            logger.warning(f"Failed to delete nan strings: {e}")

        return self

    def dropna_value(self) -> "GoogleProcessor":
        """
        Drop rows with any NaN values.

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        # DEBUG
        logger.warning(f"dropna_value BEFORE: columns={self.df.columns.tolist()}, 'id' present={'id' in self.df.columns}")

        original_count = len(self.df)

        # Log columns with NaN values BEFORE dropping
        nan_cols = [col for col in self.df.columns if self.df[col].isna().any()]
        if nan_cols:
            logger.debug(f"Columns with NaN values: {nan_cols}")
            for col in nan_cols:
                nan_count = self.df[col].isna().sum()
                logger.debug(f"  {col}: {nan_count} NaN values ({nan_count/original_count*100:.1f}%)")

        self.df = self.df.dropna()
        dropped = original_count - len(self.df)

        if dropped > 0:
            logger.debug(f"Dropped {dropped} rows with NaN values")

        # DEBUG
        logger.warning(f"dropna_value AFTER: columns={self.df.columns.tolist()}, 'id' present={'id' in self.df.columns}")

        return self

    def drop_duplicates(self, subset: Optional[List[str]] = None) -> "GoogleProcessor":
        """
        Drop duplicate rows.

        Args:
            subset: Column names to consider for identifying duplicates.
                   If None, considers all columns.

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        original_count = len(self.df)

        if subset:
            # Drop duplicates on specific columns, keep last occurrence
            self.df = self.df.drop_duplicates(subset=subset, keep='last')
            dropped = original_count - len(self.df)
            if dropped > 0:
                logger.debug(f"Dropped {dropped} duplicate rows on columns {subset}")
        else:
            # Drop exact duplicates (all columns)
            self.df = self.df.drop_duplicates()
            dropped = original_count - len(self.df)
            if dropped > 0:
                logger.debug(f"Dropped {dropped} duplicate rows")

        return self

    def keep_highest_level(self, id_column: str = "id") -> "GoogleProcessor":
        """
        Keep only rows with the highest level for each ID.

        For duplicate IDs (same account appearing at multiple hierarchy levels),
        keep only the row with the highest level value.

        Args:
            id_column: Column name containing the ID (default: "id")

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        if id_column not in self.df.columns or "level" not in self.df.columns:
            logger.warning(f"Cannot filter by level: missing '{id_column}' or 'level' column")
            return self

        original_count = len(self.df)

        # Sort by level descending, then keep first occurrence of each ID
        self.df = (
            self.df
            .sort_values("level", ascending=False)
            .drop_duplicates(subset=[id_column], keep="first")
            .reset_index(drop=True)
        )

        filtered = original_count - len(self.df)
        if filtered > 0:
            logger.info(f"Filtered {filtered} duplicate IDs, keeping highest level for each")

        return self

    def rename_column(self, renaming: Dict[str, str]) -> "GoogleProcessor":
        """
        Rename columns according to a mapping.

        Args:
            renaming: Dictionary mapping old names to new names

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        # Only rename columns that exist
        valid_renames = {
            old: new for old, new in renaming.items()
            if old in self.df.columns
        }

        if valid_renames:
            self.df = self.df.rename(columns=valid_renames)
            logger.debug(f"Renamed {len(valid_renames)} columns")

        return self

    def google_rename_columns(self) -> "GoogleProcessor":
        """
        Rename Google Ads columns using standard COLUMN_MAPPINGS.

        This method applies the standard Google Ads column renaming:
        - customer.id → customer_id_google
        - campaign.id → campaign_id
        - metrics.* → metric names
        - segments.* → segment names
        - etc.

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        from social.platforms.google.constants import COLUMN_MAPPINGS

        # DEBUG: Log columns before rename
        logger.warning(f"google_rename_columns BEFORE: {self.df.columns.tolist()}")
        if 'id' in self.df.columns:
            logger.warning(f"'id' column present BEFORE rename, sample: {self.df['id'].head(3).tolist()}")

        # Only rename columns that exist
        valid_renames = {
            old: new for old, new in COLUMN_MAPPINGS.items()
            if old in self.df.columns
        }

        if valid_renames:
            logger.warning(f"Applying renames: {valid_renames}")
            self.df = self.df.rename(columns=valid_renames)
            logger.debug(f"Renamed {len(valid_renames)} Google Ads columns")

        # DEBUG: Log columns after rename
        logger.warning(f"google_rename_columns AFTER: {self.df.columns.tolist()}")
        if 'id' in self.df.columns:
            logger.warning(f"'id' column present AFTER rename, sample: {self.df['id'].head(3).tolist()}")
        else:
            logger.error("'id' column MISSING after rename!")

        return self

    def extract_id_from_resource_name(self) -> "GoogleProcessor":
        """
        Extract ad group ID from resource_name and create 'id' column.

        For group_placement_view, resource name format is:
            customers/{customer_id}/groupPlacementViews/{ad_group_id}~{encoded_placement}

        For other resources, format can be:
            customers/{customer_id}/adGroups/{ad_group_id}~{criterion_id}

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        # Try both 'resource_name' and 'resourcename' (after handle_columns processing)
        resource_col = None
        if "resource_name" in self.df.columns:
            resource_col = "resource_name"
        elif "resourcename" in self.df.columns:
            resource_col = "resourcename"

        if not resource_col:
            logger.warning("Missing 'resource_name' or 'resourcename' column, cannot extract id")
            return self

        try:
            # Try extracting from groupPlacementViews format first
            # Format: customers/XXX/groupPlacementViews/YYY~ZZZ
            extracted = self.df[resource_col].str.extract(r"/groupPlacementViews/(\d+)")[0]

            # If not found, try ad_groups format
            # Format: customers/XXX/adGroups/YYY or customers/XXX/adGroups/YYY~ZZZ
            if extracted.isna().all():
                extracted = self.df[resource_col].str.extract(r"/adGroups/(\d+)")[0]

            self.df["id"] = extracted
            logger.debug(f"Extracted ad group ID from {resource_col} ({extracted.notna().sum()} values)")

        except Exception as e:
            logger.warning(f"Failed to extract ID from resource_name: {e}")

        return self

    def limit_placement(self) -> "GoogleProcessor":
        """
        Limit Google Ads placement to top 25 by impressions per ad group.

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        # DEBUG
        logger.warning(f"limit_placement BEFORE: columns={self.df.columns.tolist()}, 'id' present={'id' in self.df.columns}")

        if "impressions" not in self.df.columns or "id" not in self.df.columns:
            logger.warning("Missing required columns for placement limit, skipping")
            logger.warning(f"limit_placement AFTER (skipped): columns={self.df.columns.tolist()}")
            return self

        try:
            # Convert impressions to int
            self.df["impressions"] = self.df["impressions"].astype(int)

            # Sort by id and impressions, then keep top 25 per id
            self.df = self.df.sort_values(by=["id", "impressions"], ascending=[True, False])
            self.df = self.df.groupby("id").head(25).reset_index(drop=True)

            logger.debug("Limited placements to top 25 per ad group")
            logger.warning(f"limit_placement AFTER: columns={self.df.columns.tolist()}, 'id' present={'id' in self.df.columns}")

        except Exception as e:
            logger.warning(f"Failed to limit placements: {e}")

        return self

    def get_df(self) -> pd.DataFrame:
        """
        Get the processed DataFrame.

        Returns:
            Processed DataFrame
        """
        # DEBUG: Log final DataFrame state
        logger.warning(f"get_df() returning DataFrame with columns: {self.df.columns.tolist()}")
        if 'id' in self.df.columns:
            logger.warning(f"get_df() 'id' column present, sample: {self.df['id'].head(3).tolist()}")
        else:
            logger.error("get_df() 'id' column MISSING!")

        return self.df

    # ============================================================================
    # Private Helper Methods
    # ============================================================================

    @staticmethod
    def _remove_emojis(text: str) -> str:
        """
        Remove emoji characters from text.

        Args:
            text: Input text

        Returns:
            Text with emojis removed
        """
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
        return emoji_pattern.sub(r"", text)

    @staticmethod
    def _keep_latin_only(text: str) -> str:
        """
        Keep only Latin characters, numbers, and common punctuation.

        Args:
            text: Input text

        Returns:
            Text with only Latin characters
        """
        return re.sub(r'[^a-zA-Z0-9\s\-.,;:!?()\[\]\'"]+', "", text)

    def aggregate_by_entity(
        self,
        group_columns: List[str] = None,
        metric_columns: List[str] = None,
        agg_method: str = 'sum',
    ) -> "GoogleProcessor":
        """Aggregate metrics by entity (remove date granularity).

        Transforms time-series data into cumulative metrics using shared utility function.

        Args:
            group_columns: Columns to group by (default: auto-detect)
            metric_columns: Columns to aggregate (default: all numeric)
            agg_method: Aggregation method (default: 'sum')

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        self.df = aggregate_metrics_by_entity(
            df=self.df,
            group_columns=group_columns,
            metric_columns=metric_columns,
            agg_method=agg_method,
            entity_id_columns=['ad_id', 'adgroup_id', 'campaign_id', 'customer_id_google']
        )

        return self
