"""LinkedIn Ads Data Processor Module.

This module provides a chainable processor for transforming LinkedIn Ads data.
It implements the Fluent Interface pattern for clean, readable data transformations.

Key Features:
- Chainable methods (fluent interface)
- Type-safe transformations
- Comprehensive error handling
- URN extraction and date handling
- Emoji and special character cleaning

Architecture:
- LinkedInProcessor: Main processor class with chainable methods
- Each method returns self for chaining
- get_df() returns the final DataFrame
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from social.platforms.linkedin.constants import COMPANY_ACCOUNT_MAP
from social.utils.aggregation import aggregate_metrics_by_entity


class LinkedInProcessor:
    """Chainable data processor for LinkedIn Ads data.

    This processor provides a fluent interface for transforming raw API responses
    into clean, database-ready DataFrames.

    Example:
        >>> processor = LinkedInProcessor(raw_df)
        >>> clean_df = (processor
        ...     .extract_id_from_urn(['id', 'campaign'])
        ...     .add_company()
        ...     .add_load_date()
        ...     .get_df())

    Attributes:
        df: The DataFrame being processed
    """

    def __init__(self, df: pd.DataFrame):
        """Initialize processor with a DataFrame.

        Args:
            df: Raw DataFrame from API response
        """
        self.df = df.copy() if not df.empty else pd.DataFrame()
        logger.debug(f"LinkedInProcessor initialized with {len(self.df)} rows")

    def get_df(self) -> pd.DataFrame:
        """Get the processed DataFrame.

        Returns:
            Processed DataFrame
        """
        return self.df

    def extract_id_from_urn(self, columns: List[str]) -> "LinkedInProcessor":
        """Extract numeric IDs from URN format columns.

        Converts URNs like 'urn:li:sponsoredAccount:123' to '123'.

        Args:
            columns: List of column names containing URNs

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found, skipping URN extraction")
                continue

            self.df[col] = self.df[col].apply(
                lambda x: re.findall(r"\d+", str(x))[0] if pd.notna(x) and re.findall(r"\d+", str(x)) else x
            )

        logger.debug(f"Extracted IDs from URN columns: {columns}")
        return self

    def build_date_field(
        self,
        fields_date: List[str] = None,
        begin_end: List[str] = None,
        exclude: bool = True
    ) -> "LinkedInProcessor":
        """Build date columns from separate year/month/day fields.

        LinkedIn API returns dates as separate fields (e.g., dateRange_start_year,
        dateRange_start_month, dateRange_start_day). This method combines them
        into proper date columns.

        Args:
            fields_date: Date component names (default: ["year", "month", "day"])
            begin_end: Date types (default: ["start", "end"])
            exclude: If True, keep only start date as 'date' column

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        fields_date = fields_date or ["year", "month", "day"]
        begin_end = begin_end or ["start", "end"]

        for timerange in begin_end:
            cols = [f"dateRange_{timerange}_{field}" for field in fields_date]

            # Check if all required columns exist
            missing_cols = [col for col in cols if col not in self.df.columns]
            if missing_cols:
                logger.warning(f"Missing date columns: {missing_cols}, skipping date building")
                continue

            # Combine into single date string
            self.df[f"date_{timerange}"] = self.df[cols].apply(
                lambda x: "-".join(x.astype(str)), axis=1
            )

            # Convert to datetime
            self.df[f"date_{timerange}"] = pd.to_datetime(
                self.df[f"date_{timerange}"],
                format="%Y-%m-%d",
                errors="coerce"
            )

            # Drop component columns
            self.df = self.df.drop(columns=cols)

        # If exclude=True, keep only start date as 'date'
        if exclude and "date_start" in self.df.columns:
            if "date_end" in self.df.columns:
                self.df = self.df.drop(columns=["date_end"])
            self.df = self.df.rename(columns={"date_start": "date"})

        logger.debug("Built date fields from components")
        return self

    def convert_unix_timestamp_to_date(self, columns: List[str]) -> "LinkedInProcessor":
        """Convert Unix timestamp columns (milliseconds) to datetime.

        LinkedIn API returns timestamps in milliseconds since epoch.

        Args:
            columns: List of column names with Unix timestamps

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found, skipping timestamp conversion")
                continue

            try:
                # Convert from milliseconds to datetime
                self.df[col] = pd.to_datetime(self.df[col], unit="ms", utc=True, errors="coerce")
                logger.debug(f"Converted Unix timestamp column: {col}")
            except Exception as e:
                logger.error(f"Failed to convert timestamp column '{col}': {e}")

        return self

    def response_decoration(
        self,
        field: str,
        new_col_name: Optional[str] = None
    ) -> "LinkedInProcessor":
        """Extract numeric ID from URN field and optionally rename column.

        Args:
            field: Column name containing URNs
            new_col_name: Optional new column name (drops original if provided)

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        if field not in self.df.columns:
            logger.warning(f"Column '{field}' not found, skipping response decoration")
            return self

        # Extract numeric ID using regex
        extracted = self.df[field].apply(
            lambda x: re.search(r"\d+", str(x)).group(0)
            if pd.notna(x) and not isinstance(x, (int, float)) and re.search(r"\d+", str(x))
            else x
        )

        if new_col_name:
            self.df[new_col_name] = extracted
            self.df = self.df.drop(columns=[field])
            logger.debug(f"Extracted ID from '{field}' to new column '{new_col_name}'")
        else:
            self.df[field] = extracted
            logger.debug(f"Extracted ID in place for column '{field}'")

        return self

    def add_company(self, account_column: str = "id", **kwargs) -> "LinkedInProcessor":
        """Add company ID column based on account mapping.

        Uses the COMPANY_ACCOUNT_MAP from constants to map account IDs to company IDs.

        Args:
            account_column: Name of the column containing account IDs (default: "id")

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        if account_column not in self.df.columns:
            logger.warning(f"Account column '{account_column}' not found, skipping company mapping")
            return self

        # Map account IDs to company IDs
        self.df["companyid"] = self.df[account_column].apply(
            lambda x: COMPANY_ACCOUNT_MAP.get(str(x), 1)  # Default to 1 if not found
        )

        logger.debug(f"Added company IDs for {len(self.df)} rows")
        return self

    def add_load_date(self) -> "LinkedInProcessor":
        """Add load_date column with current date.

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        today = datetime.now().date()
        self.df["load_date"] = today
        logger.debug("Added load_date column")
        return self

    # Alias for backward compatibility with YAML config
    def add_row_loaded_date(self) -> "LinkedInProcessor":
        """Alias for add_load_date() - for backward compatibility with config."""
        return self.add_load_date()

    def modify_name(self, columns: List[str]) -> "LinkedInProcessor":
        """Clean special characters and emojis from name columns.

        Replaces pipe characters (|) with hyphens (-) and removes emojis.

        Args:
            columns: List of column names to modify

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found, skipping name modification")
                continue

            if self.df[col].dtype == object:  # String column
                # Replace pipe characters (used as delimiter in COPY statements)
                self.df[col] = self.df[col].str.replace("|", "-", regex=False)

                # Remove emojis using deEmojify logic
                self.df[col] = self.df[col].apply(
                    lambda x: self._remove_emoji(str(x)) if pd.notna(x) else x
                )

        logger.debug(f"Modified name columns: {columns}")
        return self

    def convert_nat_to_nan(self, columns: List[str]) -> "LinkedInProcessor":
        """Convert pandas NaT (Not a Time) to None for database compatibility.

        Args:
            columns: List of column names to convert

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col not in self.df.columns:
                continue

            # Replace NaT with None
            self.df[col] = self.df[col].replace({pd.NaT: None})

            # Also handle string "NaT" just in case
            if self.df[col].dtype == object:
                self.df[col] = self.df[col].replace("NaT", None)

            # Handle datetime columns with NaT
            if pd.api.types.is_datetime64_any_dtype(self.df[col]):
                self.df[col] = self.df[col].where(self.df[col].notna(), None)

        logger.debug(f"Converted NaT to None for columns: {columns}")
        return self

    def replace_nan_with_zero(self, columns: List[str]) -> "LinkedInProcessor":
        """Replace NaN values with 0 for numeric columns.

        Args:
            columns: List of column names

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        existing_columns = [c for c in columns if c in self.df.columns]

        if existing_columns:
            self.df[existing_columns] = self.df[existing_columns].fillna(0)
            logger.debug(f"Replaced NaN with 0 for columns: {existing_columns}")

        return self

    def rename_column(self, renaming: Dict[str, str]) -> "LinkedInProcessor":
        """Rename columns according to a mapping.

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

    def convert_string(self, columns: List[str]) -> "LinkedInProcessor":
        """Convert specified columns to string type.

        Args:
            columns: List of column names to convert

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found, skipping string conversion")
                continue

            self.df[col] = self.df[col].astype(str)

        logger.debug(f"Converted columns to string: {columns}")
        return self

    def convert_int(self, columns: List[str]) -> "LinkedInProcessor":
        """Convert specified columns to integer type.

        Args:
            columns: List of column names to convert

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found, skipping int conversion")
                continue

            try:
                # Convert to numeric, coercing errors to NaN
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                # Convert to Int64 (nullable integer type)
                self.df[col] = self.df[col].astype('Int64')
            except Exception as e:
                logger.error(f"Failed to convert column '{col}' to int: {e}")

        logger.debug(f"Converted columns to int: {columns}")
        return self

    def modify_urn_li_sponsoredAccount(self, **kwargs) -> "LinkedInProcessor":
        """Extract account ID from URN in 'account' column.

        Specific to LinkedIn's account URN format: urn:li:sponsoredAccount:123

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        if "account" not in self.df.columns:
            logger.warning("Column 'account' not found, skipping URN modification")
            return self

        self.df["account"] = self.df["account"].apply(
            lambda x: str(x).split("urn:li:sponsoredAccount:")[-1]
            if pd.notna(x) and "urn:li:sponsoredAccount:" in str(x)
            else str(x)
        )

        logger.debug("Modified account URN column")
        return self

    @staticmethod
    def _remove_emoji(text: str) -> str:
        """Remove emoji characters from text.

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
            flags=re.UNICODE
        )
        return emoji_pattern.sub(r'', text)

    def aggregate_by_entity(
        self,
        group_columns: List[str] = None,
        metric_columns: List[str] = None,
        agg_method: str = 'sum',
    ) -> "LinkedInProcessor":
        """Aggregate metrics by entity (remove date granularity).

        Transforms time-series data into cumulative metrics using shared utility function.

        Example:
            Before aggregation:
                creative_id | date       | impressions | clicks
                123        | 2026-01-20 | 100        | 5
                123        | 2026-01-21 | 150        | 8
                123        | 2026-01-22 | 120        | 6

            After aggregation:
                creative_id | impressions | clicks
                123        | 370         | 19

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
            entity_id_columns=['creative_id', 'campaign_id', 'account', 'id']
        )

        return self

    def extract_creative_type_from_content(self) -> "LinkedInProcessor":
        """Extract creative type from flattened content columns.

        LinkedIn API v202601+ no longer returns explicit 'type' field.
        After flattening, the content field becomes columns like:
        - 'reference' → Sponsored Content (UGC: image/video/article)
        - 'spotlight' → Spotlight Ads
        - 'jobs' → Jobs Ads
        - 'documentAd' → Document Ads
        - 'thirdPartyVastTagVideoAd' → Third-party video

        This method detects which column is present to infer the creative type.

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        # Check available columns
        columns = set(self.df.columns)

        def get_type_from_columns(row):
            """Infer type from presence of specific flattened content columns.

            LinkedIn creative types mapping:
            - follow → FOLLOW_COMPANY_V2 (Follow Company Ads)
            - reference → SPONSORED_STATUS_UPDATE (Sponsored Posts/UGC)
            - spotlight → SPOTLIGHT_AD (Spotlight Ads - Dynamic Ads)
            - jobs → JOBS_AD (Jobs Ads - Dynamic Ads)
            - documentAd → DOCUMENT_AD (Document Ads)
            - thirdPartyVastTagVideoAd → THIRD_PARTY_VIDEO_AD
            """
            # Priority order matters: follow before reference
            # (some creatives might have both)
            if 'follow' in columns and pd.notna(row.get('follow')):
                return 'FOLLOW_COMPANY_V2'
            elif 'reference' in columns and pd.notna(row.get('reference')):
                return 'SPONSORED_STATUS_UPDATE'
            elif 'spotlight' in columns and pd.notna(row.get('spotlight')):
                return 'SPOTLIGHT_AD'
            elif 'jobs' in columns and pd.notna(row.get('jobs')):
                return 'JOBS_AD'
            elif 'documentAd' in columns and pd.notna(row.get('documentAd')):
                return 'DOCUMENT_AD'
            elif 'thirdPartyVastTagVideoAd' in columns and pd.notna(row.get('thirdPartyVastTagVideoAd')):
                return 'THIRD_PARTY_VIDEO_AD'
            else:
                # Unknown type - log for debugging
                content_cols = [c for c in columns if c in ['reference', 'follow', 'spotlight', 'jobs', 'documentAd']]
                if content_cols:
                    logger.debug(f"Row {row.get('id')}: Found columns {content_cols} but all NULL")
                return None

        self.df['type'] = self.df.apply(get_type_from_columns, axis=1)

        # Log type distribution summary
        type_counts = self.df['type'].value_counts(dropna=False)
        logger.info(f"Creative type distribution: {type_counts.to_dict()}")

        return self
