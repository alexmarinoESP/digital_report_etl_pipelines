"""Facebook Ads Data Processor Module.

This module provides a chainable processor for transforming Facebook Ads data.
It implements the Fluent Interface pattern for clean, readable data transformations.

Key Features:
- Chainable methods (fluent interface)
- Type-safe transformations
- Nested action extraction (actions, action_values)
- Targeting field parsing
- Pixel rule extraction
- Comprehensive error handling

Architecture:
- FacebookProcessor: Main processor class with chainable methods
- Each method returns self for chaining
- get_df() returns the final DataFrame
"""

import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from social.platforms.facebook.constants import COMPANY_ACCOUNT_MAP
from social.utils.aggregation import aggregate_metrics_by_entity


class FacebookProcessor:
    """Chainable data processor for Facebook Ads data.

    This processor provides a fluent interface for transforming raw API responses
    into clean, database-ready DataFrames.

    Facebook-specific transformations:
    - Extract nested actions (list of dicts to separate columns)
    - Extract nested action_values (revenue/value metrics)
    - Parse targeting field (JSON to custom_audiences)
    - Extract pixel rules from custom conversions
    - Extract custom conversion IDs from action_type

    Example:
        >>> processor = FacebookProcessor(raw_df)
        >>> clean_df = (processor
        ...     .extract_nested_actions()
        ...     .add_company()
        ...     .add_row_loaded_date()
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
        logger.debug(f"FacebookProcessor initialized with {len(self.df)} rows")

    def get_df(self) -> pd.DataFrame:
        """Get the processed DataFrame.

        Returns:
            Processed DataFrame
        """
        return self.df

    def extract_nested_actions(
        self,
        action_col: str = "actions",
        prefix: str = "action_",
    ) -> "FacebookProcessor":
        """Extract nested actions array into separate columns.

        Facebook returns actions as an array of dictionaries:
        [{"action_type": "link_click", "value": 123}, {"action_type": "purchase", "value": 5}]

        This method creates a separate column for each action_type.

        Args:
            action_col: Column name containing actions array (default: "actions")
            prefix: Prefix for new columns (default: "action_")

        Returns:
            Self for chaining
        """
        if self.df.empty or action_col not in self.df.columns:
            logger.debug(f"Column '{action_col}' not found, skipping action extraction")
            return self

        logger.info(f"Extracting nested actions from '{action_col}'")

        # Convert actions to dictionaries
        for idx, row in self.df.iterrows():
            actions = row[action_col]

            if isinstance(actions, list) and len(actions) > 0:
                for action in actions:
                    if isinstance(action, dict):
                        action_type = action.get("action_type")
                        value = action.get("value", 0)

                        if action_type:
                            # Create column name: action_link_click, action_purchase, etc.
                            col_name = f"{prefix}{action_type}"
                            self.df.at[idx, col_name] = value

        logger.debug(f"Extracted actions into separate columns")
        return self

    def extract_nested_action_values(
        self,
        col: str = "action_values",
        prefix: str = "action_value_",
    ) -> "FacebookProcessor":
        """Extract nested action_values array into separate columns.

        Similar to actions, but for value/revenue metrics.

        Args:
            col: Column name containing action_values array (default: "action_values")
            prefix: Prefix for new columns (default: "action_value_")

        Returns:
            Self for chaining
        """
        if self.df.empty or col not in self.df.columns:
            logger.debug(f"Column '{col}' not found, skipping action value extraction")
            return self

        logger.info(f"Extracting nested action values from '{col}'")

        # Convert action_values to dictionaries
        for idx, row in self.df.iterrows():
            action_values = row[col]

            if isinstance(action_values, list) and len(action_values) > 0:
                for action_val in action_values:
                    if isinstance(action_val, dict):
                        action_type = action_val.get("action_type")
                        value = action_val.get("value", 0)

                        if action_type:
                            col_name = f"{prefix}{action_type}"
                            self.df.at[idx, col_name] = value

        logger.debug(f"Extracted action values into separate columns")
        return self

    def parse_targeting_field(
        self,
        targeting_col: str = "targeting",
    ) -> "FacebookProcessor":
        """Parse targeting field to extract custom_audiences.

        The targeting field contains JSON with audience segments.
        This method extracts custom_audiences and expands them into rows.

        Args:
            targeting_col: Column name containing targeting JSON (default: "targeting")

        Returns:
            Self for chaining
        """
        if self.df.empty or targeting_col not in self.df.columns:
            logger.debug(f"Column '{targeting_col}' not found, skipping targeting parse")
            return self

        logger.info(f"Parsing targeting field from '{targeting_col}'")

        df_list = []

        for idx, row in self.df.iterrows():
            targeting = row[targeting_col]

            if isinstance(targeting, dict):
                custom_audiences = targeting.get("custom_audiences", [])

                if custom_audiences:
                    for audience in custom_audiences:
                        audience_row = {
                            "campaign_id": row.get("campaign_id"),
                            "adset_id": row.get("id"),
                            "audience_id": audience.get("id"),
                            "audience_name": audience.get("name"),
                        }
                        df_list.append(audience_row)

        if df_list:
            # Replace DataFrame with extracted audiences
            self.df = pd.DataFrame(df_list)
            logger.success(f"Extracted {len(self.df)} audience targeting records")
        else:
            logger.warning("No custom audiences found in targeting field")
            self.df = pd.DataFrame()

        return self

    def convert_timestamp_to_date(self, columns: List[str]) -> "FacebookProcessor":
        """Convert timestamp columns to date format.

        Facebook API may return timestamps that need to be converted to dates.

        Args:
            columns: List of column names with timestamps

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
                # Try parsing as datetime string
                self.df[col] = pd.to_datetime(self.df[col], errors="coerce")
                logger.debug(f"Converted timestamp column: {col}")
            except Exception as e:
                logger.error(f"Failed to convert timestamp column '{col}': {e}")

        return self

    def add_company(self, account_column: str = "account_id") -> "FacebookProcessor":
        """Add company ID column based on account mapping.

        Uses the COMPANY_ACCOUNT_MAP from constants to map account IDs to company IDs.

        Args:
            account_column: Name of the column containing account IDs (default: "account_id")

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
            lambda x: self._get_company_id(str(x))
        )

        logger.debug(f"Added company IDs for {len(self.df)} rows")
        return self

    def add_row_loaded_date(self, **kwargs) -> "FacebookProcessor":
        """Add row_loaded_date column with current timestamp.

        Args:
            **kwargs: Ignored for compatibility with YAML config (params: None)

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        self.df["row_loaded_date"] = datetime.now()
        logger.debug("Added row_loaded_date column")
        return self

    def fix_id_type(self, columns: List[str]) -> "FacebookProcessor":
        """Ensure ID columns are string type.

        Facebook IDs should be stored as strings to preserve leading zeros
        and avoid numeric precision issues.

        Args:
            columns: List of column names containing IDs

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found, skipping ID type fix")
                continue

            self.df[col] = self.df[col].astype(str)

        logger.debug(f"Fixed ID types for columns: {columns}")
        return self

    def replace_nan_with_zero(self, columns: List[str]) -> "FacebookProcessor":
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

    def rename_column(self, renaming: Dict[str, str]) -> "FacebookProcessor":
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

    def convert_string(self, columns: List[str]) -> "FacebookProcessor":
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

    def drop_columns(self, columns: Optional[List[str]] = None, cols: Optional[List[str]] = None) -> "FacebookProcessor":
        """Drop specified columns.

        Args:
            columns: List of column names to drop (preferred)
            cols: Alias for columns (for backward compatibility)

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        # Support both 'columns' and 'cols' parameter names
        col_list = columns or cols or []
        existing_columns = [c for c in col_list if c in self.df.columns]

        if existing_columns:
            self.df = self.df.drop(columns=existing_columns)
            logger.debug(f"Dropped columns: {existing_columns}")

        return self

    def modify_name(self, columns: Optional[List[str]] = None, cols: Optional[List[str]] = None) -> "FacebookProcessor":
        """Clean special characters from name columns.

        Replaces pipe characters (|) with hyphens (-) to avoid issues
        with COPY statement delimiters.

        Args:
            columns: List of column names to modify (preferred)
            cols: Alias for columns (for backward compatibility)

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        # Support both 'columns' and 'cols' parameter names
        col_list = columns or cols or []

        for col in col_list:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found, skipping name modification")
                continue

            if self.df[col].dtype == object:  # String column
                self.df[col] = self.df[col].str.replace("|", "-", regex=False)

        logger.debug(f"Modified name columns: {columns}")
        return self

    def extract_pixel_rule(
        self,
        rule_column: str = "rule",
        id_column: str = "conversion_id",
        **kwargs
    ) -> "FacebookProcessor":
        """Extract pixel rule from custom conversion rule JSON.

        Parses the rule JSON to extract event name and URL triggers.

        Args:
            rule_column: Column containing rule JSON (default: "rule")
            id_column: Column containing conversion ID (default: "conversion_id")

        Returns:
            Self for chaining
        """
        if self.df.empty or rule_column not in self.df.columns:
            logger.debug(f"Column '{rule_column}' not found, skipping pixel rule extraction")
            return self

        logger.info("Extracting pixel rules from custom conversions")

        pixel_rules = []

        for idx, row in self.df.iterrows():
            rule = row[rule_column]

            if isinstance(rule, str) and rule:
                try:
                    jrule = json.loads(rule)

                    # Extract event rule and URL triggers
                    event_rule = jrule.get("and", [{}])[0].get("event", {}).get("eq")
                    all_url_rule = jrule.get("and", [{}] * 2)[1].get("or", {})

                    pixel_rules.append({
                        "RULE": event_rule,
                        "CONV_TRIGGER": str(all_url_rule),
                    })
                except (json.JSONDecodeError, IndexError, KeyError, AttributeError) as e:
                    logger.warning(f"Failed to parse rule for conversion {row.get(id_column)}: {e}")
                    pixel_rules.append({"RULE": None, "CONV_TRIGGER": None})
            else:
                pixel_rules.append({"RULE": None, "CONV_TRIGGER": None})

        # Add pixel_rule columns
        self.df["pixel_rule"] = [r["RULE"] for r in pixel_rules]
        self.df["pixel_trigger"] = [r["CONV_TRIGGER"] for r in pixel_rules]

        logger.debug(f"Extracted pixel rules for {len(self.df)} conversions")
        return self

    def convert_actions_to_df(
        self,
        id_column: str = "ad_id",
        actions_column: str = "actions",
        **kwargs
    ) -> "FacebookProcessor":
        """Convert actions list to long DataFrame format.

        Facebook Ads API returns actions as a list of dictionaries.
        This method converts each action_type into a separate row.
        Used for fb_ads_insight_actions table.

        Args:
            id_column: Name of ID column to preserve (default: "ad_id")
            actions_column: Name of column containing actions list (default: "actions")

        Returns:
            Self for chaining
        """
        if self.df.empty or actions_column not in self.df.columns:
            logger.warning(f"Column '{actions_column}' not found in DataFrame")
            # Create empty DataFrame with expected schema for fb_ads_insight_actions
            self.df = pd.DataFrame(columns=['ad_id', 'action_type', 'action_target_id', 'value'])
            return self

        logger.info("Converting actions to long format DataFrame")

        response_list = []
        i = 0

        for idx, row in self.df.iterrows():
            row_id = row.get(id_column)
            actions = row.get(actions_column)

            if isinstance(actions, list) and len(actions) > 0:
                # Convert list of dicts to DataFrame
                act_tmp = pd.DataFrame(actions)
                act_tmp[id_column] = row_id
                response_list.append(act_tmp)
            else:
                # No actions - create empty row with NaN
                response_list.append(
                    pd.DataFrame(
                        {
                            "action_target_id": [None],
                            "action_type": [None],
                            "value": [None],
                            id_column: [row_id],
                        },
                        index=[i],
                    )
                )
                i += 1

        if response_list:
            self.df = pd.concat(response_list, ignore_index=True)
            logger.success(f"Converted to {len(self.df)} action rows")
        else:
            self.df = pd.DataFrame()

        return self

    def extract_custom_conversion_id(self, **kwargs) -> "FacebookProcessor":
        """Extract custom conversion ID from action_type field.

        Facebook actions may contain action_type like:
        'offsite_conversion.custom.123456789'

        This method extracts the conversion ID (123456789) into a separate column.

        Args:
            **kwargs: Ignored for compatibility with YAML config (params: None)

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        if "action_type" not in self.df.columns:
            logger.warning("Column 'action_type' not found, adding None conversion_id")
            self.df["conversion_id"] = None
            return self

        # Split action_type by 'offsite_conversion.custom.'
        id_parts = self.df["action_type"].str.split("offsite_conversion.custom.", expand=True)

        try:
            # Column 1 contains the conversion ID (if split succeeded)
            self.df["conversion_id"] = id_parts[1]
            logger.debug(f"Extracted conversion IDs for {self.df['conversion_id'].notna().sum()} rows")
        except KeyError:
            # No conversion ID found in any row
            self.df["conversion_id"] = None
            logger.debug("No conversion IDs found in action_type")

        return self

    def nan_conversion(self, **kwargs) -> "FacebookProcessor":
        """Convert NaN values to None for database compatibility.

        Args:
            **kwargs: Ignored for compatibility with YAML config (params: None)

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        # Replace NaN with None throughout DataFrame
        self.df = self.df.where(pd.notna(self.df), None)
        logger.debug("Converted NaN to None")
        return self

    def deal_with_date(self, columns: List[str]) -> "FacebookProcessor":
        """Convert date strings to datetime with support for ISO8601 and simple dates.

        Handles multiple date formats:
        - ISO8601: "2024-01-15T10:30:00+0000"
        - Simple date: "2024-01-15"
        - Replaces NaT with None for database compatibility

        Args:
            columns: List of column names containing dates

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found, skipping date conversion")
                continue

            try:
                # Replace NaN with None first
                self.df[col] = self.df[col].replace(pd.NaT, None).replace({pd.NA: None})

                # Try to parse dates with automatic format detection
                self.df[col] = self.df[col].apply(
                    lambda x: pd.to_datetime(x, format="%Y-%m-%dT%H:%M:%S%z", errors="coerce")
                    if isinstance(x, str) and "T" in x
                    else pd.to_datetime(x, errors="coerce")
                    if x is not None
                    else None
                )

                # Replace NaT with None after conversion
                self.df[col] = self.df[col].where(pd.notna(self.df[col]), None)

                logger.debug(f"Converted date column: {col}")
            except Exception as e:
                logger.warning(f"Failed to convert column '{col}' to datetime: {e}")

        return self

    def convert_unix_timestamp_to_date(self, columns: List[str]) -> "FacebookProcessor":
        """Convert Unix timestamp columns to datetime.

        Facebook API returns some timestamps as Unix epoch integers.

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
                # Convert Unix timestamp to datetime with UTC timezone
                self.df[col] = pd.to_datetime(self.df[col], unit="s", errors="coerce", utc=True)
                # Convert to timezone-naive for database compatibility
                self.df[col] = self.df[col].dt.tz_localize(None)
                logger.debug(f"Converted Unix timestamp column: {col}")
            except Exception as e:
                logger.error(f"Failed to convert Unix timestamp column '{col}': {e}")

        return self

    def convert_nat_to_nan(self, columns: List[str]) -> "FacebookProcessor":
        """Convert NaT (Not a Time) values to None for database compatibility.

        Args:
            columns: List of column names to process

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        for col in columns:
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found, skipping NaT conversion")
                continue

            try:
                # Replace NaT with None
                self.df[col] = self.df[col].where(pd.notna(self.df[col]), None)
                logger.debug(f"Converted NaT to None for column: {col}")
            except Exception as e:
                logger.warning(f"Failed to convert NaT for column '{col}': {e}")

        return self

    @staticmethod
    def _get_company_id(account_id: str) -> int:
        """Get company ID for a Facebook account ID.

        Args:
            account_id: Facebook account ID (may include "act_" prefix)

        Returns:
            Company ID (defaults to 1 if not found)
        """
        # Remove "act_" prefix if present
        clean_id = account_id.replace("act_", "")

        # Try both formats
        company_id = COMPANY_ACCOUNT_MAP.get(account_id)
        if company_id is None:
            company_id = COMPANY_ACCOUNT_MAP.get(clean_id, 1)

        return company_id

    def aggregate_by_entity(
        self,
        group_columns: List[str] = None,
        metric_columns: List[str] = None,
        agg_method: str = 'sum',
    ) -> "FacebookProcessor":
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
            entity_id_columns=['ad_id', 'adset_id', 'campaign_id', 'account_id']
        )

        return self
