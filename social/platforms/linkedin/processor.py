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

    def __init__(self, df: pd.DataFrame, adapter=None):
        """Initialize processor with a DataFrame and optional adapter.

        Args:
            df: Raw DataFrame from API response
            adapter: LinkedInAdapter for API lookups (optional)
        """
        self.df = df.copy() if not df.empty else pd.DataFrame()
        self.adapter = adapter
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

    # =========================================================================
    # Demographics Processing Methods
    # =========================================================================

    def extract_pivot_value(
        self,
        pivot_field: str = "pivotValues",
        urn_type: str = "organization",
        id_column: str = "demographic_id"
    ) -> "LinkedInProcessor":
        """Extract ID from demographics pivotValues URN.

        pivotValues contains URNs like:
        - ['urn:li:organization:123'] for MEMBER_COMPANY
        - ['urn:li:title:456'] for MEMBER_JOB_TITLE
        - ['urn:li:seniority:4'] for MEMBER_SENIORITY

        Args:
            pivot_field: Source field name (default: pivotValues)
            urn_type: URN type to extract (organization, title, seniority)
            id_column: Target column name for extracted ID

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        if pivot_field not in self.df.columns:
            logger.warning(f"Column '{pivot_field}' not found, skipping pivot extraction")
            return self

        def extract_id(pivot_values):
            """Extract ID from pivotValues list."""
            if not pivot_values or not isinstance(pivot_values, list):
                return None

            urn = pivot_values[0] if pivot_values else None
            if not urn:
                return None

            # Extract ID from URN: urn:li:organization:123 → 123
            if f":{urn_type}:" in urn:
                return urn.split(f":{urn_type}:")[-1]
            # Fallback: just get last part
            return urn.split(":")[-1]

        self.df[id_column] = self.df[pivot_field].apply(extract_id)

        # Drop pivotValues column (no longer needed)
        self.df = self.df.drop(columns=[pivot_field])

        logger.debug(f"Extracted {id_column} from {pivot_field}")
        return self

    def lookup_organization_names(
        self,
        id_column: str = "company_id",
        name_column: str = "company_name"
    ) -> "LinkedInProcessor":
        """Lookup organization names from LinkedIn API.

        Uses the adapter to batch lookup organization details.
        Requires adapter to be accessible (injected via processing context).

        Args:
            id_column: Column containing organization IDs
            name_column: Target column for organization names

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        if id_column not in self.df.columns:
            logger.warning(f"Column '{id_column}' not found, skipping organization lookup")
            return self

        # Get unique organization IDs
        org_ids = self.df[id_column].dropna().unique().tolist()

        if not org_ids:
            logger.warning("No organization IDs to lookup")
            self.df[name_column] = None
            return self

        logger.info(f"Looking up {len(org_ids)} organizations from LinkedIn API")

        # Use adapter if available, otherwise fallback to placeholder
        if self.adapter:
            try:
                # Batch lookup organization details
                org_details = self.adapter.lookup_organizations(org_ids)

                # Map organization IDs to names
                self.df[name_column] = self.df[id_column].apply(
                    lambda x: org_details.get(str(x), {}).get('name', f"Organization {x}") if pd.notna(x) else None
                )
                logger.info(f"Successfully looked up {len(org_details)} organization names")
            except Exception as e:
                logger.error(f"Organization lookup failed: {e}, using placeholder")
                self.df[name_column] = self.df[id_column].apply(lambda x: f"Organization {x}" if pd.notna(x) else None)
        else:
            logger.warning("No adapter provided - using placeholder organization names")
            self.df[name_column] = self.df[id_column].apply(lambda x: f"Organization {x}" if pd.notna(x) else None)

        return self

    def lookup_title_names(
        self,
        id_column: str = "job_title_id",
        name_column: str = "job_title_name"
    ) -> "LinkedInProcessor":
        """Lookup job title names from LinkedIn API.

        Args:
            id_column: Column containing title IDs
            name_column: Target column for title names

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        if id_column not in self.df.columns:
            logger.warning(f"Column '{id_column}' not found, skipping title lookup")
            return self

        title_ids = self.df[id_column].dropna().unique().tolist()

        if not title_ids:
            logger.warning("No title IDs to lookup")
            self.df[name_column] = None
            return self

        logger.info(f"Looking up {len(title_ids)} job titles from LinkedIn API")

        # Use adapter if available, otherwise fallback to placeholder
        if self.adapter:
            try:
                # Batch lookup title details via pagination
                title_details = self.adapter.lookup_titles(title_ids)

                # Map title IDs to names
                self.df[name_column] = self.df[id_column].apply(
                    lambda x: title_details.get(str(x), {}).get('name', f"Title {x}") if pd.notna(x) else None
                )
                logger.info(f"Successfully looked up {len(title_details)} title names")
            except Exception as e:
                logger.error(f"Title lookup failed: {e}, using placeholder")
                self.df[name_column] = self.df[id_column].apply(lambda x: f"Title {x}" if pd.notna(x) else None)
        else:
            logger.warning("No adapter provided - using placeholder title names")
            self.df[name_column] = self.df[id_column].apply(lambda x: f"Title {x}" if pd.notna(x) else None)

        return self

    def map_seniority_names(
        self,
        code_column: str = "seniority_code",
        name_column: str = "seniority_name"
    ) -> "LinkedInProcessor":
        """Map seniority codes to names using reference table.

        LinkedIn seniority codes (from reference table):
        1=Unpaid, 2=Training, 3=Entry level, 4=Senior, 5=Manager,
        6=Director, 7=VP, 8=CXO, 9=Partner, 10=Owner

        Args:
            code_column: Column containing seniority codes
            name_column: Target column for seniority names

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        if code_column not in self.df.columns:
            logger.warning(f"Column '{code_column}' not found, skipping seniority mapping")
            return self

        # LinkedIn seniority reference table
        seniority_map = {
            "1": "Unpaid",
            "2": "Training",
            "3": "Entry level",
            "4": "Senior",
            "5": "Manager",
            "6": "Director",
            "7": "VP",
            "8": "CXO",
            "9": "Partner",
            "10": "Owner",
            # Handle URN format too
            "urn:li:seniority:1": "Unpaid",
            "urn:li:seniority:2": "Training",
            "urn:li:seniority:3": "Entry level",
            "urn:li:seniority:4": "Senior",
            "urn:li:seniority:5": "Manager",
            "urn:li:seniority:6": "Director",
            "urn:li:seniority:7": "VP",
            "urn:li:seniority:8": "CXO",
            "urn:li:seniority:9": "Partner",
            "urn:li:seniority:10": "Owner",
        }

        self.df[name_column] = self.df[code_column].astype(str).map(seniority_map)

        # Log unmapped values
        unmapped = self.df[self.df[name_column].isna()][code_column].unique()
        if len(unmapped) > 0:
            logger.warning(f"Unmapped seniority codes: {unmapped.tolist()}")

        logger.debug(f"Mapped seniority codes to names")
        return self

    def add_campaign_info(self, **kwargs) -> "LinkedInProcessor":
        """Add campaign information (already present from extraction).

        This is a no-op method since campaign_id and campaign_name
        are already added during the extraction phase in pipeline.

        Returns:
            Self for chaining
        """
        # Campaign info is already added in _extract_demographics
        # This method exists for config compatibility
        if "campaign_id" not in self.df.columns:
            logger.warning("campaign_id not found - should be added during extraction")

        return self

    def rename_columns(self, mapping: dict = None, **kwargs) -> "LinkedInProcessor":
        """Rename columns according to mapping.

        Args:
            mapping: Dictionary of old_name -> new_name mappings

        Returns:
            Self for chaining
        """
        if self.df.empty:
            return self

        if not mapping:
            logger.warning("No mapping provided for rename_columns")
            return self

        # Only rename columns that exist
        rename_dict = {old: new for old, new in mapping.items() if old in self.df.columns}

        if rename_dict:
            self.df = self.df.rename(columns=rename_dict)
            logger.debug(f"Renamed {len(rename_dict)} columns")
        else:
            logger.warning(f"No columns found to rename from mapping: {list(mapping.keys())}")

        return self
