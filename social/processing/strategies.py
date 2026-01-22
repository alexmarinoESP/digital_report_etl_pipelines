"""Processing strategies for data transformation.

This module implements the Strategy pattern, allowing different data
transformation operations to be composed flexibly. Each strategy is
a small, focused class that performs one specific transformation.
"""

import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any, Optional
import pandas as pd
from loguru import logger

from social.domain.services import CompanyMappingService, URNExtractor
from social.core.exceptions import DataValidationError


class ProcessingStrategy(ABC):
    """Abstract base class for data processing strategies.

    Each strategy implements a single transformation operation that can
    be applied to a DataFrame. Strategies can be chained together to
    create complex processing pipelines.
    """

    @abstractmethod
    def process(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Apply the transformation to the DataFrame.

        Args:
            df: DataFrame to transform
            **kwargs: Strategy-specific parameters

        Returns:
            Transformed DataFrame

        Raises:
            DataValidationError: If transformation fails
        """
        pass

    @abstractmethod
    def get_name(self) -> str:
        """Get the strategy name for logging and debugging.

        Returns:
            Strategy name
        """
        pass


class AddCompanyStrategy(ProcessingStrategy):
    """Add company ID column based on account mapping.

    Maps account IDs to company IDs using the CompanyMappingService.
    """

    def __init__(self, company_mapping_service: CompanyMappingService):
        """Initialize strategy with company mapping service.

        Args:
            company_mapping_service: Service for account-to-company mapping
        """
        self.company_mapping = company_mapping_service

    def process(self, df: pd.DataFrame, account_column: str = "id", **kwargs) -> pd.DataFrame:
        """Add companyid column based on account ID.

        Args:
            df: DataFrame with account IDs
            account_column: Name of the column containing account IDs
            **kwargs: Ignored

        Returns:
            DataFrame with companyid column added
        """
        if account_column not in df.columns:
            raise DataValidationError(
                f"Account column '{account_column}' not found in DataFrame",
                field=account_column,
                actual=list(df.columns)
            )

        df["companyid"] = df[account_column].apply(
            lambda x: self.company_mapping.get_company_id(str(x))
        )

        logger.debug(f"Added company IDs for {len(df)} rows")
        return df

    def get_name(self) -> str:
        return "add_company"


class AddRowLoadedDateStrategy(ProcessingStrategy):
    """Add row_loaded_date column with current timestamp."""

    def process(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Add row_loaded_date column.

        Args:
            df: DataFrame
            **kwargs: Ignored

        Returns:
            DataFrame with row_loaded_date column
        """
        df["row_loaded_date"] = datetime.now()
        return df

    def get_name(self) -> str:
        return "add_row_loaded_date"


class ExtractIDFromURNStrategy(ProcessingStrategy):
    """Extract numeric IDs from URN format columns.

    Converts URNs like 'urn:li:sponsoredAccount:123' to '123'.
    """

    def __init__(self, urn_extractor: Optional[URNExtractor] = None):
        """Initialize strategy with URN extractor.

        Args:
            urn_extractor: URN extraction service (creates default if None)
        """
        self.urn_extractor = urn_extractor or URNExtractor()

    def process(self, df: pd.DataFrame, columns: List[str], **kwargs) -> pd.DataFrame:
        """Extract IDs from URN columns.

        Args:
            df: DataFrame with URN columns
            columns: List of column names containing URNs
            **kwargs: Ignored

        Returns:
            DataFrame with extracted IDs
        """
        for col in columns:
            if col not in df.columns:
                logger.warning(f"Column '{col}' not found, skipping URN extraction")
                continue

            df[col] = df[col].apply(
                lambda x: self.urn_extractor.extract_id(str(x)) if pd.notna(x) else x
            )

        return df

    def get_name(self) -> str:
        return "extract_id_from_urn"


class BuildDateFieldStrategy(ProcessingStrategy):
    """Build date columns from separate year/month/day fields.

    LinkedIn API returns dates as separate fields (e.g., dateRange_start_year,
    dateRange_start_month, dateRange_start_day). This strategy combines them
    into proper date columns.
    """

    def process(
        self,
        df: pd.DataFrame,
        fields_date: List[str] = None,
        begin_end: List[str] = None,
        exclude: bool = True,
        **kwargs
    ) -> pd.DataFrame:
        """Build date columns from component fields.

        Args:
            df: DataFrame with date component columns
            fields_date: Date component names (default: ["year", "month", "day"])
            begin_end: Date types (default: ["start", "end"])
            exclude: If True, keep only start date as 'date' column
            **kwargs: Ignored

        Returns:
            DataFrame with combined date columns
        """
        fields_date = fields_date or ["year", "month", "day"]
        begin_end = begin_end or ["start", "end"]

        for timerange in begin_end:
            cols = [f"dateRange_{timerange}_{field}" for field in fields_date]

            # Check if all required columns exist
            missing_cols = [col for col in cols if col not in df.columns]
            if missing_cols:
                logger.warning(f"Missing date columns: {missing_cols}, skipping date building")
                continue

            # Combine into single date string
            df[f"date_{timerange}"] = df[cols].apply(
                lambda x: "-".join(x.astype(str)), axis=1
            )

            # Convert to datetime
            df[f"date_{timerange}"] = pd.to_datetime(
                df[f"date_{timerange}"],
                format="%Y-%m-%d",
                errors="coerce"
            )

            # Drop component columns
            df = df.drop(columns=cols)

        # If exclude=True, keep only start date as 'date'
        if exclude and "date_start" in df.columns:
            if "date_end" in df.columns:
                df = df.drop(columns=["date_end"])
            df = df.rename(columns={"date_start": "date"})

        return df

    def get_name(self) -> str:
        return "build_date_field"


class ConvertUnixTimestampStrategy(ProcessingStrategy):
    """Convert Unix timestamp columns to datetime."""

    def process(self, df: pd.DataFrame, columns: List[str], **kwargs) -> pd.DataFrame:
        """Convert Unix timestamp columns to datetime.

        Args:
            df: DataFrame with timestamp columns
            columns: List of column names with Unix timestamps (milliseconds)
            **kwargs: Ignored

        Returns:
            DataFrame with converted datetime columns
        """
        for col in columns:
            if col not in df.columns:
                logger.warning(f"Column '{col}' not found, skipping timestamp conversion")
                continue

            try:
                df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce")
            except Exception as e:
                logger.error(f"Failed to convert timestamp column '{col}': {e}")

        return df

    def get_name(self) -> str:
        return "convert_unix_timestamp_to_date"


class ModifyNameStrategy(ProcessingStrategy):
    """Replace pipe characters in name columns.

    Pipe characters (|) are used as delimiters in COPY statements,
    so they must be replaced in data values.
    """

    def process(self, df: pd.DataFrame, columns: List[str], **kwargs) -> pd.DataFrame:
        """Replace pipe characters with hyphens in specified columns.

        Args:
            df: DataFrame
            columns: List of column names to modify
            **kwargs: Ignored

        Returns:
            DataFrame with modified columns
        """
        for col in columns:
            if col not in df.columns:
                logger.warning(f"Column '{col}' not found, skipping name modification")
                continue

            if df[col].dtype == object:  # String column
                df[col] = df[col].str.replace("|", "-", regex=False)

        return df

    def get_name(self) -> str:
        return "modify_name"


class RenameColumnStrategy(ProcessingStrategy):
    """Rename columns according to a mapping."""

    def process(self, df: pd.DataFrame, renaming: Dict[str, str], **kwargs) -> pd.DataFrame:
        """Rename columns.

        Args:
            df: DataFrame
            renaming: Dictionary mapping old names to new names
            **kwargs: Ignored

        Returns:
            DataFrame with renamed columns
        """
        # Only rename columns that exist
        valid_renames = {
            old: new for old, new in renaming.items()
            if old in df.columns
        }

        if valid_renames:
            df = df.rename(columns=valid_renames)

        return df

    def get_name(self) -> str:
        return "rename_column"


class ConvertToStringStrategy(ProcessingStrategy):
    """Convert specified columns to string type."""

    def process(self, df: pd.DataFrame, columns: List[str], **kwargs) -> pd.DataFrame:
        """Convert columns to string type.

        Args:
            df: DataFrame
            columns: List of column names to convert
            **kwargs: Ignored

        Returns:
            DataFrame with converted columns
        """
        for col in columns:
            if col not in df.columns:
                logger.warning(f"Column '{col}' not found, skipping string conversion")
                continue

            df[col] = df[col].astype(str)

        return df

    def get_name(self) -> str:
        return "convert_string"


class ReplaceNaNWithZeroStrategy(ProcessingStrategy):
    """Replace NaN values with 0 for numeric columns."""

    def process(self, df: pd.DataFrame, columns: List[str], **kwargs) -> pd.DataFrame:
        """Replace NaN with 0 in specified columns.

        Args:
            df: DataFrame
            columns: List of column names
            **kwargs: Ignored

        Returns:
            DataFrame with NaN replaced
        """
        existing_columns = [c for c in columns if c in df.columns]

        if existing_columns:
            df[existing_columns] = df[existing_columns].fillna(0)

        return df

    def get_name(self) -> str:
        return "replace_nan_with_zero"


class ConvertNaTToNanStrategy(ProcessingStrategy):
    """Convert pandas NaT (Not a Time) to None for database compatibility."""

    def process(self, df: pd.DataFrame, columns: List[str], **kwargs) -> pd.DataFrame:
        """Convert NaT to None in specified columns.

        Args:
            df: DataFrame
            columns: List of column names
            **kwargs: Ignored

        Returns:
            DataFrame with NaT converted to None
        """
        for col in columns:
            if col not in df.columns:
                continue

            # Replace NaT with None and also replace string "NaT" just in case
            df[col] = df[col].replace({pd.NaT: None})
            df[col] = df[col].replace("NaT", None)
            # Also handle datetime columns with NaT
            if df[col].dtype == 'datetime64[ns]':
                df[col] = df[col].where(df[col].notna(), None)

        return df

    def get_name(self) -> str:
        return "convert_nat_to_nan"


class ModifyURNAccountStrategy(ProcessingStrategy):
    """Extract account ID from URN in 'account' column.

    Specific to LinkedIn's account URN format.
    """

    def process(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Extract account ID from URN.

        Args:
            df: DataFrame with 'account' column containing URNs
            **kwargs: Ignored

        Returns:
            DataFrame with extracted account IDs
        """
        if "account" not in df.columns:
            logger.warning("Column 'account' not found, skipping URN modification")
            return df

        df["account"] = df["account"].apply(
            lambda x: str(x).split("urn:li:sponsoredAccount:")[-1]
            if pd.notna(x) and "urn:li:sponsoredAccount:" in str(x)
            else str(x)
        )

        return df

    def get_name(self) -> str:
        return "modify_urn_li_sponsoredAccount"


class ResponseDecorationStrategy(ProcessingStrategy):
    """Extract ID from URN and optionally rename column."""

    def process(
        self,
        df: pd.DataFrame,
        field: str,
        new_col_name: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Extract numeric ID from URN field.

        Args:
            df: DataFrame
            field: Column name containing URNs
            new_col_name: Optional new column name (drops original if provided)
            **kwargs: Ignored

        Returns:
            DataFrame with extracted IDs
        """
        if field not in df.columns:
            logger.warning(f"Column '{field}' not found, skipping response decoration")
            return df

        # Extract numeric ID
        extracted = df[field].apply(
            lambda x: re.search(r"\d+", str(x)).group(0)
            if pd.notna(x) and not isinstance(x, (int, float))
            else x
        )

        if new_col_name:
            df[new_col_name] = extracted
            df = df.drop(columns=[field])
        else:
            df[field] = extracted

        return df

    def get_name(self) -> str:
        return "response_decoration"


class GoogleRenameColumnsStrategy(ProcessingStrategy):
    """Rename Google Ads columns using hardcoded mapping.
    
    This strategy is specific to Google Ads and handles the standard column
    naming conventions from the Google Ads API (e.g., camelCase, dots in names).
    """

    # Hardcoded mapping from Google Ads API names to standard names
    GOOGLE_COLUMN_MAPPING = {
        "startdate": "start_date",
        "enddate": "end_date",
        "servingstatus": "serving_status",
        "activeviewctr": "active_view_ctr",
        "customer_id": "customer_id_google",
        "customer.id": "customer_id_google",
        "placementtype": "placement_type",
        "resourcename": "resource_name",
        "resourceName": "resource_name",
        "timeZone": "time_zone",
        "displayname": "display_name",
        "descriptiveName": "descriptive_name",
        "targeturl": "target_url",
        "costMicros": "cost_micros",
        "costmicros": "cost_micros",
        "clientcustomer": "client_customer",
        "clientCustomer": "client_customer",
        "timezone": "time_zone",
        "descriptivename": "descriptive_name",
        "currencycode": "currency_code",
        "currencyCode": "currency_code",
        "campaign.id": "campaign_id",
        "adGroup.id": "adgroup_id",
        "ad.id": "ad_id",
    }

    def process(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Rename columns using Google Ads mapping.

        Args:
            df: DataFrame with Google Ads column names
            **kwargs: Ignored (no additional parameters needed)

        Returns:
            DataFrame with renamed columns
        """
        if df.empty:
            return df

        # Only rename columns that exist in the DataFrame
        rename_dict = {
            old_name: new_name
            for old_name, new_name in self.GOOGLE_COLUMN_MAPPING.items()
            if old_name in df.columns
        }

        if rename_dict:
            df = df.rename(columns=rename_dict)
            logger.debug(f"Renamed {len(rename_dict)} Google Ads columns")

        return df

    def get_name(self) -> str:
        return "google_rename_columns"
