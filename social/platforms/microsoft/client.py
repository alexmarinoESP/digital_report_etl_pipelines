"""
Microsoft Ads API Client Module.

This module provides functionality to interact with Microsoft Advertising API
using the BingAds SDK. It handles report generation, CSV download, and data cleaning.

Key Features:
- BingAds SDK factory pattern for report generation
- CSV report download and processing
- Automatic header detection and footer removal
- Type conversions and data cleaning
- Support for both Ad and Campaign performance reports

Architecture:
- Uses dependency injection for authorization
- Protocol-based design for testability
- Proper error handling and logging
"""

import csv
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from bingads import ServiceClient
from bingads.authorization import AuthorizationData
from bingads.v13.reporting import (
    ReportingDownloadParameters,
    ReportingServiceManager,
)
from loguru import logger


class MicrosoftAdsClient:
    """
    A client for interacting with Microsoft Advertising API.

    This class encapsulates the functionality to:
    1. Connect to Microsoft Ads API using BingAds SDK
    2. Retrieve account information
    3. Generate performance reports (Ad and Campaign)
    4. Clean and process downloaded CSV files
    5. Provide all-in-one report generation and processing

    Attributes:
        version (int): The Microsoft Ads API version (default: 13)
        authorization_data (AuthorizationData): Authorization credentials for Microsoft Ads API
        service_client (ServiceClient): The Microsoft Ads reporting service client
    """

    def __init__(self, authorization_data: AuthorizationData, api_version: int = 13):
        """
        Initialize the Microsoft Ads Client.

        Args:
            authorization_data: Authorization credentials for Microsoft Ads API
            api_version: Microsoft Ads API version (default: 13)

        Raises:
            ValueError: If authorization_data is None or invalid
        """
        if authorization_data is None:
            raise ValueError("authorization_data cannot be None")

        logger.info(f"Initializing Microsoft Ads Client with API version {api_version}")
        self.version = api_version
        self.authorization_data = authorization_data
        self.service_client = ServiceClient(
            service="ReportingService",
            version=self.version,
            authorization_data=self.authorization_data,
        )
        logger.debug("Service client initialized successfully")

    def get_account_ids(self, customer_id: Optional[str] = None) -> List[str]:
        """
        Get all available account IDs for the authenticated customer.

        Args:
            customer_id: Optional customer ID. If not provided, uses the one from authorization_data

        Returns:
            List of account IDs (as strings)

        Raises:
            Exception: If API call fails
        """
        try:
            # Use customer ID from authorization data if not provided
            cust_id = customer_id or self.authorization_data.customer_id

            customer_service = ServiceClient(
                service="CustomerManagementService",
                version=self.version,
                authorization_data=self.authorization_data,
                environment="production",
            )

            logger.info(f"Fetching account IDs for customer: {cust_id}")
            accounts_info = customer_service.GetAccountsInfo(CustomerId=cust_id)

            account_ids = [str(account["Id"]) for account in accounts_info.AccountInfo]
            logger.success(f"Retrieved {len(account_ids)} account IDs")

            return account_ids

        except Exception as e:
            logger.error(f"Failed to get account IDs: {e}")
            raise

    def generate_report(
        self,
        account_ids: List[str],
        report_name: str = "Ad",
        time_period: str = "LastSixMonths",
        aggregation: str = "Monthly",
        output_path: Optional[Path] = None,
        output_filename: str = "report.csv",
        columns: Optional[List[str]] = None,
        use_temp_dir: bool = False,
        delete_after_processing: bool = False,
    ) -> Path:
        """
        Generate a performance report from Microsoft Ads API.

        This method creates a report request using the BingAds SDK factory pattern,
        submits it to the API, and downloads the resulting CSV file.

        Args:
            account_ids: List of account IDs to include in the report
            report_name: Report type - 'Ad' or 'Campaign' (default: 'Ad')
            time_period: Predefined time period (e.g., 'LastSixMonths', 'Yesterday')
            aggregation: Data aggregation level - 'Monthly', 'Daily', 'Summary', etc.
            output_path: Directory to save the report (default: current directory)
            output_filename: Name of the output CSV file
            columns: List of column names to include (uses defaults if None)
            use_temp_dir: If True, save to system temporary directory
            delete_after_processing: If True, mark file for deletion after reading

        Returns:
            Path to the downloaded CSV file

        Raises:
            ValueError: If report_name is not supported
            Exception: If report generation or download fails
        """
        logger.info(
            f"Generating {report_name} performance report for {len(account_ids)} account(s)"
        )

        # Define default columns based on report type
        if report_name == "Ad":
            default_columns = [
                "AdId",
                "CustomerId",
                "CustomerName",
                "AccountId",
                "AccountName",
                "TimePeriod",
                "CampaignId",
                "CampaignName",
                "Clicks",
                "Conversions",
                "Impressions",
                "Spend",
                "AverageCpc",
                "AverageCpm",
                "Ctr",
            ]
        elif report_name == "Campaign":
            default_columns = [
                "CustomerId",
                "CustomerName",
                "AccountId",
                "AccountName",
                "TimePeriod",
                "CampaignId",
                "CampaignName",
                "Clicks",
                "Conversions",
                "Impressions",
                "Spend",
                "AverageCpc",
                "AverageCpm",
                "Ctr",
            ]
        else:
            logger.error(f"Unsupported report name: {report_name}")
            raise ValueError(
                f"Unsupported report name: {report_name}. Supported names are 'Ad' and 'Campaign'."
            )

        # Use provided columns or defaults
        columns_to_request = list(columns) if columns else list(default_columns)

        # Handle Summary aggregation - TimePeriod conflicts with Summary
        if aggregation == "Summary":
            if "TimePeriod" in columns_to_request:
                logger.info(
                    "Aggregation is 'Summary'. Removing 'TimePeriod' from requested columns "
                    "as it conflicts with Summary aggregation."
                )
                columns_to_request.remove("TimePeriod")

        # Create report request using BingAds SDK factory
        report_request = self.service_client.factory.create(
            f"{report_name}PerformanceReportRequest"
        )
        report_request.Aggregation = aggregation

        # Set time period
        time = self.service_client.factory.create("ReportTime")
        time.PredefinedTime = time_period
        time.CustomDateRangeStart = None
        time.CustomDateRangeEnd = None
        report_request.Time = time

        # No filters
        report_request.Filter = None

        # Set scope (accounts)
        scope = self.service_client.factory.create("AccountThroughCampaignReportScope")
        scope.AccountIds = {"long": [int(aid) for aid in account_ids]}
        scope.Campaigns = None
        report_request.Scope = scope

        # Set columns
        column_array = self.service_client.factory.create(
            f"ArrayOf{report_name}PerformanceReportColumn"
        )
        if columns_to_request:
            column_array[f"{report_name}PerformanceReportColumn"].extend(
                columns_to_request
            )
        report_request.Columns = column_array

        # Set format
        report_request.Format = "Csv"

        # Determine output path
        if use_temp_dir:
            temp_dir = Path(tempfile.gettempdir())
            output_path = temp_dir
            logger.info(f"Using temporary directory: {output_path}")
        elif output_path is None:
            output_path = Path.cwd()
        else:
            output_path = Path(output_path)
            output_path.mkdir(exist_ok=True, parents=True)

        output_file = output_path / output_filename

        # Download report
        logger.info(
            f"Downloading report to {output_file} with columns: {columns_to_request}"
        )
        report_parameters = ReportingDownloadParameters(
            report_request=report_request,
            result_file_directory=str(output_path),
            result_file_name=output_filename,
        )

        reporting_service_manager = ReportingServiceManager(
            authorization_data=self.authorization_data,
            poll_interval_in_milliseconds=5000,
        )

        reporting_service_manager.download_report(report_parameters)
        logger.success(f"Report downloaded successfully to {output_file}")

        if delete_after_processing:
            logger.debug(f"File {output_file} marked for deletion after processing")

        return output_file

    def clean_csv_report(
        self,
        filepath: Union[str, Path],
        delete_after_processing: bool = False,
        header_identifier: str = "TimePeriod",
    ) -> pd.DataFrame:
        """
        Read and clean a Microsoft Ads CSV report file.

        This method handles common issues in Microsoft Ads reports:
        - Finding the actual header row (typically containing specific identifier)
        - Removing footer rows and empty lines
        - Cleaning up formatting issues
        - Converting data types

        Args:
            filepath: Path to the CSV file
            delete_after_processing: If True, delete the file after processing
            header_identifier: String to identify the header row (default: 'TimePeriod')

        Returns:
            Cleaned DataFrame with report data

        Raises:
            FileNotFoundError: If the specified file doesn't exist
            ValueError: If the file cannot be properly processed
        """
        filepath = Path(filepath)
        logger.info(f"Cleaning CSV report: {filepath}")

        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        # Step 1: Find the header row containing the identifier
        header_row_index = self._find_header_row_index(
            filepath, header_identifier=header_identifier
        )
        if header_row_index == -1:
            msg = f"Could not find header row with '{header_identifier}' in {filepath}"
            logger.error(msg)
            raise ValueError(msg)

        # Step 2: Read the CSV using the found header row index
        try:
            df = pd.read_csv(filepath, skiprows=header_row_index, encoding="utf-8")
            logger.info(f"Successfully loaded CSV starting from line {header_row_index}")
            logger.debug(f"Initial DataFrame shape: {df.shape}")
        except Exception as e:
            logger.exception(f"Error reading CSV with pandas: {e}")
            raise

        # Step 3: Clean the DataFrame
        df_cleaned = self._clean_dataframe(df)

        # Step 4: Delete the file if requested
        if delete_after_processing:
            try:
                os.remove(filepath)
                logger.info(f"Deleted file: {filepath}")
            except Exception as e:
                logger.warning(f"Failed to delete file {filepath}: {e}")

        logger.success(f"Cleaning complete. Final DataFrame shape: {df_cleaned.shape}")
        return df_cleaned

    def _find_header_row_index(
        self, filepath: Path, header_identifier: str = "TimePeriod", max_lines: int = 30
    ) -> int:
        """
        Find the row index containing the header (identified by a specific string).

        This method searches for the header identifier in any column of each row,
        not just the first column, making it more flexible for different CSV formats.

        Args:
            filepath: Path to the CSV file
            header_identifier: String to search for in the header row
            max_lines: Maximum number of lines to check

        Returns:
            Index of the header row, or -1 if not found
        """
        logger.debug(
            f"Searching for header row containing '{header_identifier}' in {filepath}"
        )

        try:
            with open(filepath, "r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                for i, row in enumerate(reader):
                    if row:  # Skip empty rows
                        # Check if the header identifier is in any column of this row
                        if any(header_identifier in str(cell) for cell in row):
                            logger.debug(
                                f"Header containing '{header_identifier}' found at line index {i}"
                            )
                            return i
                    if i >= max_lines:
                        logger.warning(
                            f"Searched first {max_lines} lines without finding '{header_identifier}'"
                        )
                        break

            logger.warning(f"Header row with '{header_identifier}' not found")
            return -1

        except Exception as e:
            logger.exception(f"Error while finding header row: {e}")
            return -1

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the DataFrame by removing footer rows and empty lines.

        Performs the following cleaning operations:
        - Removes rows where key metrics are all NaN
        - Removes footer rows containing copyright text
        - Converts string ID columns to proper types
        - Converts CTR from percentage string to float

        Args:
            df: Original DataFrame

        Returns:
            Cleaned DataFrame
        """
        # Make a copy to avoid modifying the original
        df_cleaned = df.copy()

        # Step 1: Remove rows where key metrics are all NaN
        data_cols_for_nan_check = ["Clicks", "Conversions", "Impressions", "Spend"]
        actual_data_cols = [
            col for col in data_cols_for_nan_check if col in df_cleaned.columns
        ]

        if actual_data_cols:
            rows_before = len(df_cleaned)
            df_cleaned = df_cleaned.dropna(subset=actual_data_cols, how="all")
            rows_dropped = rows_before - len(df_cleaned)
            logger.debug(
                f"Dropped {rows_dropped} row(s) with NaN values in key metrics"
            )
        else:
            logger.warning("None of the expected data columns found in DataFrame")

        # Step 2: Remove footer rows containing specific patterns
        if not df_cleaned.empty and df_cleaned.columns[0] in df_cleaned:
            first_col_name = df_cleaned.columns[0]
            footer_patterns = ["©", "Microsoft Corporation", "All rights reserved."]

            indices_to_drop = []
            for index, value in df_cleaned[first_col_name].astype(str).items():
                if any(pattern.lower() in value.lower() for pattern in footer_patterns):
                    indices_to_drop.append(index)

            if indices_to_drop:
                df_cleaned = df_cleaned.drop(index=indices_to_drop)
                logger.debug(
                    f"Dropped {len(indices_to_drop)} row(s) containing footer patterns"
                )

        # Step 3: Convert string columns to appropriate types
        string_cols = [
            "CustomerId",
            "CustomerName",
            "AccountId",
            "AccountName",
            "CampaignId",
            "CampaignName",
            "AdId",
        ]

        for col in string_cols:
            if col in df_cleaned.columns:
                try:
                    # Attempt to convert to int first, then to str (preserves ID precision)
                    df_cleaned[col] = df_cleaned[col].astype(int).astype(str).str.strip()
                except ValueError:
                    # If int conversion fails, directly convert to str
                    df_cleaned[col] = df_cleaned[col].astype(str).str.strip()
                except Exception as e:
                    logger.warning(
                        f"Error converting column '{col}': {e}. Proceeding with string conversion."
                    )
                    df_cleaned[col] = df_cleaned[col].astype(str).str.strip()

        # Step 4: Convert CTR from percentage string to float
        if "Ctr" in df_cleaned.columns:
            try:
                df_cleaned["Ctr"] = (
                    df_cleaned["Ctr"].str.rstrip("%").astype(float) / 100
                )
            except Exception as e:
                logger.warning(f"Error converting 'Ctr' column to float: {e}")

        return df_cleaned

    def generate_and_process_report(
        self,
        account_ids: List[str],
        report_name: str = "Ad",
        time_period: str = "LastSixMonths",
        aggregation: str = "Monthly",
        output_path: Optional[Path] = None,
        output_filename: str = "report.csv",
        columns: Optional[List[str]] = None,
        use_temp_dir: bool = True,
        delete_after_processing: bool = True,
        header_identifier: str = "TimePeriod",
    ) -> Tuple[Optional[Path], pd.DataFrame]:
        """
        Generate a performance report and clean it in one step.

        This is a convenience method that combines report generation and cleaning,
        providing a complete end-to-end workflow.

        Args:
            account_ids: List of account IDs to include in the report
            report_name: Report type - 'Ad' or 'Campaign' (default: 'Ad')
            time_period: Predefined time period for the report
            aggregation: Data aggregation level (default: 'Monthly')
            output_path: Directory to save the report
            output_filename: Name of output file
            columns: Columns to include in the report
            use_temp_dir: If True, store the report in a temporary directory
            delete_after_processing: If True, delete the file after processing
            header_identifier: String to identify the header row

        Returns:
            Tuple of (Path to the report file or None if deleted, cleaned DataFrame)

        Raises:
            ValueError: If parameters are invalid
            Exception: If report generation or processing fails
        """
        logger.info("Starting combined report generation and processing")

        # Generate the report
        report_path = self.generate_report(
            account_ids=account_ids,
            report_name=report_name,
            time_period=time_period,
            aggregation=aggregation,
            output_path=output_path,
            output_filename=output_filename,
            columns=columns,
            use_temp_dir=use_temp_dir,
            delete_after_processing=False,  # We'll handle deletion here
        )

        # Clean the report
        cleaned_df = self.clean_csv_report(
            report_path,
            delete_after_processing=delete_after_processing,
            header_identifier=header_identifier,
        )

        # Return None for the path if the file was deleted
        final_path = report_path if not delete_after_processing else None

        logger.success("Report generation and processing complete")
        return final_path, cleaned_df
