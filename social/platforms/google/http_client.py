"""Google Ads HTTP Client.

This module provides a completely independent HTTP client for the Google Ads API
using the official Google Ads Python client library (gRPC/Protobuf).

Key Features:
- gRPC/Protobuf communication (NOT REST)
- GAQL query execution (Google Ads Query Language)
- Both regular and streaming request types
- Account hierarchy traversal
- Protobuf to dict conversion
- Comprehensive error handling

Design:
- NO base class inheritance (SOLID principles)
- Protocol-based contracts
- Type-safe with 100% type hints
- Production-ready error handling
"""

from typing import Any, Dict, List, Optional

import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from google.api_core.grpc_helpers import _StreamingResponseIterator
from google.protobuf.json_format import MessageToDict
from loguru import logger

from social.core.exceptions import APIError, AuthenticationError
from social.core.protocols import TokenProvider


class GoogleHTTPClient:
    """
    HTTP client for Google Ads API using gRPC/Protobuf.

    This client handles all communication with the Google Ads API:
    - Authentication via google-ads.yaml config file
    - GAQL query execution (SearchGoogleAdsRequest and SearchGoogleAdsStreamRequest)
    - Account hierarchy traversal (MCC to customer accounts)
    - Response conversion from Protobuf to DataFrame
    - Error handling and logging

    Design:
    - Completely independent (NO base class)
    - Uses official Google Ads Python client
    - Supports both regular and streaming requests
    - Converts Protobuf messages to Python dicts

    Attributes:
        token_provider: Token provider (not directly used, credentials in config file)
        config_file_path: Path to google-ads.yaml configuration file
        api_version: Google Ads API version (e.g., "v18")
        manager_customer_id: Manager account ID (MCC)
        client: Google Ads client instance
    """

    def __init__(
        self,
        token_provider: TokenProvider,
        config_file_path: str,
        api_version: str = "v18",
        manager_customer_id: str = "9474097201",
    ) -> None:
        """
        Initialize Google Ads HTTP client.

        Args:
            token_provider: Token provider (not directly used, kept for protocol compatibility)
            config_file_path: Path to google-ads.yaml configuration file
            api_version: Google Ads API version (default: v18)
            manager_customer_id: Manager account ID (MCC)

        Raises:
            AuthenticationError: If client initialization fails
        """
        self.token_provider = token_provider
        self.config_file_path = config_file_path
        self.api_version = api_version
        self.manager_customer_id = manager_customer_id

        # Initialize Google Ads client
        try:
            self.client = GoogleAdsClient.load_from_storage(
                path=config_file_path,
                version=api_version,
            )
            logger.info(f"Google Ads client initialized (API version: {api_version})")
        except Exception as e:
            raise AuthenticationError(
                f"Failed to initialize Google Ads client: {str(e)}",
                details={
                    "config_file": config_file_path,
                    "api_version": api_version,
                },
            )

    def get_all_accounts(self) -> List[Dict[str, Any]]:
        """
        Get all accessible customer accounts under the manager account.

        This method:
        1. Retrieves list of accessible customers
        2. Queries customer hierarchy for each seed account
        3. Recursively fetches child accounts from manager accounts
        4. Returns flattened list of all customer accounts

        Returns:
            List of customer account dictionaries with fields:
                - id: Customer ID (int)
                - descriptive_name: Account name (str)
                - manager: Whether account is a manager (bool)
                - status: Account status (str: ENABLED, DISABLED, etc.)
                - currency_code: Account currency (str)
                - time_zone: Account timezone (str)
                - level: Account level in hierarchy (int)
                - client_customer: Client customer resource name (str)

        Raises:
            APIError: If account retrieval fails
        """
        try:
            customer_service = self.client.get_service("CustomerService")
            google_ads_service = self.client.get_service("GoogleAdsService")

            # Get accessible customers
            accessible_customers = customer_service.list_accessible_customers()
            customer_resource_names = accessible_customers.resource_names

            # Extract customer IDs from resource names
            seed_customer_ids = []
            for resource_name in customer_resource_names:
                customer = google_ads_service.parse_customer_path(resource_name)
                seed_customer_ids.append(customer["customer_id"])

            logger.info(f"Found {len(seed_customer_ids)} accessible customer account(s)")

            # Query customer hierarchy for each seed account
            query = """
                SELECT
                  customer_client.client_customer,
                  customer_client.level,
                  customer_client.manager,
                  customer_client.descriptive_name,
                  customer_client.currency_code,
                  customer_client.time_zone,
                  customer_client.id,
                  customer_client.status
                FROM customer_client
            """

            all_customers = []
            processed_managers = set()

            for seed_id in seed_customer_ids:
                try:
                    response = google_ads_service.search(
                        customer_id=str(seed_id),
                        query=query,
                    )

                    # Extract customer_client objects and add manager_id
                    for row in response:
                        customer_dict = MessageToDict(row.customer_client._pb)
                        customer_dict["managerId"] = seed_id  # Track which seed/manager this came from
                        all_customers.append(customer_dict)

                    # Find manager accounts and get their child accounts
                    for row in response:
                        if row.customer_client.manager:
                            manager_id = str(row.customer_client.id)

                            # Skip if already processed or is seed account
                            if manager_id in processed_managers or manager_id in seed_customer_ids:
                                continue

                            processed_managers.add(manager_id)
                            logger.debug(f"Querying child accounts for manager {manager_id}")

                            try:
                                manager_response = google_ads_service.search(
                                    customer_id=manager_id,
                                    query=query,
                                )

                                for manager_row in manager_response:
                                    # Only add non-manager accounts (actual customer accounts)
                                    if not manager_row.customer_client.manager:
                                        customer_dict = MessageToDict(manager_row.customer_client._pb)
                                        customer_dict["managerId"] = manager_id  # Track the direct manager
                                        all_customers.append(customer_dict)

                            except Exception as e:
                                logger.warning(f"Error querying manager {manager_id}: {str(e)}")
                                continue

                except Exception as e:
                    logger.warning(f"Error querying customer {seed_id}: {str(e)}")
                    continue

            logger.info(f"Retrieved {len(all_customers)} total customer accounts")
            return all_customers

        except Exception as e:
            raise APIError(
                f"Failed to retrieve Google Ads accounts: {str(e)}",
                details={"manager_id": self.manager_customer_id},
            )

    def execute_query(
        self,
        customer_id: str,
        query: str,
        use_streaming: bool = False,
    ) -> pd.DataFrame:
        """
        Execute a GAQL query and return results as DataFrame.

        This method:
        1. Validates customer ID and query
        2. Chooses request type (SearchGoogleAdsRequest or SearchGoogleAdsStreamRequest)
        3. Executes query via Google Ads service
        4. Converts Protobuf response to DataFrame
        5. Returns normalized data

        Args:
            customer_id: Customer ID to query (must be string)
            query: GAQL query string
            use_streaming: If True, use streaming API for large result sets

        Returns:
            DataFrame with query results (empty DataFrame if no results)

        Raises:
            APIError: If query execution fails
        """
        try:
            google_ads_service = self.client.get_service("GoogleAdsService")

            logger.debug(f"Executing query for customer {customer_id} (streaming={use_streaming})")
            logger.trace(f"Query: {query}")

            if use_streaming:
                # Use SearchGoogleAdsStreamRequest for large datasets
                search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
                search_request.customer_id = str(customer_id)
                search_request.query = query

                response = google_ads_service.search_stream(search_request)
                return self._convert_streaming_response_to_df(response)
            else:
                # Use SearchGoogleAdsRequest for regular queries
                search_request = self.client.get_type("SearchGoogleAdsRequest")
                search_request.customer_id = str(customer_id)
                search_request.query = query

                response = google_ads_service.search(request=search_request)
                return self._convert_response_to_df(response)

        except Exception as e:
            raise APIError(
                f"Failed to execute Google Ads query: {str(e)}",
                details={
                    "customer_id": customer_id,
                    "query": query[:200] if len(query) > 200 else query,
                    "use_streaming": use_streaming,
                },
            )

    def _convert_streaming_response_to_df(
        self,
        response: _StreamingResponseIterator,
    ) -> pd.DataFrame:
        """
        Convert streaming response to DataFrame.

        Streaming responses return data in batches. This method:
        1. Extracts the first result from stored response
        2. Converts Protobuf to dict using MessageToDict
        3. Normalizes nested JSON into flat DataFrame
        4. Returns empty DataFrame if no results

        Args:
            response: Streaming response from Google Ads API

        Returns:
            DataFrame with normalized results
        """
        try:
            # Access stored first result from streaming iterator
            dict_obj = MessageToDict(response._stored_first_result._pb)

            if "results" in dict_obj:
                df = pd.json_normalize(dict_obj["results"])
            else:
                df = pd.DataFrame()

            return df

        except (KeyError, AttributeError) as e:
            logger.debug(f"No results in streaming response: {str(e)}")
            return pd.DataFrame()

    def _convert_response_to_df(self, response: Any) -> pd.DataFrame:
        """
        Convert regular response to DataFrame.

        Regular responses may be paginated. This method:
        1. Iterates through all pages (if paginated)
        2. Converts each Protobuf message to dict
        3. Extracts 'results' from each page
        4. Flattens into single DataFrame

        Args:
            response: Response from Google Ads API

        Returns:
            DataFrame with normalized results
        """
        try:
            all_results = []

            # Convert response to list (handles both single and paginated responses)
            for row in response:
                # Convert Protobuf message to dict
                row_dict = MessageToDict(row._pb)
                all_results.append(row_dict)

            if all_results:
                df = pd.json_normalize(all_results)
            else:
                df = pd.DataFrame()

            return df

        except (KeyError, AttributeError) as e:
            logger.debug(f"No results in response: {str(e)}")
            return pd.DataFrame()

    def execute_query_stream(
        self,
        customer_id: str,
        query: str,
    ) -> pd.DataFrame:
        """
        Execute a GAQL query using streaming API.

        Convenience method that calls execute_query with use_streaming=True.

        Args:
            customer_id: Customer ID to query
            query: GAQL query string

        Returns:
            DataFrame with query results

        Raises:
            APIError: If query execution fails
        """
        return self.execute_query(
            customer_id=customer_id,
            query=query,
            use_streaming=True,
        )

    def protobuf_to_dict(self, protobuf_message: Any) -> Dict[str, Any]:
        """
        Convert Protobuf message to Python dictionary.

        This is a utility method for manual Protobuf conversion if needed.

        Args:
            protobuf_message: Protobuf message object

        Returns:
            Dictionary representation of Protobuf message
        """
        return MessageToDict(protobuf_message)

    def close(self) -> None:
        """
        Close the client and release resources.

        Note: Google Ads client doesn't require explicit cleanup,
        but this method is provided for protocol compatibility.
        """
        logger.debug("Google Ads HTTP client closed")
