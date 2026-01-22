"""Google Ads HTTP client implementation.

This module provides a specialized HTTP client for Google Ads API,
using the official Google Ads Python client library.
"""

from typing import Dict, Any, List, Optional
from loguru import logger
from google.ads.googleads.client import GoogleAdsClient
from google.api_core.grpc_helpers import _StreamingResponseIterator
from google.protobuf.json_format import MessageToDict
import pandas as pd

from social.core.protocols import TokenProvider
from social.core.exceptions import APIError, AuthenticationError


class GoogleAdsHTTPClient:
    """HTTP client for Google Ads API using official Google Ads Python client.

    This client handles:
    - Authentication via Google Ads configuration file
    - GAQL query execution (SearchGoogleAdsRequest and SearchGoogleAdsStreamRequest)
    - Account hierarchy traversal
    - Response conversion to DataFrame
    - Error handling and retries
    """

    def __init__(
        self,
        token_provider: TokenProvider,
        config_file_path: str,
        api_version: str = "v18",
        manager_customer_id: str = "9474097201"
    ):
        """Initialize Google Ads HTTP client.

        Args:
            token_provider: Provider for OAuth tokens (not used directly, credentials in config file)
            config_file_path: Path to google-ads.yaml configuration file
            api_version: Google Ads API version (default: v18)
            manager_customer_id: Manager account ID (MCC)
        """
        self.token_provider = token_provider
        self.config_file_path = config_file_path
        self.api_version = api_version
        self.manager_customer_id = manager_customer_id

        # Initialize Google Ads client
        try:
            self.client = GoogleAdsClient.load_from_storage(
                path=config_file_path,
                version=api_version
            )
            logger.info(f"Google Ads client initialized with API version {api_version}")
        except Exception as e:
            raise AuthenticationError(
                f"Failed to initialize Google Ads client: {str(e)}",
                details={"config_file": config_file_path, "api_version": api_version}
            )

    def get_all_accounts(self) -> List[Dict[str, Any]]:
        """Get all accessible customer accounts under the manager account.

        Returns:
            List of customer account dictionaries with fields:
                - id: Customer ID
                - descriptive_name: Account name
                - manager: Whether account is a manager
                - status: Account status (ENABLED, DISABLED, etc.)
                - currency_code: Account currency
                - time_zone: Account timezone
                - level: Account level in hierarchy

        Raises:
            APIError: If account retrieval fails
        """
        try:
            customer_service = self.client.get_service("CustomerService")
            google_ads_service = self.client.get_service("GoogleAdsService")

            # Get accessible customers
            accessible_customers = customer_service.list_accessible_customers()
            customer_resource_names = accessible_customers.resource_names

            # Extract customer IDs
            seed_customer_ids = []
            for resource_name in customer_resource_names:
                customer = google_ads_service.parse_customer_path(resource_name)
                seed_customer_ids.append(customer["customer_id"])

            logger.info(f"Found {len(seed_customer_ids)} accessible customer accounts")

            # Query customer hierarchy
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
            for seed_id in seed_customer_ids:
                try:
                    response = google_ads_service.search(
                        customer_id=str(seed_id),
                        query=query
                    )

                    # Extract customer_client objects
                    for row in response:
                        customer_dict = MessageToDict(row.customer_client._pb)
                        all_customers.append(customer_dict)

                    # Find managers and get their child accounts
                    for row in response:
                        if row.customer_client.manager and str(row.customer_client.id) not in seed_customer_ids:
                            manager_id = str(row.customer_client.id)
                            logger.info(f"Querying child accounts for manager {manager_id}")

                            manager_response = google_ads_service.search(
                                customer_id=manager_id,
                                query=query
                            )

                            for manager_row in manager_response:
                                if not manager_row.customer_client.manager:
                                    customer_dict = MessageToDict(manager_row.customer_client._pb)
                                    all_customers.append(customer_dict)

                except Exception as e:
                    logger.warning(f"Error querying customer {seed_id}: {str(e)}")
                    continue

            logger.info(f"Retrieved {len(all_customers)} total customer accounts")
            return all_customers

        except Exception as e:
            raise APIError(
                f"Failed to retrieve Google Ads accounts: {str(e)}",
                details={"manager_id": self.manager_customer_id}
            )

    def execute_query(
        self,
        customer_id: str,
        query: str,
        use_streaming: bool = False
    ) -> pd.DataFrame:
        """Execute a GAQL query and return results as DataFrame.

        Args:
            customer_id: Customer ID to query
            query: GAQL query string
            use_streaming: Whether to use streaming API (for large result sets)

        Returns:
            DataFrame with query results

        Raises:
            APIError: If query execution fails
        """
        try:
            google_ads_service = self.client.get_service("GoogleAdsService")

            logger.debug(f"Executing query for customer {customer_id}: {query[:100]}...")

            if use_streaming:
                # Use streaming for large result sets
                search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
                search_request.customer_id = customer_id
                search_request.query = query

                response = google_ads_service.search_stream(search_request)
                return self._convert_streaming_response_to_df(response)
            else:
                # Use regular search
                search_request = self.client.get_type("SearchGoogleAdsRequest")
                search_request.customer_id = customer_id
                search_request.query = query

                response = google_ads_service.search(request=search_request)
                return self._convert_response_to_df(response)

        except Exception as e:
            raise APIError(
                f"Failed to execute Google Ads query: {str(e)}",
                details={
                    "customer_id": customer_id,
                    "query": query[:200],
                    "use_streaming": use_streaming
                }
            )

    def _convert_streaming_response_to_df(
        self,
        response: _StreamingResponseIterator
    ) -> pd.DataFrame:
        """Convert streaming response to DataFrame.

        Args:
            response: Streaming response from Google Ads API

        Returns:
            DataFrame with normalized results
        """
        try:
            dict_obj = MessageToDict(response._stored_first_result._pb)
            if "results" in dict_obj:
                df = pd.json_normalize(dict_obj["results"])
            else:
                df = pd.DataFrame()

            return df

        except (KeyError, AttributeError) as e:
            logger.warning(f"No results in streaming response: {str(e)}")
            return pd.DataFrame()

    def _convert_response_to_df(self, response) -> pd.DataFrame:
        """Convert regular response to DataFrame.

        Args:
            response: Response from Google Ads API

        Returns:
            DataFrame with normalized results
        """
        try:
            results = []

            # Handle paginated responses
            if hasattr(response, "pages"):
                for page in response.pages:
                    results.append(page)

                dict_objs = [MessageToDict(result._pb) for result in results]
            else:
                # Single result
                dict_obj = MessageToDict(response._pb)
                if "results" in dict_obj:
                    return pd.json_normalize(dict_obj["results"])
                else:
                    return pd.DataFrame()

            # Flatten all results
            all_results = []
            for dict_obj in dict_objs:
                if "results" in dict_obj:
                    all_results.extend(dict_obj["results"])

            if all_results:
                df = pd.json_normalize(all_results)
            else:
                df = pd.DataFrame()

            return df

        except (KeyError, AttributeError) as e:
            logger.warning(f"No results in response: {str(e)}")
            return pd.DataFrame()
