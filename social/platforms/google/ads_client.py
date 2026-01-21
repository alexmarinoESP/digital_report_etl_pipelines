"""
Google Ads API client.
Handles authentication and data retrieval from Google Ads API.
"""

import logging
from datetime import datetime, timedelta
from typing import AnyStr, List

import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from google.api_core.grpc_helpers import _StreamingResponseIterator
from google.protobuf.json_format import MessageToDict
from loguru import logger

from social.platforms.google import dispatcher
from social.utils.commons import retry, sleeper, log_df_dimension


class GoogleAdsService:
    """
    Google Ads API service.
    Handles data requests and transformations.
    """

    def __init__(self, manager_id, client):
        self.manager_id = manager_id
        self.client = client

    def _get_service(self, service_name):
        """Get Google Ads service by name."""
        return self.client.get_service(service_name)

    def _get_type(self, client, type_name):
        """Get Google Ads type by name."""
        return client.get_type(type_name)

    @retry
    def _request_ads_stream(self, customer_id, query, request_type):
        """Request data using streaming API."""
        service = self._get_service("GoogleAdsService")
        search_request = self.client.get_type(request_type)
        search_request.customer_id = customer_id
        search_request.query = query
        return service.search_stream(search_request)

    @retry
    def _request_ads_service(self, customer_id, query, request_type):
        """Request data using standard search API."""
        service = self._get_service("GoogleAdsService")
        search_request = self._get_type(client=self.client, type_name=request_type)
        search_request.customer_id = customer_id
        search_request.query = query
        return service.search(request=search_request)

    @sleeper(secs=5)
    def request_data(self, customer_id, query, request_type, day):
        """
        Request data from Google Ads API.

        Args:
            customer_id: Google Ads customer ID
            query: Query name from dispatcher
            request_type: Type of request (SearchGoogleAdsStreamRequest, etc.)
            day: Number of days to look back

        Returns:
            API response
        """
        q = dispatcher.get(query)
        if day:
            prev = (datetime.now() - timedelta(days=day)).strftime("%Y-%m-%d")
            act = datetime.now().strftime("%Y-%m-%d")
            q = q.format(prev, act)

        if request_type == "SearchGoogleAdsStreamRequest":
            return self._request_ads_stream(customer_id, q, request_type)

        if request_type == "SearchGoogleAdsRequest":
            return self._request_ads_service(customer_id, q, request_type)

    def get_all_account(self, query):
        """Get all accessible accounts."""
        serv_cust = self._get_service(service_name="CustomerService")
        serv_ads = self._get_service(service_name="GoogleAdsService")

        seed_customer_ids = []
        customer_resource_names = serv_cust.list_accessible_customers().resource_names

        for customer_resource_name in customer_resource_names:
            customer = serv_ads.parse_customer_path(customer_resource_name)
            seed_customer_ids.append(customer.get("customer_id"))

        customer_ids_to_child_accounts = []
        for seed_customer_id in seed_customer_ids:
            try:
                response = serv_ads.search(
                    customer_id=str(seed_customer_id), query=query
                )
                custs = [i.customer_client for i in response]
                customer_ids_to_child_accounts.extend(custs)
            except Exception as e:
                logger.debug(e)

            managers = [
                i for i in custs
                if (i.manager and str(i.id) not in seed_customer_ids)
            ]
            for m in managers:
                response = serv_ads.search(customer_id=str(m.id), query=query)
                custs = [
                    i.customer_client for i in response
                    if not i.customer_client.manager
                ]
                customer_ids_to_child_accounts.extend(custs)

        return customer_ids_to_child_accounts

    @log_df_dimension
    def convert_to_df(self, response, **ignored) -> pd.DataFrame:
        """
        Convert Google Ads response to DataFrame.

        Args:
            response: Google Ads API response

        Returns:
            DataFrame with response data
        """
        try:
            if isinstance(response, _StreamingResponseIterator):
                dictobj = MessageToDict(response._stored_first_result._pb)
                df = pd.json_normalize(dictobj, record_path=["results"])

            elif isinstance(response, list):
                dictobj = [MessageToDict(i._pb) for i in response]
                df = pd.json_normalize(dictobj)

            else:
                res = []
                if hasattr(response, "pages"):
                    for page in response.pages:
                        res.append(page)
                    dictobj = [MessageToDict(i._pb) for i in res]
                else:
                    dictobj = MessageToDict(response._pb)
                df = pd.json_normalize(dictobj, record_path=["results"])

            return df

        except (KeyError, AttributeError) as e:
            logging.info("No results key found")
            return pd.DataFrame()

    def transform_account_into_df(self, results, **ignored) -> pd.DataFrame:
        """Transform account results into DataFrame."""
        dfs = []
        for manager, account in results.items():
            df = self.transform_into_df(account)
            df["manager_id"] = manager
            dfs.append(df)
        return pd.concat(dfs)


class GoogleAdsServiceBuilder:
    """Builder for GoogleAdsService instances."""

    def __init__(self):
        self._instance = None

    def __call__(self, config_file, version, manager_id, **ignored) -> GoogleAdsService:
        """
        Create GoogleAdsService instance.

        Args:
            config_file: Path to Google Ads YAML config
            version: API version
            manager_id: Manager account ID

        Returns:
            Configured GoogleAdsService
        """
        self.reset()
        if not self._instance:
            client = self._init_session(config_file=config_file, version=version)
            self._instance = GoogleAdsService(client=client, manager_id=manager_id)
        return self._instance

    def _init_session(self, config_file, version):
        """Initialize Google Ads client from config file."""
        return GoogleAdsClient.load_from_storage(path=config_file, version=version)

    def reset(self):
        """Reset the builder instance."""
        self._instance = None
