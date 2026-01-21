"""
LinkedIn Ads API client.
Handles authentication and data retrieval from LinkedIn Ads API.
"""

import datetime
import urllib.parse
from typing import AnyStr, Dict, List

import pandas as pd
from requests import Response
from loguru import logger

from social.platforms.linkedin import company_account
from social.platforms.linkedin.endpoints import LinkedinEndPoint
from social.platforms.linkedin.noquotedsession import NoQuotedCommasSession
from social.utils.commons import log_df_dimension, fix_id_type, handle_nested_response, handle_simple_response


def raise_for_error_linkedin(response: Response):
    """Raise exception for LinkedIn API errors."""
    import requests
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            if len(response.content) == 0:
                return
            resp_json = response.json()
            status = resp_json.get("status")
            msg = resp_json.get("message")
            logger.error(f"LinkedIn API error: status={status}, message={msg}")
        except (ValueError, TypeError):
            logger.error(f"LinkedIn API error: {error}")


class LinkedinAdsService:
    """
    LinkedIn Ads API service.
    Handles data requests and authentication.
    """

    def __init__(
        self, access_token, client_id, client_secret, refresh_token, timeout=100
    ):
        assert access_token, "access_token is required"
        assert client_id, "client_id is required"
        assert client_secret, "client_secret is required"
        assert refresh_token, "refresh_token is required"

        self.access_token = access_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.timeout = timeout
        self.session = NoQuotedCommasSession()

    @log_df_dimension
    def convert_to_df(self, response, table: AnyStr, nested_element) -> pd.DataFrame:
        """Convert API response to DataFrame."""
        if table == "linkedin_ads_campaign_audience":
            return self._convert_targeting_to_df(response=response)

        df = self._convert_to_df(response=response, nested_element=nested_element)
        df = fix_id_type(df)
        return df.drop_duplicates()

    def _convert_targeting_to_df(self, response) -> pd.DataFrame:
        """Convert targeting data to DataFrame."""
        audiences = [
            "urn:li:adTargetingFacet:audienceMatchingSegments",
            "urn:li:adTargetingFacet:dynamicSegments",
        ]
        segments = []
        ids = []

        for resp in response:
            for r in resp:
                try:
                    target = r.get("targetingCriteria").get("include").get("and")
                    elements_target = [i.get("or") for i in target]
                    segment = [[e.get(a) for e in elements_target] for a in audiences]
                    segment = [e for v in segment for e in v if v is not None]
                    segment = [i[0] if i is not None else i for i in segment]
                    segment = list(filter(None, segment))

                    if len(segment) > 0:
                        seg = segment[0].split("urn:li:adSegment:")[1]
                        segments.append(seg)
                    else:
                        segments.append(None)
                    ids.append(r.get("id"))
                except Exception as e:
                    logger.exception(e)

        return pd.DataFrame({"id": ids, "audience_id": segments})

    def _convert_to_df(self, response, nested_element) -> pd.DataFrame:
        """Convert response to DataFrame."""
        if nested_element:
            return handle_nested_response(response, nested_element=nested_element)
        return handle_simple_response(response)

    @staticmethod
    def build_args(kwarg):
        """Build API arguments with proper formatting."""
        old_key_list = [
            "dateRange_start",
            "timeIntervals_timeGranularityType",
            "timeIntervals_timeRange",
            "search_campaign_values",
            "pivot_value",
            "timeGranularity_value",
        ]

        for old_k in old_key_list:
            kwarg = dict(
                (k.replace("_", "."), v) if k.startswith(old_k) else (k, v)
                for k, v in kwarg.items()
            )

        kwarg = dict(
            (k + "[0]", v) if k == "search.campaign.values" else (k, v)
            for k, v in kwarg.items()
        )
        return kwarg

    @staticmethod
    def build_fields(fields: list) -> str:
        """Build fields string from list."""
        return ",".join(fields)

    def _request(
        self,
        path: AnyStr,
        headers: Dict,
        method: AnyStr = "GET",
        args: Dict = None,
        no_encoded_args=None,
        **ignored,
    ) -> Response:
        """Make HTTP request to LinkedIn API."""
        path = path[0] if isinstance(path, tuple) else path

        if headers.get("Content-Type") == "application/x-www-form-urlencoded":
            return self.session.request(
                method, path, timeout=self.timeout, data=args, headers=headers
            )
        return self.session.request(
            method,
            path,
            timeout=self.timeout,
            params=args,
            no_encoded_args=no_encoded_args,
            headers=headers,
        )

    def request_data(
        self,
        request,
        request_type,
        method="GET",
        headers: Dict = None,
        pagination=False,
        no_encoded_args=None,
        **kwargs,
    ):
        """
        Request data from LinkedIn API.

        Args:
            request: Request method name
            request_type: LinkedIn endpoint type
            method: HTTP method
            headers: Request headers
            **kwargs: Additional arguments

        Returns:
            API response
        """
        default_headers = {"x-li-format": "json", "Content-Type": "application/json"}
        headers = headers or {}
        headers.update(default_headers)
        headers.update({"Authorization": f"Bearer {self.access_token}"})

        args = self.build_args(kwargs)

        response = getattr(self, request)(
            type=request_type,
            method=method,
            path=LinkedinEndPoint[request_type].value,
            headers=headers,
            pagination=pagination,
            no_encoded_args=no_encoded_args,
            args=args,
        )

        if isinstance(response, list):
            response = [i.json() for i in response]
            response = [i.get("elements", i) for i in response]
        else:
            response = response.json()
            response = response.get("elements", response)

        return response

    def get_campaigns(self, headers, args, path, pagination, **ignored):
        """Get campaigns for all accounts."""
        responses = []
        path = path[0] if isinstance(path, tuple) else path
        _base = LinkedinEndPoint["API_BASE_PATH"].value
        arg_no_encoded = {
            "search": "(status:(values:List(ACTIVE,PAUSED,COMPLETED,ARCHIVED)))"
        }

        for id, _ in company_account.items():
            args.update(arg_no_encoded)
            complete_path = path.format(_base, id)

            response = self._request(
                path=complete_path,
                headers=headers,
                no_encoded_args=arg_no_encoded,
                args=args,
            )
            raise_for_error_linkedin(response)
            responses.append(response)

        return responses

    def get_audience(self, headers, args, path, **ignored):
        """Get audience data for all accounts."""
        _base = LinkedinEndPoint["API_BASE_PATH"].value
        path = path[0] if isinstance(path, tuple) else path
        complete_path = path.format(_base)

        responses = []
        for id, company in company_account.items():
            encoded_urn = urllib.parse.quote(f"urn:li:sponsoredAccount:{id}")
            no_encoded_arg = f"List({encoded_urn})"
            args.update({"accounts": no_encoded_arg})
            no_encoded_args = {"accounts": no_encoded_arg}

            response = self._request(
                path=complete_path,
                headers=headers,
                no_encoded_args=no_encoded_args,
                args=args,
            )
            raise_for_error_linkedin(response)
            responses.append(response)

        return responses

    def get_account(self, args: Dict, path, headers: Dict = None, **ignored):
        """Get account data."""
        responses = []
        path = path[0] if isinstance(path, tuple) else path
        _base = LinkedinEndPoint["API_BASE_PATH"].value
        complete_path = path.format(_base)

        response = self._request(path=complete_path, headers=headers, args=args)
        raise_for_error_linkedin(response)
        responses.append(response)

        return responses


class LinkedinAdsBuilder:
    """Builder for LinkedinAdsService instances."""

    def __init__(self):
        self._instance = None

    def __call__(
        self, access_token, client_id, client_secret, refresh_token, **ignored
    ) -> LinkedinAdsService:
        """Create LinkedinAdsService instance."""
        if not self._instance:
            self._instance = LinkedinAdsService(
                access_token=access_token,
                client_id=client_id,
                client_secret=client_secret,
                refresh_token=refresh_token,
            )
        return self._instance
