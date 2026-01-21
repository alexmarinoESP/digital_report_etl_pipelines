import datetime
import urllib.parse
from datetime import timedelta
from typing import AnyStr, Dict

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


class LinkedinAdsBuilder:
    def __init__(self):
        self._instance = None

    def __call__(
        self, access_token, client_id, client_secret, refresh_token, **ignored
    ):
        if not self._instance:
            self._instance = LinkedinAdsService(
                access_token=access_token,
                client_id=client_id,
                client_secret=client_secret,
                refresh_token=refresh_token,
            )

            return self._instance


class LinkedinAdsService:
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
    def convert_to_df(self, response, table: AnyStr, nested_element):
        if table == "linkedin_ads_campaign_audience":
            df = self._convert_targeting_to_df(response=response)
            return df

        df = self._convert_to_df(
            response=response, nested_element=nested_element)

        df = fix_id_type(df)

        df = df.drop_duplicates()
        # dfs.append(df)

        return df  # pd.concat(dfs)

    def _convert_targeting_to_df(self, response):
        audiences = [
            "urn:li:adTargetingFacet:audienceMatchingSegments",
            "urn:li:adTargetingFacet:dynamicSegments",
        ]
        segments = []
        ids = []
        for resp in response:
            for r in resp:
                try:
                    target = r.get("targetingCriteria").get(
                        "include").get("and")
                    elements_target = [i.get("or") for i in target]

                    segment = [[e.get(a) for e in elements_target]
                               for a in audiences]
                    segment = [e for v in segment for e in v if v is not None]
                    segment = [i[0] if i is not None else i for i in segment]
                    segment = list(filter(None, segment))

                    if len(segment) > 0:
                        seg = segment[0]
                        seg = seg.split("urn:li:adSegment:")[1]
                        segments.append(seg)

                    else:
                        segments.append(None)
                    ids.append(r.get("id"))

                except Exception as e:
                    logger.exception(e)

        df = pd.DataFrame({"id": ids, "audience_id": segments})

        return df

    def _convert_to_df(self, response, nested_element):
        if nested_element:
            df = handle_nested_response(
                response, nested_element=nested_element)
        else:
            df = handle_simple_response(response)

        return df

    @staticmethod
    def build_args(kwarg):
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
    def build_fields(fields: list):
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
        """
        Create a request

        Args:
            obj: endpoint object
            root: root url path
            target: method api
            method: str, GET
            args: dictionary, query string parameter

        Returns:

        """

        path = path[0] if isinstance(path, tuple) else path
        print(path)
        if headers["Content-Type"] == "application/x-www-form-urlencoded":
            r = self.session.request(
                method, path, timeout=self.timeout, data=args, headers=headers
            )
        else:
            r = self.session.request(
                method,
                path,
                timeout=self.timeout,
                params=args,
                no_encoded_args=no_encoded_args,
                headers=headers,
            )

        return r

    def request_data(
        self,
        request,
        type,
        method="GET",
        headers: Dict = None,
        pagination=False,
        no_encoded_args=None,
        **kwargs,
    ):
        default_headers = {
            "x-li-format": "json",
            "Content-Type": "application/json",
            "LinkedIn-Version": LinkedinEndPoint["VERSION"].value  # Required by new API
        }
        if headers is not None:
            headers.update(headers)
        else:
            headers = {}
            headers.update(default_headers)

        #kwargs.update({"oauth2_access_token": self.access_token})
        #kwargs.update({"client_id": self.client_id})
        #kwargs.update({"client_secret": self.client_secret})

        headers.update({"Authorization": f"Bearer {self.access_token}"})

        # Extract URNs if present (for insights/creatives)
        urns = kwargs.pop('urns', None)

        args = self.build_args(kwargs)

        # Pass URNs to method if present
        method_kwargs = {
            'type': type,
            'method': method,
            'path': LinkedinEndPoint[type].value,
            'headers': headers,
            'pagination': pagination,
            'no_encoded_args': no_encoded_args,
            'args': args,
        }
        if urns is not None:
            method_kwargs['urns'] = urns

        response = getattr(self, request)(**method_kwargs)
        # from bs4 import BeautifulSoup
        # soup = BeautifulSoup(response[0].content, 'html.parser')
        # # Find all meta tags with a name attribute
        # meta_tags = soup.find_all('meta', {'name': True})
        #
        # # Extract metadata key-value pairs
        # metadata = {}
        # for tag in meta_tags:
        #     name = tag.get('name')
        #     content = tag.get('content')
        #     metadata[name] = content
        #
        # # Convert metadata dictionary to DataFrame
        # df = pd.DataFrame(metadata.items(), columns=['Name', 'Content'])

        # pd.read_html(str(response[0].content))
        if isinstance(response, list):
            response = [i.json() for i in response]
            response = [i.get("elements", i) for i in response]
        else:
            response = response.json()
            response = response.get("elements", response)

        return response

    def get_campaigns(self, headers, args, path, pagination, **ignored):
        responses = []
        path = path[0] if isinstance(path, tuple) else path
        _base = LinkedinEndPoint["API_BASE_PATH"].value

        for id, _ in company_account.items():
            complete_path = path.format(_base, id)

            # Don't filter by status - get all campaigns
            # The old search syntax is no longer supported in v202509
            response = self._request(
                path=complete_path,
                headers=headers,
                no_encoded_args={},
                args=args,
            )

            raise_for_error_linkedin(response)
            responses.append(response)

        return responses

    def get_audience(self, headers, args, path, **ignored):
        _base = LinkedinEndPoint["API_BASE_PATH"].value
        path = path[0] if isinstance(path, tuple) else path
        complete_path = path.format(_base)

        responses = []
        for id, company in company_account.items():
            # API v202509: Use array format without List() wrapper
            # accounts should be: urn:li:sponsoredAccount:123456
            urn = f"urn:li:sponsoredAccount:{id}"

            # Pass as array parameter (no encoding needed for URN itself)
            args_copy = args.copy()
            args_copy.update({"accounts": urn})

            logger.debug(f"Audience request for account: {urn}")

            response = self._request(
                path=complete_path,
                headers=headers,
                no_encoded_args={},
                args=args_copy,
            )

            raise_for_error_linkedin(response)
            responses.append(response)

        return responses

    def get_account(self, args: Dict, path, headers: Dict = None, **ignored):
        responses = []

        path = path[0] if isinstance(path, tuple) else path
        _base = LinkedinEndPoint["API_BASE_PATH"].value
        complete_path = path.format(_base)

        logger.info(f"get_account: complete_path={complete_path}")
        logger.info(f"get_account: headers={headers}")
        logger.info(f"get_account: args={args}")

        response = self._request(
            path=complete_path, headers=headers, args=args)

        logger.info(f"get_account: status_code={response.status_code}")
        logger.info(f"get_account: response_text={response.text[:500] if response.text else 'empty'}")

        raise_for_error_linkedin(response)
        responses.append(response)

        return responses

    def get_insights(self, path: AnyStr, args: Dict, headers: Dict = None, urns=None, **ignored):
        responses = []

        # URNs must be provided by caller (from database query)
        if urns is None or urns.empty:
            logger.warning("No campaign URNs provided for insights query")
            return responses

        dateRange = datetime.datetime.now() - timedelta(days=150)
        year, month, day = str(dateRange.year), str(
            dateRange.month), str(dateRange.day)

        path = path[0] if isinstance(path, tuple) else path
        _base = LinkedinEndPoint["API_BASE_PATH"].value
        complete_path = path.format(_base)

        # Date format: (start:(year:2024,month:1,day:1))
        date = f"(start:(year:{year},month:{month},day:{day}))"

        logger.info(f"Fetching insights for {len(urns)} campaigns from {year}-{month}-{day}")

        for urn in list(urns.id):
            try:
                # Create campaign URN with List() wrapper (still required by API)
                campaign_urn = f"urn:li:sponsoredCampaign:{str(round(float(urn)))}"
                campaign_param = f"List({campaign_urn})"

                # Build args (URL-encoded parameters)
                request_args = {
                    "q": "analytics",  # Required finder parameter
                    "pivot": "CREATIVE",
                    "timeGranularity": "DAILY",
                }

                # Parameters that should NOT be URL-encoded (special LinkedIn format)
                no_encoded_args = {
                    "campaigns": campaign_param,
                    "dateRange": date,
                }

                # Add fields from config if present (not URL-encoded)
                # Fields should be comma-separated string
                if args.get("fields"):
                    fields_list = args.get("fields")
                    if isinstance(fields_list, list):
                        fields_str = ",".join(fields_list)
                    else:
                        fields_str = fields_list
                    no_encoded_args["fields"] = fields_str

                response = self._request(
                    path=complete_path,
                    method='GET',
                    headers=headers,
                    no_encoded_args=no_encoded_args,
                    args=request_args,
                )

                if response.status_code != 200:
                    logger.error(f"Insights API failed for campaign {urn}: status={response.status_code}, url={response.url if hasattr(response, 'url') else 'N/A'}")

                raise_for_error_linkedin(response)
                responses.append(response)

            except Exception as e:
                logger.error(f"Error fetching insights for campaign {urn}: {e}")
                continue

        logger.info(f"Successfully fetched insights for {len(responses)} campaigns")
        return responses

    def get_creatives(self, path: AnyStr, args: Dict, headers: Dict = None, urns=None, **ignored):
        responses = []

        # URNs must be provided by caller (from database query)
        if urns is None or urns.empty:
            logger.warning("No creative URNs provided for creatives query")
            return responses

        _base = LinkedinEndPoint["API_BASE_PATH"].value
        path = path[0] if isinstance(path, tuple) else path

        logger.info(f"Fetching creatives for {len(urns)} creative URNs")

        for id, _ in company_account.items():
            for urn in list(urns.id):
                try:
                    # Create creative URN - URL encoded
                    creative_urn = f"urn%3Ali%3AsponsoredCreative%3A{urn}"
                    complete_path = path.format(_base, id, creative_urn)

                    response = self._request(
                        path=complete_path,
                        headers=headers,
                        no_encoded_args={},
                        args={},
                    )

                    raise_for_error_linkedin(response)
                    if response.status_code == 200:
                        responses.append(response)

                except Exception as e:
                    logger.error(f"Error fetching creative {urn} for account {id}: {e}")
                    continue

        return responses
