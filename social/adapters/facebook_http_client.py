"""Facebook Ads HTTP client implementation.

This module provides a specialized HTTP client for Facebook Marketing API,
using the official Facebook Business SDK.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import time
import pandas as pd
from loguru import logger
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookResponse

from social.core.protocols import TokenProvider
from social.core.exceptions import APIError, AuthenticationError


class FacebookAdsHTTPClient:
    """HTTP client for Facebook Marketing API using official Facebook Business SDK.

    This client handles:
    - Authentication via Facebook App credentials and access token
    - AdAccount-level API requests
    - Response conversion to DataFrame
    - Rate limit handling
    - Error handling and retries
    """

    def __init__(
        self,
        token_provider: TokenProvider,
        app_id: str,
        app_secret: str,
        ad_account_ids: List[str]
    ):
        """Initialize Facebook Ads HTTP client.

        Args:
            token_provider: Provider for Facebook access tokens
            app_id: Facebook App ID
            app_secret: Facebook App Secret
            ad_account_ids: List of Ad Account IDs (format: "act_123456789")
        """
        self.token_provider = token_provider
        self.app_id = app_id
        self.app_secret = app_secret
        self.ad_account_ids = ad_account_ids

        # Initialize Facebook Ads API
        try:
            access_token = token_provider.get_access_token()
            self.api = FacebookAdsApi.init(
                app_id=app_id,
                app_secret=app_secret,
                access_token=access_token
            )
            logger.info("Facebook Ads API initialized successfully")
        except Exception as e:
            raise AuthenticationError(
                f"Failed to initialize Facebook Ads API: {str(e)}",
                details={
                    "app_id": app_id,
                    "ad_account_count": len(ad_account_ids)
                }
            )

    def get_ad_account(self, account_id: str) -> AdAccount:
        """Get AdAccount object for a specific account.

        Args:
            account_id: Ad Account ID (format: "act_123456789")

        Returns:
            AdAccount object

        Raises:
            APIError: If account retrieval fails
        """
        try:
            return AdAccount(account_id, api=self.api)
        except Exception as e:
            raise APIError(
                f"Failed to get AdAccount: {str(e)}",
                details={"account_id": account_id}
            )

    def request_data(
        self,
        account_id: str,
        method_name: str,
        fields: List[str],
        params: Optional[Dict[str, Any]] = None
    ) -> FacebookResponse:
        """Request data from Facebook Ads API for a specific account.

        Args:
            account_id: Ad Account ID
            method_name: API method to call (e.g., "get_campaigns", "get_insights")
            fields: List of fields to retrieve
            params: Optional parameters (date_preset, time_range, level, etc.)

        Returns:
            FacebookResponse object

        Raises:
            APIError: If API request fails
        """
        try:
            account = self.get_ad_account(account_id)

            # Get the method from account object
            if not hasattr(account, method_name):
                raise APIError(
                    f"Invalid method: {method_name}",
                    details={"account_id": account_id, "method": method_name}
                )

            method = getattr(account, method_name)

            # Set default params if not provided
            if params is None:
                params = {}

            # Execute API request
            logger.debug(f"Calling {method_name} for account {account_id}")
            response = method(fields=fields, params=params)

            # Add rate limit delay to avoid hitting limits
            time.sleep(60)  # Facebook rate limits are strict

            return response

        except Exception as e:
            raise APIError(
                f"Failed to request data from Facebook API: {str(e)}",
                details={
                    "account_id": account_id,
                    "method": method_name,
                    "fields_count": len(fields) if fields else 0
                }
            )

    def request_data_chunked(
        self,
        account_id: str,
        method_name: str,
        fields: List[str],
        start_date: datetime,
        end_date: datetime,
        chunk_days: int = 90
    ) -> List[FacebookResponse]:
        """Request data in chunks for large date ranges.

        Facebook API has data size limits, so large date ranges need to be
        split into smaller chunks.

        Args:
            account_id: Ad Account ID
            method_name: API method to call
            fields: List of fields to retrieve
            start_date: Start date for data
            end_date: End date for data
            chunk_days: Days per chunk (default: 90)

        Returns:
            List of FacebookResponse objects

        Raises:
            APIError: If any chunk request fails
        """
        chunks = self._generate_date_chunks(start_date, end_date, chunk_days)
        responses = []

        for chunk in chunks:
            logger.info(f"Requesting chunk: {chunk['since']} to {chunk['until']}")

            params = {
                "time_range": chunk,
                "level": "ad",
                "action_attribution_windows": ["7d_click", "1d_view"]
            }

            response = self.request_data(
                account_id=account_id,
                method_name=method_name,
                fields=fields,
                params=params
            )

            if response is not None:
                responses.append(response)
                logger.info(f"Chunk received with {len(response) if hasattr(response, '__len__') else 'unknown'} items")
            else:
                logger.warning(f"No response for chunk: {chunk['since']} to {chunk['until']}")

        logger.info(f"Total chunks collected: {len(responses)}")
        return responses

    def _generate_date_chunks(
        self,
        start_date: datetime,
        end_date: datetime,
        chunk_days: int = 90
    ) -> List[Dict[str, str]]:
        """Generate date chunks for large date ranges.

        Args:
            start_date: Start date
            end_date: End date
            chunk_days: Days per chunk

        Returns:
            List of time_range dictionaries
        """
        chunks = []
        current_date = start_date

        while current_date < end_date:
            chunk_end = min(current_date + timedelta(days=chunk_days), end_date)
            chunks.append({
                'since': current_date.strftime('%Y-%m-%d'),
                'until': chunk_end.strftime('%Y-%m-%d')
            })
            current_date = chunk_end + timedelta(days=1)

        return chunks

    def convert_to_df(self, response: Any) -> pd.DataFrame:
        """Convert Facebook API response to DataFrame.

        Args:
            response: FacebookResponse or list of FacebookResponse objects

        Returns:
            DataFrame with response data
        """
        dfs = []

        if isinstance(response, list):
            # Handle list of responses (from chunked requests)
            for el in response:
                if isinstance(el, list):
                    # Nested list
                    for sub_el in el:
                        dfs.append(pd.DataFrame(sub_el))
                else:
                    dfs.append(pd.DataFrame(el))

            if dfs:
                return pd.concat(dfs, ignore_index=True)
            else:
                return pd.DataFrame()
        else:
            # Single response
            return pd.DataFrame(response)

    def convert_targeting_field(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert targeting field to separate DataFrame.

        The targeting field contains custom_audiences information that needs
        to be extracted and normalized.

        Args:
            df: DataFrame with targeting field

        Returns:
            DataFrame with audience data
        """
        if df.empty or 'targeting' not in df.columns:
            return pd.DataFrame()

        df_list = []

        for idx, row in df.iterrows():
            if hasattr(row.targeting, '_data'):
                targeting_data = row.targeting._data
                audiences = targeting_data.get("custom_audiences", [])

                if audiences:
                    audiences_df = pd.DataFrame(audiences)
                    audiences_df["campaign_id"] = row.get("campaign_id")
                    audiences_df["adset_id"] = row.get("id")
                    audiences_df.rename(columns={"id": "audience_id"}, inplace=True)
                    df_list.append(audiences_df)

        if df_list:
            return pd.concat(df_list, ignore_index=True)
        else:
            return pd.DataFrame()

    def check_rate_limit(self, response: FacebookResponse) -> int:
        """Check CPU usage from response headers to avoid rate limiting.

        Args:
            response: FacebookResponse object

        Returns:
            CPU usage percentage
        """
        try:
            if hasattr(response, '_headers'):
                headers = response._headers
                usage = headers.get('x-business-use-case-usage', '{}')
                # Parse usage JSON
                import json
                usage_data = json.loads(usage)
                # Get total CPU usage
                for key, value in usage_data.items():
                    return value.get('total_cputime', 0)
            return 0
        except Exception as e:
            logger.warning(f"Failed to check rate limit: {str(e)}")
            return 0
