"""
Facebook Ads API client.
Handles authentication and data retrieval from Facebook Ads API.
"""

import time
from datetime import datetime, timedelta
from typing import List, Dict, AnyStr, Optional

import pandas as pd
from facebook_business.api import FacebookResponse, FacebookSession, FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from loguru import logger

from social.platforms.facebook import dispatcher
from social.utils.commons import retrybackoffexp, log_df_dimension


class FBAdsServiceAccount:
    """
    Facebook Ads service for account-level operations.
    Handles data requests from Facebook Ads API.
    """

    def __init__(self, app_secret, app_id, access_token, id_account, **ignored):
        self.app_secret = app_secret
        self.app_id = app_id
        self.access_token = access_token
        self.account = id_account

    def _generate_date_chunks(
        self, start_date: datetime, end_date: datetime, chunk_days: int = 90
    ):
        """Generate date chunks for large date ranges."""
        chunks = []
        current_date = start_date

        while current_date < end_date:
            chunk_end = min(current_date + timedelta(days=chunk_days), end_date)
            chunks.append({
                "since": current_date.strftime("%Y-%m-%d"),
                "until": chunk_end.strftime("%Y-%m-%d"),
            })
            current_date = chunk_end + timedelta(days=1)

        return chunks

    @retrybackoffexp
    def request_data(self, cfg: Dict, **ignored) -> List[FacebookResponse]:
        """
        Request data from Facebook Ads API.

        Args:
            cfg: Configuration dict with type, fields, date_preset

        Returns:
            Facebook API response
        """
        fields = cfg.get("fields")
        request_type = cfg.get("type")
        date_preset = cfg.get("date_preset", "last_7d")

        # Handle 'maximum' date preset by chunking
        if date_preset == "maximum":
            logger.info("Using chunked date ranges for 'maximum' preset")

            end_date = datetime.now()
            start_date = end_date - timedelta(days=730)  # 2 years

            date_chunks = self._generate_date_chunks(start_date, end_date, chunk_days=90)
            responses = []

            for chunk in date_chunks:
                logger.info(f"Requesting: {chunk['since']} to {chunk['until']}")

                params = {
                    "time_range": chunk,
                    "level": "ad",
                    "action_attribution_windows": ["7d_click", "1d_view"],
                }

                response = getattr(self.account, request_type)(
                    fields=dispatcher[fields], params=params
                )

                if response is not None:
                    responses.append(response)
                    logger.info(f"Chunk received: {len(response) if hasattr(response, '__len__') else 'unknown'} items")

                time.sleep(60)  # Rate limiting

            return responses
        else:
            params = {
                "date_preset": date_preset,
                "level": "ad",
                "action_attribution_windows": ["7d_click", "1d_view"],
            }

            response = getattr(self.account, request_type)(
                fields=dispatcher[fields], params=params
            )

            time.sleep(60)
            return response

    @log_df_dimension
    @retrybackoffexp
    def convert_to_df(self, response) -> pd.DataFrame:
        """
        Convert Facebook response to DataFrame.

        Args:
            response: Facebook API response (single or list)

        Returns:
            DataFrame with response data
        """
        dfs = []
        if isinstance(response, list):
            for el in response:
                if isinstance(el, list):
                    for sub_el in el:
                        dfs.append(pd.DataFrame(sub_el))
                else:
                    dfs.append(pd.DataFrame(el))

            if dfs:
                return pd.concat(dfs, ignore_index=True)
            return pd.DataFrame()
        else:
            return pd.DataFrame(response)

    def convert_targeting_field(self, response: pd.DataFrame, **ignored) -> pd.DataFrame:
        """Convert targeting field to separate DataFrame."""
        if not response.empty:
            target = response.targeting.apply(lambda x: x._data)
            df_list = []

            for idx, t in enumerate(target):
                audiences = t.get("custom_audiences")
                audiences = pd.DataFrame(audiences)
                audiences["campaign_id"] = response.iloc[idx].campaign_id
                audiences["adset_id"] = response.iloc[idx].id
                audiences.rename(columns={"id": "audience_id"}, inplace=True)
                df_list.append(audiences)

            return pd.concat(df_list)
        return response


class FBAdsServiceBuilderAccount:
    """Builder for FBAdsServiceAccount instances."""

    def __init__(self):
        self._instance = None

    def __call__(
        self,
        id_account: AnyStr,
        app_secret: AnyStr,
        app_id: AnyStr,
        access_token: AnyStr,
        **ignored,
    ) -> FBAdsServiceAccount:
        """
        Create FBAdsServiceAccount instance.

        Args:
            id_account: Ad account ID
            app_secret: Facebook app secret
            app_id: Facebook app ID
            access_token: Access token

        Returns:
            Configured FBAdsServiceAccount
        """
        api = self._init_session(app_secret, app_id, access_token)
        account = self._set_account(id_account=id_account, api=api)
        self._instance = FBAdsServiceAccount(
            app_secret=app_secret,
            app_id=app_id,
            access_token=access_token,
            id_account=account,
        )
        return self._instance

    def _init_session(
        self, app_secret: AnyStr, app_id: AnyStr, access_token: AnyStr
    ) -> FacebookSession:
        """Initialize Facebook API session."""
        return FacebookAdsApi.init(
            app_id=app_id,
            app_secret=app_secret,
            access_token=access_token,
        )

    def _set_account(self, id_account: AnyStr, api: FacebookSession) -> AdAccount:
        """Set up AdAccount object."""
        return AdAccount(id_account, api=api)
