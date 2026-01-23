"""LinkedIn Ads Adapter Module.

This module provides a completely independent adapter for LinkedIn Ads API v202509.
It follows SOLID principles with no base class inheritance, using only protocol contracts.

Key Features:
- Independent implementation (no base classes)
- Protocol compliance (TokenProvider, DataSink)
- Complete type hints and docstrings
- URN-based resource identification
- Dependency management (insights needs campaigns, creatives needs insights)

Architecture:
- LinkedInAdapter: Main adapter class
- LinkedInHTTPClient: HTTP communication layer
- Protocol-based dependency injection
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger

from social.core.exceptions import APIError, ConfigurationError
from social.core.protocols import DataSink, TokenProvider
from social.platforms.linkedin.http_client import LinkedInHTTPClient


class LinkedInAdapter:
    """Independent adapter for LinkedIn Marketing API v202509.

    This adapter provides methods for extracting data from LinkedIn Ads API
    without inheriting from any base class. It uses protocol-based contracts
    for flexibility and testability.

    Attributes:
        token_provider: Provider for OAuth2 access tokens
        http_client: LinkedIn-specific HTTP client
        data_sink: Optional data sink for database queries
    """

    def __init__(
        self,
        token_provider: TokenProvider,
        data_sink: Optional[DataSink] = None,
    ):
        """Initialize LinkedIn adapter.

        Args:
            token_provider: Provider for authentication tokens
            data_sink: Optional data sink for database queries
        """
        self.token_provider = token_provider
        self.data_sink = data_sink

        # Initialize HTTP client with access token
        access_token = token_provider.get_access_token()
        self.http_client = LinkedInHTTPClient(access_token=access_token)

        logger.info("LinkedInAdapter initialized")

    def get_campaigns(self, account_id: str) -> List[Dict[str, Any]]:
        """Get campaigns for a specific account.

        Args:
            account_id: LinkedIn account ID (numeric string)

        Returns:
            List of campaign dictionaries with metadata

        Raises:
            APIError: If API request fails
        """
        logger.info(f"Fetching campaigns for account {account_id}")

        try:
            url = f"https://api.linkedin.com/rest/adAccounts/{account_id}/adCampaigns"

            # LinkedIn API 202509 - MUST use exact format from old working code
            params = {
                "q": "search",
            }

            # CRITICAL: Use exact format from old working code
            # Format: search=(status:(values:List(ACTIVE,PAUSED,COMPLETED,ARCHIVED)))
            no_encoded_params = {
                "search": "(status:(values:List(ACTIVE,PAUSED,COMPLETED,ARCHIVED)))"
            }

            response = self.http_client.get(url=url, params=params, no_encoded_params=no_encoded_params)

            campaigns = response.get("elements", [])
            logger.success(f"Retrieved {len(campaigns)} campaigns for account {account_id}")
            return campaigns

        except Exception as e:
            logger.error(f"Failed to fetch campaigns for account {account_id}: {e}")
            raise APIError(
                f"Failed to fetch campaigns for account {account_id}",
                details={"account_id": account_id, "error": str(e)}
            )

    def get_insights(
        self,
        campaign_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Get insights (performance metrics) for a specific campaign.

        Args:
            campaign_id: Campaign ID (numeric string)
            start_date: Start date for metrics
            end_date: End date for metrics

        Returns:
            List of insight records with daily metrics

        Raises:
            APIError: If API request fails
        """
        logger.debug(f"Fetching insights for campaign {campaign_id}")

        try:
            url = "https://api.linkedin.com/rest/adAnalytics"

            # Non-encoded parameters (LinkedIn special format)
            campaign_urn_param = self.http_client.format_campaign_urns_for_insights([campaign_id])
            date_param = self.http_client.format_date_range(
                start_date.year,
                start_date.month,
                start_date.day,
                end_date.year,
                end_date.month,
                end_date.day
            )

            fields = [
                "actionClicks",
                "adUnitClicks",
                "clicks",
                "comments",
                "costInLocalCurrency",
                "landingPageClicks",
                "likes",
                "reactions",
                "shares",
                "totalEngagements",
                "dateRange",
                "pivotValues",
                "impressions",
                "externalWebsiteConversions",
                "conversionValueInLocalCurrency"
            ]
            fields_str = self.http_client.format_fields(fields)

            # Simple parameters that will be URL-encoded normally
            params = {
                "q": "analytics",
                "pivot": "CREATIVE",
                "timeGranularity": "DAILY",
            }

            # Complex parameters must NOT be URL-encoded
            no_encoded_params = {
                "campaigns": campaign_urn_param,
                "dateRange": date_param,
                "fields": fields_str,
            }

            response = self.http_client.get(
                url=url,
                params=params,
                no_encoded_params=no_encoded_params
            )

            insights = response.get("elements", [])
            logger.debug(f"Retrieved {len(insights)} insight records for campaign {campaign_id}")
            return insights

        except Exception as e:
            logger.error(f"Failed to fetch insights for campaign {campaign_id}: {e}")
            raise APIError(
                f"Failed to fetch insights for campaign {campaign_id}",
                details={"campaign_id": campaign_id, "error": str(e)}
            )

    def get_creatives(
        self,
        account_id: str,
        creative_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get creative details for a specific creative.

        Args:
            account_id: Account ID (numeric string)
            creative_id: Creative ID (numeric string)

        Returns:
            Creative dictionary or None if not found

        Raises:
            APIError: If API request fails (except 404)
        """
        logger.debug(f"Fetching creative {creative_id} from account {account_id}")

        try:
            # URL-encode the creative URN for path parameter
            creative_urn = self.http_client.format_creative_urn_encoded(creative_id)
            url = f"https://api.linkedin.com/rest/adAccounts/{account_id}/creatives/{creative_urn}"

            response = self.http_client.get(url=url, params={})

            logger.debug(f"Retrieved creative {creative_id}")
            return response

        except Exception as e:
            # 404 is expected when creative not in this account
            if "404" in str(e):
                logger.debug(f"Creative {creative_id} not found in account {account_id}")
                return None
            else:
                logger.error(f"Failed to fetch creative {creative_id}: {e}")
                raise APIError(
                    f"Failed to fetch creative {creative_id}",
                    details={"account_id": account_id, "creative_id": creative_id, "error": str(e)}
                )

    def get_audiences(self, account_id: str) -> List[Dict[str, Any]]:
        """Get audience segments for a specific account.

        Args:
            account_id: Account ID (numeric string)

        Returns:
            List of audience dictionaries

        Raises:
            APIError: If API request fails
        """
        logger.info(f"Fetching audiences for account {account_id}")

        try:
            url = "https://api.linkedin.com/rest/adSegments"

            # Create account URN parameter - MUST match old working code
            # Format: List(urn%3Ali%3AsponsoredAccount%3A{id})
            account_urn_encoded = self.http_client.format_account_urn_for_audiences(account_id)

            params = {
                "q": "accounts",
                "count": "400",
            }

            # CRITICAL: accounts parameter must NOT be URL-encoded (old working format)
            no_encoded_params = {
                "accounts": account_urn_encoded
            }

            response = self.http_client.get(url=url, params=params, no_encoded_params=no_encoded_params)

            audiences = response.get("elements", [])
            logger.success(f"Retrieved {len(audiences)} audiences for account {account_id}")
            return audiences

        except Exception as e:
            logger.error(f"Failed to fetch audiences for account {account_id}: {e}")
            raise APIError(
                f"Failed to fetch audiences for account {account_id}",
                details={"account_id": account_id, "error": str(e)}
            )

    def get_account(self, account_id: str) -> Optional[Dict[str, Any]]:
        """Get account details.

        Args:
            account_id: Account ID (numeric string)

        Returns:
            Account dictionary or None if not found

        Raises:
            APIError: If API request fails
        """
        logger.info(f"Fetching account details for {account_id}")

        try:
            url = "https://api.linkedin.com/rest/adAccounts"
            params = {
                "q": "search",
            }

            response = self.http_client.get(url=url, params=params)

            # Find the specific account in the results
            accounts = response.get("elements", [])
            for account in accounts:
                # Extract ID from URN or compare directly
                acc_id = str(account.get("id", ""))
                if ":" in acc_id:
                    acc_id = acc_id.split(":")[-1]

                if acc_id == str(account_id):
                    logger.success(f"Found account {account_id}")
                    return account

            logger.warning(f"Account {account_id} not found in response")
            return None

        except Exception as e:
            logger.error(f"Failed to fetch account {account_id}: {e}")
            raise APIError(
                f"Failed to fetch account {account_id}",
                details={"account_id": account_id, "error": str(e)}
            )

    def close(self) -> None:
        """Close the HTTP client session."""
        if self.http_client:
            self.http_client.close()
            logger.debug("LinkedInAdapter closed")
