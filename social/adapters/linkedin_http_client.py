"""LinkedIn-specific HTTP client with special parameter encoding.

LinkedIn API v202509 requires special handling for certain parameters:
- Some parameters must NOT be URL-encoded (campaigns, dateRange, fields)
- URN parameters have specific formats
- Certain filters require List() wrapper while others don't
"""

from typing import Dict, Any, Optional
import requests
from urllib.parse import urlencode
from loguru import logger

from social.adapters.http_client import AuthenticatedHTTPClient
from social.core.protocols import TokenProvider
from social.core.constants import LINKEDIN_API_VERSION


class NoQuotedCommasSession(requests.Session):
    """Custom session that prevents encoding of commas in certain parameters.

    LinkedIn API requires comma-separated values in some parameters
    (e.g., fields=field1,field2,field3) but requests library encodes commas.
    This session prevents that encoding for specific parameters.
    """

    def request(self, method, url, params=None, no_encoded_args=None, **kwargs):
        """Override request to handle no-encoding parameters.

        Args:
            method: HTTP method
            url: Request URL
            params: Regular URL-encoded parameters
            no_encoded_args: Parameters that should NOT be URL-encoded
            **kwargs: Additional request arguments

        Returns:
            Response object
        """
        # Start with base URL
        final_url = url

        # Add regular params (URL-encoded)
        if params:
            encoded_params = urlencode(params, safe='')
            separator = '&' if '?' in final_url else '?'
            final_url = f"{final_url}{separator}{encoded_params}"

        # Add non-encoded params (preserve special characters)
        if no_encoded_args:
            # Join with & but don't encode the values
            no_encoded_str = '&'.join(f"{k}={v}" for k, v in no_encoded_args.items())
            separator = '&' if '?' in final_url else '?'
            final_url = f"{final_url}{separator}{no_encoded_str}"

        return super().request(method, final_url, **kwargs)


class LinkedInHTTPClient(AuthenticatedHTTPClient):
    """HTTP client specifically for LinkedIn Marketing API v202509.

    This client handles LinkedIn-specific requirements:
    - Version header injection
    - Special parameter encoding for campaigns, dates, fields
    - URN formatting
    """

    def __init__(self, token_provider: TokenProvider, **kwargs):
        """Initialize LinkedIn HTTP client.

        Args:
            token_provider: Token provider for authentication
            **kwargs: Additional arguments for base client
        """
        super().__init__(token_provider, **kwargs)
        # Override session with LinkedIn-specific session
        self._session = NoQuotedCommasSession()

    def _build_headers(self, additional_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Build headers with LinkedIn version requirement.

        Args:
            additional_headers: Optional headers to merge

        Returns:
            Complete headers dictionary including LinkedIn-Version
        """
        headers = super()._build_headers(additional_headers)

        # LinkedIn API requires version header
        headers.update({
            "LinkedIn-Version": LINKEDIN_API_VERSION,
            "x-li-format": "json",
        })

        if additional_headers:
            headers.update(additional_headers)

        return headers

    def get_with_special_params(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        no_encoded_params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Execute GET request with LinkedIn's special parameter encoding.

        LinkedIn API requires certain parameters to NOT be URL-encoded:
        - campaigns: List(urn:li:sponsoredCampaign:123)
        - dateRange: (start:(year:2024,month:1,day:1))
        - fields: field1,field2,field3

        Args:
            url: Request URL
            params: Regular URL-encoded parameters
            no_encoded_params: Parameters that should NOT be URL-encoded
            headers: Additional headers

        Returns:
            Response data as dictionary

        Raises:
            APIError: If request fails
        """
        complete_headers = self._build_headers(headers)

        logger.debug(f"GET {url}")
        if params:
            logger.debug(f"Encoded params: {params}")
        if no_encoded_params:
            logger.debug(f"Non-encoded params: {no_encoded_params}")

        try:
            response = self._session.request(
                method="GET",
                url=url,
                params=params,
                no_encoded_args=no_encoded_params,
                headers=complete_headers,
                timeout=self.timeout,
            )

            response.raise_for_status()

            if response.content:
                return response.json()
            else:
                return {}

        except Exception as e:
            # Use parent class error handling
            logger.error(f"LinkedIn API request failed: {e}")
            raise

    @staticmethod
    def format_campaign_urns_for_insights(campaign_ids: list) -> str:
        """Format campaign IDs into LinkedIn's required URN format for insights.

        Insights API requires: campaigns=List(urn:li:sponsoredCampaign:123)

        Args:
            campaign_ids: List of campaign IDs (strings or ints)

        Returns:
            Formatted URN string with List() wrapper
        """
        if not campaign_ids:
            return ""

        # For insights, we typically query one campaign at a time
        campaign_id = str(campaign_ids[0]) if isinstance(campaign_ids, list) else str(campaign_ids)
        urn = f"urn:li:sponsoredCampaign:{campaign_id}"
        return f"List({urn})"

    @staticmethod
    def format_date_range(year: int, month: int, day: int) -> str:
        """Format date into LinkedIn's required format.

        LinkedIn requires: (start:(year:2024,month:1,day:1))

        Args:
            year: Year (e.g., 2024)
            month: Month (1-12)
            day: Day (1-31)

        Returns:
            Formatted date string
        """
        return f"(start:(year:{year},month:{month},day:{day}))"

    @staticmethod
    def format_fields(fields: list) -> str:
        """Format fields list into comma-separated string.

        Args:
            fields: List of field names

        Returns:
            Comma-separated string (no spaces)
        """
        if isinstance(fields, list):
            return ",".join(fields)
        return fields

    @staticmethod
    def format_account_urn(account_id: str) -> str:
        """Format account ID into URN.

        Args:
            account_id: Account ID

        Returns:
            Formatted URN: urn:li:sponsoredAccount:123
        """
        return f"urn:li:sponsoredAccount:{account_id}"

    @staticmethod
    def format_creative_urn_encoded(creative_id: str) -> str:
        """Format creative ID into URL-encoded URN for path parameter.

        Args:
            creative_id: Creative ID

        Returns:
            URL-encoded URN: urn%3Ali%3AsponsoredCreative%3A123
        """
        return f"urn%3Ali%3AsponsoredCreative%3A{creative_id}"
