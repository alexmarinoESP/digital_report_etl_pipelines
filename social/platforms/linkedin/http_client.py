"""LinkedIn-specific HTTP client with NoQuotedCommasSession.

This module provides the HTTP client for LinkedIn Marketing API v202601,
handling the special parameter encoding requirements unique to LinkedIn.

Key Features:
- NoQuotedCommasSession: Custom requests.Session that prevents URL encoding of commas
- Version header injection (LinkedIn-Version: 202601)
- URN formatting utilities
- Date range formatting for insights API

LinkedIn API Specifics:
- Certain parameters must NOT be URL-encoded (campaigns, dateRange, fields)
- API requires LinkedIn-Version header
- URN-based resource identification
"""

from typing import Dict, Any, Optional, List
from loguru import logger
import urllib.parse

from social.infrastructure.no_quoted_commas_session import NoQuotedCommasSession
from social.core.constants import LINKEDIN_API_VERSION


class LinkedInHTTPClient:
    """HTTP client for LinkedIn Marketing API v202601.

    This client handles LinkedIn-specific requirements:
    - NoQuotedCommasSession for special parameter encoding
    - Version header injection
    - URN formatting utilities
    - Date range formatting

    Unlike other platforms, this client is completely independent and does not
    inherit from any base class.
    """

    def __init__(self, access_token: str, timeout: int = 30):
        """Initialize LinkedIn HTTP client.

        Args:
            access_token: OAuth2 access token for authentication
            timeout: Request timeout in seconds (default: 30)
        """
        self.access_token = access_token
        self.timeout = timeout
        self._session = NoQuotedCommasSession()

        logger.debug(f"LinkedInHTTPClient initialized with API version {LINKEDIN_API_VERSION}")

    def _build_headers(self, additional_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Build headers with LinkedIn version requirement.

        Args:
            additional_headers: Optional headers to merge

        Returns:
            Complete headers dictionary including LinkedIn-Version
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "LinkedIn-Version": LINKEDIN_API_VERSION,
            "X-Restli-Protocol-Version": "2.0.0",
            "x-li-format": "json",
            "Content-Type": "application/json",
        }

        if additional_headers:
            headers.update(additional_headers)

        return headers

    def get(
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
            requests.HTTPError: If request fails
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
            # For 404s (resource not found), log as debug instead of error
            # This is expected when searching for creatives across multiple accounts
            if "404" in str(e):
                logger.debug(f"LinkedIn API request returned 404: {e}")
            else:
                logger.error(f"LinkedIn API request failed: {e}")
            raise

    def close(self):
        """Close the HTTP session."""
        if self._session:
            self._session.close()

    # ============================================================================
    # URN and Parameter Formatting Utilities
    # ============================================================================

    @staticmethod
    def format_campaign_urns_for_insights(campaign_ids: List[str]) -> str:
        """Format campaign IDs into LinkedIn's required URN format for insights.

        Insights API requires: campaigns=List(urn%3Ali%3AsponsoredCampaign%3A123)
        Note: The URN must be URL-encoded before wrapping in List()

        Args:
            campaign_ids: List of campaign IDs (strings or ints)

        Returns:
            Formatted URN string with List() wrapper and URL-encoded URN

        Example:
            >>> format_campaign_urns_for_insights(['123'])
            'List(urn%3Ali%3AsponsoredCampaign%3A123)'
        """
        if not campaign_ids:
            return ""

        # For insights, we typically query one campaign at a time
        campaign_id = str(campaign_ids[0]) if isinstance(campaign_ids, list) else str(campaign_ids)

        # Convert to int/float to handle decimal strings, then back to int
        try:
            campaign_id = str(round(float(campaign_id)))
        except (ValueError, TypeError):
            pass  # Keep as string if conversion fails

        # Build and URL-encode the URN (LinkedIn API requirement)
        urn = f"urn:li:sponsoredCampaign:{campaign_id}"
        encoded_urn = urllib.parse.quote(urn)

        return f"List({encoded_urn})"

    @staticmethod
    def format_date_range(
        start_year: int,
        start_month: int,
        start_day: int,
        end_year: Optional[int] = None,
        end_month: Optional[int] = None,
        end_day: Optional[int] = None
    ) -> str:
        """Format date range into LinkedIn's required format.

        LinkedIn Insights API requires only start date: (start:(year:2024,month:1,day:1))
        The API will automatically return data from the start date to today.

        Args:
            start_year: Start year (e.g., 2024)
            start_month: Start month (1-12)
            start_day: Start day (1-31)
            end_year: End year (ignored - kept for backward compatibility)
            end_month: End month (ignored - kept for backward compatibility)
            end_day: End day (ignored - kept for backward compatibility)

        Returns:
            Formatted date range string with only start date

        Example:
            >>> format_date_range(2024, 1, 15)
            '(start:(year:2024,month:1,day:15))'
        """
        return f"(start:(year:{start_year},month:{start_month},day:{start_day}))"

    @staticmethod
    def format_fields(fields: List[str]) -> str:
        """Format fields list into comma-separated string.

        Args:
            fields: List of field names

        Returns:
            Comma-separated string (no spaces)

        Example:
            >>> format_fields(['id', 'name', 'status'])
            'id,name,status'
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

        Example:
            >>> format_account_urn('12345')
            'urn:li:sponsoredAccount:12345'
        """
        return f"urn:li:sponsoredAccount:{account_id}"

    @staticmethod
    def format_account_urn_for_audiences(account_id: str) -> str:
        """Format account ID into List(encoded URN) for audiences endpoint.

        This MUST match the old working code format exactly.
        The URN is URL-encoded, then wrapped in List().

        Args:
            account_id: Account ID

        Returns:
            Formatted string: List(urn%3Ali%3AsponsoredAccount%3A123)

        Example:
            >>> format_account_urn_for_audiences('12345')
            'List(urn%3Ali%3AsponsoredAccount%3A12345)'
        """
        urn = f"urn:li:sponsoredAccount:{account_id}"
        encoded_urn = urllib.parse.quote(urn)
        return f"List({encoded_urn})"

    @staticmethod
    def format_creative_urn_encoded(creative_id: str) -> str:
        """Format creative ID into URL-encoded URN for path parameter.

        Args:
            creative_id: Creative ID

        Returns:
            URL-encoded URN: urn%3Ali%3AsponsoredCreative%3A123

        Example:
            >>> format_creative_urn_encoded('12345')
            'urn%3Ali%3AsponsoredCreative%3A12345'
        """
        return f"urn%3Ali%3AsponsoredCreative%3A{creative_id}"
