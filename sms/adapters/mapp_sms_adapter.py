"""
MAPP SMS API Adapter.

Implements IMappSMSClient interface for MAPP Engage API interactions.
Handles authentication, error handling, and data transformation.

Credentials are loaded from environment variables for security.

API Endpoints used:
- GET /message/getStatistics - Retrieves SMS delivery statistics
- GET /preparedmessage/get - Retrieves SMS message content including smsText
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

import requests
from loguru import logger
from requests.auth import HTTPBasicAuth

from sms.domain.interfaces import IMappSMSClient, APIError
from sms.domain.models import Company
from sms.config import MAPP_USERNAME, MAPP_PASSWORD


class MappSMSAdapter(IMappSMSClient):
    """
    Adapter for MAPP Engage API SMS operations.

    Implements IMappSMSClient interface using MAPP REST API v19.
    Handles authentication with HTTP Basic Auth and error handling.

    Following SOLID principles:
    - Single Responsibility: API communication and response transformation
    - Open/Closed: Extensible via inheritance, closed for modification
    - Liskov Substitution: Fully implements IMappSMSClient contract
    - Dependency Inversion: Depends on abstractions (interfaces)

    Attributes:
        company: Company this adapter is configured for
        base_url: MAPP API base URL
        timeout: Request timeout in seconds
    """

    # MAPP API statistic field mappings (API field -> our field)
    _STAT_FIELD_SENT = "majorSent"
    _STAT_FIELD_DELIVERED = "accepted"
    _STAT_FIELD_BOUNCED = "bounces"
    _STAT_FIELD_ACCEPTANCE_RATE = "acceptedRate"

    def __init__(
        self,
        company: Company,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 30,
    ):
        """
        Initialize MAPP SMS adapter.

        Credentials are loaded from environment variables by default.
        Can be overridden for testing purposes.

        Args:
            company: Company to connect to
            username: MAPP API username (default: from MAPP_USERNAME env var)
            password: MAPP API password (default: from MAPP_PASSWORD env var)
            timeout: Request timeout in seconds

        Raises:
            ValueError: If company not configured or credentials not available
        """
        self.company = company
        self.base_url = f"https://newsletter{company.api_region}.esprinet.com/api/rest/v19"
        self.timeout = timeout

        # Load credentials from env vars if not provided
        self._username = username or MAPP_USERNAME
        self._password = password or MAPP_PASSWORD
        self._auth = HTTPBasicAuth(self._username, self._password)
        self._headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    # =========================================================================
    # Public Interface Methods (IMappSMSClient implementation)
    # =========================================================================

    def get_sms_statistics(self, message_id: int) -> dict:
        """
        Get SMS campaign statistics from MAPP.

        Calls GET /message/getStatistics endpoint and transforms the response
        from MAPP's statisticValues array format to a normalized dictionary.

        Args:
            message_id: MAPP message ID

        Returns:
            Dictionary with statistics:
                - sent_count: Number of SMS sent (int)
                - delivered_count: Number delivered (int)
                - bounced_count: Number bounced (int)
                - sendout_date: Send date (datetime or None)
                - acceptance_rate: Delivery success percentage (Decimal or None)

        Raises:
            APIError: If MAPP API call fails
        """
        endpoint = f"{self.base_url}/message/getStatistics"
        params = {"messageId": message_id}

        try:
            logger.debug(f"Fetching SMS statistics for message {message_id}")

            response = self._make_request("GET", endpoint, params=params)
            data = response.json()

            # Parse statisticValues array into dict
            stats_dict = self._parse_statistic_values(data.get("statisticValues", []))

            # Extract and convert statistics
            sent_count = self._safe_int(stats_dict.get(self._STAT_FIELD_SENT))
            delivered_count = self._safe_int(stats_dict.get(self._STAT_FIELD_DELIVERED))
            bounced_count = self._safe_int(stats_dict.get(self._STAT_FIELD_BOUNCED))

            # Get acceptance rate from API or calculate if missing
            acceptance_rate = self._get_acceptance_rate(
                stats_dict=stats_dict,
                sent_count=sent_count,
                delivered_count=delivered_count,
            )

            # Parse sendout date from timestamp (milliseconds)
            sendout_date = self._parse_timestamp_ms(data.get("sendoutStartDate"))

            result = {
                "sent_count": sent_count,
                "delivered_count": delivered_count,
                "bounced_count": bounced_count,
                "sendout_date": sendout_date,
                "acceptance_rate": acceptance_rate,
            }

            logger.debug(f"Statistics retrieved for {message_id}: {result}")
            return result

        except requests.RequestException as e:
            raise APIError(f"MAPP API request failed: {str(e)}") from e
        except (KeyError, ValueError, TypeError) as e:
            raise APIError(f"Failed to parse MAPP statistics response: {str(e)}") from e

    def get_sms_content(self, message_id: int, contact_id: int = None) -> dict:
        """
        Get SMS message content from MAPP.

        Uses GET /preparedmessage/get endpoint which returns the full message
        object including the smsText field nested in the message object.

        Args:
            message_id: MAPP message ID
            contact_id: Contact ID (optional, not required for SMS content)

        Returns:
            Dictionary with content:
                - sms_text: SMS message text
                - campaign_name: Campaign name

        Raises:
            APIError: If MAPP API call fails or message not found
        """
        endpoint = f"{self.base_url}/preparedmessage/get"
        params = {"messageId": message_id}

        try:
            logger.debug(f"Fetching SMS content for message {message_id}")

            response = self._make_request("GET", endpoint, params=params)
            data = response.json()

            # Extract SMS text from nested message object
            sms_text = self._extract_sms_text(data)
            campaign_name = self._extract_campaign_name(data)

            if not sms_text:
                raise APIError(
                    f"No SMS text found for message {message_id}. "
                    "This may not be an SMS campaign."
                )

            result = {
                "sms_text": sms_text,
                "campaign_name": campaign_name,
            }

            logger.debug(f"SMS content retrieved: name={campaign_name}, text_len={len(sms_text)}")
            return result

        except requests.RequestException as e:
            raise APIError(f"MAPP API request failed: {str(e)}") from e
        except (KeyError, ValueError, TypeError) as e:
            raise APIError(f"Failed to parse MAPP content response: {str(e)}") from e

    # =========================================================================
    # Private Helper Methods (Single Responsibility Principle)
    # =========================================================================

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        json_data: Optional[dict] = None,
    ) -> requests.Response:
        """
        Make HTTP request to MAPP API with error handling.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: Full endpoint URL
            params: Query parameters
            json_data: JSON body data

        Returns:
            Response object

        Raises:
            APIError: If request fails or returns non-200 status
        """
        response = requests.request(
            method=method,
            url=endpoint,
            params=params,
            json=json_data,
            auth=self._auth,
            headers=self._headers,
            timeout=self.timeout,
        )

        if response.status_code != 200:
            error_text = response.text[:200] if response.text else "No response body"
            raise APIError(
                f"MAPP API error {response.status_code}: {error_text}"
            )

        return response

    def _parse_statistic_values(self, statistic_values: list) -> dict:
        """
        Parse MAPP statisticValues array into a dictionary.

        MAPP returns statistics as an array of {name, value} objects.
        This method converts it to a simple {name: value} dict for easy access.

        Args:
            statistic_values: List of {"name": str, "value": str} objects

        Returns:
            Dictionary mapping statistic names to their string values
        """
        if not statistic_values:
            return {}

        return {
            item.get("name"): item.get("value")
            for item in statistic_values
            if item.get("name") is not None
        }

    def _parse_timestamp_ms(self, timestamp_ms: Any) -> Optional[datetime]:
        """
        Parse timestamp in milliseconds to datetime.

        MAPP returns sendoutStartDate as Unix timestamp in milliseconds.

        Args:
            timestamp_ms: Timestamp in milliseconds (int or None)

        Returns:
            datetime object in UTC or None if parsing fails
        """
        if timestamp_ms is None:
            return None

        try:
            timestamp_sec = int(timestamp_ms) / 1000.0
            return datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
        except (ValueError, TypeError, OSError) as e:
            logger.warning(f"Could not parse timestamp {timestamp_ms}: {e}")
            return None

    def _safe_int(self, value: Any, default: int = 0) -> int:
        """
        Safely convert value to integer.

        MAPP returns numeric values as strings in statisticValues.

        Args:
            value: Value to convert (string, int, or None)
            default: Default value if conversion fails

        Returns:
            Integer value or default
        """
        if value is None:
            return default

        try:
            return int(float(value))
        except (ValueError, TypeError):
            logger.warning(f"Could not convert '{value}' to int, using default {default}")
            return default

    def _safe_decimal(self, value: Any, default: Optional[Decimal] = None) -> Optional[Decimal]:
        """
        Safely convert value to Decimal.

        Args:
            value: Value to convert (string, float, or None)
            default: Default value if conversion fails

        Returns:
            Decimal value or default
        """
        if value is None:
            return default

        try:
            # Handle "NaN" string from MAPP
            if isinstance(value, str) and value.lower() == "nan":
                return default
            return Decimal(str(value)).quantize(Decimal("0.01"))
        except (ValueError, TypeError):
            logger.warning(f"Could not convert '{value}' to Decimal, using default")
            return default

    def _get_acceptance_rate(
        self,
        stats_dict: dict,
        sent_count: int,
        delivered_count: int,
    ) -> Optional[Decimal]:
        """
        Get acceptance rate from API response or calculate it.

        Args:
            stats_dict: Parsed statistics dictionary
            sent_count: Number of SMS sent
            delivered_count: Number of SMS delivered

        Returns:
            Acceptance rate as Decimal percentage or None
        """
        # Try to get from API first
        api_rate = self._safe_decimal(stats_dict.get(self._STAT_FIELD_ACCEPTANCE_RATE))
        if api_rate is not None:
            return api_rate

        # Calculate if not provided
        if sent_count > 0:
            rate = (delivered_count / sent_count) * 100
            return Decimal(str(rate)).quantize(Decimal("0.01"))

        return None

    def _extract_sms_text(self, data: dict) -> Optional[str]:
        """
        Extract SMS text from preparedmessage/get response.

        The smsText field is nested inside the 'message' object.

        Args:
            data: API response data

        Returns:
            SMS text string or None
        """
        message_obj = data.get("message", {})
        if isinstance(message_obj, dict):
            return message_obj.get("smsText")
        return None

    def _extract_campaign_name(self, data: dict) -> Optional[str]:
        """
        Extract campaign name from preparedmessage/get response.

        Args:
            data: API response data

        Returns:
            Campaign name string or None
        """
        # Primary: 'name' field at top level
        name = data.get("name")
        if name:
            return name

        # Fallback: try nested message object
        message_obj = data.get("message", {})
        if isinstance(message_obj, dict):
            return message_obj.get("subject")

        return None

    def __repr__(self) -> str:
        """String representation of adapter."""
        return f"MappSMSAdapter(company={self.company.name}, url={self.base_url})"
