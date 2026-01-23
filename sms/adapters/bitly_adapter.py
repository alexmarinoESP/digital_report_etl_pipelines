"""
Bitly API Adapter.

Implements IBitlyClient interface for Bitly API interactions.
Handles authentication, error handling, and data transformation.

API token is loaded from environment variables for security.
"""

from dataclasses import replace
from typing import Optional

import requests
from loguru import logger

from sms.domain.interfaces import IBitlyClient, APIError
from sms.domain.models import BitlyLink
from sms.config import BITLY_TOKEN


class BitlyAdapter(IBitlyClient):
    """
    Adapter for Bitly API operations.

    Implements IBitlyClient interface using Bitly REST API v4.
    Handles authentication with Bearer token and error handling.

    Attributes:
        token: Bitly API access token
        base_url: Bitly API base URL
        timeout: Request timeout in seconds
    """

    BASE_URL = "https://api-ssl.bitly.com/v4"

    def __init__(
        self,
        token: Optional[str] = None,
        timeout: int = 30,
    ):
        """
        Initialize Bitly adapter.

        Token is loaded from environment variables by default.
        Can be overridden for testing purposes.

        Args:
            token: Bitly API access token (default: from BITLY_TOKEN env var)
            timeout: Request timeout in seconds

        Raises:
            ValueError: If token not available
        """
        # Load token from env var if not provided
        self.token = token or BITLY_TOKEN
        self.base_url = self.BASE_URL
        self.timeout = timeout
        self._headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def get_link_info(self, bitlink_id: str) -> dict:
        """
        Get Bitly link information.

        Calls /bitlinks/{bitlink_id} endpoint.

        Args:
            bitlink_id: Bitly link ID (e.g., 'bit.ly/abc123')

        Returns:
            Dictionary with link info:
                - short_url: Shortened URL
                - long_url: Original long URL
                - title: Optional link title

        Raises:
            APIError: If Bitly API call fails
        """
        endpoint = f"{self.base_url}/bitlinks/{bitlink_id}"

        try:
            logger.debug(f"Fetching Bitly link info for {bitlink_id}")
            response = requests.get(
                endpoint,
                headers=self._headers,
                timeout=self.timeout,
            )

            if response.status_code == 404:
                raise APIError(f"Bitly link not found: {bitlink_id}")
            elif response.status_code != 200:
                raise APIError(
                    f"Bitly API error: {response.status_code} - {response.text[:200]}"
                )

            data = response.json()

            result = {
                "short_url": data.get("link", f"https://{bitlink_id}"),
                "long_url": data.get("long_url"),
                "title": data.get("title"),
            }

            logger.debug(f"Link info retrieved: {result['short_url']}")
            return result

        except requests.RequestException as e:
            raise APIError(f"Bitly API request failed: {str(e)}") from e
        except (KeyError, ValueError) as e:
            raise APIError(f"Failed to parse Bitly response: {str(e)}") from e

    def get_total_clicks(self, bitlink_id: str) -> int:
        """
        Get total clicks for a Bitly link (all time).

        Calls /bitlinks/{bitlink_id}/clicks/summary with units=-1.

        Args:
            bitlink_id: Bitly link ID (e.g., 'bit.ly/abc123')

        Returns:
            Total number of clicks

        Raises:
            APIError: If Bitly API call fails
        """
        endpoint = f"{self.base_url}/bitlinks/{bitlink_id}/clicks/summary"
        params = {
            "unit": "month",  # Required with units=-1
            "units": -1,      # -1 = all time
        }

        try:
            logger.debug(f"Fetching total clicks for {bitlink_id}")
            response = requests.get(
                endpoint,
                params=params,
                headers=self._headers,
                timeout=self.timeout,
            )

            if response.status_code == 404:
                raise APIError(f"Bitly link not found: {bitlink_id}")
            elif response.status_code != 200:
                raise APIError(
                    f"Bitly API error: {response.status_code} - {response.text[:200]}"
                )

            data = response.json()
            total_clicks = data.get("total_clicks", 0)

            logger.debug(f"Total clicks retrieved: {total_clicks}")
            return total_clicks

        except requests.RequestException as e:
            raise APIError(f"Bitly API request failed: {str(e)}") from e
        except (KeyError, ValueError) as e:
            raise APIError(f"Failed to parse Bitly response: {str(e)}") from e

    def enrich_link(self, link: BitlyLink) -> BitlyLink:
        """
        Enrich a BitlyLink with data from Bitly API.

        Fetches both link info and click statistics, then creates
        a new BitlyLink with updated data.

        Args:
            link: BitlyLink to enrich

        Returns:
            New BitlyLink with enriched data

        Raises:
            APIError: If Bitly API calls fail
        """
        bitlink_id = link.bitlink_id

        try:
            # Fetch link info and clicks in parallel would be ideal,
            # but for simplicity we do sequential calls
            link_info = self.get_link_info(bitlink_id)
            total_clicks = self.get_total_clicks(bitlink_id)

            # Create new link with enriched data
            enriched_link = replace(
                link,
                bitly_long_url=link_info.get("long_url"),
                total_clicks=total_clicks,
            )

            logger.info(
                f"Enriched link {link.bitly_short_url}: "
                f"{total_clicks} clicks, long_url={link_info.get('long_url')}"
            )

            return enriched_link

        except APIError as e:
            logger.error(f"Failed to enrich link {link.bitly_short_url}: {e}")
            raise

    def __repr__(self) -> str:
        """String representation of adapter."""
        return f"BitlyAdapter(base_url={self.base_url})"
