"""
Mapp API adapter.
Implements IMappClient interface for Mapp Newsletter API communication.
"""

from typing import Optional, Dict, Any

import requests
from requests import Response
from requests.auth import HTTPBasicAuth
from loguru import logger

from newsletter.domain.interfaces import IMappClient
from newsletter.domain.models import Company
from shared.utils.env import get_env_or_raise, get_env


class MappApiError(Exception):
    """Exception for Mapp API errors."""
    pass


class RecipientNotFoundError(MappApiError):
    """Exception raised when Mapp recipient/contact does not exist."""
    pass


class MappAdapter(IMappClient):
    """
    Adapter for Mapp Newsletter API.
    Implements IMappClient interface.

    Follows:
    - Single Responsibility: Only handles Mapp API communication
    - Dependency Inversion: Implements abstract interface
    - Open/Closed: Can be extended without modification
    """

    ENDPOINT_PREVIEW = "message/getHistorical"

    def __init__(
        self,
        company: Company,
        user: Optional[str] = None,
        password: Optional[str] = None,
        version: Optional[str] = None,
        timeout: int = 120,
    ):
        """
        Initialize Mapp adapter.

        Args:
            company: Company for API endpoint selection
            user: API username (from env if not provided)
            password: API password (from env if not provided)
            version: API version (from env if not provided)
            timeout: Request timeout in seconds
        """
        self._company = company
        self._user = user or get_env_or_raise("MAPP_USER")
        self._password = password or get_env_or_raise("MAPP_PASSWORD")
        self._version = version or get_env("MAPP_VERSION", "v19")
        self._timeout = timeout

        self._auth = HTTPBasicAuth(self._user, self._password)
        self._base_url = self._build_base_url(company)
        self._session = requests.Session()

    def _build_base_url(self, company: Company) -> str:
        """Build API base URL based on company."""
        return f"https://newsletter{company.api_region}.esprinet.com/api/rest/"

    def _request(
        self,
        path: str,
        params: Optional[Dict] = None,
        method: str = "GET",
    ) -> Optional[Dict]:
        """
        Make HTTP request to Mapp API.

        Args:
            path: API path
            params: Query parameters
            method: HTTP method

        Returns:
            JSON response or None

        Raises:
            MappApiError: If request fails
        """
        url = f"{self._base_url}{self._version}/{path}"

        try:
            response = self._session.request(
                method=method,
                url=url,
                timeout=self._timeout,
                params=params,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                auth=self._auth,
            )

            # Check for "recipient not found" before raise_for_status
            if response.status_code == 400:
                try:
                    error_data = response.json()
                    if "does not exist" in error_data.get("message", ""):
                        raise RecipientNotFoundError(
                            f"Recipient not found: {error_data.get('message')}"
                        )
                except (ValueError, KeyError):
                    pass

            response.raise_for_status()

            if "json" in response.headers.get("Content-Type", ""):
                return response.json()
            return None

        except RecipientNotFoundError:
            raise
        except requests.RequestException as e:
            logger.error(f"Mapp API request failed: {e}")
            raise MappApiError(f"Request failed: {e}") from e

    def get_html_content(self, message_id: int, contact_id: int) -> Optional[str]:
        """
        Get HTML content for a newsletter.

        Args:
            message_id: Mapp message ID
            contact_id: Mapp contact ID

        Returns:
            HTML string or None if not found
        """
        try:
            params = {"messageId": message_id, "contactId": contact_id}
            data = self._request(self.ENDPOINT_PREVIEW, params)

            if data:
                return data.get("htmlVersion", "")
            return None

        except MappApiError:
            return None

    def get_external_id(self, message_id: int, contact_id: int) -> Optional[str]:
        """
        Get external ID for naming the image.

        Args:
            message_id: Mapp message ID
            contact_id: Mapp contact ID

        Returns:
            External ID string or None
        """
        try:
            params = {"messageId": message_id, "contactId": contact_id}
            data = self._request(self.ENDPOINT_PREVIEW, params)

            if data:
                return data.get("externalId")
            return None

        except MappApiError:
            return None

    def get_preview_data(
        self, message_id: int, contact_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get full preview data including HTML and external ID.

        Args:
            message_id: Mapp message ID
            contact_id: Mapp contact ID

        Returns:
            Dictionary with htmlVersion and externalId, or None

        Raises:
            RecipientNotFoundError: If the contact does not exist in Mapp
        """
        try:
            params = {"messageId": message_id, "contactId": contact_id}
            return self._request(self.ENDPOINT_PREVIEW, params)
        except RecipientNotFoundError:
            raise
        except MappApiError:
            return None
