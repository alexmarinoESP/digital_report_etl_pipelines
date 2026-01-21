"""
Mapp Newsletter API client.
Handles communication with Mapp Newsletter API for HTML preview retrieval.
"""

from typing import Optional, Dict, Any

import numpy as np
import requests
from requests import Response
from requests.auth import HTTPBasicAuth
from loguru import logger

from newsletter import Endpoint
from shared.utils.env import get_env_or_raise, get_env


class RequestException(Exception):
    """Exception for HTTP request errors."""

    def __init__(self, msg: str):
        self.msg = msg

    def __repr__(self) -> str:
        return self.msg


class MappConnector:
    """
    Connector for Mapp Newsletter API.
    Retrieves HTML content from newsletter messages.
    """

    def __init__(
        self,
        comp_flag: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        version: Optional[str] = None,
    ):
        """
        Initialize Mapp connector.

        Args:
            comp_flag: Company flag (it, es, vvit)
            user: API username (from env if not provided)
            password: API password (from env if not provided)
            version: API version (from env if not provided)
        """
        self.comp_flag = np.where(comp_flag == "es", "es", "it")
        self.user = user or get_env_or_raise("MAPP_USER")
        self.password = password or get_env_or_raise("MAPP_PASSWORD")
        self.version = version or get_env("MAPP_VERSION", "v19")

        self.auth = HTTPBasicAuth(self.user, self.password)
        self.base_url = f"https://newsletter{self.comp_flag}.esprinet.com/api/rest/"
        self.session = requests.Session()
        self._timeout = 120
        self.proxies = []

    def _parse_response(self, response: Response) -> Optional[Dict]:
        """
        Parse API response.

        Args:
            response: Response from API

        Returns:
            JSON data or None
        """
        content_type = response.headers.get("Content-Type", "")
        if "json" in content_type:
            return response.json()
        return None

    def _request(
        self,
        url: str,
        args: Optional[Dict] = None,
        post_args: Optional[Dict] = None,
        files: Optional[Dict] = None,
        verb: str = "GET",
        **kwargs,
    ) -> Response:
        """
        Make an HTTP request to the API.

        Args:
            url: Resource URL
            args: Query parameters
            post_args: Form parameters
            files: Files for multipart upload
            verb: HTTP method
            **kwargs: Additional parameters

        Returns:
            HTTP Response
        """
        if not url.startswith("http"):
            url = self.base_url + url

        try:
            response = self.session.request(
                method=verb,
                url=url,
                timeout=self._timeout,
                params=args,
                data=post_args,
                files=files,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                proxies=self.proxies,
                **kwargs,
            )
        except requests.HTTPError as ex:
            raise RequestException(str(ex.args))

        return response

    def get(self, path: str, args: Dict) -> Response:
        """
        Send GET request.

        Args:
            path: Resource path
            args: Query arguments

        Returns:
            API response
        """
        return self._request(
            url=f"{self.version}/{path}",
            auth=self.auth,
            args=args,
        )

    def get_preview_html(
        self, message_id: int, contact_id: int
    ) -> Dict[str, Any]:
        """
        Get HTML preview for a newsletter message.

        Args:
            message_id: Newsletter message ID
            contact_id: Contact ID for preview

        Returns:
            Dictionary with HTML content
        """
        args = {"messageId": message_id, "contactId": contact_id}
        url = Endpoint.preview.value

        resp = self.get(path=url, args=args)
        data = self._parse_response(resp)

        return data

    def get_preview_name(
        self,
        html_str: Dict,
        id_nl: str,
        extension: str = "png",
    ) -> str:
        """
        Get preview image name from response.

        Args:
            html_str: HTML response dictionary
            id_nl: Newsletter ID fallback
            extension: Image extension

        Returns:
            Preview image filename
        """
        name = f"{html_str.get('externalId', None)}.{extension}"

        if name == f"None.{extension}":
            name = f"{id_nl}.{extension}"

        return name
