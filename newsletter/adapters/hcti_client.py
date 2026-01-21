"""
HTML to Image API client.
Uses hcti.io service to render HTML as images.
"""

from typing import Optional, Dict

import requests
from PIL import Image
from io import BytesIO
from loguru import logger

from shared.utils.env import get_env_or_raise


class HctiClient:
    """
    Client for HTML CSS to Image API (hcti.io).
    Renders HTML content as PNG images.
    """

    def __init__(
        self,
        user_id: Optional[str] = None,
        api_key: Optional[str] = None,
        endpoint: Optional[str] = None,
    ):
        """
        Initialize HCTI client.

        Args:
            user_id: HCTI API user ID (from env if not provided)
            api_key: HCTI API key (from env if not provided)
            endpoint: API endpoint (from env if not provided)
        """
        self.user_id = user_id or get_env_or_raise("HCTI_API_USER_ID")
        self.api_key = api_key or get_env_or_raise("HCTI_API_KEY")
        self.endpoint = endpoint or get_env_or_raise("HCTI_API_ENDPOINT")

    def create_image_and_extract_url(self, html_str: str) -> str:
        """
        Create image from HTML and return the URL.

        Args:
            html_str: Raw HTML string

        Returns:
            URL of the created image
        """
        data = {
            "html": html_str,
            "css": ".box { color: white; background-color: #0f79b9; padding: 10px; font-family: Roboto }",
            "google_fonts": "Roboto",
        }

        response = requests.post(
            url=self.endpoint,
            data=data,
            auth=(self.user_id, self.api_key),
        )

        url_for_get = response.json()["url"]
        logger.info(f"URL creation img on service: {url_for_get}")

        return url_for_get

    def get_image_from_url(self, url_for_get: str) -> Image.Image:
        """
        Download image from URL.

        Args:
            url_for_get: URL of the image

        Returns:
            PIL Image object
        """
        response = requests.get(
            url=url_for_get,
            auth=(self.user_id, self.api_key),
        )
        img = Image.open(BytesIO(response.content))

        return img

    def render_html_to_image(self, html_str: str) -> Image.Image:
        """
        Render HTML to image.

        Args:
            html_str: Raw HTML string

        Returns:
            PIL Image object
        """
        url_for_get = self.create_image_and_extract_url(html_str)
        img = self.get_image_from_url(url_for_get)

        return img


class ApiRenderize:
    """Legacy wrapper for HCTI client (backward compatibility)."""

    def __init__(self, credentials_zapier: Dict[str, str]):
        """
        Initialize with credentials dictionary.

        Args:
            credentials_zapier: Dictionary with HCTI credentials
        """
        self.client = HctiClient(
            user_id=credentials_zapier.get("HCTI_API_USER_ID"),
            api_key=credentials_zapier.get("HCTI_API_KEY"),
            endpoint=credentials_zapier.get("HCTI_API_ENDPOINT"),
        )

    def create_image_and_extract_url(self, html_str: str) -> str:
        """Create image and return URL."""
        return self.client.create_image_and_extract_url(html_str)

    def get_image_from_url(self, url_for_get: str) -> Image.Image:
        """Get image from URL."""
        return self.client.get_image_from_url(url_for_get)

    def api_renderization(self, html_str: str) -> Image.Image:
        """Render HTML to image."""
        return self.client.render_html_to_image(html_str)
