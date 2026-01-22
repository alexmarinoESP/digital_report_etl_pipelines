"""
HCTI (HTML CSS to Image) adapter.
Implements IHtmlRenderer interface for hcti.io service.
"""

from typing import Optional
from io import BytesIO

import requests
from PIL import Image
from loguru import logger

from newsletter.domain.interfaces import IHtmlRenderer, RenderingError
from shared.utils.env import get_env_or_raise


class HctiAdapter(IHtmlRenderer):
    """
    Adapter for HCTI (hcti.io) rendering service.
    Implements IHtmlRenderer interface.

    Follows:
    - Single Responsibility: Only handles HTML to image conversion
    - Dependency Inversion: Implements abstract interface
    - Open/Closed: New renderers can be added without modification
    """

    DEFAULT_CSS = (
        ".box { color: white; background-color: #0f79b9; "
        "padding: 10px; font-family: Roboto }"
    )
    DEFAULT_FONTS = "Roboto"

    def __init__(
        self,
        user_id: Optional[str] = None,
        api_key: Optional[str] = None,
        endpoint: Optional[str] = None,
        css: Optional[str] = None,
        google_fonts: Optional[str] = None,
        max_retries: int = 3,
    ):
        """
        Initialize HCTI adapter.

        Args:
            user_id: HCTI API user ID (from env if not provided)
            api_key: HCTI API key (from env if not provided)
            endpoint: API endpoint (from env if not provided)
            css: Custom CSS for rendering
            google_fonts: Google fonts to include
            max_retries: Maximum retry attempts on failure
        """
        self._user_id = user_id or get_env_or_raise("HCTI_API_USER_ID")
        self._api_key = api_key or get_env_or_raise("HCTI_API_KEY")
        self._endpoint = endpoint or get_env_or_raise("HCTI_API_ENDPOINT")
        self._css = css or self.DEFAULT_CSS
        self._google_fonts = google_fonts or self.DEFAULT_FONTS
        self._max_retries = max_retries

    def _create_image_url(self, html: str) -> str:
        """
        Create image from HTML and return URL.

        Args:
            html: HTML content

        Returns:
            URL of the created image

        Raises:
            RenderingError: If creation fails
        """
        data = {
            "html": html,
            "css": self._css,
            "google_fonts": self._google_fonts,
        }

        try:
            response = requests.post(
                url=self._endpoint,
                data=data,
                auth=(self._user_id, self._api_key),
                timeout=60,
            )
            response.raise_for_status()

            url = response.json().get("url")
            if not url:
                raise RenderingError("No URL in response")

            logger.debug(f"Created image URL: {url}")
            return url

        except requests.RequestException as e:
            raise RenderingError(f"Failed to create image: {e}") from e

    def _download_image(self, url: str) -> Image.Image:
        """
        Download image from URL.

        Args:
            url: Image URL

        Returns:
            PIL Image object

        Raises:
            RenderingError: If download fails
        """
        try:
            response = requests.get(
                url=url,
                auth=(self._user_id, self._api_key),
                timeout=60,
            )
            response.raise_for_status()

            return Image.open(BytesIO(response.content))

        except requests.RequestException as e:
            raise RenderingError(f"Failed to download image: {e}") from e

    def render(self, html: str) -> Image.Image:
        """
        Render HTML string to image with retry logic.

        Args:
            html: HTML content to render

        Returns:
            PIL Image object

        Raises:
            RenderingError: If rendering fails after all retries
        """
        last_error = None

        for attempt in range(1, self._max_retries + 1):
            try:
                logger.debug(f"Rendering attempt {attempt}/{self._max_retries}")
                url = self._create_image_url(html)
                image = self._download_image(url)
                return image

            except RenderingError as e:
                last_error = e
                logger.warning(f"Rendering attempt {attempt} failed: {e}")

                if attempt < self._max_retries:
                    continue

        raise RenderingError(
            f"Rendering failed after {self._max_retries} attempts: {last_error}"
        )
