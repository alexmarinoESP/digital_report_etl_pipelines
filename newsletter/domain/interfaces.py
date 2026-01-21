"""
Interfaces (Abstract Base Classes) for newsletter module.
Defines contracts for dependency injection and testability.

Following Interface Segregation Principle (ISP):
- Small, focused interfaces
- Clients depend only on what they use
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any

from PIL import Image

from newsletter.domain.models import Newsletter, Company


# =============================================================================
# Domain Exceptions
# =============================================================================

class NewsletterError(Exception):
    """Base exception for newsletter module."""
    pass


class ExtractionError(NewsletterError):
    """Exception raised during newsletter extraction."""
    pass


class RenderingError(NewsletterError):
    """Exception raised during HTML rendering."""
    pass


class UploadError(NewsletterError):
    """Exception raised during image upload."""
    pass


class StorageError(NewsletterError):
    """Exception raised during storage operations."""
    pass


class INewsletterRepository(ABC):
    """
    Interface for newsletter data retrieval.
    Abstracts the data source (Vertica, mock, etc).
    """

    @abstractmethod
    def get_mapp_newsletters(
        self, company: Company, years_behind: int
    ) -> List[Newsletter]:
        """
        Retrieve Mapp newsletters for a company.

        Args:
            company: Company to retrieve newsletters for
            years_behind: Number of years to look back

        Returns:
            List of Newsletter objects
        """
        pass

    @abstractmethod
    def get_dynamics_newsletters(
        self, company: Company, years_behind: int
    ) -> List[Newsletter]:
        """
        Retrieve Dynamics newsletters for a company.

        Args:
            company: Company to retrieve newsletters for
            years_behind: Number of years to look back

        Returns:
            List of Newsletter objects
        """
        pass


class IMappClient(ABC):
    """
    Interface for Mapp API communication.
    Retrieves HTML content from Mapp Newsletter API.
    """

    @abstractmethod
    def get_html_content(self, message_id: int, contact_id: int) -> Optional[str]:
        """
        Get HTML content for a newsletter.

        Args:
            message_id: Mapp message ID
            contact_id: Mapp contact ID

        Returns:
            HTML string or None if not found
        """
        pass

    @abstractmethod
    def get_external_id(self, message_id: int, contact_id: int) -> Optional[str]:
        """
        Get external ID for naming the image.

        Args:
            message_id: Mapp message ID
            contact_id: Mapp contact ID

        Returns:
            External ID string or None
        """
        pass

    @abstractmethod
    def get_preview_data(
        self, message_id: int, contact_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get full preview data including HTML and external ID.
        More efficient than calling get_html_content and get_external_id separately.

        Args:
            message_id: Mapp message ID
            contact_id: Mapp contact ID

        Returns:
            Dictionary with 'htmlVersion' and 'externalId' keys, or None
        """
        pass


class IHtmlRenderer(ABC):
    """
    Interface for HTML to image rendering.
    Abstracts the rendering service (hcti.io, local, etc).
    """

    @abstractmethod
    def render(self, html: str) -> Image.Image:
        """
        Render HTML string to image.

        Args:
            html: HTML content to render

        Returns:
            PIL Image object

        Raises:
            RenderingError: If rendering fails
        """
        pass


class IImageProcessor(ABC):
    """
    Interface for image post-processing.
    Handles cropping, resizing, and other transformations.
    """

    @abstractmethod
    def process(self, image: Image.Image) -> Image.Image:
        """
        Process an image (crop, resize, etc).

        Args:
            image: Input PIL Image

        Returns:
            Processed PIL Image
        """
        pass


class IImageStorage(ABC):
    """
    Interface for image storage.
    Abstracts storage backend (S3, local filesystem, etc).
    """

    @abstractmethod
    def exists(self, image_name: str) -> bool:
        """
        Check if an image already exists in storage.

        Args:
            image_name: Name of the image file

        Returns:
            True if exists, False otherwise
        """
        pass

    @abstractmethod
    def upload(self, image: Image.Image, image_name: str) -> bool:
        """
        Upload an image to storage.

        Args:
            image: PIL Image to upload
            image_name: Target filename

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    def list_existing(self, folder: str = "") -> List[str]:
        """
        List existing images in storage.

        Args:
            folder: Optional folder path

        Returns:
            List of image names
        """
        pass


class IHtmlProcessor(ABC):
    """
    Interface for HTML content processing.
    Handles cleaning, placeholder removal, etc.
    """

    @abstractmethod
    def clean(self, html: str, company: Company) -> str:
        """
        Clean HTML content.

        Args:
            html: Raw HTML string
            company: Company for company-specific processing

        Returns:
            Cleaned HTML string
        """
        pass
