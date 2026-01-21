"""
Newsletter upload service.
Handles uploading rendered images to storage.
"""

from typing import List, Tuple

from loguru import logger

from newsletter.domain.models import NewsletterImage, PipelineStats
from newsletter.domain.interfaces import IImageStorage, UploadError


class UploadService:
    """
    Service for uploading newsletter images to storage.

    Follows:
    - Single Responsibility: Only handles uploads
    - Dependency Inversion: Depends on IImageStorage abstraction
    - Open/Closed: New storage backends can be injected
    """

    def __init__(self, storage: IImageStorage):
        """
        Initialize upload service.

        Args:
            storage: Image storage backend
        """
        self._storage = storage

    def upload_image(self, image: NewsletterImage) -> bool:
        """
        Upload a single image to storage.

        Args:
            image: NewsletterImage to upload

        Returns:
            True if successful, False otherwise
        """
        if not image.has_image:
            logger.warning(f"No image data for {image.newsletter_id}")
            return False

        if not image.image_name:
            logger.warning(f"No image name for {image.newsletter_id}")
            return False

        return self._storage.upload(image.image, image.image_name)

    def upload_images(
        self,
        images: List[NewsletterImage],
        skip_existing: bool = True,
    ) -> Tuple[int, PipelineStats]:
        """
        Upload multiple images to storage.

        Args:
            images: List of NewsletterImage objects
            skip_existing: Whether to skip images already in storage

        Returns:
            Tuple of (uploaded_count, stats)
        """
        stats = PipelineStats()

        for image in images:
            # Check if should upload
            if skip_existing and self._storage.exists(image.image_name):
                logger.debug(f"Skipping existing: {image.image_name}")
                stats.add_skipped()
                continue

            # Upload
            logger.info(f"Uploading: {image.image_name}")
            success = self.upload_image(image)

            if success:
                stats.add_processed()
            else:
                stats.add_failed(f"Upload failed: {image.image_name}")

        logger.info(f"Upload completed: {stats}")
        return stats.processed, stats
