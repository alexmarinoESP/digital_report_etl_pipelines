"""
Local filesystem storage adapter.
Implements IImageStorage interface for local testing.
Saves images to local folder instead of S3/Minio.
"""

import os
from typing import List, Optional

from PIL import Image
from loguru import logger

from newsletter.domain.interfaces import IImageStorage


class LocalStorageAdapter(IImageStorage):
    """
    Adapter for local filesystem storage.
    Implements IImageStorage interface.

    Use this for testing without S3/Minio connection.
    Images are saved to a local folder.
    """

    def __init__(self, output_folder: str):
        """
        Initialize local storage adapter.

        Args:
            output_folder: Path to local folder for saving images
        """
        self._output_folder = output_folder
        self._ensure_folder_exists()
        self._existing_cache: Optional[List[str]] = None

    def _ensure_folder_exists(self) -> None:
        """Create output folder if it doesn't exist."""
        if not os.path.exists(self._output_folder):
            os.makedirs(self._output_folder)
            logger.info(f"Created output folder: {self._output_folder}")

    def _get_full_path(self, image_name: str) -> str:
        """Get full local path for an image."""
        return os.path.join(self._output_folder, image_name)

    def list_existing(self, folder: str = "") -> List[str]:
        """
        List existing images in storage.

        Args:
            folder: Ignored for local storage

        Returns:
            List of image names
        """
        if self._existing_cache is not None:
            return self._existing_cache

        if not os.path.exists(self._output_folder):
            self._existing_cache = []
            return []

        images = [
            f for f in os.listdir(self._output_folder)
            if f.lower().endswith(('.png', '.jpg', '.jpeg', '.gif'))
        ]
        self._existing_cache = images
        return images

    def exists(self, image_name: str) -> bool:
        """
        Check if an image already exists in storage.

        Args:
            image_name: Name of the image file

        Returns:
            True if exists, False otherwise
        """
        return os.path.exists(self._get_full_path(image_name))

    def upload(self, image: Image.Image, image_name: str) -> bool:
        """
        Save an image to local storage.

        Args:
            image: PIL Image to save
            image_name: Target filename

        Returns:
            True if successful, False otherwise
        """
        try:
            full_path = self._get_full_path(image_name)
            image.save(full_path, format="PNG")
            logger.info(f"Saved image: {full_path}")

            # Update cache
            if self._existing_cache is not None:
                self._existing_cache.append(image_name)

            return True

        except Exception as e:
            logger.error(f"Failed to save {image_name}: {e}")
            return False

    def invalidate_cache(self) -> None:
        """Invalidate the existing images cache."""
        self._existing_cache = None

    @property
    def output_folder(self) -> str:
        """Get the output folder path."""
        return self._output_folder
