"""
S3/Minio storage adapter.
Implements IImageStorage interface for S3-compatible storage.
"""

import io
from typing import List, Optional

from PIL import Image
from loguru import logger

from newsletter.domain.interfaces import IImageStorage
from shared.storage.s3_handler import S3Handler
from shared.utils.env import get_env


class S3StorageAdapter(IImageStorage):
    """
    Adapter for S3/Minio image storage.
    Implements IImageStorage interface.

    Follows:
    - Single Responsibility: Only handles image storage operations
    - Dependency Inversion: Implements abstract interface
    - Liskov Substitution: Can be swapped with any IImageStorage implementation
    """

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        folder: Optional[str] = None,
        s3_handler: Optional[S3Handler] = None,
    ):
        """
        Initialize S3 storage adapter.

        Args:
            bucket_name: S3 bucket name (from env S3_BUCKET_NAME if not provided)
            folder: Target folder in bucket (from env S3_FOLDER if not provided)
            s3_handler: Optional S3Handler instance (creates new if not provided)
        """
        self._bucket_name = bucket_name or get_env(
            "S3_BUCKET_NAME", "report-digital-preview"
        )
        self._folder = folder or get_env(
            "S3_FOLDER", "correct-images"
        )
        self._s3 = s3_handler or S3Handler()
        self._existing_cache: Optional[List[str]] = None

    def _get_full_path(self, image_name: str) -> str:
        """Get full S3 path for an image."""
        if self._folder:
            return f"{self._folder}/{image_name}"
        return image_name

    def _refresh_cache(self) -> None:
        """Refresh the cache of existing images."""
        self._existing_cache = self._s3.list_objects_in_folder(
            bucket_name=self._bucket_name,
            folder_name=self._folder,
        )
        logger.debug(f"Cached {len(self._existing_cache)} existing images")

    def list_existing(self, folder: str = "") -> List[str]:
        """
        List existing images in storage.

        Args:
            folder: Optional folder path (uses default if empty)

        Returns:
            List of image names
        """
        target_folder = folder or self._folder

        if target_folder == self._folder and self._existing_cache is not None:
            return self._existing_cache

        images = self._s3.list_objects_in_folder(
            bucket_name=self._bucket_name,
            folder_name=target_folder,
        )

        if target_folder == self._folder:
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
        if self._existing_cache is None:
            self._refresh_cache()

        return image_name in self._existing_cache

    def upload(self, image: Image.Image, image_name: str) -> bool:
        """
        Upload an image to storage.

        Args:
            image: PIL Image to upload
            image_name: Target filename

        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert PIL Image to bytes
            buffer = io.BytesIO()
            image.save(buffer, format="PNG")
            buffer.seek(0)

            # Get full path
            full_path = self._get_full_path(image_name)

            # Upload using minio client directly for bytes
            self._s3.client.put_object(
                bucket_name=self._bucket_name,
                object_name=full_path,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type="image/png",
            )

            logger.info(f"Uploaded {image_name} to {self._bucket_name}/{full_path}")

            # Update cache
            if self._existing_cache is not None:
                self._existing_cache.append(image_name)

            return True

        except Exception as e:
            logger.error(f"Failed to upload {image_name}: {e}")
            return False

    def invalidate_cache(self) -> None:
        """Invalidate the existing images cache."""
        self._existing_cache = None
