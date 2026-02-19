"""
Azure Blob Storage adapter.
Implements IImageStorage interface for Azure Blob Storage.
"""

import io
from typing import List, Optional

from azure.storage.blob import BlobServiceClient, ContentSettings
from PIL import Image
from loguru import logger

from newsletter.domain.interfaces import IImageStorage
from shared.utils.env import get_env, get_env_or_raise


class AzureBlobStorageAdapter(IImageStorage):
    """
    Adapter for Azure Blob Storage image storage.
    Implements IImageStorage interface.
    """

    def __init__(
        self,
        container_name: Optional[str] = None,
        folder: Optional[str] = None,
        connection_string: Optional[str] = None,
    ):
        """
        Initialize Azure Blob storage adapter.

        Args:
            container_name: Azure Blob container name (env AZURE_STORAGE_CONTAINER if not provided)
            folder: Target folder/prefix in container (env AZURE_STORAGE_FOLDER if not provided)
            connection_string: Azure Storage connection string (env AZURE_STORAGE_CONNECTION_STRING if not provided)
        """
        self._container_name = container_name or get_env(
            "AZURE_STORAGE_CONTAINER", "newsletter-images"
        )
        self._folder = folder or get_env(
            "AZURE_STORAGE_FOLDER", "correct-images"
        )

        conn_str = connection_string or get_env_or_raise("AZURE_STORAGE_CONNECTION_STRING")
        self._blob_service = BlobServiceClient.from_connection_string(conn_str)
        self._container_client = self._blob_service.get_container_client(self._container_name)
        self._existing_cache: Optional[List[str]] = None

        # Ensure container exists
        try:
            self._container_client.get_container_properties()
        except Exception:
            self._container_client.create_container()
            logger.info(f"Created Azure Blob container '{self._container_name}'")

    def _get_full_path(self, image_name: str) -> str:
        """Get full blob path for an image."""
        if self._folder:
            return f"{self._folder}/{image_name}"
        return image_name

    def _refresh_cache(self) -> None:
        """Refresh the cache of existing images."""
        prefix = f"{self._folder}/" if self._folder else ""
        blobs = self._container_client.list_blobs(name_starts_with=prefix)
        self._existing_cache = [
            blob.name.replace(prefix, "", 1) for blob in blobs
        ]
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

        prefix = f"{target_folder}/" if target_folder else ""
        blobs = self._container_client.list_blobs(name_starts_with=prefix)
        images = [blob.name.replace(prefix, "", 1) for blob in blobs]

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

            # Upload to Azure Blob
            blob_client = self._container_client.get_blob_client(full_path)
            blob_client.upload_blob(
                buffer,
                overwrite=True,
                content_settings=ContentSettings(content_type="image/png"),
            )

            logger.info(f"Uploaded {image_name} to {self._container_name}/{full_path}")

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
