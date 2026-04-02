"""
Azure Blob Storage adapter for campaign images upload.
Maintains compatibility with MinIO structure: report-digital/{campaign_code}/{activity_code}.{ext}
"""

import io
from typing import List, Tuple
from azure.storage.blob import BlobServiceClient, ContentSettings
from loguru import logger
from shared.utils.env import get_env_or_raise


class AzureBlobImageStorage:
    """
    Azure Blob Storage adapter for campaign images.
    Maintains same folder structure as MinIO: {container}/{campaign_code}/{image_name}
    """

    def __init__(self, container_name: str = "report-digital", connection_string: str = None):
        """
        Initialize Azure Blob storage adapter.

        Args:
            container_name: Azure Blob container name (default: "report-digital" to match MinIO)
            connection_string: Azure Storage connection string (env AZURE_STORAGE_CONNECTION_STRING if not provided)
        """
        self._container_name = container_name
        conn_str = connection_string or get_env_or_raise("AZURE_STORAGE_CONNECTION_STRING")
        self._blob_service = BlobServiceClient.from_connection_string(conn_str)
        self._container_client = self._blob_service.get_container_client(self._container_name)

        # Ensure container exists
        try:
            self._container_client.get_container_properties()
            logger.info(f"Connected to Azure Blob container '{self._container_name}'")
        except Exception:
            self._container_client.create_container()
            logger.info(f"Created Azure Blob container '{self._container_name}'")

    def list_images_in_campaign(self, campaign_code: str) -> Tuple[List[str], List[str]]:
        """
        List all images in a campaign folder.
        Replicates the MinIO logic from extract_list_of_image().

        Args:
            campaign_code: Campaign code (folder name)

        Returns:
            Tuple of (list_bucket_object, image_in_bucket)
            - list_bucket_object: List of all file names in the campaign folder
            - image_in_bucket: List of activity codes (without extension and progressive number)
        """
        prefix = f"{campaign_code}/"
        blobs = self._container_client.list_blobs(name_starts_with=prefix)

        # Extract file names (without folder prefix)
        list_bucket_object = [blob.name.split('/')[-1] for blob in blobs]

        # Extract activity codes from image files (png, jpg)
        image_in_bucket = [
            ".".join(x.split('.')[:2]).split("_")[0]
            for x in list_bucket_object
            if x.split('.')[-1].lower() in ['png', 'jpg']
        ]

        logger.debug(f"Found {len(list_bucket_object)} files in campaign {campaign_code}")
        return list_bucket_object, image_in_bucket

    def upload_image(
        self,
        file_content: bytes,
        campaign_code: str,
        file_name: str,
        content_type: str = "image/png"
    ) -> str:
        """
        Upload an image to Azure Blob Storage.
        Maintains same path structure as MinIO: {campaign_code}/{file_name}

        Args:
            file_content: Image file content as bytes
            campaign_code: Campaign code (folder name)
            file_name: File name with extension
            content_type: MIME type (default: image/png)

        Returns:
            Full blob path of uploaded file

        Raises:
            Exception: If upload fails
        """
        full_path = f"{campaign_code}/{file_name}"

        try:
            blob_client = self._container_client.get_blob_client(full_path)
            blob_client.upload_blob(
                file_content,
                overwrite=True,
                content_settings=ContentSettings(content_type=content_type),
            )

            logger.info(f"Uploaded image to {self._container_name}/{full_path}")
            return full_path

        except Exception as e:
            logger.error(f"Failed to upload {full_path}: {e}")
            raise

    def get_next_progressive_number(
        self,
        campaign_code: str,
        activity_code: str,
        existing_images: List[str]
    ) -> int:
        """
        Calculate next progressive number for duplicate images.
        Replicates MinIO logic for progressive numbering: {activity_code}_{number}.{ext}

        Args:
            campaign_code: Campaign code
            activity_code: Activity code (without extension)
            existing_images: List of existing image files in the campaign

        Returns:
            Next progressive number to use
        """
        # Find all images for this activity
        image_activity = [
            ".".join(x.split(".")[:-1])  # Remove extension
            for x in existing_images
            if ".".join(x.split('.')[:2]).split("_")[0] == activity_code
        ]

        # Extract progressive numbers
        progressive_numbers = []
        for img in image_activity:
            parts = img.split("_")
            if len(parts) > 1:
                try:
                    progressive_numbers.append(int(float(parts[-1])))
                except ValueError:
                    continue

        # Return next number (or 1 if no numbers found)
        return max(progressive_numbers) + 1 if progressive_numbers else 1
