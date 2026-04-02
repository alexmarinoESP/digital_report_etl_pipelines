"""
Script to migrate images from MinIO to Azure Blob Storage.
Copies all files from the last month from MinIO bucket 'report-digital' to Azure Blob Storage.
"""

import io
from datetime import datetime, timedelta, timezone
from minio import Minio
from azure.storage.blob import BlobServiceClient
from loguru import logger
import urllib3
from dotenv import load_dotenv
from shared.utils.env import get_env_or_raise

# Load environment variables from .env file
load_dotenv()

# Disable SSL warnings for MinIO self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_minio_client():
    """Create MinIO client."""
    endpoint = get_env_or_raise("S3_ENDPOINT")
    access_key = get_env_or_raise("S3_ACCESS_KEY")
    secret_key = get_env_or_raise("S3_SECRET_KEY")

    http_client = urllib3.PoolManager(cert_reqs="CERT_NONE")

    return Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        http_client=http_client,
        secure=True
    )


def get_azure_client():
    """Create Azure Blob Service client."""
    conn_str = get_env_or_raise("AZURE_STORAGE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(conn_str)


def migrate_minio_to_azure():
    """
    Migrate files from MinIO to Azure Blob Storage.
    Only copies files modified in the last month.
    """
    # Initialize clients
    logger.info("Connecting to MinIO...")
    minio_client = get_minio_client()

    logger.info("Connecting to Azure Blob Storage...")
    azure_client = get_azure_client()

    # MinIO bucket and Azure container names
    minio_bucket = "report-digital"
    azure_container = "report-digital"

    # Ensure Azure container exists
    container_client = azure_client.get_container_client(azure_container)
    try:
        container_client.get_container_properties()
        logger.info(f"Azure container '{azure_container}' exists")
    except Exception:
        container_client.create_container()
        logger.info(f"Created Azure container '{azure_container}'")

    # Calculate date threshold (1 month ago) - timezone aware
    one_month_ago = datetime.now(timezone.utc) - timedelta(days=30)
    logger.info(f"Migrating files modified after {one_month_ago.strftime('%Y-%m-%d')}")

    # List all objects in MinIO bucket
    logger.info(f"Listing objects in MinIO bucket '{minio_bucket}'...")
    objects = minio_client.list_objects(minio_bucket, recursive=True)

    migrated_count = 0
    skipped_count = 0
    error_count = 0

    for obj in objects:
        try:
            # Check if file was modified in the last month
            if obj.last_modified < one_month_ago:
                skipped_count += 1
                logger.debug(f"Skipping {obj.object_name} (too old: {obj.last_modified})")
                continue

            logger.info(f"Migrating: {obj.object_name} (size: {obj.size} bytes, date: {obj.last_modified})")

            # Download from MinIO
            response = minio_client.get_object(minio_bucket, obj.object_name)
            file_data = response.read()
            response.close()
            response.release_conn()

            # Determine content type based on extension
            content_type = "application/octet-stream"
            if obj.object_name.lower().endswith('.png'):
                content_type = "image/png"
            elif obj.object_name.lower().endswith(('.jpg', '.jpeg')):
                content_type = "image/jpeg"

            # Upload to Azure Blob Storage
            blob_client = container_client.get_blob_client(obj.object_name)
            blob_client.upload_blob(
                file_data,
                overwrite=True,
                content_type=content_type
            )

            migrated_count += 1
            logger.success(f"✓ Migrated: {obj.object_name}")

        except Exception as e:
            error_count += 1
            logger.error(f"✗ Failed to migrate {obj.object_name}: {e}")

    # Summary
    logger.info("=" * 60)
    logger.info("MIGRATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"✓ Migrated: {migrated_count} files")
    logger.info(f"⊘ Skipped (older than 1 month): {skipped_count} files")
    logger.info(f"✗ Errors: {error_count} files")
    logger.info("=" * 60)


if __name__ == "__main__":
    logger.info("Starting MinIO to Azure Blob Storage migration...")
    logger.info("Source: MinIO bucket 'report-digital'")
    logger.info("Destination: Azure Blob Storage container 'report-digital'")
    logger.info("Filter: Files from the last month only")
    logger.info("")

    migrate_minio_to_azure()

    logger.info("Migration completed!")
