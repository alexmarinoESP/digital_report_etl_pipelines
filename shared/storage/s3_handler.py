"""
S3/Minio storage handler module.
Provides unified interface for S3-compatible storage.
"""

import io
import logging
import pickle
import os
from typing import Dict, List, Optional, Any

import urllib3
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource
from loguru import logger

from shared.utils.env import get_env_or_raise, get_env


class S3Handler:
    """
    Handler for S3/Minio storage operations.
    Supports file upload, download, listing, and management.
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        secure: bool = True,
    ):
        """
        Initialize S3 handler.

        Args:
            endpoint: S3/Minio endpoint
            access_key: Access key ID
            secret_key: Secret access key
            secure: Use HTTPS
        """
        self.endpoint = endpoint or get_env_or_raise("S3_ENDPOINT")
        self.access_key = access_key or get_env_or_raise("S3_ACCESS_KEY")
        self.secret_key = secret_key or get_env_or_raise("S3_SECRET_KEY")
        self.secure = secure

        self.client = self._create_client()

    def _create_client(self) -> Minio:
        """Create Minio client with SSL configuration."""
        cert_reqs = "CERT_NONE"
        http_client = urllib3.PoolManager(cert_reqs=cert_reqs)

        return Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            http_client=http_client,
            secure=self.secure,
        )

    def get_data(self, bucket: str, file_names: List[str]) -> Dict[str, Any]:
        """
        Get data from S3 bucket.

        Args:
            bucket: Bucket name
            file_names: List of file names to retrieve

        Returns:
            Dictionary mapping file names to their contents
        """
        logging.info("Startup api. Retrieving data from Minio")

        result = {}
        for file_name in file_names:
            try:
                logging.info(f"Retrieving file: {file_name}")
                response = self.client.get_object(
                    bucket_name=bucket,
                    object_name=file_name,
                )
                df = pickle.load(response)
                key = os.path.splitext(file_name)[0]
                result[key] = df

            except S3Error as e:
                logging.exception(e)
                result[file_name] = None
            finally:
                response.close()
                response.release_conn()

        return result

    def list_information_bucket(self, bucket: str) -> List[str]:
        """
        List all objects in a bucket.

        Args:
            bucket: Bucket name

        Returns:
            List of object names
        """
        objects = self.client.list_objects(bucket_name=bucket, recursive=True)
        return [obj.object_name for obj in objects]

    def list_objects_in_folder(
        self, bucket_name: str, folder_name: str
    ) -> List[str]:
        """
        List objects in a specific folder.

        Args:
            bucket_name: Bucket name
            folder_name: Folder path

        Returns:
            List of object names in the folder
        """
        try:
            if folder_name == "":
                return self.list_information_bucket(bucket=bucket_name)

            if not folder_name.endswith("/"):
                folder_name += "/"

            objects = self.list_information_bucket(bucket=bucket_name)
            return [
                obj.replace(folder_name, "")
                for obj in objects
                if folder_name in obj
            ]

        except Exception as err:
            print(err)
            return []

    def _check_if_file_existed(self, bucket: str, file: str) -> bool:
        """Check if a file exists in the bucket."""
        try:
            self.client.get_object(bucket_name=bucket, object_name=file)
            return True
        except S3Error:
            return False

    def deposit_file(
        self, filename: str, bucket_name: str, data: Any
    ) -> None:
        """
        Deposit pickled data to S3.

        Args:
            filename: Target filename
            bucket_name: Bucket name
            data: Data to pickle and upload
        """
        bytes_file = pickle.dumps(data)

        try:
            self.client.put_object(
                bucket_name=bucket_name,
                object_name=filename,
                data=io.BytesIO(bytes_file),
                length=len(bytes_file),
            )
            logging.info(f"Deposited {filename} on minio {bucket_name} bucket")

        except IOError as e:
            logging.exception(
                f"Exception while writing {filename} to minio bucket.\n{e}"
            )

    def deposit_file_from_filesystem(
        self, bucket_name: str, file_path: str, filename: str
    ) -> None:
        """
        Upload a file from filesystem to S3.

        Args:
            bucket_name: Target bucket
            file_path: Local file path
            filename: Target filename in S3
        """
        if not self._check_if_file_existed(bucket=bucket_name, file=filename):
            try:
                self.client.fput_object(
                    bucket_name=bucket_name,
                    object_name=filename,
                    file_path=file_path,
                )
                logger.info(
                    f"Deposited {filename} on minio {bucket_name} bucket"
                )

            except IOError as e:
                logger.exception(
                    f"Exception while writing {filename} to minio bucket.\n{e}"
                )

    def create_bucket(self, bucket_name: str) -> None:
        """Create a bucket if it doesn't exist."""
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)

    def list_bucket_files(self, bucket_name: str) -> List[str]:
        """
        List all files in a bucket including subfolders.

        Args:
            bucket_name: Bucket name

        Returns:
            List of all file paths
        """
        objects = self.client.list_objects(bucket_name)
        list_folders = [obj.object_name for obj in objects]

        list_files = []
        for fold in list_folders:
            objects = self.client.list_objects(
                bucket_name, prefix=fold, recursive=True
            )
            list_files.extend([obj.object_name for obj in objects])

        return list_files

    def copy_file(
        self,
        orig_bucket: str,
        orig_path: str,
        move_bucket: str,
        move_path: str,
    ) -> None:
        """Copy a file from one location to another."""
        result = self.client.copy_object(
            move_bucket,
            move_path,
            CopySource(orig_bucket, orig_path),
        )
        print(result.object_name, result.version_id)

    def delete_file(self, bucket_name: str, file_path: str) -> None:
        """Delete a file from S3."""
        self.client.remove_object(bucket_name, file_path)
