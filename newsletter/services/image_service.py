"""
Image processing and upload service.
Orchestrates HTML rendering and image storage.
"""

import os
from typing import List, Optional

import pandas as pd
from PIL import Image
from loguru import logger

from newsletter.adapters.hcti_client import HctiClient, ApiRenderize
from newsletter.post_processing.smart_image_cropper import SmartImageCropper
from newsletter.services.html_processor import string_decoding, remove_placeholders
from shared.storage.s3_handler import S3Handler
from newsletter import config, BUCKET_NAME


def config_cropper(image_path: str, output_path: str) -> SmartImageCropper:
    """
    Create a preconfigured SmartImageCropper instance.

    Args:
        image_path: Path to input image
        output_path: Path for output image

    Returns:
        Configured SmartImageCropper
    """
    postprocessing_config = config.get("postprocessing", {})
    return SmartImageCropper(
        image_path=image_path,
        output_path=output_path,
        uniformity_threshold=postprocessing_config.get("uniformity_threshold", 0.95),
        margin_ratio=postprocessing_config.get("margin_ratio", 0.05),
        additional_margin_ratio=postprocessing_config.get("additional_margin_ratio", 0.02),
        extra_crop_ratio=postprocessing_config.get("extra_crop_ratio", 0.02),
    )


class ImageService:
    """
    Service for image rendering and storage.
    Handles the complete pipeline from HTML to stored image.
    """

    def __init__(
        self,
        hcti_client: Optional[HctiClient] = None,
        s3_handler: Optional[S3Handler] = None,
        bucket_name: str = BUCKET_NAME,
    ):
        """
        Initialize image service.

        Args:
            hcti_client: HCTI client for rendering
            s3_handler: S3 handler for storage
            bucket_name: Target S3 bucket
        """
        self.hcti_client = hcti_client or HctiClient()
        self.s3_handler = s3_handler or S3Handler()
        self.bucket_name = bucket_name

    def get_existing_images(self, folder: str = "correct-images") -> List[str]:
        """
        Get list of images already in S3.

        Args:
            folder: S3 folder path

        Returns:
            List of existing image names
        """
        return self.s3_handler.list_objects_in_folder(
            bucket_name=self.bucket_name,
            folder_name=folder,
        )

    def render_and_save(
        self,
        html_str: str,
        name_preview: str,
        raw_image_path: str,
        cropped_image_path: str,
    ) -> Optional[str]:
        """
        Render HTML to image, crop, and save locally.

        Args:
            html_str: HTML content to render
            name_preview: Preview/activity name
            raw_image_path: Path for raw rendered image
            cropped_image_path: Path for cropped image

        Returns:
            Path to cropped image or None on failure
        """
        try:
            # Render HTML to image
            img = self.hcti_client.render_html_to_image(html_str)
            image_path = os.path.join(raw_image_path, name_preview)
            img.save(image_path)
            logger.info(f"Saved raw image: {image_path}")

            # Crop image
            cropper = config_cropper(
                image_path=image_path,
                output_path=cropped_image_path,
            )
            cropped_img, path_out_img = cropper.process_image()
            cropped_img.save(path_out_img)
            logger.info(f"Saved cropped image: {path_out_img}")

            return path_out_img

        except Exception as e:
            logger.error(f"Error rendering {name_preview}: {e}")
            return None

    def upload_to_s3(
        self,
        local_path: str,
        s3_folder: str = "correct-images",
    ) -> bool:
        """
        Upload image to S3.

        Args:
            local_path: Local file path
            s3_folder: Target S3 folder

        Returns:
            True if successful
        """
        try:
            filename = os.path.basename(local_path)
            s3_path = f"{s3_folder}/{filename}"

            self.s3_handler.deposit_file_from_filesystem(
                bucket_name=self.bucket_name,
                file_path=local_path,
                filename=s3_path,
            )
            return True

        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            return False

    def process_row(
        self,
        row: pd.Series,
        comp_preview: str,
        raw_image_path: str,
        cropped_image_path: str,
        existing_images: List[str],
    ) -> bool:
        """
        Process a single newsletter row.

        Args:
            row: DataFrame row with newsletter data
            comp_preview: Company preview flag
            raw_image_path: Path for raw images
            cropped_image_path: Path for cropped images
            existing_images: List of already existing images

        Returns:
            True if processed successfully
        """
        name_preview = string_decoding(row["NAMEPREVIEW"]).replace(r"'", "").replace("b", "")
        html_str = remove_placeholders(
            string_decoding(row["PREVIEWHTML"]),
            comp_preview,
        )

        logger.info(f"Processing: {name_preview}")

        # Check conditions
        if not name_preview:
            logger.info("No activity code, skipping")
            return False

        if name_preview in existing_images:
            logger.info(f"Already on Minio: {name_preview}")
            return False

        if not html_str:
            logger.info("Empty HTML, skipping")
            return False

        # Render and save
        cropped_path = self.render_and_save(
            html_str=html_str,
            name_preview=name_preview,
            raw_image_path=raw_image_path,
            cropped_image_path=cropped_image_path,
        )

        return cropped_path is not None
