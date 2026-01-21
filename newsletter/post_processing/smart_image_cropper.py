"""
Smart Image Cropper.
Automatically detects and removes uniform margins from images.
"""

import os
from typing import Tuple

import numpy as np
from PIL import Image
from loguru import logger


class SmartImageCropper:
    """
    Smart cropper that detects and removes uniform margins.
    Uses adaptive boundary detection for clean cropping.
    """

    def __init__(
        self,
        image_path: str,
        output_path: str,
        uniformity_threshold: float = 0.95,
        margin_ratio: float = 0.05,
        additional_margin_ratio: float = 0.02,
        extra_crop_ratio: float = 0.02,
    ):
        """
        Initialize cropper.

        Args:
            image_path: Path to input image
            output_path: Directory for output image
            uniformity_threshold: Threshold for uniform detection (0-1)
            margin_ratio: Base margin ratio
            additional_margin_ratio: Additional margin for safety
            extra_crop_ratio: Extra crop ratio for edges
        """
        self.image_path = image_path
        self.output_path = output_path
        self.uniformity_threshold = uniformity_threshold
        self.margin_ratio = margin_ratio
        self.additional_margin_ratio = additional_margin_ratio
        self.extra_crop_ratio = extra_crop_ratio

    def is_uniform(self, line: np.ndarray) -> bool:
        """
        Check if a line/row is uniform (single color).

        Args:
            line: Numpy array of pixel values

        Returns:
            True if line is uniform
        """
        if line.ndim == 1:
            return np.std(line) < (1 - self.uniformity_threshold) * 255

        # For RGB images, check each channel
        return all(
            np.std(line[:, c]) < (1 - self.uniformity_threshold) * 255
            for c in range(line.shape[1])
        )

    def find_content_boundaries(
        self, img_array: np.ndarray
    ) -> Tuple[int, int, int, int]:
        """
        Find the boundaries of actual content in the image.

        Args:
            img_array: Image as numpy array

        Returns:
            Tuple of (top, bottom, left, right) boundaries
        """
        height, width = img_array.shape[:2]

        # Find top boundary
        top = 0
        for i in range(height):
            if not self.is_uniform(img_array[i]):
                top = i
                break

        # Find bottom boundary
        bottom = height
        for i in range(height - 1, -1, -1):
            if not self.is_uniform(img_array[i]):
                bottom = i + 1
                break

        # Find left boundary
        left = 0
        for i in range(width):
            if not self.is_uniform(img_array[:, i]):
                left = i
                break

        # Find right boundary
        right = width
        for i in range(width - 1, -1, -1):
            if not self.is_uniform(img_array[:, i]):
                right = i + 1
                break

        return top, bottom, left, right

    def process_image(self) -> Tuple[Image.Image, str]:
        """
        Process and crop the image.

        Returns:
            Tuple of (cropped image, output path)
        """
        # Load image
        img = Image.open(self.image_path)
        img_array = np.array(img)

        # Find content boundaries
        top, bottom, left, right = self.find_content_boundaries(img_array)

        # Add margins
        height, width = img_array.shape[:2]
        margin_h = int(height * self.additional_margin_ratio)
        margin_w = int(width * self.additional_margin_ratio)

        # Apply extra crop
        extra_h = int(height * self.extra_crop_ratio)
        extra_w = int(width * self.extra_crop_ratio)

        # Calculate final boundaries
        top = max(0, top - margin_h + extra_h)
        bottom = min(height, bottom + margin_h - extra_h)
        left = max(0, left - margin_w + extra_w)
        right = min(width, right + margin_w - extra_w)

        # Crop image
        cropped = img.crop((left, top, right, bottom))

        # Generate output path
        filename = os.path.basename(self.image_path)
        output_file = os.path.join(self.output_path, filename)

        logger.info(
            f"Cropped from ({left}, {top}, {right}, {bottom}), "
            f"original size: {width}x{height}"
        )

        return cropped, output_file
