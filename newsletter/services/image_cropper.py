"""
Image cropper service.
Implements IImageProcessor interface for smart image cropping.
"""

from typing import Tuple
from dataclasses import dataclass

import numpy as np
from PIL import Image
from loguru import logger

from newsletter.domain.interfaces import IImageProcessor


@dataclass
class CropConfig:
    """Configuration for image cropping."""
    uniformity_threshold: float = 0.95
    margin_ratio: float = 0.05
    additional_margin_ratio: float = 0.02
    extra_crop_ratio: float = 0.02


class SmartCropperService(IImageProcessor):
    """
    Smart image cropper service.
    Implements IImageProcessor interface.

    Detects and removes uniform margins from images
    using adaptive boundary detection.

    Follows:
    - Single Responsibility: Only handles image cropping
    - Open/Closed: Can be configured without modification
    - Dependency Inversion: Implements abstract interface
    """

    def __init__(self, config: CropConfig = None):
        """
        Initialize cropper service.

        Args:
            config: Cropping configuration (uses defaults if not provided)
        """
        self._config = config or CropConfig()

    def _is_uniform(self, line: np.ndarray) -> bool:
        """
        Check if a line/row is uniform (single color).

        Args:
            line: Numpy array of pixel values

        Returns:
            True if line is uniform within threshold
        """
        threshold = (1 - self._config.uniformity_threshold) * 255

        if line.ndim == 1:
            return np.std(line) < threshold

        # For RGB/RGBA images, check each channel
        return all(
            np.std(line[:, c]) < threshold
            for c in range(min(line.shape[1], 3))  # Only check RGB, not alpha
        )

    def _find_content_boundaries(
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

        # Find top boundary (first non-uniform row)
        top = 0
        for i in range(height):
            if not self._is_uniform(img_array[i]):
                top = i
                break

        # Find bottom boundary (last non-uniform row)
        bottom = height
        for i in range(height - 1, -1, -1):
            if not self._is_uniform(img_array[i]):
                bottom = i + 1
                break

        # Find left boundary (first non-uniform column)
        left = 0
        for i in range(width):
            if not self._is_uniform(img_array[:, i]):
                left = i
                break

        # Find right boundary (last non-uniform column)
        right = width
        for i in range(width - 1, -1, -1):
            if not self._is_uniform(img_array[:, i]):
                right = i + 1
                break

        return top, bottom, left, right

    def _apply_margins(
        self,
        boundaries: Tuple[int, int, int, int],
        dimensions: Tuple[int, int],
    ) -> Tuple[int, int, int, int]:
        """
        Apply margin adjustments to boundaries.

        Args:
            boundaries: (top, bottom, left, right)
            dimensions: (height, width)

        Returns:
            Adjusted boundaries
        """
        top, bottom, left, right = boundaries
        height, width = dimensions

        # Calculate margins
        margin_h = int(height * self._config.additional_margin_ratio)
        margin_w = int(width * self._config.additional_margin_ratio)

        # Calculate extra crop
        extra_h = int(height * self._config.extra_crop_ratio)
        extra_w = int(width * self._config.extra_crop_ratio)

        # Apply adjustments with bounds checking
        top = max(0, top - margin_h + extra_h)
        bottom = min(height, bottom + margin_h - extra_h)
        left = max(0, left - margin_w + extra_w)
        right = min(width, right + margin_w - extra_w)

        return top, bottom, left, right

    def process(self, image: Image.Image) -> Image.Image:
        """
        Process an image by detecting and removing uniform margins.

        Args:
            image: Input PIL Image

        Returns:
            Cropped PIL Image
        """
        # Convert to numpy array
        img_array = np.array(image)
        height, width = img_array.shape[:2]

        # Find content boundaries
        boundaries = self._find_content_boundaries(img_array)

        # Apply margin adjustments
        top, bottom, left, right = self._apply_margins(
            boundaries, (height, width)
        )

        # Validate boundaries
        if right <= left or bottom <= top:
            logger.warning("Invalid crop boundaries, returning original image")
            return image

        # Crop image
        cropped = image.crop((left, top, right, bottom))

        logger.debug(
            f"Cropped image: ({left}, {top}, {right}, {bottom}), "
            f"original: {width}x{height}, new: {cropped.size}"
        )

        return cropped


class NoCropService(IImageProcessor):
    """
    No-op image processor.
    Returns image unchanged. Useful for testing or bypassing cropping.
    """

    def process(self, image: Image.Image) -> Image.Image:
        """Return image unchanged."""
        return image
