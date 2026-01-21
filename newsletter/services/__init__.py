"""
Newsletter services layer.
Contains business logic and service orchestration.

Services:
- HtmlCleanerService: Cleans HTML by removing placeholders and fixing encoding
- SmartCropperService: Intelligently crops images to remove whitespace
- ExtractionService: Extracts newsletters from data sources
- RenderingService: Renders HTML to images
- UploadService: Uploads images to storage
"""

from newsletter.services.html_cleaner import HtmlCleanerService
from newsletter.services.image_cropper import SmartCropperService, CropConfig, NoCropService
from newsletter.services.extraction_service import ExtractionService
from newsletter.services.rendering_service import RenderingService
from newsletter.services.upload_service import UploadService

__all__ = [
    "HtmlCleanerService",
    "SmartCropperService",
    "CropConfig",
    "NoCropService",
    "ExtractionService",
    "RenderingService",
    "UploadService",
]
