"""
Newsletter domain layer.
Contains models and interfaces for the newsletter module.
"""

from newsletter.domain.models import Newsletter, NewsletterImage, Company, PipelineStats
from newsletter.domain.interfaces import (
    INewsletterRepository,
    IHtmlRenderer,
    IImageStorage,
    IImageProcessor,
    IHtmlProcessor,
    IMappClient,
    # Exceptions
    NewsletterError,
    ExtractionError,
    RenderingError,
    UploadError,
    StorageError,
)

__all__ = [
    # Models
    "Newsletter",
    "NewsletterImage",
    "Company",
    "PipelineStats",
    # Interfaces
    "INewsletterRepository",
    "IHtmlRenderer",
    "IImageStorage",
    "IImageProcessor",
    "IHtmlProcessor",
    "IMappClient",
    # Exceptions
    "NewsletterError",
    "ExtractionError",
    "RenderingError",
    "UploadError",
    "StorageError",
]
