"""
Newsletter adapters layer.
Implements interfaces for external services and data sources.

Adapters:
- MappAdapter: Mapp Newsletter API client
- HctiAdapter: HTML to image rendering (hcti.io)
- S3StorageAdapter: S3/Minio image storage
- LocalStorageAdapter: Local filesystem storage (for testing)
- NewsletterRepositoryAdapter: Vertica database access
"""

from newsletter.adapters.mapp_adapter import MappAdapter, MappApiError
from newsletter.adapters.hcti_adapter import HctiAdapter, RenderingError
from newsletter.adapters.s3_adapter import S3StorageAdapter
from newsletter.adapters.local_storage_adapter import LocalStorageAdapter
from newsletter.adapters.repository_adapter import NewsletterRepositoryAdapter

__all__ = [
    "MappAdapter",
    "MappApiError",
    "HctiAdapter",
    "RenderingError",
    "S3StorageAdapter",
    "LocalStorageAdapter",
    "NewsletterRepositoryAdapter",
]
