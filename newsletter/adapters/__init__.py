"""
Newsletter adapters layer.
Implements interfaces for external services and data sources.

Adapters:
- MappAdapter: Mapp Newsletter API client
- HctiAdapter: HTML to image rendering (hcti.io)
- AzureBlobStorageAdapter: Azure Blob Storage image storage
- LocalStorageAdapter: Local filesystem storage (for testing)
- NewsletterRepositoryAdapter: Vertica database access
"""

from newsletter.adapters.mapp_adapter import MappAdapter, MappApiError, RecipientNotFoundError
from newsletter.adapters.hcti_adapter import HctiAdapter, RenderingError
from newsletter.adapters.azure_blob_adapter import AzureBlobStorageAdapter
from newsletter.adapters.local_storage_adapter import LocalStorageAdapter
from newsletter.adapters.repository_adapter import NewsletterRepositoryAdapter

__all__ = [
    "MappAdapter",
    "MappApiError",
    "RecipientNotFoundError",
    "HctiAdapter",
    "RenderingError",
    "AzureBlobStorageAdapter",
    "LocalStorageAdapter",
    "NewsletterRepositoryAdapter",
]
