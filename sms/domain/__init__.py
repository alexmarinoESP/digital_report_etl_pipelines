"""
SMS Domain Layer.

This module contains the domain models and interfaces for the SMS campaign pipeline.
Following Domain-Driven Design principles, this layer is independent of infrastructure.
"""

from sms.domain.models import (
    Company,
    SMSCampaign,
    BitlyLink,
    PipelineStats,
)
from sms.domain.interfaces import (
    ISMSRepository,
    IMappSMSClient,
    IBitlyClient,
    # Exceptions
    SMSError,
    ExtractionError,
    APIError,
    RepositoryError,
)

__all__ = [
    # Models
    "Company",
    "SMSCampaign",
    "BitlyLink",
    "PipelineStats",
    # Interfaces
    "ISMSRepository",
    "IMappSMSClient",
    "IBitlyClient",
    # Exceptions
    "SMSError",
    "ExtractionError",
    "APIError",
    "RepositoryError",
]
