"""
Domain models for SMS campaign pipeline.

This module defines the core business entities and value objects.
All models are immutable (frozen dataclasses) to ensure data integrity.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import List, Optional


class Company(Enum):
    """
    Company enumeration.
    Single source of truth for company configuration.
    """
    IT = ("it", 1, "it")
    ES = ("es", 2, "es")
    VVIT = ("vvit", 32, "it")  # V-Valley uses IT API endpoint

    def __init__(self, code: str, company_id: int, api_region: str):
        self.code = code
        self.company_id = company_id
        self.api_region = api_region  # Used for MAPP API endpoint

    @classmethod
    def from_code(cls, code: str) -> "Company":
        """Get Company by code string."""
        for company in cls:
            if company.code == code:
                return company
        raise ValueError(f"Unknown company code: {code}")

    @property
    def name_str(self) -> str:
        """Get human-readable company name."""
        return self.name


@dataclass(frozen=True)
class SMSCampaign:
    """
    SMS Campaign domain model.

    Represents a single SMS campaign with delivery statistics.
    Immutable to ensure data integrity throughout the pipeline.

    Attributes:
        message_id: MAPP message ID (primary key)
        activity_id: Activity code (externalMessageId from MAPP)
        campaign_name: Human-readable campaign name
        company_id: Company identifier
        sms_text: SMS message text content
        sendout_date: Date and time when SMS was sent
        sent_count: Number of SMS sent
        delivered_count: Number of SMS successfully delivered
        bounced_count: Number of SMS that bounced
        acceptance_rate: Percentage of successfully delivered SMS
        bitly_links: List of Bitly links contained in this SMS
        load_date: Date when record was loaded into database
    """
    message_id: int
    activity_id: str
    campaign_name: Optional[str] = None
    company_id: Optional[int] = None
    sms_text: Optional[str] = None
    sendout_date: Optional[datetime] = None
    sent_count: Optional[int] = None
    delivered_count: Optional[int] = None
    bounced_count: Optional[int] = None
    acceptance_rate: Optional[Decimal] = None
    bitly_links: List['BitlyLink'] = field(default_factory=list)
    load_date: date = field(default_factory=date.today)

    def __post_init__(self) -> None:
        """Validate model after initialization."""
        if not self.activity_id:
            raise ValueError("activity_id cannot be empty")
        if self.message_id <= 0:
            raise ValueError("message_id must be positive")
        if self.sms_text and len(self.sms_text) > 1000:
            raise ValueError("sms_text exceeds maximum length of 1000 characters")
        if self.activity_id and len(self.activity_id) > 50:
            raise ValueError("activity_id exceeds maximum length of 50 characters")

    @property
    def has_links(self) -> bool:
        """Check if campaign has any Bitly links."""
        return len(self.bitly_links) > 0

    @property
    def total_clicks(self) -> int:
        """Calculate total clicks across all links."""
        return sum(link.total_clicks for link in self.bitly_links)

    def __str__(self) -> str:
        """String representation of the campaign."""
        return (
            f"SMSCampaign(id={self.message_id}, activity={self.activity_id}, "
            f"sent={self.sent_count}, delivered={self.delivered_count}, "
            f"links={len(self.bitly_links)})"
        )


@dataclass(frozen=True)
class BitlyLink:
    """
    Bitly Link domain model.

    Represents a shortened Bitly link with click statistics.

    Attributes:
        message_id: Reference to parent SMS campaign
        activity_id: Activity code (denormalized for query performance)
        bitly_short_url: Shortened Bitly URL (e.g., https://bit.ly/abc123)
        bitly_long_url: Original long URL
        total_clicks: Total number of clicks on this link (all time)
        load_date: Date when record was loaded into database
    """
    message_id: int
    activity_id: str
    bitly_short_url: str
    bitly_long_url: Optional[str] = None
    total_clicks: int = 0
    load_date: date = field(default_factory=date.today)

    def __post_init__(self) -> None:
        """Validate model after initialization."""
        if not self.bitly_short_url:
            raise ValueError("bitly_short_url cannot be empty")
        if not self.bitly_short_url.startswith(("http://", "https://")):
            raise ValueError("bitly_short_url must be a valid URL")
        if self.message_id <= 0:
            raise ValueError("message_id must be positive")
        if self.total_clicks < 0:
            raise ValueError("total_clicks cannot be negative")

    @property
    def bitlink_id(self) -> str:
        """
        Get Bitly API bitlink ID from short URL.

        Converts https://bit.ly/abc123 to bit.ly/abc123
        """
        return self.bitly_short_url.replace("https://", "").replace("http://", "")

    def __str__(self) -> str:
        """String representation of the link."""
        return f"BitlyLink(url={self.bitly_short_url}, clicks={self.total_clicks})"


@dataclass
class PipelineStats:
    """
    Pipeline execution statistics.

    Tracks success, failure, and skip counts for pipeline stages.
    Mutable to allow incrementing counters during processing.

    Attributes:
        success: Number of successful operations
        failed: Number of failed operations
        skipped: Number of skipped operations
    """
    success: int = 0
    failed: int = 0
    skipped: int = 0

    @property
    def total(self) -> int:
        """Total number of operations."""
        return self.success + self.failed + self.skipped

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total == 0:
            return 0.0
        return (self.success / self.total) * 100

    def increment_success(self) -> None:
        """Increment success counter."""
        self.success += 1

    def increment_failed(self) -> None:
        """Increment failed counter."""
        self.failed += 1

    def increment_skipped(self) -> None:
        """Increment skipped counter."""
        self.skipped += 1

    def __str__(self) -> str:
        """String representation of statistics."""
        return (
            f"success={self.success}, failed={self.failed}, "
            f"skipped={self.skipped}, total={self.total}"
        )
