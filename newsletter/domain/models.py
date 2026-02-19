"""
Domain models for newsletter module.
Pure data structures without business logic dependencies.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from PIL import Image


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
        self.api_region = api_region  # Used for Mapp API endpoint

    @classmethod
    def from_code(cls, code: str) -> "Company":
        """Get Company by code string."""
        for company in cls:
            if company.code == code:
                return company
        raise ValueError(f"Unknown company code: {code}")

    @classmethod
    def all_codes(cls) -> list:
        """Get all company codes."""
        return [c.code for c in cls]


@dataclass
class Newsletter:
    """
    Newsletter entity.
    Represents a newsletter with its metadata and content.
    """
    newsletter_id: str
    company: Company
    source: str  # 'mapp' or 'dynamics'
    html_content: Optional[str] = None
    message_id: Optional[int] = None
    contact_id: Optional[int] = None
    contact_id_2: Optional[int] = None  # fallback contact (min USER_ID)
    image_name: Optional[str] = None

    def __post_init__(self):
        """Generate image name if not provided."""
        if self.image_name is None:
            self.image_name = f"{self.newsletter_id}.png"

    @property
    def has_content(self) -> bool:
        """Check if newsletter has HTML content."""
        return bool(self.html_content and self.html_content.strip())

    @property
    def is_mapp(self) -> bool:
        """Check if newsletter is from Mapp source."""
        return self.source == "mapp"

    @property
    def is_dynamics(self) -> bool:
        """Check if newsletter is from Dynamics source."""
        return self.source == "dynamics"


@dataclass
class NewsletterImage:
    """
    Newsletter image entity.
    Represents a rendered newsletter image.
    """
    newsletter_id: str
    image_name: str
    image: Optional[Image.Image] = None
    is_cropped: bool = False

    @property
    def has_image(self) -> bool:
        """Check if image has been rendered."""
        return self.image is not None


@dataclass
class ProcessingResult:
    """
    Result of processing a newsletter.
    Used for tracking success/failure across the pipeline.
    """
    newsletter_id: str
    success: bool
    stage: str  # 'extraction', 'rendering', 'upload'
    error_message: Optional[str] = None

    @classmethod
    def success_result(cls, newsletter_id: str, stage: str) -> "ProcessingResult":
        """Create a success result."""
        return cls(newsletter_id=newsletter_id, success=True, stage=stage)

    @classmethod
    def failure_result(
        cls, newsletter_id: str, stage: str, error: str
    ) -> "ProcessingResult":
        """Create a failure result."""
        return cls(
            newsletter_id=newsletter_id,
            success=False,
            stage=stage,
            error_message=error,
        )


@dataclass
class PipelineStats:
    """
    Statistics for pipeline execution.
    Tracks processed, skipped, and failed items.
    """
    processed: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list = field(default_factory=list)

    def add_processed(self):
        """Increment processed count."""
        self.processed += 1

    def add_skipped(self):
        """Increment skipped count."""
        self.skipped += 1

    def add_failed(self, error: str):
        """Increment failed count and record error."""
        self.failed += 1
        self.errors.append(error)

    @property
    def total(self) -> int:
        """Total items processed."""
        return self.processed + self.skipped + self.failed

    def __str__(self) -> str:
        return (
            f"Processed: {self.processed}, "
            f"Skipped: {self.skipped}, "
            f"Failed: {self.failed}"
        )
