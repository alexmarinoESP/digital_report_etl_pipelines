"""Domain models for advertising data.

These models represent the core business entities in a platform-agnostic way.
They use dataclasses for immutability and type safety.
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, Dict, Any, List
from decimal import Decimal


@dataclass(frozen=True)
class DateRange:
    """Represents a date range for querying data."""

    start_date: date
    end_date: date

    def __post_init__(self):
        """Validate date range."""
        if self.start_date > self.end_date:
            raise ValueError(
                f"start_date ({self.start_date}) must be before or equal to end_date ({self.end_date})"
            )

    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary with ISO format dates.

        Returns:
            Dictionary with start_date and end_date as strings
        """
        return {
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
        }

    @property
    def days(self) -> int:
        """Get the number of days in the range.

        Returns:
            Number of days (inclusive)
        """
        return (self.end_date - self.start_date).days + 1


@dataclass
class AdAccount:
    """Represents an advertising account across platforms."""

    id: str
    name: str
    platform: str
    company_id: int
    currency: Optional[str] = None
    status: Optional[str] = None
    created_time: Optional[datetime] = None
    row_loaded_date: datetime = field(default_factory=datetime.now)
    raw_data: Dict[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self):
        """Validate required fields."""
        if not self.id:
            raise ValueError("Account ID cannot be empty")
        if not self.platform:
            raise ValueError("Platform cannot be empty")


@dataclass
class Campaign:
    """Represents an advertising campaign across platforms."""

    id: str
    account_id: str
    name: str
    platform: str
    company_id: int
    status: Optional[str] = None
    objective: Optional[str] = None
    daily_budget: Optional[Decimal] = None
    lifetime_budget: Optional[Decimal] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    created_time: Optional[datetime] = None
    updated_time: Optional[datetime] = None
    row_loaded_date: datetime = field(default_factory=datetime.now)
    raw_data: Dict[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self):
        """Validate required fields."""
        if not self.id:
            raise ValueError("Campaign ID cannot be empty")
        if not self.account_id:
            raise ValueError("Account ID cannot be empty")

    @property
    def is_active(self) -> bool:
        """Check if campaign is currently active.

        Returns:
            True if status indicates active campaign
        """
        active_statuses = {"ACTIVE", "ENABLED", "active", "enabled"}
        return self.status in active_statuses if self.status else False


@dataclass
class Audience:
    """Represents a targeting audience across platforms."""

    id: str
    account_id: str
    name: str
    platform: str
    company_id: int
    audience_type: Optional[str] = None
    size: Optional[int] = None
    status: Optional[str] = None
    created_time: Optional[datetime] = None
    updated_time: Optional[datetime] = None
    row_loaded_date: datetime = field(default_factory=datetime.now)
    raw_data: Dict[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self):
        """Validate required fields."""
        if not self.id:
            raise ValueError("Audience ID cannot be empty")


@dataclass
class Creative:
    """Represents an ad creative across platforms."""

    id: str
    account_id: str
    campaign_id: str
    name: str
    platform: str
    company_id: int
    creative_type: Optional[str] = None
    title: Optional[str] = None
    body: Optional[str] = None
    call_to_action: Optional[str] = None
    image_url: Optional[str] = None
    video_url: Optional[str] = None
    destination_url: Optional[str] = None
    status: Optional[str] = None
    created_time: Optional[datetime] = None
    updated_time: Optional[datetime] = None
    row_loaded_date: datetime = field(default_factory=datetime.now)
    raw_data: Dict[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self):
        """Validate required fields."""
        if not self.id:
            raise ValueError("Creative ID cannot be empty")


@dataclass
class Insight:
    """Represents advertising performance metrics across platforms."""

    date: date
    campaign_id: str
    platform: str
    company_id: int

    # Common metrics across platforms
    impressions: int = 0
    clicks: int = 0
    spend: Decimal = Decimal("0.00")

    # Optional metrics
    reach: Optional[int] = None
    frequency: Optional[Decimal] = None
    conversions: Optional[int] = None
    conversion_value: Optional[Decimal] = None

    # Granular dimensions
    creative_id: Optional[str] = None
    audience_id: Optional[str] = None
    device_type: Optional[str] = None
    placement: Optional[str] = None

    row_loaded_date: datetime = field(default_factory=datetime.now)
    raw_data: Dict[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self):
        """Validate required fields and calculated metrics."""
        if not self.campaign_id:
            raise ValueError("Campaign ID cannot be empty")

    @property
    def ctr(self) -> Optional[Decimal]:
        """Calculate Click-Through Rate.

        Returns:
            CTR as percentage, or None if impressions is 0
        """
        if self.impressions == 0:
            return None
        return Decimal(self.clicks) / Decimal(self.impressions) * Decimal(100)

    @property
    def cpc(self) -> Optional[Decimal]:
        """Calculate Cost Per Click.

        Returns:
            CPC, or None if clicks is 0
        """
        if self.clicks == 0:
            return None
        return self.spend / Decimal(self.clicks)

    @property
    def cpm(self) -> Optional[Decimal]:
        """Calculate Cost Per Mille (thousand impressions).

        Returns:
            CPM, or None if impressions is 0
        """
        if self.impressions == 0:
            return None
        return (self.spend / Decimal(self.impressions)) * Decimal(1000)

    @property
    def roas(self) -> Optional[Decimal]:
        """Calculate Return on Ad Spend.

        Returns:
            ROAS ratio, or None if spend is 0 or no conversion value
        """
        if self.spend == 0 or self.conversion_value is None:
            return None
        return self.conversion_value / self.spend


@dataclass
class CampaignAudience:
    """Represents the relationship between campaigns and audiences."""

    campaign_id: str
    audience_id: str
    platform: str
    company_id: int
    created_time: Optional[datetime] = None
    row_loaded_date: datetime = field(default_factory=datetime.now)
    raw_data: Dict[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self):
        """Validate required fields."""
        if not self.campaign_id:
            raise ValueError("Campaign ID cannot be empty")
        if not self.audience_id:
            raise ValueError("Audience ID cannot be empty")
