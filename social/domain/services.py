"""Domain services containing business logic.

Domain services encapsulate business rules that don't naturally fit
within a single entity. They operate on domain models and provide
platform-agnostic business logic.
"""

import re
from datetime import date, timedelta
from typing import Dict, Optional, List
from loguru import logger

from social.domain.models import DateRange
from social.core.exceptions import DataValidationError
from social.core.constants import INSIGHTS_LOOKBACK_DAYS, DEFAULT_COMPANY_ID


class CompanyMappingService:
    """Service for mapping advertising accounts to company IDs.

    This service encapsulates the business logic for determining which
    company owns a particular advertising account.
    """

    def __init__(self, account_to_company: Dict[str, int], default_company_id: int = DEFAULT_COMPANY_ID):
        """Initialize the company mapping service.

        Args:
            account_to_company: Dictionary mapping account IDs to company IDs
            default_company_id: Default company ID for unmapped accounts
        """
        self._mapping = account_to_company
        self._default_company_id = default_company_id

    def get_company_id(self, account_id: str) -> int:
        """Get the company ID for an account.

        Args:
            account_id: Account identifier (string or numeric)

        Returns:
            Company ID (integer)
        """
        # Try exact match first
        company_id = self._mapping.get(str(account_id))
        if company_id is not None:
            return company_id

        # Try numeric conversion for cases like "urn:li:sponsoredAccount:123"
        numeric_id = self._extract_numeric_id(account_id)
        if numeric_id:
            company_id = self._mapping.get(numeric_id)
            if company_id is not None:
                return company_id

        logger.debug(
            f"Account '{account_id}' not found in mapping, using default company ID {self._default_company_id}"
        )
        return self._default_company_id

    def _extract_numeric_id(self, account_id: str) -> Optional[str]:
        """Extract numeric ID from URN or other formatted IDs.

        Args:
            account_id: Account identifier that may contain a numeric ID

        Returns:
            Numeric ID as string, or None if not found
        """
        # Match one or more digits
        match = re.search(r'\d+', account_id)
        return match.group(0) if match else None

    def add_mapping(self, account_id: str, company_id: int) -> None:
        """Add a new account-to-company mapping.

        Args:
            account_id: Account identifier
            company_id: Company ID to map to
        """
        self._mapping[str(account_id)] = company_id
        logger.info(f"Added mapping: account '{account_id}' -> company {company_id}")

    def get_all_mappings(self) -> Dict[str, int]:
        """Get all account-to-company mappings.

        Returns:
            Dictionary of all mappings
        """
        return self._mapping.copy()


class DateRangeCalculator:
    """Service for calculating date ranges for data extraction.

    This service implements business rules around date ranges, such as
    lookback periods for insights, date partitioning, and validation.
    """

    @staticmethod
    def get_insights_date_range(lookback_days: int = INSIGHTS_LOOKBACK_DAYS) -> DateRange:
        """Get the date range for insights extraction.

        Business rule: Extract insights from the last N days to avoid
        processing too much historical data.

        Args:
            lookback_days: Number of days to look back

        Returns:
            DateRange covering the lookback period
        """
        end_date = date.today() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=lookback_days - 1)

        return DateRange(start_date=start_date, end_date=end_date)

    @staticmethod
    def get_full_date_range(start_date: Optional[date] = None, end_date: Optional[date] = None) -> DateRange:
        """Get a date range with defaults for None values.

        Args:
            start_date: Start date (defaults to 90 days ago)
            end_date: End date (defaults to yesterday)

        Returns:
            DateRange with validated dates
        """
        if end_date is None:
            end_date = date.today() - timedelta(days=1)

        if start_date is None:
            start_date = end_date - timedelta(days=90)

        return DateRange(start_date=start_date, end_date=end_date)

    @staticmethod
    def split_into_daily_ranges(date_range: DateRange) -> List[DateRange]:
        """Split a date range into individual daily ranges.

        Useful for APIs that require daily granularity or for parallel processing.

        Args:
            date_range: Date range to split

        Returns:
            List of DateRange objects, one per day
        """
        daily_ranges = []
        current_date = date_range.start_date

        while current_date <= date_range.end_date:
            daily_ranges.append(DateRange(start_date=current_date, end_date=current_date))
            current_date += timedelta(days=1)

        return daily_ranges

    @staticmethod
    def split_into_weekly_ranges(date_range: DateRange) -> List[DateRange]:
        """Split a date range into weekly ranges.

        Useful for batch processing to reduce API calls.

        Args:
            date_range: Date range to split

        Returns:
            List of DateRange objects, approximately one week each
        """
        weekly_ranges = []
        current_start = date_range.start_date

        while current_start <= date_range.end_date:
            current_end = min(current_start + timedelta(days=6), date_range.end_date)
            weekly_ranges.append(DateRange(start_date=current_start, end_date=current_end))
            current_start = current_end + timedelta(days=1)

        return weekly_ranges

    @staticmethod
    def validate_date_range(start_date: date, end_date: date, max_days: Optional[int] = None) -> None:
        """Validate a date range according to business rules.

        Args:
            start_date: Start date
            end_date: End date
            max_days: Maximum allowed days in range (optional)

        Raises:
            DataValidationError: If validation fails
        """
        if start_date > end_date:
            raise DataValidationError(
                "Start date must be before or equal to end date",
                field="date_range",
                expected=f"start <= end",
                actual=f"{start_date} > {end_date}"
            )

        if end_date > date.today():
            raise DataValidationError(
                "End date cannot be in the future",
                field="end_date",
                expected=f"<= {date.today()}",
                actual=str(end_date)
            )

        if max_days is not None:
            days_diff = (end_date - start_date).days + 1
            if days_diff > max_days:
                raise DataValidationError(
                    f"Date range exceeds maximum of {max_days} days",
                    field="date_range",
                    expected=f"<= {max_days} days",
                    actual=f"{days_diff} days"
                )


class URNExtractor:
    """Service for extracting IDs from URN (Uniform Resource Name) formats.

    Many advertising platforms (especially LinkedIn) use URN format:
    urn:li:sponsoredAccount:123456789

    This service provides consistent URN parsing across the application.
    """

    # Common URN patterns for different entities
    URN_PATTERNS = {
        "account": r"urn:li:sponsoredAccount:(\d+)",
        "campaign": r"urn:li:sponsoredCampaign:(\d+)",
        "creative": r"urn:li:sponsoredCreative:(\d+)",
        "audience": r"urn:li:adSegment:(\d+)",
        "organization": r"urn:li:organization:(\d+)",
    }

    @classmethod
    def extract_id(cls, urn: str, entity_type: Optional[str] = None) -> Optional[str]:
        """Extract numeric ID from a URN.

        Args:
            urn: URN string (e.g., "urn:li:sponsoredAccount:123")
            entity_type: Expected entity type (account, campaign, etc.)
                        If provided, validates the URN matches this type

        Returns:
            Extracted ID as string, or None if extraction fails

        Raises:
            DataValidationError: If entity_type provided and URN doesn't match
        """
        if not urn or not isinstance(urn, str):
            return None

        # If entity type specified, use specific pattern
        if entity_type:
            pattern = cls.URN_PATTERNS.get(entity_type)
            if not pattern:
                logger.warning(f"Unknown entity type: {entity_type}")
                return cls._extract_any_numeric_id(urn)

            match = re.search(pattern, urn)
            if not match:
                raise DataValidationError(
                    f"Invalid URN format for {entity_type}",
                    field="urn",
                    expected=pattern,
                    actual=urn
                )
            return match.group(1)

        # Otherwise, try all patterns
        for pattern in cls.URN_PATTERNS.values():
            match = re.search(pattern, urn)
            if match:
                return match.group(1)

        # Fallback: extract any numeric ID
        return cls._extract_any_numeric_id(urn)

    @staticmethod
    def _extract_any_numeric_id(value: str) -> Optional[str]:
        """Extract any numeric ID from a string.

        Args:
            value: String potentially containing a numeric ID

        Returns:
            First numeric sequence found, or None
        """
        match = re.search(r'\d+', value)
        return match.group(0) if match else None

    @classmethod
    def build_urn(cls, entity_type: str, entity_id: str) -> str:
        """Build a URN from entity type and ID.

        Args:
            entity_type: Type of entity (account, campaign, etc.)
            entity_id: Numeric ID

        Returns:
            Formatted URN string

        Raises:
            DataValidationError: If entity_type is unknown
        """
        urn_templates = {
            "account": "urn:li:sponsoredAccount:{}",
            "campaign": "urn:li:sponsoredCampaign:{}",
            "creative": "urn:li:sponsoredCreative:{}",
            "audience": "urn:li:adSegment:{}",
            "organization": "urn:li:organization:{}",
        }

        template = urn_templates.get(entity_type)
        if not template:
            raise DataValidationError(
                f"Unknown entity type: {entity_type}",
                field="entity_type",
                expected=list(urn_templates.keys()),
                actual=entity_type
            )

        return template.format(entity_id)

    @classmethod
    def extract_bulk(cls, urns: List[str], entity_type: Optional[str] = None) -> List[str]:
        """Extract IDs from multiple URNs.

        Args:
            urns: List of URN strings
            entity_type: Expected entity type for all URNs

        Returns:
            List of extracted IDs (preserves order)
        """
        extracted_ids = []
        for urn in urns:
            try:
                extracted_id = cls.extract_id(urn, entity_type)
                if extracted_id:
                    extracted_ids.append(extracted_id)
            except DataValidationError as e:
                logger.warning(f"Failed to extract ID from URN '{urn}': {e}")

        return extracted_ids
