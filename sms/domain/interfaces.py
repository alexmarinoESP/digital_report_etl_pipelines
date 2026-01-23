"""
Domain interfaces for SMS campaign pipeline.

This module defines the abstractions that decouple the domain from infrastructure.
Following Dependency Inversion Principle - depend on abstractions, not concretions.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

from sms.domain.models import Company, SMSCampaign, BitlyLink


# ============================================================================
# Custom Exceptions
# ============================================================================


class SMSError(Exception):
    """Base exception for SMS pipeline errors."""
    pass


class ExtractionError(SMSError):
    """Exception raised during SMS extraction from data sources."""
    pass


class APIError(SMSError):
    """Exception raised during external API calls (MAPP, Bitly)."""
    pass


class RepositoryError(SMSError):
    """Exception raised during database operations."""
    pass


# ============================================================================
# Repository Interfaces
# ============================================================================


class ISMSRepository(ABC):
    """
    Interface for SMS campaign data repository.

    Abstracts database access for SMS campaigns.
    Implementations handle specific database systems (Vertica, PostgreSQL, etc.).
    """

    @abstractmethod
    def get_sms_campaigns(
        self,
        company: Company,
        years_behind: int = 2,
    ) -> List[Tuple[int, str, int]]:
        """
        Get SMS campaigns from database.

        Args:
            company: Company to filter by
            years_behind: Number of years to look back

        Returns:
            List of tuples (message_id, activity_id, contact_id)

        Raises:
            RepositoryError: If database query fails
        """
        pass

    @abstractmethod
    def save_campaigns(self, campaigns: List[SMSCampaign]) -> int:
        """
        Save SMS campaigns to database.

        Args:
            campaigns: List of SMSCampaign objects to save

        Returns:
            Number of campaigns saved

        Raises:
            RepositoryError: If database insert fails
        """
        pass

    @abstractmethod
    def save_links(self, links: List[BitlyLink]) -> int:
        """
        Save Bitly links to database.

        Args:
            links: List of BitlyLink objects to save

        Returns:
            Number of links saved

        Raises:
            RepositoryError: If database insert fails
        """
        pass

    @abstractmethod
    def campaign_exists(self, message_id: int) -> bool:
        """
        Check if campaign already exists in database.

        Args:
            message_id: MAPP message ID to check

        Returns:
            True if campaign exists, False otherwise

        Raises:
            RepositoryError: If database query fails
        """
        pass

    @abstractmethod
    def upsert_links(self, links: List[BitlyLink]) -> int:
        """
        Upsert Bitly links - Insert new or update existing.

        Uses MERGE for atomic upsert. Preferred for nightly refresh jobs.

        Args:
            links: List of BitlyLink objects to upsert

        Returns:
            Number of links processed

        Raises:
            RepositoryError: If database operation fails
        """
        pass

    @abstractmethod
    def get_links_by_campaign(self, message_id: int) -> List[BitlyLink]:
        """
        Get existing Bitly links for a campaign.

        Args:
            message_id: MAPP message ID

        Returns:
            List of BitlyLink objects

        Raises:
            RepositoryError: If database query fails
        """
        pass

    @abstractmethod
    def get_campaigns_for_click_refresh(self, days_back: int = 90) -> List[Tuple[int, str]]:
        """
        Get campaigns that need click count refresh.

        Args:
            days_back: Number of days to look back

        Returns:
            List of tuples (message_id, activity_id)

        Raises:
            RepositoryError: If database query fails
        """
        pass


# ============================================================================
# External API Interfaces
# ============================================================================


class IMappSMSClient(ABC):
    """
    Interface for MAPP API client for SMS data.

    Abstracts MAPP Engage API interactions.
    Implementations handle authentication, rate limiting, retries.
    """

    @abstractmethod
    def get_sms_statistics(
        self,
        message_id: int,
    ) -> dict:
        """
        Get SMS campaign statistics from MAPP.

        Args:
            message_id: MAPP message ID

        Returns:
            Dictionary with statistics:
                - sent_count: Number of SMS sent
                - delivered_count: Number delivered
                - bounced_count: Number bounced
                - sendout_date: Send date

        Raises:
            APIError: If MAPP API call fails
        """
        pass

    @abstractmethod
    def get_sms_content(
        self,
        message_id: int,
        contact_id: int,
    ) -> dict:
        """
        Get SMS message content from MAPP.

        Args:
            message_id: MAPP message ID
            contact_id: Contact ID (required for preparedmessage endpoint)

        Returns:
            Dictionary with content:
                - sms_text: SMS message text
                - campaign_name: Optional campaign name

        Raises:
            APIError: If MAPP API call fails
        """
        pass


class IBitlyClient(ABC):
    """
    Interface for Bitly API client.

    Abstracts Bitly API interactions for link information and click statistics.
    Implementations handle authentication and rate limiting.
    """

    @abstractmethod
    def get_link_info(self, bitlink_id: str) -> dict:
        """
        Get Bitly link information.

        Args:
            bitlink_id: Bitly link ID (e.g., 'bit.ly/abc123')

        Returns:
            Dictionary with link info:
                - short_url: Shortened URL
                - long_url: Original long URL
                - title: Optional link title

        Raises:
            APIError: If Bitly API call fails
        """
        pass

    @abstractmethod
    def get_total_clicks(self, bitlink_id: str) -> int:
        """
        Get total clicks for a Bitly link (all time).

        Args:
            bitlink_id: Bitly link ID (e.g., 'bit.ly/abc123')

        Returns:
            Total number of clicks

        Raises:
            APIError: If Bitly API call fails
        """
        pass

    @abstractmethod
    def enrich_link(self, link: BitlyLink) -> BitlyLink:
        """
        Enrich a BitlyLink with data from Bitly API.

        Convenience method that calls get_link_info and get_total_clicks,
        then creates a new BitlyLink with updated data.

        Args:
            link: BitlyLink to enrich

        Returns:
            New BitlyLink with enriched data

        Raises:
            APIError: If Bitly API calls fail
        """
        pass
