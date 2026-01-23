"""
Extraction Service.

Business logic for extracting and enriching SMS campaigns.
Orchestrates repository, MAPP client, Bitly client, and link utilities.
"""

from typing import List, Optional, Tuple

from loguru import logger

from sms.domain.interfaces import (
    ISMSRepository,
    IMappSMSClient,
    IBitlyClient,
    ExtractionError,
    APIError,
)
from sms.domain.models import Company, SMSCampaign, BitlyLink, PipelineStats
from sms.services.link_utils import extract_bitly_links


class ExtractionService:
    """
    Service for extracting and enriching SMS campaigns.

    Orchestrates the following steps:
    1. Query database for SMS campaign IDs
    2. Fetch SMS statistics from MAPP
    3. Fetch SMS content from MAPP
    4. Extract Bitly links from SMS text
    5. Enrich links with Bitly API data

    Follows Single Responsibility Principle - only handles extraction logic.
    """

    def __init__(
        self,
        repository: ISMSRepository,
        mapp_clients: dict[Company, IMappSMSClient],
        bitly_client: IBitlyClient,
    ):
        """
        Initialize extraction service.

        Args:
            repository: SMS repository for database access
            mapp_clients: Dict mapping Company to MAPP client instances
            bitly_client: Bitly client for link enrichment
        """
        self.repository = repository
        self.mapp_clients = mapp_clients
        self.bitly_client = bitly_client

    def extract_campaigns(
        self,
        companies: Optional[List[Company]] = None,
        years_behind: int = 2,
        enrich_bitly: bool = True,
    ) -> Tuple[List[SMSCampaign], PipelineStats]:
        """
        Extract SMS campaigns for specified companies.

        Args:
            companies: List of companies to process (all if None)
            years_behind: Number of years to look back
            enrich_bitly: Whether to enrich links with Bitly API data

        Returns:
            Tuple of (campaigns list, statistics)

        Raises:
            ExtractionError: If extraction fails
        """
        if companies is None:
            companies = list(Company)

        stats = PipelineStats()
        all_campaigns = []

        logger.info(f"Starting SMS extraction for {len(companies)} companies")

        for company in companies:
            logger.info(f"Processing company: {company.name}")

            try:
                campaigns = self._extract_company_campaigns(
                    company=company,
                    years_behind=years_behind,
                    enrich_bitly=enrich_bitly,
                )
                all_campaigns.extend(campaigns)
                stats.success += len(campaigns)

            except ExtractionError as e:
                logger.error(f"Failed to extract campaigns for {company.name}: {e}")
                stats.increment_failed()

        logger.info(
            f"Extraction complete: {len(all_campaigns)} campaigns extracted"
        )
        logger.info(f"Stats: {stats}")

        return all_campaigns, stats

    def _extract_company_campaigns(
        self,
        company: Company,
        years_behind: int,
        enrich_bitly: bool,
    ) -> List[SMSCampaign]:
        """
        Extract SMS campaigns for a single company.

        Args:
            company: Company to process
            years_behind: Number of years to look back
            enrich_bitly: Whether to enrich with Bitly data

        Returns:
            List of extracted SMS campaigns

        Raises:
            ExtractionError: If extraction fails
        """
        # Get MAPP client for company
        mapp_client = self.mapp_clients.get(company)
        if not mapp_client:
            raise ExtractionError(
                f"No MAPP client configured for company {company.name}"
            )

        # Query database for SMS campaigns
        campaign_ids = self.repository.get_sms_campaigns(
            company=company,
            years_behind=years_behind,
        )

        if not campaign_ids:
            logger.info(f"No SMS campaigns found for {company.name}")
            return []

        logger.info(
            f"Found {len(campaign_ids)} SMS campaigns for {company.name}"
        )

        # Extract each campaign
        campaigns = []
        for message_id, activity_id, contact_id in campaign_ids:
            try:
                campaign = self._extract_single_campaign(
                    message_id=message_id,
                    activity_id=activity_id,
                    contact_id=contact_id,
                    company_id=company.company_id,
                    mapp_client=mapp_client,
                    enrich_bitly=enrich_bitly,
                )
                campaigns.append(campaign)

            except (APIError, ValueError) as e:
                logger.error(
                    f"Failed to extract campaign {message_id}: {e}"
                )
                continue

        return campaigns

    def _extract_single_campaign(
        self,
        message_id: int,
        activity_id: str,
        contact_id: int,
        company_id: int,
        mapp_client: IMappSMSClient,
        enrich_bitly: bool,
    ) -> SMSCampaign:
        """
        Extract a single SMS campaign with full enrichment.

        Args:
            message_id: MAPP message ID
            activity_id: Activity code
            contact_id: Contact ID for MAPP API
            company_id: Company identifier
            mapp_client: MAPP client to use
            enrich_bitly: Whether to enrich with Bitly data

        Returns:
            Fully enriched SMSCampaign

        Raises:
            APIError: If MAPP or Bitly API calls fail
        """
        logger.debug(f"Extracting campaign {message_id}")

        # Fetch statistics from MAPP
        stats = mapp_client.get_sms_statistics(message_id)

        # Fetch content from MAPP
        content = mapp_client.get_sms_content(message_id, contact_id)

        # Extract Bitly links from SMS text
        sms_text = content.get("sms_text")
        bitly_links = []

        if sms_text:
            bitly_links = extract_bitly_links(
                sms_text=sms_text,
                message_id=message_id,
                activity_id=activity_id,
            )

            # Enrich links with Bitly API data if requested
            if enrich_bitly and bitly_links:
                bitly_links = self._enrich_links(bitly_links)

        # Create campaign object
        campaign = SMSCampaign(
            message_id=message_id,
            activity_id=activity_id,
            campaign_name=content.get("campaign_name"),
            company_id=company_id,
            sms_text=sms_text,
            sendout_date=stats.get("sendout_date"),
            sent_count=stats.get("sent_count"),
            delivered_count=stats.get("delivered_count"),
            bounced_count=stats.get("bounced_count"),
            acceptance_rate=stats.get("acceptance_rate"),
            bitly_links=bitly_links,
        )

        logger.info(
            f"Extracted campaign {message_id}: "
            f"{len(bitly_links)} links, {campaign.sent_count} sent"
        )

        return campaign

    def _enrich_links(self, links: List[BitlyLink]) -> List[BitlyLink]:
        """
        Enrich Bitly links with API data.

        Args:
            links: List of BitlyLink objects to enrich

        Returns:
            List of enriched BitlyLink objects
        """
        enriched_links = []

        for link in links:
            try:
                enriched_link = self.bitly_client.enrich_link(link)
                enriched_links.append(enriched_link)

            except APIError as e:
                logger.warning(
                    f"Failed to enrich link {link.bitly_short_url}: {e}. "
                    "Using partial data."
                )
                # Use original link with partial data
                enriched_links.append(link)

        return enriched_links

    def __repr__(self) -> str:
        """String representation of service."""
        return (
            f"ExtractionService("
            f"companies={len(self.mapp_clients)}, "
            f"bitly_enabled={self.bitly_client is not None})"
        )
