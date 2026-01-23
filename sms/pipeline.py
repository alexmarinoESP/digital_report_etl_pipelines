"""
SMS Pipeline Orchestrator.

Main entry point for the SMS ETL process.
Orchestrates extraction, enrichment, and database persistence.

This module follows the Dependency Injection pattern:
- All dependencies are injected through the constructor
- Allows easy testing with mocks
- Follows SOLID principles throughout
"""

from dataclasses import dataclass
from typing import List, Optional

from loguru import logger

from sms.domain.models import Company, SMSCampaign, BitlyLink, PipelineStats
from sms.domain.interfaces import (
    ISMSRepository,
    IMappSMSClient,
    IBitlyClient,
    SMSError,
)
from sms.services.extraction_service import ExtractionService


@dataclass
class PipelineResult:
    """
    Result of pipeline execution.

    Attributes:
        extraction_stats: Statistics for extraction stage
        campaigns_extracted: Number of campaigns extracted
        campaigns_saved: Number of campaigns saved to database
        links_saved: Number of Bitly links saved to database
        success: Whether pipeline completed without critical failures
    """
    extraction_stats: PipelineStats
    campaigns_extracted: int
    campaigns_saved: int
    links_saved: int

    @property
    def success(self) -> bool:
        """Check if pipeline completed without critical failures."""
        return self.extraction_stats.failed == 0

    def __str__(self) -> str:
        """String representation of result."""
        return (
            f"Pipeline Result:\n"
            f"  Extraction: {self.extraction_stats}\n"
            f"  Campaigns: {self.campaigns_extracted} extracted, "
            f"{self.campaigns_saved} saved\n"
            f"  Links: {self.links_saved} saved\n"
            f"  Success: {self.success}"
        )


class SMSPipeline:
    """
    Main orchestrator for SMS ETL pipeline.

    Coordinates the two stages:
    1. Extraction - Get SMS data from database and enrich with MAPP/Bitly
    2. Persistence - Save enriched data to database

    Follows:
    - Single Responsibility: Only orchestrates, delegates to services
    - Dependency Inversion: Depends only on abstractions
    - Open/Closed: New stages/behaviors via composition
    """

    def __init__(
        self,
        repository: ISMSRepository,
        mapp_clients: dict[Company, IMappSMSClient],
        bitly_client: IBitlyClient,
    ):
        """
        Initialize pipeline with all dependencies.

        Args:
            repository: SMS data repository
            mapp_clients: Dict mapping Company to MAPP client instances
            bitly_client: Bitly client for link enrichment
        """
        self._repository = repository
        self._mapp_clients = mapp_clients
        self._bitly_client = bitly_client

        # Initialize extraction service
        self._extraction_service = ExtractionService(
            repository=repository,
            mapp_clients=mapp_clients,
            bitly_client=bitly_client,
        )

    def run(
        self,
        companies: Optional[List[Company]] = None,
        years_behind: int = 2,
        enrich_bitly: bool = True,
        skip_existing: bool = True,
    ) -> PipelineResult:
        """
        Execute the full SMS pipeline.

        Args:
            companies: List of companies to process (all if None)
            years_behind: Number of years to look back
            enrich_bitly: Whether to enrich links with Bitly API
            skip_existing: Whether to skip existing campaigns

        Returns:
            PipelineResult with statistics

        Raises:
            SMSError: If pipeline execution fails
        """
        logger.info("=" * 60)
        logger.info("Starting SMS Pipeline")
        logger.info("=" * 60)

        # Stage 1: Extraction
        logger.info("Stage 1: Extraction and Enrichment")
        campaigns, extraction_stats = self._extraction_service.extract_campaigns(
            companies=companies,
            years_behind=years_behind,
            enrich_bitly=enrich_bitly,
        )

        if not campaigns:
            logger.warning("No SMS campaigns extracted")
            return PipelineResult(
                extraction_stats=extraction_stats,
                campaigns_extracted=0,
                campaigns_saved=0,
                links_saved=0,
            )

        logger.info(f"Extracted {len(campaigns)} SMS campaigns")

        # Stage 2: Persistence
        logger.info("Stage 2: Database Persistence")

        # Filter existing campaigns if requested
        if skip_existing:
            campaigns = self._filter_existing_campaigns(campaigns)
            logger.info(
                f"After filtering existing: {len(campaigns)} campaigns to save"
            )

        # Save campaigns
        campaigns_saved = self._repository.save_campaigns(campaigns)

        # Extract and upsert all links (insert new, update existing)
        all_links = []
        for campaign in campaigns:
            all_links.extend(campaign.bitly_links)

        links_saved = 0
        if all_links:
            links_saved = self._repository.upsert_links(all_links)

        # Build result
        result = PipelineResult(
            extraction_stats=extraction_stats,
            campaigns_extracted=len(campaigns),
            campaigns_saved=campaigns_saved,
            links_saved=links_saved,
        )

        logger.info("=" * 60)
        logger.info(str(result))
        logger.info("=" * 60)

        return result

    def run_extraction_only(
        self,
        companies: Optional[List[Company]] = None,
        years_behind: int = 2,
        enrich_bitly: bool = True,
    ) -> tuple[List[SMSCampaign], PipelineStats]:
        """
        Run only the extraction stage.

        Args:
            companies: List of companies to process (all if None)
            years_behind: Number of years to look back
            enrich_bitly: Whether to enrich with Bitly data

        Returns:
            Tuple of (campaigns, stats)
        """
        return self._extraction_service.extract_campaigns(
            companies=companies,
            years_behind=years_behind,
            enrich_bitly=enrich_bitly,
        )

    def _filter_existing_campaigns(
        self,
        campaigns: List[SMSCampaign],
    ) -> List[SMSCampaign]:
        """
        Filter out campaigns that already exist in database.

        Args:
            campaigns: List of campaigns to filter

        Returns:
            List of campaigns that don't exist in database
        """
        filtered = []
        for campaign in campaigns:
            if not self._repository.campaign_exists(campaign.message_id):
                filtered.append(campaign)
            else:
                logger.debug(
                    f"Campaign {campaign.message_id} already exists, skipping"
                )
        return filtered

    def refresh_clicks(self, days_back: int = 90) -> dict:
        """
        Refresh Bitly click counts for existing campaigns.

        This method is designed for nightly jobs to update click statistics
        without re-processing entire campaigns. It only calls Bitly API,
        not MAPP API.

        Flow:
        1. Get campaigns with links from last N days
        2. For each campaign, get existing links from DB
        3. Call Bitly API to get updated click counts
        4. Upsert links with new click values

        Args:
            days_back: Number of days to look back (default: 90)

        Returns:
            Dictionary with refresh statistics:
                - campaigns_processed: Number of campaigns checked
                - links_updated: Number of links updated
                - errors: Number of errors encountered
        """
        logger.info("=" * 60)
        logger.info(f"Refreshing Bitly clicks (last {days_back} days)")
        logger.info("=" * 60)

        stats = {
            "campaigns_processed": 0,
            "links_updated": 0,
            "errors": 0,
        }

        # Get campaigns that need refresh
        campaigns = self._repository.get_campaigns_for_click_refresh(days_back)

        if not campaigns:
            logger.info("No campaigns found for click refresh")
            return stats

        logger.info(f"Found {len(campaigns)} campaigns to refresh")

        for message_id, activity_id in campaigns:
            try:
                # Get existing links from DB
                links = self._repository.get_links_by_campaign(message_id)

                if not links:
                    logger.debug(f"Campaign {message_id} has no links, skipping")
                    continue

                # Enrich links with updated Bitly click counts
                updated_links = []
                for link in links:
                    try:
                        # Set activity_id (not returned by DB query)
                        from dataclasses import replace
                        link_with_activity = replace(link, activity_id=activity_id)

                        # Call Bitly API for updated clicks
                        enriched = self._bitly_client.enrich_link(link_with_activity)
                        updated_links.append(enriched)

                    except Exception as e:
                        logger.warning(
                            f"Failed to refresh link {link.bitly_short_url}: {e}"
                        )
                        stats["errors"] += 1

                # Upsert links with updated click counts
                if updated_links:
                    self._repository.upsert_links(updated_links)
                    stats["links_updated"] += len(updated_links)

                stats["campaigns_processed"] += 1

            except Exception as e:
                logger.error(f"Failed to refresh campaign {message_id}: {e}")
                stats["errors"] += 1

        logger.info("=" * 60)
        logger.info(f"Click refresh complete:")
        logger.info(f"  Campaigns processed: {stats['campaigns_processed']}")
        logger.info(f"  Links updated: {stats['links_updated']}")
        logger.info(f"  Errors: {stats['errors']}")
        logger.info("=" * 60)

        return stats


class PipelineFactory:
    """
    Factory for creating pipeline instances with default dependencies.

    Provides convenience methods for common configurations.
    Following DRY principle - common initialization extracted to _create_pipeline.
    """

    @staticmethod
    def create_default(connection) -> SMSPipeline:
        """
        Create pipeline with default production dependencies.

        Args:
            connection: Active Vertica database connection

        Returns:
            Configured SMSPipeline instance
        """
        from sms.adapters import (
            SMSRepositoryAdapter,
            MappSMSAdapter,
            BitlyAdapter,
        )

        # Create repository
        repository = SMSRepositoryAdapter(connection=connection)

        # Create MAPP clients for each company
        mapp_clients = {
            Company.IT: MappSMSAdapter(Company.IT),
            Company.ES: MappSMSAdapter(Company.ES),
            Company.VVIT: MappSMSAdapter(Company.VVIT),
        }

        # Create Bitly client
        bitly_client = BitlyAdapter()

        return SMSPipeline(
            repository=repository,
            mapp_clients=mapp_clients,
            bitly_client=bitly_client,
        )

    @staticmethod
    def create_custom(
        repository: ISMSRepository,
        mapp_clients: dict[Company, IMappSMSClient],
        bitly_client: IBitlyClient,
    ) -> SMSPipeline:
        """
        Create pipeline with custom dependencies.

        Use this for testing with mocks or custom implementations.

        Args:
            repository: Custom ISMSRepository implementation
            mapp_clients: Custom MAPP client implementations
            bitly_client: Custom IBitlyClient implementation

        Returns:
            Configured SMSPipeline instance
        """
        return SMSPipeline(
            repository=repository,
            mapp_clients=mapp_clients,
            bitly_client=bitly_client,
        )
