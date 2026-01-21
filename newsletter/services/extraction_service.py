"""
Newsletter extraction service.
Handles retrieving and enriching newsletter data.
"""

from typing import List, Optional, Tuple

from loguru import logger

from newsletter.domain.models import Newsletter, Company, PipelineStats
from newsletter.domain.interfaces import INewsletterRepository, IMappClient, ExtractionError


class ExtractionService:
    """
    Service for extracting and enriching newsletter data.

    Follows:
    - Single Responsibility: Only handles data extraction
    - Dependency Inversion: Depends on abstractions (interfaces)
    - Open/Closed: New sources can be added without modification
    """

    def __init__(
        self,
        repository: INewsletterRepository,
        mapp_clients: Optional[dict] = None,
    ):
        """
        Initialize extraction service.

        Args:
            repository: Newsletter repository for data retrieval
            mapp_clients: Dict mapping Company to IMappClient instances
        """
        self._repository = repository
        self._mapp_clients = mapp_clients or {}

    def set_mapp_client(self, company: Company, client: IMappClient) -> None:
        """
        Set Mapp client for a company.

        Args:
            company: Company
            client: IMappClient instance
        """
        self._mapp_clients[company] = client

    def _enrich_mapp_newsletter(
        self, newsletter: Newsletter, client: IMappClient
    ) -> Newsletter:
        """
        Enrich a Mapp newsletter with HTML content.

        Args:
            newsletter: Newsletter to enrich
            client: Mapp client for API calls

        Returns:
            Enriched Newsletter
        """
        if not newsletter.message_id or not newsletter.contact_id:
            logger.warning(
                f"Missing message_id or contact_id for {newsletter.newsletter_id}"
            )
            return newsletter

        try:
            # Get preview data
            data = client.get_preview_data(
                message_id=newsletter.message_id,
                contact_id=newsletter.contact_id,
            )

            if data:
                newsletter.html_content = data.get("htmlVersion", "")
                external_id = data.get("externalId")
                if external_id:
                    newsletter.image_name = f"{external_id}.png"

        except Exception as e:
            logger.warning(
                f"Failed to enrich newsletter {newsletter.newsletter_id}: {e}"
            )

        return newsletter

    def extract_newsletters(
        self,
        companies: Optional[List[Company]] = None,
        sources: Optional[List[str]] = None,
        years_behind: int = 2,
        enrich_mapp: bool = True,
    ) -> Tuple[List[Newsletter], PipelineStats]:
        """
        Extract newsletters from all sources.

        Args:
            companies: List of companies (all if None)
            sources: List of sources ['mapp', 'dynamics'] (all if None)
            years_behind: Number of years to look back
            enrich_mapp: Whether to enrich Mapp newsletters with HTML

        Returns:
            Tuple of (newsletters, stats)
        """
        if companies is None:
            companies = [Company.IT, Company.ES, Company.VVIT]

        if sources is None:
            sources = ["mapp", "dynamics"]

        stats = PipelineStats()
        all_newsletters = []

        for company in companies:
            # Extract Mapp newsletters
            if "mapp" in sources:
                newsletters = self._repository.get_mapp_newsletters(
                    company, years_behind
                )

                if enrich_mapp and company in self._mapp_clients:
                    client = self._mapp_clients[company]
                    for nl in newsletters:
                        self._enrich_mapp_newsletter(nl, client)
                        if nl.has_content:
                            stats.add_processed()
                        else:
                            stats.add_failed(
                                f"No content for Mapp newsletter {nl.newsletter_id}"
                            )

                all_newsletters.extend(newsletters)

            # Extract Dynamics newsletters
            if "dynamics" in sources:
                newsletters = self._repository.get_dynamics_newsletters(
                    company, years_behind
                )

                for nl in newsletters:
                    if nl.has_content:
                        stats.add_processed()
                    else:
                        stats.add_failed(
                            f"No content for Dynamics newsletter {nl.newsletter_id}"
                        )

                all_newsletters.extend(newsletters)

        logger.info(f"Extraction completed: {stats}")
        return all_newsletters, stats

    def extract_for_company(
        self,
        company: Company,
        source: str,
        years_behind: int = 2,
        enrich_mapp: bool = True,
    ) -> List[Newsletter]:
        """
        Extract newsletters for a single company and source.

        Args:
            company: Company to extract for
            source: Source ('mapp' or 'dynamics')
            years_behind: Number of years to look back
            enrich_mapp: Whether to enrich Mapp newsletters

        Returns:
            List of Newsletter objects
        """
        newsletters, _ = self.extract_newsletters(
            companies=[company],
            sources=[source],
            years_behind=years_behind,
            enrich_mapp=enrich_mapp,
        )
        return newsletters
