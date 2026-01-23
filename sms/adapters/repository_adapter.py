"""
SMS Repository Adapter.

Implements ISMSRepository interface for Vertica database operations.
Handles reading SMS campaigns and writing enriched data.

SQL queries are defined in sms.sql module following clean architecture:
- Domain logic separated from infrastructure
- Queries documented with business context
- Easy to maintain and audit
"""

from typing import List, Tuple

import vertica_python
from jinjasql import JinjaSql
from loguru import logger

from sms.domain.interfaces import ISMSRepository, RepositoryError
from sms.domain.models import Company, SMSCampaign, BitlyLink
from sms.sql import queries


class SMSRepositoryAdapter(ISMSRepository):
    """
    Adapter for SMS campaign database operations.

    Implements ISMSRepository interface using Vertica database.
    Uses JinjaSQL for safe SQL query building.

    SQL queries are defined in queries.py module for better separation
    of concerns and maintainability.

    Attributes:
        connection: Vertica database connection
        jinja_sql: JinjaSql instance for query templating
    """

    def __init__(self, connection: vertica_python.Connection):
        """
        Initialize repository adapter.

        Args:
            connection: Active Vertica database connection
        """
        self.connection = connection
        self.jinja_sql = JinjaSql(param_style="named")  # Vertica uses :name placeholders

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
        try:
            # Build query with JinjaSQL
            query, bind_params = self.jinja_sql.prepare_query(
                queries.QUERY_SMS_CAMPAIGNS,
                {
                    "company_id": company.company_id,
                    "years_behind": years_behind,
                },
            )

            logger.debug(f"Executing SMS campaigns query for {company.name}")
            logger.debug(f"Query: {query}")

            cursor = self.connection.cursor()
            cursor.execute(query, bind_params)
            results = cursor.fetchall()
            cursor.close()

            campaigns = [
                (row[0], row[1], row[2]) for row in results
            ]

            logger.info(f"Found {len(campaigns)} SMS campaigns for {company.name}")
            return campaigns

        except vertica_python.Error as e:
            raise RepositoryError(
                f"Failed to query SMS campaigns: {str(e)}"
            ) from e

    def save_campaigns(self, campaigns: List[SMSCampaign]) -> int:
        """
        Save SMS campaigns to database.

        Uses MERGE or INSERT to avoid duplicates.

        Args:
            campaigns: List of SMSCampaign objects to save

        Returns:
            Number of campaigns saved

        Raises:
            RepositoryError: If database insert fails
        """
        if not campaigns:
            return 0

        try:
            # First, filter out campaigns that already exist
            # Do this BEFORE creating the insert cursor to avoid cursor conflicts
            campaigns_to_save = []
            for campaign in campaigns:
                if self.campaign_exists(campaign.message_id):
                    logger.debug(
                        f"Campaign {campaign.message_id} already exists, skipping"
                    )
                else:
                    campaigns_to_save.append(campaign)

            if not campaigns_to_save:
                logger.info("No new campaigns to save (all already exist)")
                return 0

            # Now create cursor and insert all new campaigns
            cursor = self.connection.cursor()
            saved_count = 0

            for campaign in campaigns_to_save:
                # Build insert query
                query, bind_params = self.jinja_sql.prepare_query(
                    queries.INSERT_CAMPAIGN,
                    {
                        "message_id": campaign.message_id,
                        "activity_id": campaign.activity_id,
                        "campaign_name": campaign.campaign_name,
                        "company_id": campaign.company_id,
                        "sms_text": campaign.sms_text,
                        "sendout_date": campaign.sendout_date,
                        "sent_count": campaign.sent_count,
                        "delivered_count": campaign.delivered_count,
                        "bounced_count": campaign.bounced_count,
                        "acceptance_rate": campaign.acceptance_rate,
                    },
                )

                cursor.execute(query, bind_params)
                saved_count += 1

            self.connection.commit()
            cursor.close()

            logger.info(f"Saved {saved_count} SMS campaigns to database")
            return saved_count

        except vertica_python.Error as e:
            self.connection.rollback()
            raise RepositoryError(
                f"Failed to save SMS campaigns: {str(e)}"
            ) from e

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
        if not links:
            return 0

        try:
            cursor = self.connection.cursor()
            saved_count = 0

            for link in links:
                # Build insert query
                query, bind_params = self.jinja_sql.prepare_query(
                    queries.INSERT_LINK,
                    {
                        "message_id": link.message_id,
                        "activity_id": link.activity_id,
                        "bitly_short_url": link.bitly_short_url,
                        "bitly_long_url": link.bitly_long_url,
                        "total_clicks": link.total_clicks,
                    },
                )

                try:
                    cursor.execute(query, bind_params)
                    saved_count += 1
                except vertica_python.Error as e:
                    # Skip duplicate links (UNIQUE constraint violation)
                    if "duplicate key" in str(e).lower() or "unique" in str(e).lower():
                        logger.debug(
                            f"Link {link.bitly_short_url} already exists, skipping"
                        )
                        continue
                    raise

            self.connection.commit()
            cursor.close()

            logger.info(f"Saved {saved_count} Bitly links to database")
            return saved_count

        except vertica_python.Error as e:
            self.connection.rollback()
            raise RepositoryError(
                f"Failed to save Bitly links: {str(e)}"
            ) from e

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
        try:
            query, bind_params = self.jinja_sql.prepare_query(
                queries.CHECK_CAMPAIGN_EXISTS,
                {"message_id": message_id},
            )

            cursor = self.connection.cursor()
            cursor.execute(query, bind_params)
            result = cursor.fetchone()
            cursor.close()

            return result[0] > 0 if result else False

        except vertica_python.Error as e:
            raise RepositoryError(
                f"Failed to check campaign existence: {str(e)}"
            ) from e

    def upsert_links(self, links: List[BitlyLink]) -> int:
        """
        Upsert Bitly links - Insert new links or update clicks for existing.

        Uses UPDATE + INSERT strategy (Vertica doesn't support MERGE with
        IDENTITY columns). For each link:
        1. Check if link exists
        2. If exists: UPDATE total_clicks
        3. If not exists: INSERT new link

        Args:
            links: List of BitlyLink objects to upsert

        Returns:
            Number of links processed (inserted or updated)

        Raises:
            RepositoryError: If database operation fails
        """
        if not links:
            return 0

        try:
            updated_count = 0
            inserted_count = 0

            for link in links:
                # Check if link exists
                if self._link_exists(link.message_id, link.bitly_short_url):
                    # UPDATE existing link
                    self._update_link_clicks(link)
                    updated_count += 1
                else:
                    # INSERT new link
                    self._insert_link(link)
                    inserted_count += 1

            self.connection.commit()

            total = updated_count + inserted_count
            logger.info(
                f"Upserted {total} Bitly links "
                f"({updated_count} updated, {inserted_count} inserted)"
            )
            return total

        except vertica_python.Error as e:
            self.connection.rollback()
            raise RepositoryError(
                f"Failed to upsert Bitly links: {str(e)}"
            ) from e

    def _link_exists(self, message_id: int, bitly_short_url: str) -> bool:
        """Check if a Bitly link already exists in database."""
        query, bind_params = self.jinja_sql.prepare_query(
            queries.CHECK_LINK_EXISTS,
            {
                "message_id": message_id,
                "bitly_short_url": bitly_short_url,
            },
        )

        cursor = self.connection.cursor()
        cursor.execute(query, bind_params)
        result = cursor.fetchone()
        cursor.close()

        return result[0] > 0 if result else False

    def _update_link_clicks(self, link: BitlyLink) -> None:
        """Update total_clicks for an existing Bitly link."""
        query, bind_params = self.jinja_sql.prepare_query(
            queries.UPDATE_LINK_CLICKS,
            {
                "message_id": link.message_id,
                "bitly_short_url": link.bitly_short_url,
                "total_clicks": link.total_clicks,
            },
        )

        cursor = self.connection.cursor()
        cursor.execute(query, bind_params)
        cursor.close()

    def _insert_link(self, link: BitlyLink) -> None:
        """Insert a new Bitly link."""
        query, bind_params = self.jinja_sql.prepare_query(
            queries.INSERT_LINK,
            {
                "message_id": link.message_id,
                "activity_id": link.activity_id,
                "bitly_short_url": link.bitly_short_url,
                "bitly_long_url": link.bitly_long_url,
                "total_clicks": link.total_clicks,
            },
        )

        cursor = self.connection.cursor()
        cursor.execute(query, bind_params)
        cursor.close()

    def get_links_by_campaign(self, message_id: int) -> List[BitlyLink]:
        """
        Get existing Bitly links for a campaign.

        Used to refresh click counts for already processed campaigns
        without re-calling MAPP API.

        Args:
            message_id: MAPP message ID

        Returns:
            List of BitlyLink objects with current DB values

        Raises:
            RepositoryError: If database query fails
        """
        try:
            query, bind_params = self.jinja_sql.prepare_query(
                queries.GET_LINKS_BY_CAMPAIGN,
                {"message_id": message_id},
            )

            cursor = self.connection.cursor()
            cursor.execute(query, bind_params)
            rows = cursor.fetchall()
            cursor.close()

            links = []
            for row in rows:
                link = BitlyLink(
                    message_id=message_id,
                    activity_id="",  # Not returned by query, will be set by caller
                    bitly_short_url=row[0],
                    bitly_long_url=row[1],
                    total_clicks=row[2] or 0,
                )
                links.append(link)

            return links

        except vertica_python.Error as e:
            raise RepositoryError(
                f"Failed to get links for campaign {message_id}: {str(e)}"
            ) from e

    def get_campaigns_for_click_refresh(self, days_back: int = 90) -> List[tuple]:
        """
        Get campaigns that need click count refresh.

        Returns campaigns from the last N days that have Bitly links.
        Used by nightly job to efficiently update only recent campaigns.

        Args:
            days_back: Number of days to look back (default: 90)

        Returns:
            List of tuples (message_id, activity_id)

        Raises:
            RepositoryError: If database query fails
        """
        try:
            query, bind_params = self.jinja_sql.prepare_query(
                queries.GET_CAMPAIGNS_FOR_CLICK_REFRESH,
                {"days_back": days_back},
            )

            cursor = self.connection.cursor()
            cursor.execute(query, bind_params)
            rows = cursor.fetchall()
            cursor.close()

            campaigns = [(row[0], row[1]) for row in rows]

            logger.info(
                f"Found {len(campaigns)} campaigns for click refresh "
                f"(last {days_back} days)"
            )
            return campaigns

        except vertica_python.Error as e:
            raise RepositoryError(
                f"Failed to get campaigns for refresh: {str(e)}"
            ) from e

    def __repr__(self) -> str:
        """String representation of adapter."""
        return f"SMSRepositoryAdapter(connection={self.connection})"
