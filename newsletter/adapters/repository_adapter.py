"""
Newsletter repository adapter.
Implements INewsletterRepository interface for Vertica database.

Contains all database access logic including:
- SQL queries
- Cursor wrapper
- Data mapping to domain models
"""

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from jinjasql import JinjaSql
from loguru import logger

from newsletter.domain.interfaces import INewsletterRepository
from newsletter.domain.models import Newsletter, Company
from shared.connection.vertica import VerticaConnection


# =============================================================================
# SQL Queries
# =============================================================================

QUERY_MAPP_NEWSLETTERS = """
SELECT DISTINCT d.COMPANYID, f.MESSAGE_EXTERNALID AS NEWSLETTERID, f.MESSAGE_ID, max(f.USER_ID) as CONTACT_ID,
min(f.USER_ID) as CONTACT_ID_2
FROM ESPODS.ESP_ODS_MAPP_RENDER f
JOIN ESPDDS.ESP_DCAMPAIGN_NEW d
on d.newsletterid = f.message_externalid
WHERE YEAR(CAST(f.RENDER_TIMESTAMP AS DATE)) >= YEAR(CURRENT_DATE)-{{years_behind | sqlsafe}}
AND d.COMPANYID={{comp_id | sqlsafe}}
GROUP BY d.COMPANYID, f.MESSAGE_EXTERNALID, f.MESSAGE_ID
"""

QUERY_DYNAMICS_NEWSLETTERS = """
select distinct d2.COMPANYID, mm.CAMPAIGNACTIVITY_CODE as NEWSLETTERID, mm.PREVIEWHTML
from ESPDDS.ESP_DCRM_MARKETING_EMAIL mm
JOIN ESPDDS.CRM_CAMPAIGNINFO d
on d.CODE = SPLIT_PART(CAMPAIGNACTIVITY_CODE, '.', 1)
JOIN ESPDDS.ESP_DCAMPAIGN_NEW d2
on d.CODE = d2.CAMPAIGNID
WHERE (CASE WHEN PROPOSEDSTART is not null THEN
       TO_CHAR(PROPOSEDSTART, 'RRRR') - 1
       ELSE TO_CHAR(ACTUALSTART, 'RRRR') - 1 END) >= YEAR(CURRENT_DATE)-{{years_behind | sqlsafe}}
AND d2.COMPANYID={{comp_id | sqlsafe}}  AND mm.PREVIEWHTML is not null
ORDER BY mm.CAMPAIGNACTIVITY_CODE
"""


# =============================================================================
# Database Cursor
# =============================================================================

class DatabaseCursor:
    """
    Cursor wrapper for database operations.
    Supports JinjaSQL templating for parameterized queries.
    """

    def __init__(self, connection: Any, db_type: str = "vertica"):
        """
        Initialize cursor.

        Args:
            connection: Database connection object
            db_type: Database type (vertica, oracle)
        """
        self._cursor = connection.cursor()
        self._db_type = db_type
        self._jinja = JinjaSql()

    def execute_query(
        self,
        query: str,
        params: Optional[Dict] = None,
        description: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Execute query and return DataFrame.

        Args:
            query: SQL query (JinjaSQL template)
            params: Parameters for JinjaSQL template
            description: Description for logging

        Returns:
            DataFrame with query results
        """
        # Prepare query with parameters
        if params:
            prepared_query, bind_params = self._jinja.prepare_query(query, params)
            if self._db_type == "vertica":
                self._cursor.execute(prepared_query, bind_params)
            else:
                self._cursor.execute(prepared_query)
        else:
            self._cursor.execute(query)

        # Fetch results
        rows = self._cursor.fetchall()

        # Decode bytes to UTF-8
        for row in rows:
            for i in range(len(row)):
                try:
                    row[i] = str(row[i]).encode("utf-8")
                except Exception:
                    pass

        # Create DataFrame
        columns = [col[0] for col in self._cursor.description]
        df = pd.DataFrame(rows, columns=columns)

        logger.info(f"Extracted {len(df)} rows. Information: {description}")
        return df

    def close(self) -> None:
        """Close the cursor."""
        self._cursor.close()


# =============================================================================
# Repository Adapter
# =============================================================================

class NewsletterRepositoryAdapter(INewsletterRepository):
    """
    Adapter for newsletter data retrieval from Vertica.
    Implements INewsletterRepository interface.

    Follows:
    - Single Responsibility: Only handles data retrieval
    - Dependency Inversion: Implements abstract interface
    - Open/Closed: New data sources can be added as new adapters
    """

    def __init__(self, connection: Optional[VerticaConnection] = None):
        """
        Initialize repository adapter.

        Args:
            connection: Vertica connection (creates new if not provided)
        """
        self._connection = connection or VerticaConnection()

    def _decode_bytes(self, value: Any) -> str:
        """Decode bytes to string if needed."""
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value) if value else ""

    def get_mapp_newsletters(
        self, company: Company, years_behind: int
    ) -> List[Newsletter]:
        """
        Retrieve Mapp newsletters for a company.

        Args:
            company: Company to retrieve newsletters for
            years_behind: Number of years to look back

        Returns:
            List of Newsletter objects (without HTML content - needs enrichment)
        """
        try:
            with self._connection.get_connection() as conn:
                cursor = DatabaseCursor(conn, db_type="vertica")
                df = cursor.execute_query(
                    query=QUERY_MAPP_NEWSLETTERS,
                    params={"years_behind": years_behind, "comp_id": company.company_id},
                    description=f"Querying Mapp newsletters for {company.code}",
                )

            if df.empty:
                logger.info(f"No Mapp newsletters found for {company.code}")
                return []

            # Remove duplicates
            df.drop_duplicates(subset=["NEWSLETTERID"], inplace=True)

            # Convert to Newsletter objects
            newsletters = []
            for _, row in df.iterrows():
                newsletter = Newsletter(
                    newsletter_id=self._decode_bytes(row.get("NEWSLETTERID", "")),
                    company=company,
                    source="mapp",
                    message_id=int(row.get("MESSAGE_ID", 0)) if row.get("MESSAGE_ID") else None,
                    contact_id=int(row.get("CONTACT_ID", 0)) if row.get("CONTACT_ID") else None,
                    contact_id_2=int(row.get("CONTACT_ID_2", 0)) if row.get("CONTACT_ID_2") else None,
                )
                newsletters.append(newsletter)

            logger.info(f"Retrieved {len(newsletters)} Mapp newsletters for {company.code}")
            return newsletters

        except Exception as e:
            logger.warning(f"Error retrieving Mapp newsletters for {company.code}: {e}")
            return []

    def get_dynamics_newsletters(
        self, company: Company, years_behind: int
    ) -> List[Newsletter]:
        """
        Retrieve Dynamics newsletters for a company.

        Args:
            company: Company to retrieve newsletters for
            years_behind: Number of years to look back

        Returns:
            List of Newsletter objects (with HTML content)
        """
        try:
            with self._connection.get_connection() as conn:
                cursor = DatabaseCursor(conn, db_type="vertica")
                df = cursor.execute_query(
                    query=QUERY_DYNAMICS_NEWSLETTERS,
                    params={"years_behind": years_behind, "comp_id": company.company_id},
                    description=f"Querying Dynamics newsletters for {company.code}",
                )

            if df.empty:
                logger.info(f"No Dynamics newsletters found for {company.code}")
                return []

            # Convert to Newsletter objects
            newsletters = []
            for _, row in df.iterrows():
                newsletter = Newsletter(
                    newsletter_id=self._decode_bytes(row.get("NEWSLETTERID", "")),
                    company=company,
                    source="dynamics",
                    html_content=self._decode_bytes(row.get("PREVIEWHTML", "")),
                )
                newsletters.append(newsletter)

            logger.info(f"Retrieved {len(newsletters)} Dynamics newsletters for {company.code}")
            return newsletters

        except Exception as e:
            logger.warning(f"Error retrieving Dynamics newsletters for {company.code}: {e}")
            return []
