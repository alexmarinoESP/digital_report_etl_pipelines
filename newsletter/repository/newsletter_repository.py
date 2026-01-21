"""
Newsletter data repository.
Handles all database operations for newsletter data.
"""

from typing import Tuple

import pandas as pd
from loguru import logger

from shared.connection.vertica import VerticaConnection
from newsletter.repository.cursor import Cur
from newsletter.repository.queries import query_mapp_html, query_dynamics_html
from newsletter import COMPANY_DICT


class NewsletterRepository:
    """
    Repository for newsletter data operations.
    Retrieves newsletter data from Vertica database.
    """

    def __init__(self, connection: VerticaConnection = None):
        """
        Initialize repository.

        Args:
            connection: Vertica connection (creates new if not provided)
        """
        self.connection = connection or VerticaConnection()

    def get_mapp_data(
        self, years_behind: int, comp_id: int
    ) -> pd.DataFrame:
        """
        Get Mapp newsletter data.

        Args:
            years_behind: Number of years to look back
            comp_id: Company ID

        Returns:
            DataFrame with Mapp newsletter data
        """
        with self.connection.get_connection() as conn:
            cur = Cur(connection=conn, conn_db="vertica")
            try:
                df = cur.query_data(
                    query=query_mapp_html,
                    info="Querying distinct messageid mapp",
                    data={"years_behind": years_behind, "comp_id": comp_id},
                )
                df.drop_duplicates(subset=["NEWSLETTERID"], inplace=True)
                return df
            except Exception as e:
                logger.warning(f"No Mapp data for company {comp_id}: {e}")
                return pd.DataFrame()

    def get_dynamics_data(
        self, years_behind: int, comp_id: int
    ) -> pd.DataFrame:
        """
        Get Dynamics newsletter data.

        Args:
            years_behind: Number of years to look back
            comp_id: Company ID

        Returns:
            DataFrame with Dynamics newsletter data
        """
        with self.connection.get_connection() as conn:
            cur = Cur(connection=conn, conn_db="vertica")
            df = cur.query_data(
                query=query_dynamics_html,
                info="Querying html dynamics",
                data={"years_behind": years_behind, "comp_id": comp_id},
            )
            df["NEWSLETTERID"] = df["NEWSLETTERID"].apply(
                lambda x: x.decode("utf-8") if isinstance(x, bytes) else x
            )
            return df

    def get_all_data(
        self, years_behind: int = 2
    ) -> Tuple[
        pd.DataFrame, pd.DataFrame, pd.DataFrame,
        pd.DataFrame, pd.DataFrame, pd.DataFrame
    ]:
        """
        Get all newsletter data for all companies.

        Args:
            years_behind: Number of years to look back

        Returns:
            Tuple of DataFrames: (mapp_it, mapp_es, mapp_vvit, dyn_it, dyn_es, dyn_vvit)
        """
        # Italy (comp_id=1)
        df_mapp_it = self.get_mapp_data(years_behind, COMPANY_DICT["it"])
        df_dynamics_it = self.get_dynamics_data(years_behind, COMPANY_DICT["it"])

        # Spain (comp_id=2)
        df_mapp_es = self.get_mapp_data(years_behind, COMPANY_DICT["es"])
        df_dynamics_es = self.get_dynamics_data(years_behind, COMPANY_DICT["es"])

        # V-Valley Italy (comp_id=32)
        df_mapp_vvit = self.get_mapp_data(years_behind, COMPANY_DICT["vvit"])
        df_dynamics_vvit = self.get_dynamics_data(years_behind, COMPANY_DICT["vvit"])

        return (
            df_mapp_it, df_mapp_es, df_mapp_vvit,
            df_dynamics_it, df_dynamics_es, df_dynamics_vvit
        )
