"""LinkedIn Organic Posts ETL Pipeline Module.

This module provides the main pipeline orchestrator for LinkedIn organic posts
data extraction, transformation, and loading (ETL).

Key Features:
- Multi-table support (posts, insights, page stats, followers)
- Dependency injection for TokenProvider and DataSink
- Proper error handling and recovery
- Detailed logging and progress tracking
- UPSERT support for incremental updates

Architecture:
- SOLID principles with dependency injection
- Protocol-based contracts for flexibility
- Clear separation of concerns (adapter/processor/sink)
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml
from loguru import logger

from social.core.exceptions import ConfigurationError, PipelineError
from social.core.protocols import DataSink, TokenProvider
from social.platforms.linkedin_posts.adapter import LinkedInPostsAdapter
from social.platforms.linkedin_posts.processor import LinkedInPostsProcessor
from social.platforms.linkedin_posts.http_client import LinkedInPostsHTTPClient
from social.platforms.linkedin_posts.constants import ORGANIZATION_IDS


class LinkedInPostsPipeline:
    """Main ETL pipeline for LinkedIn Organic Posts.

    This pipeline orchestrates the complete data extraction process:
    1. Authentication (via TokenProvider)
    2. Data extraction (via LinkedInPostsAdapter)
    3. Data processing (via LinkedInPostsProcessor)
    4. Data loading (via DataSink)

    Tables processed:
    - linkedin_organic_posts: Post content and metadata
    - linkedin_organic_posts_insights: Post engagement metrics
    - linkedin_organic_page_stats: Page view statistics
    - linkedin_organic_followers: Follower growth tracking
    - linkedin_organic_follower_demographics: Follower demographics

    Attributes:
        config: Platform configuration with table definitions
        token_provider: Authentication token provider
        data_sink: Data sink for loading processed data
        adapter: LinkedIn Posts API adapter
    """

    def __init__(
        self,
        config: Dict[str, Any],
        token_provider: TokenProvider,
        data_sink: Optional[DataSink] = None,
    ):
        """Initialize the LinkedIn Posts pipeline.

        Args:
            config: Platform configuration dictionary (from YAML)
            token_provider: Token provider for authentication
            data_sink: Optional data sink for loading data

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if config is None:
            raise ConfigurationError("Configuration cannot be None")

        self.config = config
        self.token_provider = token_provider
        self.data_sink = data_sink

        # Initialize HTTP client
        access_token = token_provider.get_access_token()
        self.http_client = LinkedInPostsHTTPClient(access_token=access_token)

        # Initialize adapter
        self.adapter = LinkedInPostsAdapter(http_client=self.http_client)

        # Extract table names from config
        self.table_names = [
            key for key in config.keys()
            if key != "platform" and key.startswith("linkedin_organic_")
        ]

        # Cache for posts data to avoid fetching twice (posts + insights)
        self._posts_cache: Dict[str, List[Dict]] = {}

        logger.info(f"LinkedInPostsPipeline initialized with {len(self.table_names)} tables")
        logger.debug(f"Tables: {self.table_names}")

    def run(
        self,
        tables: Optional[List[str]] = None,
        max_posts_per_org: Optional[int] = None,
    ) -> Dict[str, pd.DataFrame]:
        """Run the complete ETL pipeline for specified tables.

        Args:
            tables: List of table names to process (default: all)
            max_posts_per_org: Maximum posts per organization (for testing)

        Returns:
            Dictionary mapping table names to processed DataFrames

        Raises:
            PipelineError: If pipeline execution fails
        """
        tables_to_process = tables or self.table_names
        results = {}

        logger.info(f"Starting LinkedIn Posts pipeline for tables: {tables_to_process}")

        for table_name in tables_to_process:
            try:
                logger.info(f"Processing table: {table_name}")
                df = self._process_table(table_name, max_posts_per_org)

                if df is not None and not df.empty:
                    results[table_name] = df
                    logger.info(f"Table {table_name}: {len(df)} rows processed")

                    # Load to database if data_sink is configured
                    if self.data_sink:
                        self._load_table(table_name, df)
                else:
                    logger.warning(f"Table {table_name}: No data to process")

            except Exception as e:
                logger.error(f"Failed to process table {table_name}: {e}")
                raise PipelineError(f"Pipeline failed for {table_name}: {e}")

        logger.info(f"LinkedIn Posts pipeline completed. Processed {len(results)} tables.")
        return results

    def _process_table(
        self,
        table_name: str,
        max_posts_per_org: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        """Process a single table.

        Args:
            table_name: Name of the table to process
            max_posts_per_org: Maximum posts per organization

        Returns:
            Processed DataFrame or None
        """
        if table_name == "linkedin_organic_posts":
            return self._process_posts(max_posts_per_org)

        elif table_name == "linkedin_organic_posts_insights":
            return self._process_posts_insights(max_posts_per_org)

        elif table_name == "linkedin_organic_page_stats":
            return self._process_page_stats()

        elif table_name == "linkedin_organic_followers":
            return self._process_followers()

        elif table_name == "linkedin_organic_follower_demographics":
            return self._process_follower_demographics()

        else:
            logger.warning(f"Unknown table: {table_name}")
            return None

    def _fetch_posts_for_org(
        self, org_id: str, max_posts_per_org: Optional[int] = None
    ) -> List[Dict]:
        """Fetch posts for an organization, using cache if available.

        Args:
            org_id: Organization ID
            max_posts_per_org: Maximum posts per organization

        Returns:
            List of post dictionaries
        """
        cache_key = f"{org_id}_{max_posts_per_org}"
        if cache_key in self._posts_cache:
            logger.debug(f"Using cached posts for org {org_id}")
            return self._posts_cache[cache_key]

        posts = self.adapter.get_posts(org_id, max_results=max_posts_per_org)
        for post in posts:
            post["_organization_id"] = org_id
        self._posts_cache[cache_key] = posts
        return posts

    def _process_posts(self, max_posts_per_org: Optional[int] = None) -> pd.DataFrame:
        """Process posts table.

        Args:
            max_posts_per_org: Maximum posts per organization

        Returns:
            Processed DataFrame
        """
        logger.info("Fetching posts from all organizations...")

        # Fetch posts for all organizations (populates cache)
        all_posts = []
        for org_id in ORGANIZATION_IDS:
            try:
                posts = self._fetch_posts_for_org(org_id, max_posts_per_org)
                all_posts.extend(posts)
            except Exception as e:
                logger.error(f"Failed to fetch posts for org {org_id}: {e}")

        # Transform
        processor = LinkedInPostsProcessor(all_posts)
        df = (processor
              .to_posts_dataframe()
              .clean_text_columns(["commentary", "media_title"])
              .add_row_loaded_date()
              .add_last_updated_date()
              .get_df())

        return df

    def _process_posts_insights(self, max_posts_per_org: Optional[int] = None) -> pd.DataFrame:
        """Process posts insights table.

        Uses cached posts data to avoid redundant API calls.

        Args:
            max_posts_per_org: Maximum posts per organization

        Returns:
            Processed DataFrame
        """
        logger.info("Fetching posts insights...")

        all_insights_data = []

        for org_id in ORGANIZATION_IDS:
            try:
                # Fetch posts (uses cache if already fetched by _process_posts)
                posts = self._fetch_posts_for_org(org_id, max_posts_per_org)

                if not posts:
                    continue

                # Get post URNs
                post_urns = [p.get("id") for p in posts if p.get("id")]

                # Fetch statistics for these posts
                stats = self.adapter.get_share_statistics_by_posts(org_id, post_urns)

                # Transform
                processor = LinkedInPostsProcessor(None)
                df = (processor
                      .to_posts_insights_dataframe(posts, stats)
                      .add_row_loaded_date()
                      .add_last_updated_date()
                      .get_df())

                if not df.empty:
                    all_insights_data.append(df)

            except Exception as e:
                logger.error(f"Failed to process insights for org {org_id}: {e}")

        if all_insights_data:
            return pd.concat(all_insights_data, ignore_index=True)
        return pd.DataFrame()

    def _process_page_stats(self) -> pd.DataFrame:
        """Process page statistics table.

        Returns:
            Processed DataFrame
        """
        logger.info("Fetching page statistics...")

        # Fetch page stats (lifetime)
        stats = self.adapter.get_all_page_statistics(time_bound=False)

        # Transform
        processor = LinkedInPostsProcessor(stats)
        df = (processor
              .to_page_stats_dataframe()
              .add_row_loaded_date()
              .add_last_updated_date()
              .get_df())

        return df

    def _process_followers(self) -> pd.DataFrame:
        """Process followers table (time-bound gains).

        Returns:
            Processed DataFrame
        """
        logger.info("Fetching follower statistics...")

        all_data = []

        for org_id in ORGANIZATION_IDS:
            try:
                # Fetch time-bound follower stats
                stats = self.adapter.get_follower_statistics_timebound(org_id)

                # Transform
                processor = LinkedInPostsProcessor(stats)
                df = (processor
                      .to_follower_timebound_dataframe()
                      .add_row_loaded_date()
                      .add_last_updated_date()
                      .get_df())

                if not df.empty:
                    all_data.append(df)

            except Exception as e:
                logger.error(f"Failed to process follower stats for org {org_id}: {e}")

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def _process_follower_demographics(self) -> pd.DataFrame:
        """Process follower demographics table.

        Returns:
            Processed DataFrame
        """
        logger.info("Fetching follower demographics...")

        all_data = []

        for org_id in ORGANIZATION_IDS:
            try:
                # Fetch lifetime demographics
                stats = self.adapter.get_follower_statistics(org_id)

                if stats:
                    # Transform
                    processor = LinkedInPostsProcessor([stats])
                    df = (processor
                          .to_follower_demographics_dataframe()
                          .add_row_loaded_date()
                          .add_last_updated_date()
                          .get_df())

                    if not df.empty:
                        all_data.append(df)

            except Exception as e:
                logger.error(f"Failed to process demographics for org {org_id}: {e}")

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def _load_table(self, table_name: str, df: pd.DataFrame):
        """Load processed data to database.

        Determines load mode from table configuration:
        - upsert: MERGE on pk_columns (INSERT new + UPDATE existing)
        - append: INSERT only (default)

        Args:
            table_name: Target table name
            df: DataFrame to load
        """
        table_config = self.config.get(table_name, {})

        # Determine load mode and parameters from config
        load_mode = "append"
        pk_columns = None
        increment_columns = None

        if "increment" in table_config:
            load_mode = "increment"
            increment_config = table_config["increment"]
            pk_columns = increment_config.get("pk_columns")
            increment_columns = increment_config.get("increment_columns")
        elif "upsert" in table_config:
            load_mode = "upsert"
            pk_columns = table_config["upsert"].get("pk_columns")

        logger.info(f"Loading {table_name} with mode={load_mode}, pk={pk_columns}")

        rows_loaded = self.data_sink.load(
            df=df,
            table_name=table_name,
            mode=load_mode,
            dedupe_columns=pk_columns,
            increment_columns=increment_columns,
        )

        logger.info(f"Loaded {rows_loaded} rows to {table_name}")

    def close(self):
        """Clean up resources."""
        if self.http_client:
            self.http_client.close()


def load_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load pipeline configuration from YAML file.

    Args:
        config_path: Path to config file (default: config_linkedin_posts.yml)

    Returns:
        Configuration dictionary
    """
    if config_path is None:
        config_path = Path(__file__).parent / "config_linkedin_posts.yml"

    if not config_path.exists():
        raise ConfigurationError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
