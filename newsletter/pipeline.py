"""
Newsletter pipeline orchestrator.
Main entry point for the newsletter ETL process.

This module follows the Dependency Injection pattern:
- All dependencies are injected through the constructor
- Allows easy testing with mocks
- Follows SOLID principles throughout
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple

from loguru import logger

from newsletter.domain.models import Company, Newsletter, NewsletterImage, PipelineStats
from newsletter.domain.interfaces import (
    INewsletterRepository,
    IHtmlRenderer,
    IImageProcessor,
    IHtmlProcessor,
    IImageStorage,
    IMappClient,
    # Domain exceptions
    NewsletterError,
    ExtractionError,
    RenderingError,
    UploadError,
    StorageError,
)
from newsletter.services.extraction_service import ExtractionService
from newsletter.services.rendering_service import RenderingService
from newsletter.services.upload_service import UploadService


@dataclass
class PipelineResult:
    """Result of pipeline execution."""
    extraction_stats: PipelineStats
    rendering_stats: PipelineStats
    upload_stats: PipelineStats
    newsletters_extracted: int
    images_rendered: int
    images_uploaded: int

    @property
    def success(self) -> bool:
        """Check if pipeline completed without critical failures."""
        return self.images_uploaded > 0 or (
            self.newsletters_extracted == 0 and
            self.rendering_stats.failed == 0
        )

    def __str__(self) -> str:
        return (
            f"Pipeline Result:\n"
            f"  Extraction: {self.extraction_stats}\n"
            f"  Rendering: {self.rendering_stats}\n"
            f"  Upload: {self.upload_stats}\n"
            f"  Total: {self.newsletters_extracted} extracted, "
            f"{self.images_rendered} rendered, {self.images_uploaded} uploaded"
        )


class NewsletterPipeline:
    """
    Main orchestrator for newsletter ETL pipeline.

    Coordinates the three stages:
    1. Extraction - Get newsletter data from sources
    2. Rendering - Convert HTML to images
    3. Upload - Store images in S3/Minio

    Follows:
    - Single Responsibility: Only orchestrates, delegates to services
    - Dependency Inversion: Depends only on abstractions
    - Open/Closed: New stages/behaviors via composition
    """

    def __init__(
        self,
        repository: INewsletterRepository,
        renderer: IHtmlRenderer,
        html_processor: IHtmlProcessor,
        image_processor: IImageProcessor,
        storage: IImageStorage,
        mapp_clients: Optional[dict] = None,
    ):
        """
        Initialize pipeline with all dependencies.

        Args:
            repository: Newsletter data repository
            renderer: HTML to image renderer
            html_processor: HTML cleaner
            image_processor: Image post-processor
            storage: Image storage backend
            mapp_clients: Dict mapping Company to IMappClient instances
        """
        self._repository = repository
        self._renderer = renderer
        self._html_processor = html_processor
        self._image_processor = image_processor
        self._storage = storage
        self._mapp_clients = mapp_clients or {}

        # Initialize services
        self._extraction_service = ExtractionService(
            repository=repository,
            mapp_clients=mapp_clients,
        )
        self._rendering_service = RenderingService(
            renderer=renderer,
            html_processor=html_processor,
            image_processor=image_processor,
            storage=storage,
        )
        self._upload_service = UploadService(storage=storage)

    def set_mapp_client(self, company: Company, client: IMappClient) -> None:
        """Set Mapp client for a company."""
        self._mapp_clients[company] = client
        self._extraction_service.set_mapp_client(company, client)

    def run(
        self,
        companies: Optional[List[Company]] = None,
        sources: Optional[List[str]] = None,
        years_behind: int = 2,
        skip_existing: bool = True,
    ) -> PipelineResult:
        """
        Execute the full pipeline.

        Args:
            companies: List of companies to process (all if None)
            sources: List of sources ['mapp', 'dynamics'] (all if None)
            years_behind: Number of years to look back
            skip_existing: Whether to skip existing images

        Returns:
            PipelineResult with statistics
        """
        logger.info("=" * 60)
        logger.info("Starting Newsletter Pipeline")
        logger.info("=" * 60)

        # Stage 1: Extraction
        logger.info("Stage 1: Extraction")
        newsletters, extraction_stats = self._extraction_service.extract_newsletters(
            companies=companies,
            sources=sources,
            years_behind=years_behind,
            enrich_mapp=True,
        )

        # Filter newsletters with content
        newsletters_with_content = [nl for nl in newsletters if nl.has_content]
        logger.info(f"Newsletters with content: {len(newsletters_with_content)}")

        # Stage 2: Rendering
        logger.info("Stage 2: Rendering")
        images, rendering_stats = self._rendering_service.render_newsletters(
            newsletters=newsletters_with_content,
            skip_existing=skip_existing,
        )

        # Stage 3: Upload
        logger.info("Stage 3: Upload")
        uploaded_count, upload_stats = self._upload_service.upload_images(
            images=images,
            skip_existing=skip_existing,
        )

        # Build result
        result = PipelineResult(
            extraction_stats=extraction_stats,
            rendering_stats=rendering_stats,
            upload_stats=upload_stats,
            newsletters_extracted=len(newsletters),
            images_rendered=len(images),
            images_uploaded=uploaded_count,
        )

        logger.info("=" * 60)
        logger.info(str(result))
        logger.info("=" * 60)

        return result

    def run_extraction_only(
        self,
        companies: Optional[List[Company]] = None,
        sources: Optional[List[str]] = None,
        years_behind: int = 2,
    ) -> Tuple[List[Newsletter], PipelineStats]:
        """
        Run only the extraction stage.

        Returns:
            Tuple of (newsletters, stats)
        """
        return self._extraction_service.extract_newsletters(
            companies=companies,
            sources=sources,
            years_behind=years_behind,
        )

    def run_rendering_only(
        self,
        newsletters: List[Newsletter],
        skip_existing: bool = True,
    ) -> Tuple[List[NewsletterImage], PipelineStats]:
        """
        Run only the rendering stage.

        Returns:
            Tuple of (images, stats)
        """
        return self._rendering_service.render_newsletters(
            newsletters=newsletters,
            skip_existing=skip_existing,
        )

    def run_upload_only(
        self,
        images: List[NewsletterImage],
        skip_existing: bool = True,
    ) -> Tuple[int, PipelineStats]:
        """
        Run only the upload stage.

        Returns:
            Tuple of (uploaded_count, stats)
        """
        return self._upload_service.upload_images(
            images=images,
            skip_existing=skip_existing,
        )


class PipelineFactory:
    """
    Factory for creating pipeline instances with default dependencies.

    Provides convenience methods for common configurations.
    Following DRY principle - common initialization extracted to _create_pipeline.
    """

    @staticmethod
    def _create_pipeline(storage: IImageStorage) -> NewsletterPipeline:
        """
        Create pipeline with given storage backend.
        Internal method that handles common adapter initialization.

        Args:
            storage: Storage backend to use

        Returns:
            Configured NewsletterPipeline instance
        """
        from newsletter.adapters import (
            NewsletterRepositoryAdapter,
            HctiAdapter,
            MappAdapter,
        )
        from newsletter.services import HtmlCleanerService, SmartCropperService

        # Create common adapters
        repository = NewsletterRepositoryAdapter()
        renderer = HctiAdapter()
        html_processor = HtmlCleanerService()
        image_processor = SmartCropperService()

        # Create Mapp clients for each company
        mapp_clients = {
            Company.IT: MappAdapter(Company.IT),
            Company.ES: MappAdapter(Company.ES),
            Company.VVIT: MappAdapter(Company.VVIT),
        }

        return NewsletterPipeline(
            repository=repository,
            renderer=renderer,
            html_processor=html_processor,
            image_processor=image_processor,
            storage=storage,
            mapp_clients=mapp_clients,
        )

    @staticmethod
    def create_default() -> NewsletterPipeline:
        """
        Create pipeline with default production dependencies (S3 storage).

        Returns:
            Configured NewsletterPipeline instance
        """
        from newsletter.adapters import S3StorageAdapter

        storage = S3StorageAdapter()
        return PipelineFactory._create_pipeline(storage)

    @staticmethod
    def create_with_test_storage(test_folder: str = "test-images") -> NewsletterPipeline:
        """
        Create pipeline with test storage folder (S3).

        Args:
            test_folder: Folder name for test uploads on S3

        Returns:
            Configured NewsletterPipeline instance
        """
        from newsletter.adapters import S3StorageAdapter

        storage = S3StorageAdapter(folder=test_folder)
        return PipelineFactory._create_pipeline(storage)

    @staticmethod
    def create_with_local_storage(output_folder: str) -> NewsletterPipeline:
        """
        Create pipeline with local filesystem storage.
        Use this for testing without S3/Minio connection.

        Args:
            output_folder: Local folder path for saving images

        Returns:
            Configured NewsletterPipeline instance
        """
        from newsletter.adapters import LocalStorageAdapter

        storage = LocalStorageAdapter(output_folder=output_folder)
        return PipelineFactory._create_pipeline(storage)

    @staticmethod
    def create_custom(storage: IImageStorage) -> NewsletterPipeline:
        """
        Create pipeline with custom storage backend.
        Use this for advanced configurations or testing with mocks.

        Args:
            storage: Custom IImageStorage implementation

        Returns:
            Configured NewsletterPipeline instance
        """
        return PipelineFactory._create_pipeline(storage)
