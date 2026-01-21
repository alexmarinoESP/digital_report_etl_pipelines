"""
Newsletter rendering service.
Handles HTML to image conversion.
"""

from typing import List, Optional, Tuple

from PIL import Image
from loguru import logger

from newsletter.domain.models import Newsletter, NewsletterImage, PipelineStats
from newsletter.domain.interfaces import (
    IHtmlRenderer,
    IImageProcessor,
    IHtmlProcessor,
    IImageStorage,
    RenderingError,
)


class RenderingService:
    """
    Service for rendering newsletters to images.

    Follows:
    - Single Responsibility: Only handles rendering
    - Dependency Inversion: Depends on abstractions
    - Open/Closed: New renderers/processors can be injected
    """

    def __init__(
        self,
        renderer: IHtmlRenderer,
        html_processor: IHtmlProcessor,
        image_processor: IImageProcessor,
        storage: Optional[IImageStorage] = None,
    ):
        """
        Initialize rendering service.

        Args:
            renderer: HTML to image renderer
            html_processor: HTML cleaner/processor
            image_processor: Image post-processor (cropper)
            storage: Optional storage for deduplication check
        """
        self._renderer = renderer
        self._html_processor = html_processor
        self._image_processor = image_processor
        self._storage = storage

    def _should_render(self, newsletter: Newsletter) -> bool:
        """
        Check if newsletter should be rendered.

        Args:
            newsletter: Newsletter to check

        Returns:
            True if should render, False if should skip
        """
        # Must have content
        if not newsletter.has_content:
            return False

        # Must have image name
        if not newsletter.image_name:
            return False

        # Check if already exists in storage
        if self._storage and self._storage.exists(newsletter.image_name):
            return False

        return True

    def render_newsletter(self, newsletter: Newsletter) -> Optional[NewsletterImage]:
        """
        Render a single newsletter to image.

        Args:
            newsletter: Newsletter to render

        Returns:
            NewsletterImage or None if rendering fails
        """
        try:
            # Clean HTML
            clean_html = self._html_processor.clean(
                newsletter.html_content,
                newsletter.company,
            )

            if not clean_html:
                logger.warning(f"Empty HTML after cleaning for {newsletter.newsletter_id}")
                return None

            # Render to image
            raw_image = self._renderer.render(clean_html)

            # Post-process (crop)
            processed_image = self._image_processor.process(raw_image)

            # Create result
            return NewsletterImage(
                newsletter_id=newsletter.newsletter_id,
                image_name=newsletter.image_name,
                image=processed_image,
                is_cropped=True,
            )

        except Exception as e:
            logger.error(f"Failed to render {newsletter.newsletter_id}: {e}")
            return None

    def render_newsletters(
        self,
        newsletters: List[Newsletter],
        skip_existing: bool = True,
    ) -> Tuple[List[NewsletterImage], PipelineStats]:
        """
        Render multiple newsletters to images.

        Args:
            newsletters: List of newsletters to render
            skip_existing: Whether to skip newsletters already in storage

        Returns:
            Tuple of (images, stats)
        """
        stats = PipelineStats()
        images = []

        for newsletter in newsletters:
            # Check if should render
            if skip_existing and not self._should_render(newsletter):
                if not newsletter.has_content:
                    stats.add_failed(f"No content: {newsletter.newsletter_id}")
                else:
                    stats.add_skipped()
                continue

            # Render
            logger.info(f"Rendering newsletter: {newsletter.newsletter_id}")
            image = self.render_newsletter(newsletter)

            if image and image.has_image:
                images.append(image)
                stats.add_processed()
            else:
                stats.add_failed(f"Rendering failed: {newsletter.newsletter_id}")

        logger.info(f"Rendering completed: {stats}")
        return images, stats
