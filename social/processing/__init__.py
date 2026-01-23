"""
DEPRECATED: This module is deprecated as of Phase 6 cleanup (January 2026).

The old Strategy pattern architecture (40+ classes) has been completely replaced
by simple utility functions and inline processing in platform processors.

  OLD (deprecated):
    - social.processing.strategies.ProcessingStrategy (base class)
    - social.processing.strategies.AddCompanyStrategy
    - social.processing.strategies.AddRowLoadedDateStrategy
    - social.processing.strategies.ExtractIDFromURNStrategy
    - ... (40+ more Strategy classes)
    - social.processing.factory.ProcessingStrategyFactory
    - social.processing.pipeline.DataProcessingPipeline

  NEW (use these instead):
    Simple utility functions:
    - social.utils.processing.deEmojify()
    - social.utils.processing.fix_id_type()
    - social.utils.date_utils.convert_unix_to_datetime()
    - social.utils.urn_utils.extract_id_from_urn()

    Platform-specific processors with chainable methods:
    - social.platforms.linkedin.processor.LinkedInProcessor
    - social.platforms.google.processor.GoogleProcessor
    - social.platforms.facebook.processor.FacebookProcessor
    - social.platforms.microsoft.processor.MicrosoftProcessor

Migration completed: January 2026
Architecture: Utility functions + inline processing (no Strategy classes)
Code reduction: 1,297 lines → ~100 lines (-92%)
"""

__all__ = []

# Keep module importable but empty to prevent breaking existing imports
# All functionality has been migrated to social/utils/ and platform processors
