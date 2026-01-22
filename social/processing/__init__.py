"""Data processing pipeline for transforming raw API data.

This module implements the Strategy pattern for flexible data
transformation, allowing different processing steps to be combined
and reused across platforms.
"""

from social.processing.strategies import (
    ProcessingStrategy,
    AddCompanyStrategy,
    AddRowLoadedDateStrategy,
    ExtractIDFromURNStrategy,
    BuildDateFieldStrategy,
    ConvertUnixTimestampStrategy,
    ModifyNameStrategy,
    RenameColumnStrategy,
    ConvertToStringStrategy,
    ReplaceNaNWithZeroStrategy,
    ConvertNaTToNanStrategy,
)
from social.processing.factory import ProcessingStrategyFactory
from social.processing.pipeline import DataProcessingPipeline

__all__ = [
    # Strategy base class
    "ProcessingStrategy",
    # Concrete strategies
    "AddCompanyStrategy",
    "AddRowLoadedDateStrategy",
    "ExtractIDFromURNStrategy",
    "BuildDateFieldStrategy",
    "ConvertUnixTimestampStrategy",
    "ModifyNameStrategy",
    "RenameColumnStrategy",
    "ConvertToStringStrategy",
    "ReplaceNaNWithZeroStrategy",
    "ConvertNaTToNanStrategy",
    # Factory and pipeline
    "ProcessingStrategyFactory",
    "DataProcessingPipeline",
]
