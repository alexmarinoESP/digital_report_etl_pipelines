"""Factory for creating processing strategies.

This module implements the Factory pattern to create processing
strategies based on configuration, eliminating reflection-based
method calls and providing type safety.
"""

from typing import Dict, Type
from loguru import logger

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
    ModifyURNAccountStrategy,
    ResponseDecorationStrategy,
    GoogleRenameColumnsStrategy,
)
from social.domain.services import CompanyMappingService, URNExtractor
from social.core.exceptions import ConfigurationError


class ProcessingStrategyFactory:
    """Factory for creating data processing strategies.

    This factory replaces the old reflection-based approach (getattr)
    with explicit strategy registration, providing:
    - Type safety
    - Compile-time checking
    - Better IDE support
    - Clearer error messages
    """

    def __init__(
        self,
        company_mapping_service: CompanyMappingService,
        urn_extractor: URNExtractor = None
    ):
        """Initialize the factory with required services.

        Args:
            company_mapping_service: Service for account-to-company mapping
            urn_extractor: Service for URN extraction (creates default if None)
        """
        self.company_mapping = company_mapping_service
        self.urn_extractor = urn_extractor or URNExtractor()

        # Strategy registry: maps strategy names to classes
        self._strategy_registry: Dict[str, Type[ProcessingStrategy]] = {
            "add_company": AddCompanyStrategy,
            "add_row_loaded_date": AddRowLoadedDateStrategy,
            "extract_id_from_urn": ExtractIDFromURNStrategy,
            "build_date_field": BuildDateFieldStrategy,
            "convert_unix_timestamp_to_date": ConvertUnixTimestampStrategy,
            "modify_name": ModifyNameStrategy,
            "rename_column": RenameColumnStrategy,
            "convert_string": ConvertToStringStrategy,
            "replace_nan_with_zero": ReplaceNaNWithZeroStrategy,
            "convert_nat_to_nan": ConvertNaTToNanStrategy,
            "modify_urn_li_sponsoredAccount": ModifyURNAccountStrategy,
            "response_decoration": ResponseDecorationStrategy,
            "google_rename_columns": GoogleRenameColumnsStrategy,
        }

    def create_strategy(self, strategy_name: str) -> ProcessingStrategy:
        """Create a processing strategy by name.

        Args:
            strategy_name: Name of the strategy to create

        Returns:
            Instantiated processing strategy

        Raises:
            ConfigurationError: If strategy name not recognized
        """
        if strategy_name not in self._strategy_registry:
            raise ConfigurationError(
                f"Unknown processing strategy: '{strategy_name}'",
                details={
                    "strategy": strategy_name,
                    "available": list(self._strategy_registry.keys())
                }
            )

        strategy_class = self._strategy_registry[strategy_name]

        # Instantiate with required dependencies
        if strategy_class == AddCompanyStrategy:
            return strategy_class(self.company_mapping)
        elif strategy_class == ExtractIDFromURNStrategy:
            return strategy_class(self.urn_extractor)
        else:
            # No dependencies required
            return strategy_class()

    def register_strategy(
        self,
        strategy_name: str,
        strategy_class: Type[ProcessingStrategy]
    ) -> None:
        """Register a custom processing strategy.

        This allows external code to add new strategies without modifying
        the factory, following the Open/Closed Principle.

        Args:
            strategy_name: Name to register the strategy under
            strategy_class: Strategy class to register
        """
        if not issubclass(strategy_class, ProcessingStrategy):
            raise ConfigurationError(
                f"Strategy class must inherit from ProcessingStrategy",
                details={"class": strategy_class.__name__}
            )

        if strategy_name in self._strategy_registry:
            logger.warning(f"Overriding existing strategy: {strategy_name}")

        self._strategy_registry[strategy_name] = strategy_class
        logger.info(f"Registered strategy: {strategy_name}")

    def get_available_strategies(self) -> list:
        """Get list of all available strategy names.

        Returns:
            List of strategy names
        """
        return list(self._strategy_registry.keys())
