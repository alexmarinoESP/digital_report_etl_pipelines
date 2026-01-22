"""Data processing pipeline for applying transformation strategies.

This module coordinates the application of multiple processing strategies
to transform raw API data into clean, database-ready DataFrames.
"""

from typing import List, Dict, Any
import pandas as pd
from loguru import logger

from social.processing.strategies import ProcessingStrategy
from social.processing.factory import ProcessingStrategyFactory
from social.core.exceptions import DataValidationError


class DataProcessingPipeline:
    """Pipeline for applying a sequence of data transformations.

    This class orchestrates the application of multiple processing
    strategies in order, handling errors and logging progress.
    """

    def __init__(self, strategy_factory: ProcessingStrategyFactory):
        """Initialize the pipeline with a strategy factory.

        Args:
            strategy_factory: Factory for creating processing strategies
        """
        self.factory = strategy_factory
        self._strategies: List[tuple] = []  # List of (strategy, params) tuples

    def add_step(self, strategy_name: str, params: Dict[str, Any] = None) -> "DataProcessingPipeline":
        """Add a processing step to the pipeline.

        Args:
            strategy_name: Name of the strategy to add
            params: Parameters to pass to the strategy

        Returns:
            Self for method chaining

        Raises:
            ConfigurationError: If strategy name invalid
        """
        strategy = self.factory.create_strategy(strategy_name)
        self._strategies.append((strategy, params or {}))
        return self

    def add_steps_from_config(self, processing_config: List[Dict[str, Any]]) -> "DataProcessingPipeline":
        """Add multiple steps from configuration.

        Args:
            processing_config: List of processing step configurations
                             Each item should be a dict with strategy name and params

        Returns:
            Self for method chaining

        Example:
            ```python
            config = [
                {"name": "extract_id_from_urn", "columns": ["account", "campaign"]},
                {"name": "add_company"},
                {"name": "add_row_loaded_date"}
            ]
            pipeline.add_steps_from_config(config)
            ```
        """
        # Handle dict format from LinkedIn YAML: {step_name: {params...}}
        if isinstance(processing_config, dict):
            # Convert to list of dicts with 'name' key
            processing_list = []
            for step_name, step_params in processing_config.items():
                if step_params and isinstance(step_params, dict) and step_params != {"params": None}:
                    # Add name to params
                    config_dict = {"name": step_name, **step_params}
                    processing_list.append(config_dict)
                else:
                    # Just step name
                    processing_list.append(step_name)
            processing_config = processing_list

        for step_config in processing_config:
            if isinstance(step_config, str):
                # String format: "strategy_name" or "strategy_name:param1=value1,param2=value2"
                if ":" in step_config:
                    # Parse parameters
                    strategy_name, params_str = step_config.split(":", 1)
                    params = {}
                    for param in params_str.split(","):
                        if "=" in param:
                            key, value = param.split("=", 1)
                            # Convert "null" string to None
                            if value == "null":
                                value = None
                            # Normalize parameter names (legacy compatibility)
                            # cols -> columns (old YAML format used 'cols')
                            if key.strip() == "cols":
                                params["columns"] = value
                            else:
                                params[key.strip()] = value
                    self.add_step(strategy_name.strip(), params)
                else:
                    # Just strategy name
                    self.add_step(step_config)
            elif isinstance(step_config, dict):
                # Dict format with parameters
                strategy_name = step_config.get("name") or step_config.get("strategy")
                if not strategy_name:
                    logger.warning(f"Skipping invalid step config: {step_config}")
                    continue

                # Extract parameters (all keys except 'name' and 'strategy')
                params = {
                    k: v for k, v in step_config.items()
                    if k not in ["name", "strategy"]
                }

                # Normalize parameter names for legacy compatibility
                if "cols" in params:
                    params["columns"] = params.pop("cols")

                self.add_step(strategy_name, params)
            else:
                logger.warning(f"Skipping invalid step config type: {type(step_config)}")

        return self

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all processing steps to the DataFrame.

        Args:
            df: DataFrame to process

        Returns:
            Processed DataFrame

        Raises:
            DataValidationError: If processing fails
        """
        if df.empty:
            logger.warning("Empty DataFrame provided to pipeline")
            return df

        logger.debug(f"Processing DataFrame with {len(self._strategies)} steps")
        result_df = df.copy()

        for i, (strategy, params) in enumerate(self._strategies):
            strategy_name = strategy.get_name()
            try:
                logger.debug(f"Step {i+1}/{len(self._strategies)}: {strategy_name}")
                result_df = strategy.process(result_df, **params)

            except Exception as e:
                logger.error(f"Processing step '{strategy_name}' failed: {e}")
                raise DataValidationError(
                    f"Processing step '{strategy_name}' failed",
                    field=strategy_name,
                    details={"error": str(e), "params": params}
                )

        logger.debug(f"Processing complete: {len(result_df)} rows, {len(result_df.columns)} columns")
        return result_df

    def clear(self) -> "DataProcessingPipeline":
        """Clear all processing steps from the pipeline.

        Returns:
            Self for method chaining
        """
        self._strategies.clear()
        return self

    def get_steps(self) -> List[str]:
        """Get list of strategy names in the pipeline.

        Returns:
            List of strategy names in order
        """
        return [strategy.get_name() for strategy, _ in self._strategies]

    def __len__(self) -> int:
        """Get number of processing steps.

        Returns:
            Number of steps
        """
        return len(self._strategies)

    def __repr__(self) -> str:
        """Get string representation of the pipeline.

        Returns:
            String showing all steps
        """
        steps = self.get_steps()
        return f"DataProcessingPipeline({len(steps)} steps: {steps})"
