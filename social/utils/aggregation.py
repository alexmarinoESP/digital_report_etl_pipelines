"""Aggregation utility functions for metrics processing.

This module provides reusable aggregation functions for converting
time-series data into cumulative metrics.

Architecture:
- DRY principle: Shared logic across all platform processors
- Type-safe: Proper type hints
- Logging: Detailed debugging information
"""

from typing import List, Optional
import pandas as pd
from loguru import logger


def aggregate_metrics_by_entity(
    df: pd.DataFrame,
    group_columns: Optional[List[str]] = None,
    metric_columns: Optional[List[str]] = None,
    agg_method: str = 'sum',
    entity_id_columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Aggregate time-series metrics into cumulative values per entity.

    Removes date granularity and sums metrics for each entity (creative, ad, campaign, etc.).

    Example:
        Input (time-series):
            creative_id | date       | impressions | clicks
            123        | 2026-01-20 | 100        | 5
            123        | 2026-01-21 | 150        | 8

        Output (aggregated):
            creative_id | impressions | clicks
            123        | 250         | 13

    Args:
        df: DataFrame with time-series data
        group_columns: Columns to group by (default: auto-detect entity columns)
        metric_columns: Columns to aggregate (default: all numeric columns)
        agg_method: Aggregation method (default: 'sum')
        entity_id_columns: Priority list of entity ID column names for auto-detection

    Returns:
        Aggregated DataFrame

    Raises:
        ValueError: If aggregation fails
    """
    if df.empty:
        logger.warning("Empty DataFrame, skipping aggregation")
        return df

    # Default entity ID columns (priority order)
    if entity_id_columns is None:
        entity_id_columns = [
            'creative_id', 'ad_id', 'adgroup_id', 'campaign_id',
            'account_id', 'account', 'id'
        ]

    # Auto-detect group columns
    if group_columns is None:
        metadata_cols = ['row_loaded_date', 'last_updated_date', 'date', 'date_start', 'date_stop']
        group_columns = [col for col in entity_id_columns if col in df.columns]

        if not group_columns:
            logger.warning("No entity ID columns found, using all non-metric/non-metadata columns")
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            group_columns = [
                col for col in df.columns
                if col not in numeric_cols and col not in metadata_cols
            ]

    if not group_columns:
        raise ValueError("No group columns found for aggregation")

    # Some SDKs (notably facebook-business) return numeric metrics as strings
    # ("56.77", "14455"). Coerce candidate columns to numeric *before* either
    # the auto-detect step OR the explicit-list path: with explicit
    # metric_columns we still need them numeric for groupby('sum').
    df = df.copy()
    id_patterns = ['_id', 'id_', 'account']
    candidate_cols = (
        metric_columns
        if metric_columns is not None
        else [c for c in df.columns
              if c not in group_columns
              and not any(p in c.lower() for p in id_patterns)]
    )
    for col in candidate_cols:
        if col not in df.columns:
            continue
        # Cover both legacy 'object' and modern 'string' dtypes.
        if not pd.api.types.is_numeric_dtype(df[col]):
            converted = pd.to_numeric(df[col], errors='coerce')
            # Only adopt the cast if at least one value parsed as a number;
            # otherwise the column is genuinely text (names, urls, ...).
            if converted.notna().any():
                df[col] = converted

    # Auto-detect metric columns when the caller didn't supply them.
    if metric_columns is None:
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        metric_columns = [
            col for col in numeric_cols
            if col not in group_columns
            and not any(pattern in col.lower() for pattern in id_patterns)
        ]

    if not metric_columns:
        logger.warning("No metric columns found to aggregate")
        return df

    # Remove date columns
    date_cols = [col for col in df.columns if col in ['date', 'date_start', 'date_stop']]
    if date_cols:
        logger.debug(f"Dropping date columns for aggregation: {date_cols}")
        df = df.drop(columns=date_cols)

    # Build aggregation dictionary.
    # IMPORTANT: a vanilla df.groupby(group).agg({metric: 'sum'}) drops every
    # column that's not in group_columns or agg_dict. For Facebook insights
    # this loses the descriptors that came from the same row (campaign_id,
    # adset_id, account_id, ad_name, ...) because they're not in
    # group_columns and not metrics either. Preserve them with 'first'
    # (descriptors are constant per entity, so 'first' is correct).
    descriptor_columns = [
        col for col in df.columns
        if col not in group_columns and col not in metric_columns
    ]
    agg_dict = {col: agg_method for col in metric_columns}
    for col in descriptor_columns:
        agg_dict[col] = 'first'

    # Group and aggregate
    try:
        rows_before = len(df)
        df_agg = df.groupby(group_columns, as_index=False).agg(agg_dict)
        rows_after = len(df_agg)

        # Replace NaN in aggregated metric columns with 0.
        # When the API returns null/missing values (e.g. publisher_platform
        # breakdown for an ad that didn't run on that platform), to_numeric
        # produces NaN; sum on an all-NaN group also stays NaN. The downstream
        # Vertica COPY then serializes NaN as INT64_MIN sentinel
        # (-9223372036854775808), which Vertica rejects as "out of range" for
        # BIGINT columns. 0 is the semantically correct value for a missing
        # additive metric.
        if agg_method == 'sum':
            cols_to_fill = [c for c in metric_columns if c in df_agg.columns]
            if cols_to_fill:
                df_agg[cols_to_fill] = df_agg[cols_to_fill].fillna(0)

        logger.info(
            f"✓ Aggregated metrics: {rows_before} rows → {rows_after} entities\n"
            f"  Group by: {group_columns}\n"
            f"  Metrics: {metric_columns}\n"
            f"  Method: {agg_method}"
        )

        return df_agg

    except Exception as e:
        logger.error(f"Aggregation failed: {e}")
        raise ValueError(f"Failed to aggregate metrics: {e}") from e
