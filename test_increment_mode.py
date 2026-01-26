"""Test script for INCREMENT mode implementation.

This script tests the new increment mode functionality for social media insights.
"""

import pandas as pd
from loguru import logger

# Configure logger
logger.add("test_increment.log", rotation="10 MB", level="DEBUG")


def test_aggregate_utility():
    """Test the aggregation utility function."""
    logger.info("=" * 80)
    logger.info("TEST 1: Aggregation Utility")
    logger.info("=" * 80)

    from social.utils.aggregation import aggregate_metrics_by_entity

    # Create test time-series data
    df = pd.DataFrame({
        'creative_id': [123, 123, 123, 456, 456],
        'date': ['2026-01-20', '2026-01-21', '2026-01-22', '2026-01-20', '2026-01-21'],
        'impressions': [100, 150, 120, 50, 80],
        'clicks': [5, 8, 6, 2, 4],
        'campaign_id': [1, 1, 1, 2, 2],
    })

    logger.info(f"Input DataFrame (time-series):\n{df}")

    # Aggregate
    df_agg = aggregate_metrics_by_entity(
        df=df,
        entity_id_columns=['creative_id'],
        agg_method='sum'
    )

    logger.info(f"\nAggregated DataFrame:\n{df_agg}")

    # Verify
    assert len(df_agg) == 2, f"Expected 2 rows, got {len(df_agg)}"
    assert df_agg[df_agg['creative_id'] == 123]['impressions'].values[0] == 370, "Wrong impressions sum for 123"
    assert df_agg[df_agg['creative_id'] == 123]['clicks'].values[0] == 19, "Wrong clicks sum for 123"
    assert 'date' not in df_agg.columns, "Date column should be removed"

    logger.success("✓ Aggregation utility test PASSED")


def test_processor_aggregation():
    """Test processor aggregate_by_entity method."""
    logger.info("\n" + "=" * 80)
    logger.info("TEST 2: Processor Aggregation")
    logger.info("=" * 80)

    from social.platforms.linkedin.processor import LinkedInProcessor

    # Create test data
    df = pd.DataFrame({
        'creative_id': [123, 123, 456],
        'date': ['2026-01-20', '2026-01-21', '2026-01-20'],
        'impressions': [100, 150, 50],
        'clicks': [5, 8, 2],
    })

    logger.info(f"Input DataFrame:\n{df}")

    # Process
    processor = LinkedInProcessor(df)
    processor.aggregate_by_entity()
    df_result = processor.get_df()

    logger.info(f"\nProcessed DataFrame:\n{df_result}")

    # Verify
    assert len(df_result) == 2, f"Expected 2 rows, got {len(df_result)}"
    assert 'date' not in df_result.columns, "Date should be removed"

    logger.success("✓ Processor aggregation test PASSED")


def test_vertica_increment_logic():
    """Test VerticaDataSink increment logic (mock)."""
    logger.info("\n" + "=" * 80)
    logger.info("TEST 3: VerticaDataSink INCREMENT Logic (Simulated)")
    logger.info("=" * 80)

    # Simulate existing database state
    db_state = pd.DataFrame({
        'creative_id': [123, 456],
        'impressions': [1000, 500],
        'clicks': [50, 20],
    })

    logger.info(f"Existing DB state:\n{db_state}")

    # New data to increment
    new_data = pd.DataFrame({
        'creative_id': [123, 789],  # 123 exists, 789 is new
        'impressions': [200, 100],
        'clicks': [10, 5],
    })

    logger.info(f"\nNew data to load:\n{new_data}")

    # Simulate INCREMENT logic
    existing_keys = set(db_state['creative_id'].values)
    new_rows = new_data[~new_data['creative_id'].isin(existing_keys)]
    update_rows = new_data[new_data['creative_id'].isin(existing_keys)]

    logger.info(f"\nNew rows to INSERT:\n{new_rows}")
    logger.info(f"\nExisting rows to INCREMENT:\n{update_rows}")

    # Simulate UPDATE (increment)
    for _, row in update_rows.iterrows():
        creative_id = row['creative_id']
        idx = db_state[db_state['creative_id'] == creative_id].index[0]
        db_state.loc[idx, 'impressions'] += row['impressions']
        db_state.loc[idx, 'clicks'] += row['clicks']

    # Simulate INSERT
    db_state = pd.concat([db_state, new_rows], ignore_index=True)

    logger.info(f"\nFinal DB state after INCREMENT:\n{db_state}")

    # Verify
    row_123 = db_state[db_state['creative_id'] == 123].iloc[0]
    assert row_123['impressions'] == 1200, f"Expected 1200, got {row_123['impressions']}"
    assert row_123['clicks'] == 60, f"Expected 60, got {row_123['clicks']}"
    assert 789 in db_state['creative_id'].values, "New row 789 should be inserted"

    logger.success("✓ INCREMENT logic test PASSED")


def test_config_parsing():
    """Test YAML config parsing for increment mode."""
    logger.info("\n" + "=" * 80)
    logger.info("TEST 4: Config Parsing")
    logger.info("=" * 80)

    import yaml

    # Load LinkedIn config
    with open("social/platforms/linkedin/config_linkedin_ads.yml") as f:
        config = yaml.safe_load(f)

    # Check linkedin_ads_insights
    insights_config = config.get("linkedin_ads_insights", {})

    logger.info(f"linkedin_ads_insights config keys: {insights_config.keys()}")

    # Verify increment config exists
    assert "increment" in insights_config, "Missing 'increment' config"
    assert "aggregate_by_entity" in insights_config.get("processing", {}), "Missing 'aggregate_by_entity' processing step"

    increment_cfg = insights_config["increment"]
    logger.info(f"\nIncrement config:\n  PK: {increment_cfg['pk_columns']}\n  Metrics: {increment_cfg['increment_columns']}")

    assert increment_cfg["pk_columns"] == ["creative_id"], "Wrong PK columns"
    assert "impressions" in increment_cfg["increment_columns"], "Missing impressions metric"

    logger.success("✓ Config parsing test PASSED")


def main():
    """Run all tests."""
    logger.info("Starting INCREMENT mode tests")
    logger.info("=" * 80)

    try:
        test_aggregate_utility()
        test_processor_aggregation()
        test_vertica_increment_logic()
        test_config_parsing()

        logger.info("\n" + "=" * 80)
        logger.success("✅ ALL TESTS PASSED")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
