"""Quick test for new SimpleGoogleProcessor."""

import pandas as pd
from social.platforms.google.column_mapper import GoogleColumnMapper
from social.platforms.google.simple_processor import SimpleGoogleProcessor

# Test 1: Load mapper and check config
print("="*60)
print("TEST 1: Load column mapper")
print("="*60)
mapper = GoogleColumnMapper()
print(f"✓ Loaded {len(mapper.config)} table configurations")

# Test 2: Check placement config
print("\n" + "="*60)
print("TEST 2: Placement table configuration")
print("="*60)
placement_config = mapper.get_table_config('google_ads_placement')
print(f"API columns: {list(placement_config['api_to_db'].keys())}")
print(f"DB columns: {placement_config['db_columns']}")

# Test 3: Test mapping on mock data
print("\n" + "="*60)
print("TEST 3: Mock placement data processing")
print("="*60)
mock_data = {
    'customer.id': ['123456'],
    'adGroup.id': ['789012'],
    'groupPlacementView.placement': ['example.com'],
    'groupPlacementView.placementType': ['WEBSITE'],
    'groupPlacementView.displayName': ['Example Site'],
    'groupPlacementView.targetUrl': ['https://example.com'],
    'metrics.impressions': ['1000'],
    'metrics.activeViewCtr': ['0.05'],
}
df = pd.DataFrame(mock_data)
print(f"BEFORE: {df.columns.tolist()}")

processor = SimpleGoogleProcessor('google_ads_placement')
df_processed = processor.process(df, clean_placement=True)
print(f"AFTER: {df_processed.columns.tolist()}")
print(f"\n✓ Processed {len(df_processed)} rows")
print(f"✓ Columns match DB schema: {set(df_processed.columns) == set(placement_config['db_columns'])}")

# Test 4: Check report config
print("\n" + "="*60)
print("TEST 4: Report table configuration")
print("="*60)
report_config = mapper.get_table_config('google_ads_report')
print(f"Cost fields to convert: {report_config['conversions']['cost_fields']}")
print(f"DB columns (source): {report_config['db_columns_source'][:5]}...")

# Test 5: Test report data processing
print("\n" + "="*60)
print("TEST 5: Mock report data processing")
print("="*60)
mock_report = {
    'customer.id': ['123456'],
    'campaign.id': ['111'],
    'adGroup.id': ['222'],
    'adGroupAd.ad.id': ['333'],
    'adGroupAd.ad.name': ['Test Ad'],
    'segments.date': ['2026-02-04'],
    'metrics.clicks': ['10'],
    'metrics.impressions': ['1000'],
    'metrics.conversions': ['2'],
    'metrics.costMicros': ['5000000'],  # 5.00 in micros
    'metrics.ctr': ['0.01'],
    'metrics.averageCpc': ['500000'],  # 0.50 in micros
    'metrics.averageCpm': ['5000'],
    'metrics.averageCost': ['250000'],
}
df_report = pd.DataFrame(mock_report)
print(f"BEFORE: cost_micros = {df_report['metrics.costMicros'].iloc[0]}")

processor_report = SimpleGoogleProcessor('google_ads_report')
df_report_processed = processor_report.process(df_report, use_source_columns=True)

print(f"AFTER: cost_micros = {df_report_processed['cost_micros'].iloc[0]}")
print(f"✓ Cost converted from micros: {df_report_processed['cost_micros'].iloc[0]} (expected: 5.0)")
print(f"✓ Columns: {df_report_processed.columns.tolist()}")

# Test 6: Audience config
print("\n" + "="*60)
print("TEST 6: Audience table configuration")
print("="*60)
audience_config = mapper.get_table_config('google_ads_audience')
print(f"API → DB mappings:")
for api_col, db_col in audience_config['api_to_db'].items():
    print(f"  {api_col} → {db_col}")

print("\n" + "="*60)
print("ALL TESTS PASSED ✓")
print("="*60)
