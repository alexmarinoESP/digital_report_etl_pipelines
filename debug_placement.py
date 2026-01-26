"""Debug script to check placement data columns"""
import pandas as pd
from social.platforms.google.processor import GoogleProcessor

# Simulate data with id column
data = {
    'ad_group_id': [123, 456, 789],
    'id': [123, 456, 789],
    'placement': ['test1', 'test2', 'test3'],
    'placement_type': ['WEBSITE', 'MOBILE_APP', 'WEBSITE'],
    'display_name': ['Test 1', 'Test 2', 'Test 3'],
    'target_url': ['url1', 'url2', 'url3'],
    'impressions': [1000, 2000, 3000],
    'active_view_ctr': [0.01, 0.02, 0.03],
    'customer_id': ['111', '222', '333'],
    'date': ['2024-01-01', '2024-01-01', '2024-01-01']
}

df = pd.DataFrame(data)
print("INITIAL DataFrame:")
print(df.columns.tolist())
print(df.head())

# Simulate processing pipeline
processor = GoogleProcessor(df)
result = (processor
    .handle_columns()
    .google_rename_columns()
    .add_load_date()
    .fill_view_ctr_nan(['active_view_ctr'])
    .dropna_value()
    .drop_duplicates()
    .get_df())

print("\nFINAL DataFrame columns:")
print(result.columns.tolist())
print("\nFINAL DataFrame:")
print(result.head())
print(f"\nNumber of rows: {len(result)}")
