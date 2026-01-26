import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from social.platforms.google.http_client import GoogleHTTPClient
from social.platforms.google.adapter import GoogleAdapter
from social.platforms.google.processor import GoogleProcessor
from social.core.token_provider import GoogleTokenProvider

# Initialize
token_provider = GoogleTokenProvider(
    config_file="social/platforms/google/google-ads-9474097201.yml"
)
adapter = GoogleAdapter(
    token_provider=token_provider,
    config_file_path="social/platforms/google/google-ads-9474097201.yml",
    manager_customer_id="9474097201"
)

# Extract audience
print("Extracting audiences...")
df = adapter.get_all_audiences()
print(f"\n✓ Extracted {len(df)} rows")
print(f"\nColumns BEFORE processing: {list(df.columns)}")
print(f"\nFirst row:\n{df.iloc[0].to_dict()}")

# Process
print("\n" + "="*80)
print("Processing...")
processor = GoogleProcessor(df)
processed = (processor
    .google_rename_columns()
    .clean_audience_string()
    .add_load_date()
    .get_df())

print(f"\nColumns AFTER processing: {list(processed.columns)}")
print(f"\nFirst row:\n{processed.iloc[0].to_dict()}")
