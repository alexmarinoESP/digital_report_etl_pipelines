# Facebook Ads Platform

Complete independent implementation for Facebook Marketing API ETL pipeline.

## Overview

This platform extracts advertising data from Facebook Marketing API using the Facebook Business SDK v19.0, processes it with Facebook-specific transformations, and loads it into Vertica.

## Architecture

```
facebook/
├── constants.py          # Field definitions, breakdowns, date presets
├── http_client.py       # FacebookHttpClient for Graph API integration
├── adapter.py           # Facebook API integration using SDK
├── processor.py         # Facebook-specific data transformations
├── pipeline.py          # ETL orchestration
└── config_fb_ads.yml    # Configuration (already exists)
```

## Key Features

### HTTP Client (http_client.py)
- **FacebookHttpClient**: Graph API v19.0 integration
- Automatic retry with exponential backoff
- Rate limit handling (429 responses)
- Token management and refresh
- Error handling for API-specific errors
- Multi-account support

### Data Extraction (adapter.py)
- Facebook Business SDK v19.0 integration
- 6 main entities:
  1. **Campaigns**: Campaign-level metadata
  2. **Ad Sets**: Ad set configuration and targeting
  3. **Insights**: Performance metrics with nested breakdowns
  4. **Insight Actions**: Actions/conversions array flattening
  5. **Custom Conversions**: Pixel-based conversion tracking
  6. **Audience Ad Sets**: Audience targeting join table
- SDK object-oriented approach (Campaign.api_get(), AdSet.get_insights())
- Nested breakdown support (age, gender, placement, device)
- Actions array expansion to individual rows

### Processing (processor.py)
- **Nested Breakdown Extraction**: Extract age, gender, placement from nested objects
- **Actions Array Flattening**: Convert actions/action_values lists to separate rows
- **Custom Conversion ID Extraction**: Parse conversion IDs from action types
- **Date Handling**: Convert API timestamps to proper date formats
- **Name Cleaning**: Handle special characters and encoding
- **Metadata Addition**: row_loaded_date, IngestionTimestamp

### Pipeline (pipeline.py)
- Complete ETL orchestration
- Dependency injection (TokenProvider, DataSink)
- Configuration-driven execution
- Multi-account iteration
- Error handling and logging

## Configuration

```yaml
fb_ads_campaign:
  type: get_campaigns
  fields: fields_ads_campaign
  date_preset: last_7d
  processing:
    rename_columns:
      col_dict:
        id: campaign_id
    deal_with_date:
      cols: [created_time]
  scope: account
  query:
    column_name:
      - campaign_id
  update:
    fields_id:
      - campaign_id
    fields_update:
      - status
      - effective_status
```

## Environment Variables

Required:
- `FACEBOOK_APP_ID`
- `FACEBOOK_APP_SECRET`
- `FACEBOOK_ACCESS_TOKEN`
- `FB_AD_ACCOUNT_IDS` (comma-separated list: "123456789,987654321")

Optional:
- `STORAGE_TYPE` (vertica | azure_table)
- `VERTICA_HOST`, `VERTICA_USER`, `VERTICA_PASSWORD`, etc.
- `VERTICA_SCHEMA` (default: GoogleAnalytics)
- `LOG_LEVEL` (default: INFO)

## Usage

### Local Development

```python
from social.platforms.facebook.pipeline import FacebookPipeline
from social.infrastructure.vertica_sink import VerticaDataSink

# Create pipeline
pipeline = FacebookPipeline(
    config=config,
    ad_account_ids=["123456789", "987654321"],
    data_sink=VerticaDataSink(...)
)

# Run single table
df = pipeline.run(table_name="fb_ads_campaign")

# Run all tables
results = pipeline.run_all_tables()
```

### Container App

```bash
python social/platforms/facebook/run_facebook.py
```

## Differences from Other Platforms

**vs Microsoft:**
- Facebook: SDK object-oriented + nested objects
- Microsoft: BingAds SDK + CSV download

**vs LinkedIn:**
- Facebook: Numeric IDs, SDK objects, nested breakdowns
- LinkedIn: URN-based resources, REST API, flat responses

**vs Google:**
- Facebook: SDK objects with nested attributes
- Google: gRPC + GAQL + Protobuf

## Critical Facebook Specifics

### Facebook Business SDK

The Facebook platform uses the official `facebook-business` Python SDK:

```python
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign

# Initialize API
FacebookAdsApi.init(app_id, app_secret, access_token)

# Get campaigns
account = AdAccount(f'act_{account_id}')
campaigns = account.get_campaigns(fields=[...])
```

### Nested Breakdown Structure

Facebook Insights returns nested breakdown data:

```json
{
  "age": "25-34",
  "gender": "female",
  "placement": "feed",
  "device_platform": "mobile",
  "impressions": "1234",
  "spend": "56.78"
}
```

The processor must extract and flatten these nested dimensions.

### Actions Array Format

Actions and conversions are returned as arrays:

```json
{
  "actions": [
    {"action_type": "link_click", "value": "123"},
    {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "5"}
  ],
  "action_values": [
    {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "499.99"}
  ]
}
```

The processor flattens these into separate rows with one action per row.

### Custom Conversion IDs

Custom conversions have special action types:

```
offsite_conversion.fb_pixel_custom.1234567890123456
```

The ID (1234567890123456) must be extracted for join tables.

### Ad Account Format

Facebook ad account IDs must be prefixed with `act_`:

```python
account_id = "123456789"
account = AdAccount(f"act_{account_id}")  # Becomes "act_123456789"
```

### Date Preset Options

Facebook supports several date presets:
- `last_7d`: Last 7 days
- `last_14d`: Last 14 days
- `last_30d`: Last 30 days
- `maximum`: All available data (use with caution)
- `today`: Current day only

### Field Selection

Fields must be explicitly requested:

```python
fields = [
    'id',
    'name',
    'status',
    'objective',
    'daily_budget',
    'lifetime_budget',
    'created_time'
]
campaigns = account.get_campaigns(fields=fields)
```

## Dependencies

```
facebook-business==19.0.3       # Facebook Marketing API SDK
requests==2.32.3
pandas==2.2.3
loguru==0.7.3
pyyaml==6.0.2
vertica-python==1.4.0
azure-data-tables==12.5.0       # Optional, for Azure Table Storage
```

## Testing

```bash
# Unit tests
pytest tests/unit/test_facebook_*.py -v

# Integration tests (requires credentials)
pytest tests/integration/test_facebook_pipeline.py -v
```

## Deployment

See `azure/facebook-ads-job.yaml` for Container App Job configuration.

## References

- [Facebook Marketing API Documentation](https://developers.facebook.com/docs/marketing-apis)
- [Facebook Business SDK for Python](https://github.com/facebook/facebook-python-business-sdk)
- [Graph API Reference](https://developers.facebook.com/docs/graph-api)
- [Marketing API Insights](https://developers.facebook.com/docs/marketing-api/insights)

## Known Issues & Workarounds

### Rate Limiting
**Problem:** Facebook enforces strict rate limits (200 calls/hour per user, varies by app)
**Mitigation:** Built-in retry with exponential backoff, use batch requests when possible
**Monitor:** 429 responses and `X-Business-Use-Case-Usage` headers

### Large Date Ranges
**Problem:** Requesting insights for large date ranges can timeout
**Workaround:** Use date presets (last_7d, last_14d) instead of custom ranges
**Alternative:** Implement date chunking for historical data

### Async Job Polling
**Problem:** Large insight requests return async job IDs requiring polling
**Solution:** Implemented in adapter.py with configurable polling intervals
**Status Check:** Use `job.api_get()` to check completion status

### Nested Object Depth
**Problem:** Some responses have deeply nested objects (targeting, creative)
**Solution:** Recursive flattening in processor.py
**Limitation:** Very deep nesting (>5 levels) may require custom handling

### Actions Array Explosion
**Problem:** Flattening actions creates many rows (1 insight row becomes 10+ action rows)
**Impact:** Significantly increases data volume
**Solution:** Separate table (fb_ads_insight_actions) to avoid data explosion in main insights

### Ad Account Permissions
**Problem:** Access token must have permissions for all requested accounts
**Error:** "Insufficient permissions" or "Account not found"
**Fix:** Ensure token has `ads_read` and `ads_management` permissions for all accounts

### Field Deprecation
**Problem:** Facebook regularly deprecates fields (e.g., relevance_score removed in v11)
**Detection:** API returns warnings in response
**Action:** Monitor deprecation warnings and update field lists in constants.py

### SDK Version Compatibility
**Problem:** Facebook Business SDK major versions are not backwards compatible
**Current:** Using v19.0.3 (compatible with Graph API v19.0)
**Upgrade Path:** Test thoroughly when upgrading SDK versions
