# LinkedIn Ads Platform

Complete independent implementation for LinkedIn Marketing API ETL pipeline.

## Overview

This platform extracts advertising data from LinkedIn Marketing API v202601, processes it with URN-specific transformations, and loads it into Vertica.

## Architecture

```
linkedin/
├── http_client.py         # NoQuotedCommasSession (special parameter encoding)
├── adapter.py            # LinkedIn API integration
├── processor.py          # URN extraction, date building, targeting
├── pipeline.py           # ETL orchestration
├── endpoints.py          # LinkedIn API endpoints enum
├── constants.py          # company_account mapping
└── config_linkedin_ads.yml  # Configuration (already exists, updated)
```

## Key Features

### HTTP Client (http_client.py)
- **NoQuotedCommasSession**: Custom requests.Session that prevents URL encoding of commas
- Critical for LinkedIn's special parameter format: `List(urn:li:sponsoredCampaign:123)`
- Handles both encoded and non-encoded parameters in same request

### Data Extraction (adapter.py)
- REST API v202601 integration
- 5 main entities:
  1. **Campaigns**: Account-level campaign metadata
  2. **Insights**: Performance metrics with creative pivot
  3. **Creatives**: Ad creative details
  4. **Audiences**: Targeting segments
  5. **Campaign-Audience**: Join table for campaign targeting
- URN-based iteration (get URNs from database first)
- 150-day lookback for insights
- Page size: 10000 (optimized from old 1000)

### Processing (processor.py)
- **URN Extraction**: `urn:li:sponsoredCampaign:123` → `123`
- **Date Building**: Combine year/month/day columns into single date
- **Timestamp Conversion**: Unix milliseconds → datetime
- **Response Decoration**: Extract IDs from nested fields
- **Targeting Extraction**: Parse audience segments from targeting criteria

### Pipeline (pipeline.py)
- Complete ETL orchestration
- Dependency injection (TokenProvider, DataSink)
- Configuration-driven execution
- Database lookups for URN dependencies

## Configuration

```yaml
platform:
  api_base_url: "https://api.linkedin.com/rest"
  api_version: "202601"  # Updated from 202509

linkedin_ads_campaign:
  request: get_campaigns
  type: CAMPAIGN
  pageSize: '10000'  # Updated from 1000
  nested_element: ['totalBudget', 'dailyBudget', 'unitCost']
  processing:
    response_decoration:
      field: account
    convert_unix_timestamp_to_date:
      columns: ['start', 'end']
    # ... more steps
```

## Environment Variables

Required:
- `LINKEDIN_CLIENT_ID`
- `LINKEDIN_CLIENT_SECRET`
- `LINKEDIN_REFRESH_TOKEN`

Optional:
- `STORAGE_TYPE` (vertica)
- `VERTICA_HOST`, `VERTICA_USER`, `VERTICA_PASSWORD`, etc.

## Usage

### Local Development

```python
from social.platforms.linkedin.pipeline import LinkedInPipeline
from social.infrastructure.vertica_sink import VerticaDataSink

# Create pipeline
pipeline = LinkedInPipeline(
    config=config,
    data_sink=VerticaDataSink(...)
)

# Run single table
df = pipeline.run(table_name="linkedin_ads_campaign")

# Run all tables
results = pipeline.run_all_tables()
```

### Container App

```bash
python social/platforms/linkedin/run_linkedin.py
```

## Differences from Other Platforms

**vs Microsoft:**
- LinkedIn: REST with NoQuotedCommasSession
- Microsoft: BingAds SDK + CSV download

**vs Facebook:**
- LinkedIn: URN-based resources
- Facebook: Numeric IDs + SDK object-oriented

**vs Google:**
- LinkedIn: JSON responses
- Google: gRPC + GAQL + Protobuf

## Critical LinkedIn Specifics

### NoQuotedCommasSession

LinkedIn API requires special parameter format where commas are NOT URL-encoded:

```python
# Standard URL encoding would convert:
campaigns=List(urn:li:sponsoredCampaign:123)
# to:
campaigns=List(urn:li:sponsoredCampaign:123)  # Commas stay as-is!

# This is achieved by custom Session.send() override
```

### URN Format

LinkedIn uses URNs (Uniform Resource Names) for all resources:
- Campaign: `urn:li:sponsoredCampaign:12345`
- Account: `urn:li:sponsoredAccount:67890`
- Creative: `urn:li:sponsoredCreative:11111`
- Segment: `urn:li:adSegment:22222`

### Date Range Format

```python
# LinkedIn expects nested tuple format:
dateRange = "(start:(year:2024,month:1,day:15),end:(year:2024,month:1,day:31))"
```

### Insights Dependency

Insights require campaign URNs from database:
1. Fetch campaigns → save to DB
2. Query DB for campaign URNs
3. Iterate URNs to fetch insights

## Dependencies

```
requests==2.32.3
pandas==2.2.3
loguru==0.7.3
pyyaml==6.0.2
vertica-python==1.4.0
```

## Testing

```bash
# Unit tests
pytest tests/unit/test_linkedin_*.py -v

# Integration tests (requires credentials)
pytest tests/integration/test_linkedin_pipeline.py -v
```

## Deployment

See `azure/linkedin-job.yaml` for Container App Job configuration.

## References

- [LinkedIn Marketing API v202601](https://learn.microsoft.com/en-us/linkedin/marketing/)
- [API Version History](https://learn.microsoft.com/en-us/linkedin/marketing/versioning)
- [Best Practices](https://learn.microsoft.com/en-us/linkedin/marketing/best-practices)

## Known Issues & Workarounds

### API Version 202509 Invalid
**Problem:** Previous config used non-existent version 202509
**Fix:** Updated to 202601 (current version as of Jan 2026)

### PageSize Too Small
**Problem:** PageSize 1000 loses data if >1000 campaigns
**Fix:** Updated to 10000 (old working value)

### Rate Limiting
**Mitigation:** Built-in retry with exponential backoff
**Monitor:** 429 responses in logs
