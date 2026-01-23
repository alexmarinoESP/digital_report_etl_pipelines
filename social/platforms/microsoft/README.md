# Microsoft Ads Platform

Complete independent implementation for Microsoft Advertising (Bing Ads) ETL pipeline.

## Overview

This platform extracts advertising data from Microsoft Ads using the BingAds SDK v13, processes it, and loads it into Vertica or Azure Table Storage.

## Architecture

```
microsoft/
├── authenticator.py    # OAuth2 + Service Principal auth (3 fallback strategies)
├── client.py          # BingAds SDK wrapper + CSV processing
├── processor.py       # Data transformation specific to Microsoft
├── pipeline.py        # ETL orchestration
└── config_microsoft_ads.yml  # Configuration
```

## Key Features

### Authentication (authenticator.py)
- **3-way fallback strategy**:
  1. Stored refresh token (from tokens.json)
  2. Service Principal (Azure AD client credentials)
  3. Browser OAuth flow with local HTTP callback server
- Token persistence and auto-refresh
- Container-ready (service principal priority for serverless)

### Data Extraction (client.py)
- BingAds SDK v13 integration
- Report types: AdPerformanceReport, CampaignPerformanceReport
- CSV download with dynamic header detection
- Footer cleaning (removes Microsoft copyright rows)
- Account hierarchy management

### Processing (processor.py)
- Type conversions (IDs to strings, percentages to floats)
- Metadata addition (row_loaded_date, IngestionTimestamp)
- Minimal processing (CSV cleaning done in client)

### Pipeline (pipeline.py)
- Complete ETL orchestration
- Dependency injection (TokenProvider, DataSink)
- Configuration-driven execution
- Error handling and logging

## Configuration

```yaml
platform:
  api_base_url: "https://bingads.microsoft.com"
  api_version: "v13"

microsoft_ads:
  client_id: "${MICROSOFT_ADS_CLIENT_ID}"
  client_secret: "${MICROSOFT_ADS_CLIENT_SECRET}"
  developer_token: "${MICROSOFT_ADS_DEVELOPER_TOKEN}"
  customer_id: "${MICROSOFT_ADS_CUSTOMER_ID}"
  account_id: "${MICROSOFT_ADS_ACCOUNT_ID}"
  tenant_id: "${AZURE_TENANT_ID}"  # For service principal
```

## Environment Variables

Required:
- `MICROSOFT_ADS_CLIENT_ID`
- `MICROSOFT_ADS_CLIENT_SECRET`
- `MICROSOFT_ADS_DEVELOPER_TOKEN`
- `MICROSOFT_ADS_CUSTOMER_ID`
- `MICROSOFT_ADS_ACCOUNT_ID`

Optional:
- `AZURE_TENANT_ID` (for service principal auth in containers)
- `STORAGE_TYPE` (vertica | azure_table)
- `VERTICA_HOST`, `VERTICA_USER`, `VERTICA_PASSWORD`, etc.

## Usage

### Local Development

```python
from social.platforms.microsoft.pipeline import MicrosoftAdsPipeline
from social.infrastructure.vertica_sink import VerticaDataSink

# Create data sink
data_sink = VerticaDataSink(host="...", user="...", password="...")

# Create pipeline
pipeline = MicrosoftAdsPipeline(
    config=config,
    data_sink=data_sink
)

# Run
df = pipeline.run(table_name="microsoft_ads_report")
```

### Container App

```bash
python social/platforms/microsoft/run_microsoft.py
```

## Differences from Other Platforms

**vs LinkedIn:**
- Microsoft: BingAds SDK (factory pattern) + CSV download
- LinkedIn: Custom REST client + NoQuotedCommasSession

**vs Facebook:**
- Microsoft: CSV processing with header detection
- Facebook: SDK object-oriented + nested response handling

**vs Google:**
- Microsoft: CSV download + cleaning
- Google: gRPC + GAQL queries + Protobuf parsing

## Dependencies

```
bingads==13.0.24.2              # Microsoft Ads SDK
vertica-python==1.4.0           # Database connector
azure-data-tables==12.7.0       # Azure Table Storage
pandas==2.2.3
loguru==0.7.3
pyyaml==6.0.2
```

## Testing

```bash
# Unit tests
pytest tests/unit/test_microsoft_*.py -v

# Integration tests (requires credentials)
pytest tests/integration/test_microsoft_pipeline.py -v
```

## Deployment

See `azure/microsoft-ads-job.yaml` for Container App Job configuration.

## References

- [Microsoft Advertising API Documentation](https://docs.microsoft.com/en-us/advertising/guides/)
- [BingAds Python SDK](https://github.com/BingAds/BingAds-Python-SDK)
- [OAuth2 Guide](https://docs.microsoft.com/en-us/advertising/guides/authentication-oauth)
