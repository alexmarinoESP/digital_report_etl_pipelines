# Social Ads Platform - Usage Guide

**Version**: 2.0 (Post-Refactoring 2026)
**Last Updated**: 22 January 2026

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Microsoft Ads (Bing)](#microsoft-ads-bing)
3. [LinkedIn Ads](#linkedin-ads)
4. [Facebook Ads](#facebook-ads)
5. [Google Ads](#google-ads)
6. [Orchestrator (All Platforms)](#orchestrator-all-platforms)
7. [Configuration](#configuration)
8. [Advanced Usage](#advanced-usage)

---

## Quick Start

### Installation

```bash
# Clone repository
git clone https://gitlabds.esprinet.com/datascience/digital_report_etl_pipelines.git
cd digital_report_etl_pipelines

# Install dependencies with UV
uv sync --extra social

# Copy environment template
cp .env.example .env
# Edit .env with your credentials
```

### Run Orchestrator (All Platforms)

```bash
# Run all enabled platforms in parallel
uv run python -m social.orchestrator.run_orchestrator

# Execution time: ~65 minutes (vs ~118 minutes sequential)
# Platforms run in dependency-aware parallel groups
```

### Run Single Platform

```bash
# Microsoft Ads
uv run python -m social.platforms.microsoft.pipeline

# LinkedIn Ads
uv run python -m social.platforms.linkedin.pipeline

# Facebook Ads
uv run python -m social.platforms.facebook.pipeline

# Google Ads
uv run python -m social.platforms.google.pipeline
```

---

## Microsoft Ads (Bing)

### Overview

- **API**: BingAds SDK v13
- **Data Format**: CSV download reports
- **Authentication**: OAuth2 + Service Principal
- **Lookback**: 90 days (platform limit)

### Configuration

**File**: `social/platforms/microsoft/config_microsoft_ads.yml`

```yaml
platform:
  name: microsoft
  display_name: Microsoft Advertising (Bing Ads)

credentials:
  developer_token: ${MICROSOFT_DEVELOPER_TOKEN}
  client_id: ${MICROSOFT_CLIENT_ID}
  client_secret: ${MICROSOFT_CLIENT_SECRET}
  tenant_id: ${MICROSOFT_TENANT_ID}

accounts:
  - customer_id: "123456789"
    account_id: "987654321"
    company: "ESP"

reports:
  - name: campaigns
    type: CampaignPerformanceReport
    aggregation: Daily
    columns:
      - AccountId
      - CampaignId
      - CampaignName
      - Impressions
      - Clicks
      - Spend

  - name: ad_groups
    type: AdGroupPerformanceReport
    aggregation: Daily
```

### Usage Example

```python
from social.platforms.microsoft.pipeline import MicrosoftPipeline
from social.infrastructure.file_token_provider import FileTokenProvider
from social.infrastructure.vertica_sink import VerticaSink
from social.core.config import PipelineConfig

# Load configuration
config = PipelineConfig.from_yaml(
    "social/platforms/microsoft/config_microsoft_ads.yml"
)

# Setup dependencies
token_provider = FileTokenProvider("tokens.json")
data_sink = VerticaSink(
    host="vertica.example.com",
    port=5433,
    database="analytics",
    user="etl_user",
    password="password",
    schema="social_ads"
)

# Create and run pipeline
pipeline = MicrosoftPipeline(
    token_provider=token_provider,
    data_sink=data_sink,
    config=config
)

result = pipeline.run(
    start_date="2024-01-01",
    end_date="2024-01-31"
)

print(f"Success: {result.success}")
print(f"Rows processed: {result.rows_processed}")
print(f"Duration: {result.duration_seconds}s")
```

### Key Features

**CSV Download & Processing**:
```python
# Client downloads CSV reports
client = MicrosoftAdsClient(...)
csv_path = client.download_campaigns_report(
    start_date="2024-01-01",
    end_date="2024-01-31"
)

# Processor transforms CSV to DataFrame
processor = MicrosoftProcessor(pd.read_csv(csv_path))
df = (
    processor
    .add_company("ESP")
    .fix_column_types(['account_id', 'campaign_id'])
    .get_df()
)
```

### Common Issues

**Issue**: `AuthenticationError: Invalid client credentials`
**Solution**: Verify Azure AD app registration and service principal setup

**Issue**: `ReportDownloadError: Report not ready`
**Solution**: Increase polling timeout in config (default: 300s)

---

## LinkedIn Ads

### Overview

- **API**: REST API v202601
- **Data Format**: JSON responses
- **Authentication**: OAuth2 Bearer token
- **Lookback**: 150 days (longer than other platforms)
- **Special Requirement**: NoQuotedCommasSession (commas in URLs must NOT be encoded)

### Configuration

**File**: `social/platforms/linkedin/config_linkedin_ads.yml`

```yaml
platform:
  name: linkedin
  display_name: LinkedIn Marketing API
  api_version: "202601"

credentials:
  access_token: ${LINKEDIN_ACCESS_TOKEN}

accounts:
  - account_id: "123456789"
    company: "ESP"

endpoints:
  base_url: "https://api.linkedin.com/rest"
  campaigns: "/adAccounts/{account_id}/adCampaigns"
  insights: "/adAnalytics"

date_range:
  max_lookback_days: 150
  default_time_granularity: "DAILY"
```

### Usage Example

```python
from social.platforms.linkedin.pipeline import LinkedInPipeline
from social.infrastructure.file_token_provider import FileTokenProvider
from social.infrastructure.vertica_sink import VerticaSink
from social.core.config import PipelineConfig

# Load configuration
config = PipelineConfig.from_yaml(
    "social/platforms/linkedin/config_linkedin_ads.yml"
)

# Setup dependencies
token_provider = FileTokenProvider("tokens.json")
data_sink = VerticaSink(...)

# Create and run pipeline
pipeline = LinkedInPipeline(
    token_provider=token_provider,
    data_sink=data_sink,
    config=config
)

result = pipeline.run(
    start_date="2024-01-01",
    end_date="2024-01-31"
)
```

### Key Features

**URN Extraction**:
```python
# LinkedIn uses URN format for IDs
# Example: "urn:li:sponsoredCampaign:123456"

from social.platforms.linkedin.processor import LinkedInProcessor

processor = LinkedInProcessor(raw_df)
df = (
    processor
    .extract_id_from_urn(['campaign_id', 'account_id'])  # Extract numeric IDs
    .add_company("ESP")
    .get_df()
)

# campaign_id: "urn:li:sponsoredCampaign:123456" → "123456"
```

**NoQuotedCommasSession**:
```python
# LinkedIn requires special parameter encoding
# Commas in lists must NOT be URL-encoded

from social.platforms.linkedin.http_client import LinkedInHTTPClient

client = LinkedInHTTPClient(access_token)

# Internally uses NoQuotedCommasSession
# campaigns=List(urn:li:sponsoredCampaign:123,urn:li:sponsoredCampaign:456)
# NOT: campaigns=List(urn:li:sponsoredCampaign:123%2Curn:li:sponsoredCampaign:456)
```

### Common Issues

**Issue**: `LinkedInAPIError: Invalid campaign URN`
**Solution**: Ensure URN format is correct: `urn:li:sponsoredCampaign:{id}`

**Issue**: `LinkedInAPIError: Date range exceeds 150 days`
**Solution**: Split date range into chunks ≤150 days

---

## Facebook Ads

### Overview

- **API**: Graph API SDK v19.0
- **Data Format**: JSON with nested arrays
- **Authentication**: User Access Token + App credentials
- **Special Feature**: Nested breakdowns (actions, action_values)

### Configuration

**File**: `social/platforms/facebook/config_facebook_ads.yml`

```yaml
platform:
  name: facebook
  display_name: Facebook Marketing API
  api_version: "v19.0"

credentials:
  access_token: ${FACEBOOK_ACCESS_TOKEN}
  app_id: ${FACEBOOK_APP_ID}
  app_secret: ${FACEBOOK_APP_SECRET}

accounts:
  - account_id: "act_123456789"
    company: "ESP"

insights:
  level: campaign
  breakdowns:
    - age
    - gender
  action_breakdowns:
    - action_type
  fields:
    - impressions
    - clicks
    - spend
    - actions
    - action_values
```

### Usage Example

```python
from social.platforms.facebook.pipeline import FacebookPipeline
from social.infrastructure.file_token_provider import FileTokenProvider
from social.infrastructure.vertica_sink import VerticaSink
from social.core.config import PipelineConfig

# Load configuration
config = PipelineConfig.from_yaml(
    "social/platforms/facebook/config_facebook_ads.yml"
)

# Setup dependencies
token_provider = FileTokenProvider("tokens.json")
data_sink = VerticaSink(...)

# Create and run pipeline
pipeline = FacebookPipeline(
    token_provider=token_provider,
    data_sink=data_sink,
    config=config
)

result = pipeline.run(
    start_date="2024-01-01",
    end_date="2024-01-31"
)
```

### Key Features

**Nested Breakdowns Processing**:
```python
# Facebook returns actions as arrays of dicts
# actions = [
#     {"action_type": "link_click", "value": 50},
#     {"action_type": "page_engagement", "value": 120},
#     ...
# ]

from social.platforms.facebook.processor import FacebookProcessor

processor = FacebookProcessor(raw_df)
df = (
    processor
    .extract_nested_actions(action_col='actions', prefix='action_')
    .extract_nested_action_values(action_col='action_values', prefix='value_')
    .add_company("ESP")
    .get_df()
)

# Output columns:
# - action_link_click = 50
# - action_page_engagement = 120
# - value_link_click = 5.50
# - value_page_engagement = 12.00
```

**Multi-Account Iteration**:
```python
# Facebook adapter supports multiple ad accounts
adapter = FacebookAdapter(access_token, app_id, app_secret)

accounts = ["act_123456789", "act_987654321"]
for account_id in accounts:
    campaigns = adapter.get_campaigns(account_id)
    for campaign in campaigns:
        insights = adapter.get_insights(
            campaign_id=campaign['id'],
            start_date="2024-01-01",
            end_date="2024-01-31",
            breakdowns=['age', 'gender']
        )
```

### Common Issues

**Issue**: `FacebookAPIError: Invalid access token`
**Solution**: Refresh token or generate new one from Facebook Business Manager

**Issue**: `FacebookAPIError: Rate limit exceeded`
**Solution**: Implement exponential backoff (already in pipeline)

---

## Google Ads

### Overview

- **API**: gRPC + Protobuf
- **Query Language**: GAQL (Google Ads Query Language, SQL-like)
- **Authentication**: OAuth2 refresh token
- **Critical Feature**: Micros conversion (costs are in 1/1,000,000 of currency)

### Configuration

**File**: `social/platforms/google/config_google_ads.yml`

```yaml
platform:
  name: google
  display_name: Google Ads API
  api_version: "v16"

credentials:
  developer_token: ${GOOGLE_ADS_DEVELOPER_TOKEN}
  client_id: ${GOOGLE_ADS_CLIENT_ID}
  client_secret: ${GOOGLE_ADS_CLIENT_SECRET}
  refresh_token: ${GOOGLE_ADS_REFRESH_TOKEN}

accounts:
  - customer_id: "1234567890"  # 10 digits, no hyphens
    company: "ESP"

queries:
  campaign_performance: |
    SELECT
      campaign.id,
      campaign.name,
      campaign.status,
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros,
      segments.date
    FROM campaign
    WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'

  ad_group_performance: |
    SELECT
      ad_group.id,
      ad_group.name,
      campaign.id,
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros,
      segments.date
    FROM ad_group
    WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
```

### Usage Example

```python
from social.platforms.google.pipeline import GooglePipeline
from social.infrastructure.file_token_provider import FileTokenProvider
from social.infrastructure.vertica_sink import VerticaSink
from social.core.config import PipelineConfig

# Load configuration
config = PipelineConfig.from_yaml(
    "social/platforms/google/config_google_ads.yml"
)

# Setup dependencies
token_provider = FileTokenProvider("tokens.json")
data_sink = VerticaSink(...)

# Create and run pipeline
pipeline = GooglePipeline(
    token_provider=token_provider,
    data_sink=data_sink,
    config=config
)

result = pipeline.run(
    start_date="2024-01-01",
    end_date="2024-01-31"
)
```

### Key Features

**GAQL Queries**:
```python
from social.platforms.google.adapter import GoogleAdapter

adapter = GoogleAdapter(
    developer_token="xxx",
    client_id="yyy",
    client_secret="zzz",
    refresh_token="www",
    customer_id="1234567890"
)

# Execute GAQL query
query = """
    SELECT
        campaign.id,
        campaign.name,
        metrics.cost_micros,
        metrics.conversions
    FROM campaign
    WHERE segments.date BETWEEN '2024-01-01' AND '2024-01-31'
        AND campaign.status = 'ENABLED'
    ORDER BY metrics.cost_micros DESC
    LIMIT 100
"""

results = adapter.execute_query(query)
```

**Micros Conversion** (CRITICAL):
```python
# Google Ads returns all costs in micros (1/1,000,000)
# 5000000 micros = 5.00 EUR

from social.platforms.google.processor import GoogleProcessor

processor = GoogleProcessor(raw_df)
df = (
    processor
    .convert_costs(['cost_micros', 'cpc_micros'])  # CRITICAL: divide by 1,000,000
    .add_company("ESP")
    .get_df()
)

# cost_micros: 5000000 → cost: 5.00
```

### Common Issues

**Issue**: `GoogleAdsError: Customer ID must be 10 digits`
**Solution**: Remove hyphens from customer ID (e.g., "123-456-7890" → "1234567890")

**Issue**: `GoogleAdsError: Invalid GAQL query`
**Solution**: Validate query syntax at https://developers.google.com/google-ads/api/fields/v16/overview

**Issue**: Costs appear 1,000,000x too high
**Solution**: Ensure `convert_costs()` is called in processor

---

## Orchestrator (All Platforms)

### Overview

The orchestrator coordinates execution of all platforms with:
- **Dependency management** (topological sorting)
- **Parallel execution** (configurable groups)
- **Retry logic** (exponential backoff)
- **Comprehensive monitoring**

### Configuration

**File**: `social/orchestrator/orchestrator_config.yml`

```yaml
orchestrator:
  parallel_execution: true
  max_parallel: 2
  continue_on_failure: true
  retry:
    max_attempts: 2
    backoff_seconds: 60

  default_date_range:
    start_date: "2024-01-01"
    end_date: "2024-01-31"

platforms:
  - name: microsoft
    enabled: true
    priority: 1
    timeout: 1800  # 30 minutes
    depends_on: []
    retry:
      max_attempts: 2
      backoff_seconds: 60

  - name: linkedin
    enabled: true
    priority: 2
    timeout: 1200  # 20 minutes
    depends_on: []

  - name: facebook
    enabled: true
    priority: 2
    timeout: 1200
    depends_on: []

  - name: google
    enabled: true
    priority: 3
    timeout: 1500  # 25 minutes
    depends_on: [microsoft]  # Runs after Microsoft

parallel_groups:
  # Group 1: Microsoft and LinkedIn run together
  - [microsoft, linkedin]

  # Group 2: Facebook and Google run together (after Group 1)
  - [facebook, google]

monitoring:
  log_level: INFO
  metrics_enabled: true
  slack_webhook: ${SLACK_WEBHOOK_URL}  # Optional
```

### Usage

**Run All Platforms**:
```python
from social.orchestrator.orchestrator import SocialAdsOrchestrator

orchestrator = SocialAdsOrchestrator(
    config_path="social/orchestrator/orchestrator_config.yml"
)

result = orchestrator.run_all_platforms(
    start_date="2024-01-01",  # Override config
    end_date="2024-01-31"     # Override config
)

print(f"Total platforms: {result.total_platforms}")
print(f"Successful: {result.successful_platforms}")
print(f"Failed: {result.failed_platforms}")
print(f"Total duration: {result.total_duration_seconds}s")

# Detailed platform results
for platform_result in result.platform_results:
    print(f"\n{platform_result.platform_name}:")
    print(f"  Status: {platform_result.status}")
    print(f"  Rows: {platform_result.rows_processed}")
    print(f"  Duration: {platform_result.duration_seconds}s")
    if platform_result.error:
        print(f"  Error: {platform_result.error}")
```

**Run Single Platform via Orchestrator**:
```python
orchestrator = SocialAdsOrchestrator()

result = orchestrator.run_platform(
    platform_name="linkedin",
    start_date="2024-01-01",
    end_date="2024-01-31"
)
```

**Command Line**:
```bash
# Run all platforms
uv run python -m social.orchestrator.run_orchestrator

# Run with date override
uv run python -m social.orchestrator.run_orchestrator \
    --start-date 2024-01-01 \
    --end-date 2024-01-31

# Run specific platform
uv run python -m social.orchestrator.run_orchestrator \
    --platform linkedin
```

### Execution Flow

1. **Load Configuration**: Read `orchestrator_config.yml`
2. **Validate Dependencies**: Check for circular dependencies
3. **Topological Sort**: Determine execution order
4. **Create Parallel Groups**: Group independent platforms
5. **Execute Groups**:
   - Group 1: microsoft + linkedin (parallel)
   - Wait for Group 1 completion
   - Group 2: facebook + google (parallel)
6. **Aggregate Results**: Collect metrics and status
7. **Generate Report**: Create execution summary

### Performance

| Execution Mode | Time | Platforms |
|---------------|------|-----------|
| Sequential | ~118 min | microsoft → linkedin → facebook → google |
| Parallel (2) | ~65 min | [microsoft, linkedin] → [facebook, google] |
| Parallel (4) | ~35 min | [microsoft, linkedin, facebook, google] |

**Recommendation**: Use `max_parallel: 2` to balance speed and resource usage.

---

## Configuration

### Environment Variables

**File**: `.env`

```bash
# Vertica Database
VERTICA_HOST=vertica.example.com
VERTICA_PORT=5433
VERTICA_DATABASE=analytics
VERTICA_USER=etl_user
VERTICA_PASSWORD=password
VERTICA_SCHEMA=social_ads

# Microsoft Ads
MICROSOFT_DEVELOPER_TOKEN=xxx
MICROSOFT_CLIENT_ID=yyy
MICROSOFT_CLIENT_SECRET=zzz
MICROSOFT_TENANT_ID=www

# LinkedIn Ads
LINKEDIN_ACCESS_TOKEN=xxx

# Facebook Ads
FACEBOOK_ACCESS_TOKEN=xxx
FACEBOOK_APP_ID=yyy
FACEBOOK_APP_SECRET=zzz

# Google Ads
GOOGLE_ADS_DEVELOPER_TOKEN=xxx
GOOGLE_ADS_CLIENT_ID=yyy
GOOGLE_ADS_CLIENT_SECRET=zzz
GOOGLE_ADS_REFRESH_TOKEN=www

# Azure Table Storage (Optional)
AZURE_STORAGE_CONNECTION_STRING=xxx
AZURE_STORAGE_TABLE_NAME=socialads

# Slack Notifications (Optional)
SLACK_WEBHOOK_URL=xxx
```

### Tokens File

**File**: `tokens.json`

```json
{
  "microsoft": "Bearer xxx...",
  "linkedin": "Bearer yyy...",
  "facebook": "Bearer zzz...",
  "google": "Bearer www..."
}
```

---

## Advanced Usage

### Custom Token Provider

```python
from typing import Protocol
from social.core.protocols import TokenProvider

class AzureKeyVaultTokenProvider:
    """Token provider using Azure Key Vault."""

    def __init__(self, vault_url: str):
        from azure.keyvault.secrets import SecretClient
        from azure.identity import DefaultAzureCredential

        self.client = SecretClient(
            vault_url=vault_url,
            credential=DefaultAzureCredential()
        )

    def get_token(self, platform: str) -> str:
        """Get token from Key Vault."""
        secret_name = f"{platform}-access-token"
        secret = self.client.get_secret(secret_name)
        return secret.value

    def refresh_token(self, platform: str) -> str:
        """Refresh token in Key Vault."""
        # Implement refresh logic
        return self.get_token(platform)

# Use in pipeline
token_provider = AzureKeyVaultTokenProvider(
    vault_url="https://myvault.vault.azure.net/"
)
pipeline = LinkedInPipeline(token_provider, data_sink, config)
```

### Custom Data Sink

```python
from typing import Protocol
import pandas as pd
from social.core.protocols import DataSink

class S3DataSink:
    """Data sink writing to S3 as Parquet."""

    def __init__(self, bucket: str, prefix: str):
        import boto3
        self.s3 = boto3.client('s3')
        self.bucket = bucket
        self.prefix = prefix

    def write(self, df: pd.DataFrame, table: str, **kwargs) -> None:
        """Write DataFrame to S3 as Parquet."""
        import io

        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow')

        key = f"{self.prefix}/{table}/{pd.Timestamp.now():%Y%m%d}.parquet"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer.getvalue()
        )

    def close(self) -> None:
        """No cleanup needed for S3."""
        pass

# Use in pipeline
data_sink = S3DataSink(bucket="my-bucket", prefix="social-ads")
pipeline = LinkedInPipeline(token_provider, data_sink, config)
```

### Date Range Chunking

```python
from datetime import datetime, timedelta

def chunk_date_range(start_date: str, end_date: str, chunk_days: int = 30):
    """Split date range into chunks."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_days), end)
        yield (
            current.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d")
        )
        current = chunk_end + timedelta(days=1)

# Use in pipeline
pipeline = LinkedInPipeline(token_provider, data_sink, config)

for start, end in chunk_date_range("2024-01-01", "2024-12-31", chunk_days=30):
    result = pipeline.run(start_date=start, end_date=end)
    print(f"Processed {start} to {end}: {result.rows_processed} rows")
```

### Error Handling & Retry

```python
import time
from social.platforms.linkedin.pipeline import LinkedInPipeline

def run_with_retry(pipeline, start_date, end_date, max_attempts=3):
    """Run pipeline with exponential backoff retry."""
    for attempt in range(max_attempts):
        try:
            result = pipeline.run(start_date, end_date)
            if result.success:
                return result
            else:
                print(f"Attempt {attempt + 1} failed: {result.error}")
        except Exception as e:
            print(f"Attempt {attempt + 1} error: {e}")

        if attempt < max_attempts - 1:
            wait_time = 2 ** attempt * 60  # 1min, 2min, 4min
            print(f"Retrying in {wait_time}s...")
            time.sleep(wait_time)

    raise Exception(f"Pipeline failed after {max_attempts} attempts")

# Usage
pipeline = LinkedInPipeline(token_provider, data_sink, config)
result = run_with_retry(pipeline, "2024-01-01", "2024-01-31")
```

---

## Next Steps

- [API_REFERENCE.md](API_REFERENCE.md) - Detailed API documentation
- [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) - Deploy to Azure Container Apps
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Common issues and solutions

---

**Questions or Issues?**

Contact: Alessandro Benelli (alessandro.benelli@esprinet.com)
