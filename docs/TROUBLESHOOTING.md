# Social Ads Platform - Troubleshooting Guide

**Version**: 2.0 (Post-Refactoring 2026)
**Last Updated**: 22 January 2026

---

## Table of Contents

1. [Common Issues](#common-issues)
2. [Platform-Specific Issues](#platform-specific-issues)
3. [Authentication Errors](#authentication-errors)
4. [Data Processing Errors](#data-processing-errors)
5. [Deployment Issues](#deployment-issues)
6. [Performance Problems](#performance-problems)
7. [Debugging Tools](#debugging-tools)

---

## Common Issues

### Issue: ModuleNotFoundError

**Error**:
```
ModuleNotFoundError: No module named 'social'
```

**Cause**: Python path not set correctly or dependencies not installed

**Solutions**:
```bash
# Solution 1: Install dependencies
uv sync --extra social

# Solution 2: Set PYTHONPATH
export PYTHONPATH=/path/to/digital_report_etl_pipelines

# Solution 3: Run from project root
cd /path/to/digital_report_etl_pipelines
uv run python -m social.orchestrator.run_orchestrator
```

---

### Issue: TokenError - Cannot retrieve token

**Error**:
```
TokenError: Token not found for platform 'linkedin'
```

**Cause**: Token file missing or incorrectly formatted

**Solutions**:
```bash
# Check token file exists
ls -la tokens.json

# Verify token file format
cat tokens.json
# Expected format:
# {
#   "microsoft": "Bearer xxx...",
#   "linkedin": "Bearer yyy...",
#   "facebook": "Bearer zzz...",
#   "google": "Bearer www..."
# }

# Check file permissions
chmod 600 tokens.json

# Validate JSON syntax
python -c "import json; json.load(open('tokens.json'))"
```

---

### Issue: Connection refused to Vertica

**Error**:
```
vertica_python.errors.ConnectionError: Connection refused
```

**Cause**: Database not reachable or credentials incorrect

**Solutions**:
```bash
# Test connectivity
telnet vertica.example.com 5433

# Verify credentials in .env
cat .env | grep VERTICA

# Test connection with Python
python -c "
import vertica_python
conn = vertica_python.connect(
    host='vertica.example.com',
    port=5433,
    user='user',
    password='password',
    database='analytics'
)
print('Connected successfully!')
conn.close()
"

# Check firewall rules
az network nsg rule list --resource-group rg-socialads-prod --nsg-name nsg-socialads
```

---

### Issue: Empty DataFrame / No data returned

**Error**:
```
WARNING: No data returned from API for date range 2024-01-01 to 2024-01-31
```

**Causes & Solutions**:

1. **Date range too old**:
   ```python
   # Check platform-specific lookback limits:
   # - Microsoft: 90 days
   # - LinkedIn: 150 days
   # - Facebook: 37 months
   # - Google: No hard limit (but performance degrades)

   # Solution: Use more recent dates
   result = pipeline.run(
       start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
       end_date=datetime.now().strftime("%Y-%m-%d")
   )
   ```

2. **No campaigns in account**:
   ```bash
   # Verify campaigns exist
   # For LinkedIn:
   curl -X GET "https://api.linkedin.com/rest/adAccounts/{account_id}/adCampaigns" \
     -H "Authorization: Bearer {token}" \
     -H "LinkedIn-Version: 202601"
   ```

3. **Incorrect account ID**:
   ```yaml
   # Check config file
   accounts:
     - account_id: "123456789"  # Verify this ID is correct
       company: "ESP"
   ```

---

## Platform-Specific Issues

### Microsoft Ads

#### Issue: AuthenticationError - Invalid client credentials

**Error**:
```
AuthenticationError: AADSTS700016: Application with identifier 'xxx' was not found
```

**Solution**:
```bash
# Verify Azure AD app registration
az ad app show --id {client_id}

# Check service principal exists
az ad sp show --id {client_id}

# Verify permissions
# Required: Microsoft Advertising API access
# https://ads.microsoft.com → Tools → API Center → Request Access

# Generate new client secret
az ad app credential reset --id {client_id}
```

#### Issue: ReportDownloadError - Report not ready

**Error**:
```
ReportDownloadError: Report generation timed out after 300 seconds
```

**Solutions**:
```yaml
# Increase timeout in config
client:
  report_timeout: 600  # 10 minutes
  poll_interval: 10    # Check every 10 seconds

# Or reduce date range
date_range:
  max_days_per_request: 30
```

#### Issue: CSV parsing error

**Error**:
```
pandas.errors.ParserError: Error tokenizing data
```

**Solution**:
```python
# Microsoft CSV may have encoding issues
processor = MicrosoftProcessor(
    pd.read_csv(csv_path, encoding='utf-8-sig')  # Handle BOM
)

# Or specify delimiter explicitly
processor = MicrosoftProcessor(
    pd.read_csv(csv_path, delimiter='\t')  # Tab-separated
)
```

---

### LinkedIn Ads

#### Issue: LinkedInAPIError - Invalid URN format

**Error**:
```
LinkedInAPIError: Invalid campaign URN format
```

**Solution**:
```python
# Ensure URN format is correct
# Correct: "urn:li:sponsoredCampaign:123456"
# Wrong: "123456", "urn:li:campaign:123456"

# Extract numeric ID if needed
from social.utils.urn_utils import extract_id_from_urn
numeric_id = extract_id_from_urn("urn:li:sponsoredCampaign:123456")
# Returns: "123456"
```

#### Issue: Date range exceeds 150 days

**Error**:
```
LinkedInAPIError: Date range cannot exceed 150 days
```

**Solution**:
```python
# Split date range into chunks
from datetime import datetime, timedelta

def chunk_date_range(start_date, end_date, max_days=150):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=max_days), end)
        yield (
            current.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d")
        )
        current = chunk_end + timedelta(days=1)

# Usage
pipeline = LinkedInPipeline(...)
for start, end in chunk_date_range("2024-01-01", "2024-12-31"):
    result = pipeline.run(start_date=start, end_date=end)
```

#### Issue: NoQuotedCommasSession not working

**Error**:
```
LinkedInAPIError: Invalid request parameters
```

**Solution**:
```python
# Verify NoQuotedCommasSession is being used
from social.platforms.linkedin.http_client import LinkedInHTTPClient

client = LinkedInHTTPClient(access_token)
# Internally uses NoQuotedCommasSession

# Check request parameters
# Commas in 'campaigns' parameter must NOT be URL-encoded
# Correct: campaigns=List(urn:li:sponsoredCampaign:123,urn:li:sponsoredCampaign:456)
# Wrong: campaigns=List(urn:li:sponsoredCampaign:123%2Curn:li:sponsoredCampaign:456)
```

---

### Facebook Ads

#### Issue: FacebookAPIError - Invalid access token

**Error**:
```
FacebookAPIError: Error validating access token: Session has expired
```

**Solutions**:
```bash
# Generate new long-lived token
curl "https://graph.facebook.com/v19.0/oauth/access_token \
  ?grant_type=fb_exchange_token \
  &client_id={app_id} \
  &client_secret={app_secret} \
  &fb_exchange_token={short_lived_token}"

# Or use Graph API Explorer
# https://developers.facebook.com/tools/explorer/

# Update tokens.json with new token
```

#### Issue: Rate limit exceeded

**Error**:
```
FacebookAPIError: (#17) User request limit reached
```

**Solutions**:
```yaml
# Add rate limiting to config
api:
  rate_limit:
    requests_per_hour: 200
    wait_on_limit: true
    backoff_seconds: 300

# Or reduce parallel requests
client:
  max_concurrent_requests: 5
```

#### Issue: Nested actions not extracted

**Error**:
```
KeyError: 'action_link_click'
```

**Solution**:
```python
# Ensure extract_nested_actions() is called
processor = FacebookProcessor(raw_df)
df = (
    processor
    .extract_nested_actions(action_col='actions', prefix='action_')
    .extract_nested_action_values(action_col='action_values', prefix='value_')
    .get_df()
)

# Check if 'actions' column exists and is not empty
if 'actions' in raw_df.columns:
    print(raw_df['actions'].head())
```

---

### Google Ads

#### Issue: GoogleAdsError - Invalid customer ID

**Error**:
```
GoogleAdsError: Customer ID must be 10 digits without hyphens
```

**Solution**:
```python
# Remove hyphens from customer ID
# Wrong: "123-456-7890"
# Correct: "1234567890"

# In config:
accounts:
  - customer_id: "1234567890"  # No hyphens!
    company: "ESP"
```

#### Issue: Costs appear 1,000,000x too high

**Error**:
```
WARNING: Cost appears abnormally high: 5000000.00 EUR
```

**Solution**:
```python
# CRITICAL: Google Ads returns costs in micros
# 5000000 micros = 5.00 EUR

# Ensure convert_costs() is called
processor = GoogleProcessor(raw_df)
df = (
    processor
    .convert_costs(['cost_micros', 'cpc_micros'])  # MUST divide by 1,000,000
    .get_df()
)

# Verify conversion
print(f"Before: {raw_df['cost_micros'].sum()}")
print(f"After: {df['cost'].sum()}")
```

#### Issue: Invalid GAQL query

**Error**:
```
GoogleAdsError: Invalid GAQL query syntax
```

**Solutions**:
```python
# Validate query at Google Ads Query Validator
# https://developers.google.com/google-ads/api/fields/v16/overview

# Common mistakes:
# 1. Wrong field names
query = """
    SELECT campaign.id, campaign.name
    FROM campaign  -- Correct
"""

# 2. Missing WHERE clause for date range
query = """
    SELECT campaign.id, metrics.cost_micros
    FROM campaign
    WHERE segments.date BETWEEN '2024-01-01' AND '2024-01-31'  -- Required!
"""

# 3. Invalid date format
# Correct: 'YYYY-MM-DD'
# Wrong: 'YYYY/MM/DD', 'DD-MM-YYYY'
```

---

## Authentication Errors

### Issue: OAuth2 token expired

**Error**:
```
TokenError: OAuth2 token expired
```

**Solutions**:

1. **Refresh token manually**:
   ```bash
   # LinkedIn
   curl -X POST https://www.linkedin.com/oauth/v2/accessToken \
     -d grant_type=refresh_token \
     -d refresh_token={refresh_token} \
     -d client_id={client_id} \
     -d client_secret={client_secret}

   # Google Ads
   curl -X POST https://oauth2.googleapis.com/token \
     -d grant_type=refresh_token \
     -d refresh_token={refresh_token} \
     -d client_id={client_id} \
     -d client_secret={client_secret}
   ```

2. **Implement automatic token refresh**:
   ```python
   from social.infrastructure.file_token_provider import FileTokenProvider

   class AutoRefreshTokenProvider(FileTokenProvider):
       def get_token(self, platform: str) -> str:
           try:
               return super().get_token(platform)
           except TokenError:
               # Auto-refresh if expired
               return self.refresh_token(platform)
   ```

---

### Issue: Service Principal authentication fails

**Error**:
```
AuthenticationError: AADSTS700016: Invalid service principal
```

**Solution**:
```bash
# Verify service principal exists
az ad sp show --id {client_id}

# Verify permissions
az role assignment list --assignee {client_id}

# Reset client secret
az ad app credential reset --id {client_id} --append

# Test authentication
az login --service-principal \
  -u {client_id} \
  -p {client_secret} \
  --tenant {tenant_id}
```

---

## Data Processing Errors

### Issue: DataFrame column not found

**Error**:
```
KeyError: 'campaign_id'
```

**Solutions**:
```python
# Check column names
print(df.columns.tolist())

# Handle missing columns gracefully
if 'campaign_id' in df.columns:
    df['campaign_id'] = df['campaign_id'].astype(str)
else:
    logger.warning("campaign_id column not found")

# Use get() for safe access
campaign_id = df.get('campaign_id', pd.Series())
```

---

### Issue: Data type mismatch

**Error**:
```
TypeError: Cannot convert float to int
```

**Solution**:
```python
# Fix ID types
from social.utils.processing import fix_id_type

df['campaign_id'] = df['campaign_id'].apply(fix_id_type)

# Or use pandas
df['campaign_id'] = df['campaign_id'].fillna(0).astype(int).astype(str)
```

---

### Issue: Emoji causing encoding errors

**Error**:
```
UnicodeEncodeError: 'latin-1' codec can't encode character
```

**Solution**:
```python
# Remove emojis
from social.utils.processing import deEmojify

df['campaign_name'] = df['campaign_name'].apply(deEmojify)

# Or specify UTF-8 encoding when writing
data_sink.write(df, table='campaigns', encoding='utf-8')
```

---

## Deployment Issues

### Issue: Container image pull failure

**Error**:
```
Failed to pull image: unauthorized
```

**Solutions**:
```bash
# Verify ACR credentials
az acr credential show --name {acr_name}

# Login to ACR
az acr login --name {acr_name}

# Test image pull
docker pull {acr_name}.azurecr.io/social-ads:latest

# Update Container App credentials
az containerapp job update \
  --name {job_name} \
  --resource-group {resource_group} \
  --registry-server {acr_name}.azurecr.io \
  --registry-username {acr_username} \
  --registry-password {acr_password}
```

---

### Issue: Container App Job fails to start

**Error**:
```
Container failed to start: exit code 1
```

**Solutions**:
```bash
# View logs
az containerapp job logs show \
  --name {job_name} \
  --resource-group {resource_group}

# Check execution history
az containerapp job execution list \
  --name {job_name} \
  --resource-group {resource_group}

# View specific execution logs
az containerapp job execution show \
  --name {job_name} \
  --resource-group {resource_group} \
  --job-execution-name {execution_name}
```

---

### Issue: Environment variables not set

**Error**:
```
KeyError: 'VERTICA_HOST'
```

**Solutions**:
```bash
# List environment variables
az containerapp job show \
  --name {job_name} \
  --resource-group {resource_group} \
  --query properties.template.containers[0].env

# Add missing variable
az containerapp job update \
  --name {job_name} \
  --resource-group {resource_group} \
  --set-env-vars VERTICA_HOST=vertica.example.com
```

---

## Performance Problems

### Issue: Pipeline taking too long

**Symptoms**:
- Orchestrator execution > 90 minutes
- Individual platform > 30 minutes

**Solutions**:

1. **Enable parallel execution**:
   ```yaml
   # orchestrator_config.yml
   orchestrator:
     parallel_execution: true
     max_parallel: 2  # Run 2 platforms simultaneously
   ```

2. **Reduce date range**:
   ```python
   # Instead of 1 year
   result = pipeline.run("2024-01-01", "2024-12-31")  # Slow

   # Use smaller chunks
   for month in range(1, 13):
       start = f"2024-{month:02d}-01"
       end = f"2024-{month:02d}-{calendar.monthrange(2024, month)[1]:02d}"
       result = pipeline.run(start, end)
   ```

3. **Optimize queries**:
   ```yaml
   # Only fetch required fields
   queries:
     campaign_performance: |
       SELECT
         campaign.id,
         campaign.name,
         metrics.cost_micros  -- Only essential fields
       FROM campaign
       WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
         AND campaign.status = 'ENABLED'  -- Filter inactive
   ```

---

### Issue: High memory usage

**Symptoms**:
- Container OOM (Out of Memory)
- `MemoryError` exceptions

**Solutions**:

1. **Process data in chunks**:
   ```python
   # Instead of loading all data at once
   df = pd.read_csv('large_file.csv')  # Memory issue

   # Use chunking
   for chunk in pd.read_csv('large_file.csv', chunksize=10000):
       processed = process_chunk(chunk)
       data_sink.write(processed, 'campaigns')
   ```

2. **Increase container memory**:
   ```bash
   az containerapp job update \
     --name {job_name} \
     --resource-group {resource_group} \
     --memory 4Gi  # Increase from 2Gi
   ```

3. **Use generators instead of lists**:
   ```python
   # Instead of storing all results
   results = [process(item) for item in items]  # Memory issue

   # Use generator
   results = (process(item) for item in items)
   for result in results:
       data_sink.write(result, 'table')
   ```

---

## Debugging Tools

### Enable Debug Logging

```python
# In pipeline or orchestrator
import os
os.environ['LOG_LEVEL'] = 'DEBUG'

from loguru import logger
logger.add("debug.log", level="DEBUG", rotation="10 MB")

# Run with verbose logging
logger.debug(f"Fetching campaigns for account {account_id}")
logger.debug(f"API response: {response}")
```

### Test Individual Components

```python
# Test adapter
from social.platforms.linkedin.adapter import LinkedInAdapter

adapter = LinkedInAdapter(access_token)
campaigns = adapter.get_campaigns(account_id="123456789")
print(f"Found {len(campaigns)} campaigns")

# Test processor
from social.platforms.linkedin.processor import LinkedInProcessor
import pandas as pd

df = pd.DataFrame(campaigns)
processor = LinkedInProcessor(df)
processed_df = processor.extract_id_from_urn(['id']).get_df()
print(processed_df.head())

# Test data sink
from social.infrastructure.vertica_sink import VerticaSink

sink = VerticaSink(...)
sink.write(processed_df, 'campaigns_test')
sink.close()
```

### Validate Configuration

```python
# Test YAML loading
from social.core.config import PipelineConfig

try:
    config = PipelineConfig.from_yaml("config_linkedin_ads.yml")
    print(f"Config loaded: {config.platform_name}")
except Exception as e:
    print(f"Config error: {e}")
```

### Check API Connectivity

```bash
# LinkedIn
curl -X GET "https://api.linkedin.com/rest/adAccounts/{account_id}/adCampaigns" \
  -H "Authorization: Bearer {token}" \
  -H "LinkedIn-Version: 202601"

# Facebook
curl -X GET "https://graph.facebook.com/v19.0/act_{account_id}/campaigns" \
  -H "Authorization: Bearer {token}"

# Google Ads (requires gRPC, use client library)
python -c "
from google.ads.googleads.client import GoogleAdsClient
client = GoogleAdsClient.load_from_dict({...})
print('Connected successfully!')
"
```

---

## Getting Help

### Check Logs

**Local Development**:
```bash
# Application logs
tail -f logs/social-ads.log

# Orchestrator logs
tail -f logs/orchestrator.log
```

**Azure Container Apps**:
```bash
# Real-time logs
az containerapp job logs show \
  --name {job_name} \
  --resource-group {resource_group} \
  --follow

# Query historical logs
az monitor log-analytics query \
  --workspace {workspace_id} \
  --analytics-query "ContainerAppConsoleLogs_CL | where TimeGenerated > ago(24h)"
```

### Report Issues

If you encounter an issue not covered in this guide:

1. **Collect information**:
   - Error message (full stack trace)
   - Platform (Microsoft/LinkedIn/Facebook/Google)
   - Configuration (sanitized, no secrets)
   - Logs (last 100 lines)

2. **Create issue**:
   - GitLab: https://gitlabds.esprinet.com/datascience/digital_report_etl_pipelines/-/issues
   - Or contact: Alessandro Benelli (alessandro.benelli@esprinet.com)

---

## Next Steps

- [API_REFERENCE.md](API_REFERENCE.md) - API documentation
- [USAGE_GUIDE.md](USAGE_GUIDE.md) - Platform usage examples
- [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) - Deployment procedures

---

**Questions or Issues?**

Contact: Alessandro Benelli (alessandro.benelli@esprinet.com)
