# Google Ads Platform ETL Pipeline

Complete, independent implementation of the Google Ads ETL pipeline following SOLID principles and best practices.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Components](#components)
- [Configuration](#configuration)
- [Usage](#usage)
- [Google Ads API Specifics](#google-ads-api-specifics)
- [Data Tables](#data-tables)
- [Processing Steps](#processing-steps)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)

## Overview

This module provides a production-ready ETL pipeline for extracting data from the Google Ads API, transforming it according to business rules, and loading it into a data warehouse (Vertica) or cloud storage (Azure Blob).

### Key Features

- **gRPC/Protobuf Communication**: Uses official Google Ads Python client (NOT REST API)
- **Independent Architecture**: NO base class inheritance, completely standalone
- **Multi-Account Support**: Automatically iterates all customer accounts under MCC
- **Chainable Processing**: Fluent interface for data transformation
- **Multiple Storage Backends**: Vertica, Azure Blob, or no storage
- **Container-Ready**: Designed for Azure Container Apps deployment
- **Type-Safe**: 100% type hints on all functions
- **Comprehensive Logging**: Detailed logging at every step

### Design Principles

- **SOLID Principles**: Single responsibility, dependency injection, protocol-based contracts
- **No Base Classes**: Completely independent implementation
- **Protocol-Based**: Uses TokenProvider and DataSink protocols
- **Error Handling**: Comprehensive error handling with custom exceptions
- **Production-Ready**: Logging, monitoring, and graceful error recovery

## Architecture

```
google/
├── constants.py         # GAQL queries, account mappings, constants
├── http_client.py       # gRPC/Protobuf client for Google Ads API
├── adapter.py          # Data extraction logic
├── processor.py        # Chainable data transformation
├── pipeline.py         # Main ETL orchestrator
├── run_google.py       # Container entry point
├── __init__.py         # Package exports
├── README.md           # This file
├── googleads_config.yml # Table definitions
└── google-ads-*.yml    # Credentials (gitignored)
```

## Components

### 1. Constants (`constants.py`)

Centralized constant definitions:

- `COMPANY_ACCOUNT_MAP`: Maps Google customer IDs to company IDs
- `API_VERSION`: Google Ads API version (v18)
- `DEFAULT_LOOKBACK_DAYS`: Default date range (150 days)
- `MICROS_DIVISOR`: Conversion factor for costs (1,000,000)
- `GAQL_QUERIES`: All GAQL query templates
- `COLUMN_MAPPINGS`: Protobuf field name to database column mappings

### 2. HTTP Client (`http_client.py`)

Handles all communication with Google Ads API:

- Initializes Google Ads client from `google-ads.yaml` config
- Executes GAQL queries (both regular and streaming)
- Converts Protobuf responses to DataFrames
- Traverses account hierarchy (MCC → customer accounts)

**Key Methods:**
- `get_all_accounts()`: Retrieve customer account hierarchy
- `execute_query(customer_id, query, use_streaming)`: Execute GAQL query
- `_convert_response_to_df()`: Protobuf → DataFrame conversion

### 3. Adapter (`adapter.py`)

Data extraction layer:

- Iterates all enabled customer accounts
- Executes queries for each account
- Combines results from all accounts
- Maps customer IDs to company IDs

**Key Methods:**
- `get_customer_accounts()`: Account hierarchy
- `get_all_campaigns()`: Campaign data
- `get_all_ad_report()`: Performance metrics
- `get_all_ad_creatives()`: Creative details
- `get_all_placements()`: Placement data
- `get_all_audiences()`: Audience segments
- `get_all_cost_by_device()`: Device breakdown

### 4. Processor (`processor.py`)

Chainable data transformation pipeline:

```python
result = (GoogleProcessor(df)
    .handle_columns()                    # Clean column names
    .convert_costs(['cost_micros'])      # Micros → currency
    .deal_with_date(['start_date'])      # Parse dates
    .add_company('customer_id_google')   # Add company ID
    .add_row_loaded_date()               # Add timestamp
    .modify_name(['name'])               # Clean special chars
    .drop_duplicates()                   # Remove duplicates
    .get_df())
```

**Available Methods:**
- `handle_columns()`: Remove prefixes, clean column names
- `convert_costs(columns)`: Convert micros to currency
- `deal_with_date(columns)`: Parse date strings
- `add_company(account_column)`: Map accounts to companies
- `add_row_loaded_date()`: Add ingestion timestamp
- `modify_name(columns)`: Remove emoji, special chars
- `remove_emoji(columns)`: Remove emoji characters
- `remove_non_latin(columns)`: Keep only Latin characters
- `remove_piping(columns)`: Remove pipe delimiters
- `clean_audience_string()`: Clean audience names
- `aggregate_by_keys()`: Aggregate by ad_id + device
- `fill_view_ctr_nan(columns)`: Fill NaN with 0
- `replace_nat()`: NaT → None
- `delete_nan_string()`: Remove "nan" strings
- `dropna_value()`: Drop rows with NaN
- `drop_duplicates()`: Remove duplicates
- `rename_column(mapping)`: Rename columns
- `limit_placement()`: Top 25 placements per ad group

### 5. Pipeline (`pipeline.py`)

Main ETL orchestrator:

- Loads configuration from YAML
- Coordinates extraction, processing, loading
- Handles dependencies between tables
- Manages errors and retries

**Key Methods:**
- `run(table_name)`: Process single table
- `run_all_tables()`: Process all tables in order
- `_extract_table()`: Extract raw data
- `_process_table()`: Apply transformations
- `_load_to_sink()`: Write to database/storage

### 6. Entry Point (`run_google.py`)

Container-ready entry point:

- Reads environment variables
- Initializes storage backend
- Runs pipeline
- Handles errors and exit codes

## Configuration

### Environment Variables

```bash
# Google Ads Configuration
GOOGLE_ADS_CONFIG_FILE=/path/to/google-ads-9474097201.yml
GOOGLE_MANAGER_CUSTOMER_ID=9474097201
GOOGLE_API_VERSION=v18

# Storage Backend (vertica, azure, or none)
STORAGE_TYPE=vertica

# Vertica Configuration
VERTICA_HOST=your-vertica-host
VERTICA_PORT=5433
VERTICA_DATABASE=your-database
VERTICA_USER=your-username
VERTICA_PASSWORD=your-password

# Azure Configuration
AZURE_STORAGE_CONNECTION_STRING=your-connection-string
AZURE_CONTAINER_NAME=google-ads-data
```

### Google Ads Credentials

Create a `google-ads-XXXX.yml` file:

```yaml
developer_token: YOUR_DEVELOPER_TOKEN
client_id: YOUR_CLIENT_ID
client_secret: YOUR_CLIENT_SECRET
refresh_token: YOUR_REFRESH_TOKEN
login_customer_id: YOUR_MCC_ID
```

### Table Configuration

See `googleads_config.yml` for table definitions:

```yaml
google_ads_campaign:
  queryget: [query_campaign]
  type: SearchGoogleAdsStreamRequest
  processing:
    handle_columns:
      params: None
    deal_with_date:
      cols: ['startdate','enddate']
    rename_columns:
      params: None
    add_row_loaded_date:
      params: None
  day: 150
```

## Usage

### Programmatic Usage

```python
from pathlib import Path
from social.platforms.google import GooglePipeline, load_config
from social.infrastructure.file_token_provider import FileBasedTokenProvider

# Load configuration
config = load_config(Path("social/platforms/google/googleads_config.yml"))

# Initialize pipeline
pipeline = GooglePipeline(
    config=config,
    token_provider=FileBasedTokenProvider("tokens.json"),
    google_config_file="social/platforms/google/google-ads-9474097201.yml",
    manager_customer_id="9474097201",
    data_sink=None,  # Or your DataSink implementation
)

# Run single table
df = pipeline.run("google_ads_campaign")

# Run all tables
results = pipeline.run_all_tables()

# Close pipeline
pipeline.close()
```

### Container Usage

```bash
# Set environment variables
export GOOGLE_ADS_CONFIG_FILE=/app/social/platforms/google/google-ads-9474097201.yml
export GOOGLE_MANAGER_CUSTOMER_ID=9474097201
export STORAGE_TYPE=vertica
export VERTICA_HOST=your-host
export VERTICA_USER=your-user
export VERTICA_PASSWORD=your-password

# Run pipeline
python -m social.platforms.google.run_google
```

## Google Ads API Specifics

### gRPC vs REST

Google Ads API uses **gRPC/Protobuf**, NOT REST:

- Requests: `SearchGoogleAdsRequest` or `SearchGoogleAdsStreamRequest`
- Responses: Protobuf messages
- Conversion: `MessageToDict(row._pb)` → Python dict

### GAQL (Google Ads Query Language)

SQL-like query language:

```sql
SELECT
  campaign.id,
  campaign.name,
  metrics.impressions,
  metrics.clicks,
  metrics.cost_micros
FROM campaign
WHERE segments.date BETWEEN '2024-01-01' AND '2024-01-31'
```

### Request Types

1. **SearchGoogleAdsRequest**: Regular request, returns all results
   - Use for small datasets (< 10,000 rows)
   - Example: campaigns, creatives, audiences

2. **SearchGoogleAdsStreamRequest**: Streaming request, batched results
   - Use for large datasets (> 10,000 rows)
   - Example: ad reports, placements

### Micros Conversion

Google Ads returns costs in **micros** (1/1,000,000 of currency):

```python
# API returns: cost_micros = 5_500_000
# Actual cost: 5.50 EUR

actual_cost = cost_micros / 1_000_000  # = 5.50
```

**Always divide by `MICROS_DIVISOR` (1,000,000) for:**
- `cost_micros`
- `average_cpc`
- `average_cpm`
- `average_cost`

### Account Hierarchy

```
Manager Account (MCC)
├── Customer Account 1
├── Customer Account 2
└── Customer Account 3
```

Pipeline automatically:
1. Queries MCC for accessible customers
2. Filters enabled, non-manager accounts
3. Iterates all accounts for data extraction

## Data Tables

### google_ads_account
- **Source**: Customer hierarchy
- **Type**: SearchGoogleAdsRequest
- **Data**: Account metadata (name, currency, timezone)

### google_ads_campaign
- **Source**: Campaign query
- **Type**: SearchGoogleAdsStreamRequest
- **Data**: Campaign details (name, status, dates)
- **Lookback**: 150 days

### google_ads_report
- **Source**: Ad report query
- **Type**: SearchGoogleAdsStreamRequest
- **Data**: Performance metrics (impressions, clicks, cost, conversions)
- **Lookback**: 150 days

### google_ads_ad_creatives
- **Source**: Ad creatives query
- **Type**: SearchGoogleAdsStreamRequest
- **Data**: Creative details (type, name, ad ID)

### google_ads_placement
- **Source**: Placement queries (ENABLED + PAUSED)
- **Type**: SearchGoogleAdsRequest
- **Data**: Display placements (URL, impressions, CTR)
- **Lookback**: 600 days
- **Limit**: Top 25 per ad group

### google_ads_audience
- **Source**: Audience queries (ENABLED + PAUSED)
- **Type**: SearchGoogleAdsRequest
- **Data**: Audience segments (display name, ad group)

### google_ads_cost_by_device
- **Source**: Device queries (ENABLED + PAUSED)
- **Type**: SearchGoogleAdsRequest
- **Data**: Cost breakdown by device (desktop, mobile, tablet)

## Processing Steps

### Common Transformations

1. **handle_columns**: Clean Protobuf field names
   - Remove prefixes: `campaign.id` → `id`
   - Replace dots: `customer.id` → `customer_id`
   - Lowercase all columns

2. **convert_costs**: Micros to currency
   - Divide by 1,000,000

3. **deal_with_date**: Parse dates
   - Format: YYYY-MM-DD
   - Convert to datetime objects

4. **add_company**: Map accounts to companies
   - Use `COMPANY_ACCOUNT_MAP`

5. **modify_name**: Clean text fields
   - Remove emoji
   - Remove non-Latin characters
   - Replace pipes with hyphens

### Table-Specific Steps

See `googleads_config.yml` for detailed processing steps per table.

## Deployment

### Azure Container Apps

1. **Build Docker image**:
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "-m", "social.platforms.google.run_google"]
```

2. **Push to Azure Container Registry**:
```bash
az acr build --registry myregistry --image google-ads-pipeline:latest .
```

3. **Deploy to Container Apps**:
```bash
az containerapp create \
  --name google-ads-pipeline \
  --resource-group my-rg \
  --image myregistry.azurecr.io/google-ads-pipeline:latest \
  --environment my-env \
  --secrets google-ads-config=... \
  --env-vars STORAGE_TYPE=vertica
```

### Exit Codes

- `0`: Success
- `1`: Configuration error
- `2`: Authentication error
- `3`: Pipeline execution error
- `4`: Data sink error

## Troubleshooting

### Authentication Errors

```
AuthenticationError: Failed to initialize Google Ads client
```

**Solution:**
- Check `google-ads.yaml` file exists
- Verify developer token, client ID, client secret
- Ensure refresh token is valid
- Check login_customer_id matches MCC

### GAQL Errors

```
APIError: Failed to execute Google Ads query
```

**Solution:**
- Validate GAQL syntax
- Check date format (YYYY-MM-DD)
- Verify customer account is enabled
- Ensure fields exist in API version

### Protobuf Conversion Errors

```
KeyError: 'results'
```

**Solution:**
- Check if query returns data
- Verify `use_streaming` parameter
- Try regular request instead of streaming

### Cost Conversion Issues

```
ValueError: cannot convert float NaN to integer
```

**Solution:**
- Fill NaN before conversion: `df.fillna(0)`
- Check for missing cost_micros values
- Ensure column exists before conversion

### Empty DataFrames

**Possible Causes:**
- No enabled customer accounts
- Date range outside campaign period
- Filters exclude all data
- API rate limiting

**Solution:**
- Check account status in hierarchy
- Verify date range
- Review query filters
- Add retry logic

## Support

For issues or questions:
- Review logs: Check stderr output for detailed error messages
- Validate config: Ensure `googleads_config.yml` is correct
- Test queries: Use Google Ads Query Builder to validate GAQL
- Check API: Verify API access in Google Ads console

## License

Internal use only.
