# Social Ads Platform - API Reference

**Version**: 2.0 (Post-Refactoring 2026)
**Last Updated**: 22 January 2026

---

## Table of Contents

1. [Core Protocols](#core-protocols)
2. [Platform Adapters](#platform-adapters)
3. [Platform Processors](#platform-processors)
4. [Platform Pipelines](#platform-pipelines)
5. [Orchestrator](#orchestrator)
6. [Infrastructure](#infrastructure)
7. [Utilities](#utilities)

---

## Core Protocols

### TokenProvider Protocol

**Location**: `social/core/protocols.py`

Protocol for authentication token management.

```python
from typing import Protocol

class TokenProvider(Protocol):
    """Protocol for authentication token providers."""

    def get_token(self, platform: str) -> str:
        """Get access token for platform.

        Args:
            platform: Platform name ('microsoft', 'linkedin', 'facebook', 'google')

        Returns:
            str: Valid access token

        Raises:
            TokenError: If token cannot be retrieved
        """
        ...

    def refresh_token(self, platform: str) -> str:
        """Refresh access token for platform.

        Args:
            platform: Platform name

        Returns:
            str: New access token

        Raises:
            TokenError: If token cannot be refreshed
        """
        ...
```

**Implementations**:
- `FileTokenProvider` - Reads tokens from JSON file
- `AzureKeyVaultTokenProvider` - Reads tokens from Azure Key Vault (future)

### DataSink Protocol

**Location**: `social/core/protocols.py`

Protocol for data persistence.

```python
from typing import Protocol
import pandas as pd

class DataSink(Protocol):
    """Protocol for data sinks (database, storage, etc.)."""

    def write(self, df: pd.DataFrame, table: str, **kwargs) -> None:
        """Write DataFrame to storage.

        Args:
            df: DataFrame to write
            table: Target table/container name
            **kwargs: Sink-specific options

        Raises:
            WriteError: If write operation fails
        """
        ...

    def close(self) -> None:
        """Close connection and release resources."""
        ...
```

**Implementations**:
- `VerticaSink` - Writes to Vertica database
- `TableStorageSink` - Writes to Azure Table Storage

---

## Platform Adapters

### MicrosoftAdsClient

**Location**: `social/platforms/microsoft/client.py`

Client for Microsoft Advertising API (Bing Ads SDK v13).

```python
class MicrosoftAdsClient:
    """Microsoft Ads API client using BingAds SDK v13."""

    def __init__(
        self,
        developer_token: str,
        client_id: str,
        client_secret: str,
        customer_id: str,
        account_id: str,
        environment: str = "production"
    ):
        """Initialize Microsoft Ads client.

        Args:
            developer_token: Microsoft Ads developer token
            client_id: Azure AD application ID
            client_secret: Azure AD application secret
            customer_id: Microsoft Ads customer ID
            account_id: Microsoft Ads account ID
            environment: 'production' or 'sandbox'
        """
        ...

    def download_campaigns_report(
        self,
        start_date: str,
        end_date: str,
        download_path: str = "./downloads"
    ) -> str:
        """Download campaigns performance report as CSV.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            download_path: Directory to save CSV

        Returns:
            str: Path to downloaded CSV file

        Raises:
            ReportDownloadError: If download fails
        """
        ...

    # Similar methods for other report types:
    # - download_ad_groups_report()
    # - download_keywords_report()
    # - download_ads_report()
```

### LinkedInAdapter

**Location**: `social/platforms/linkedin/adapter.py`

Adapter for LinkedIn Marketing API v202601.

```python
class LinkedInAdapter:
    """LinkedIn Marketing API v202601 adapter."""

    def __init__(
        self,
        access_token: str,
        api_version: str = "202601",
        timeout: int = 30
    ):
        """Initialize LinkedIn adapter.

        Args:
            access_token: LinkedIn OAuth2 access token
            api_version: API version (default: 202601)
            timeout: Request timeout in seconds
        """
        ...

    def get_campaigns(
        self,
        account_id: str,
        statuses: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get campaigns for account.

        Args:
            account_id: LinkedIn ad account ID (numeric)
            statuses: Filter by status (e.g., ['ACTIVE', 'PAUSED'])

        Returns:
            List[Dict]: Campaign objects

        Raises:
            LinkedInAPIError: If API call fails
        """
        ...

    def get_insights(
        self,
        campaign_id: str,
        start_date: str,
        end_date: str,
        time_granularity: str = "DAILY"
    ) -> Dict[str, Any]:
        """Get performance metrics for campaign.

        Args:
            campaign_id: Campaign URN or numeric ID
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            time_granularity: 'DAILY' or 'MONTHLY'

        Returns:
            Dict: Insights data with metrics

        Note:
            LinkedIn has 150-day lookback limit (longer than other platforms)
        """
        ...
```

### FacebookAdapter

**Location**: `social/platforms/facebook/adapter.py`

Adapter for Facebook Marketing API (Graph API SDK v19.0).

```python
class FacebookAdapter:
    """Facebook Marketing API adapter using Graph API SDK v19.0."""

    def __init__(
        self,
        access_token: str,
        app_id: str,
        app_secret: str,
        api_version: str = "v19.0"
    ):
        """Initialize Facebook adapter.

        Args:
            access_token: Facebook user access token
            app_id: Facebook App ID
            app_secret: Facebook App Secret
            api_version: Graph API version
        """
        ...

    def get_campaigns(
        self,
        account_id: str,
        fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get campaigns for ad account.

        Args:
            account_id: Facebook ad account ID (numeric or act_xxx format)
            fields: Fields to retrieve (default: all campaign fields)

        Returns:
            List[Dict]: Campaign objects
        """
        ...

    def get_insights(
        self,
        campaign_id: str,
        start_date: str,
        end_date: str,
        breakdowns: Optional[List[str]] = None,
        action_breakdowns: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get performance insights with breakdowns.

        Args:
            campaign_id: Facebook campaign ID
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            breakdowns: Breakdown dimensions (e.g., ['age', 'gender'])
            action_breakdowns: Action breakdowns (e.g., ['action_type'])

        Returns:
            List[Dict]: Insights records

        Note:
            Supports nested breakdowns (actions, action_values arrays)
        """
        ...
```

### GoogleAdapter

**Location**: `social/platforms/google/adapter.py`

Adapter for Google Ads API (gRPC + Protobuf).

```python
class GoogleAdapter:
    """Google Ads API adapter using gRPC + Protobuf."""

    def __init__(
        self,
        developer_token: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        customer_id: str,
        api_version: str = "v16"
    ):
        """Initialize Google Ads adapter.

        Args:
            developer_token: Google Ads developer token
            client_id: OAuth2 client ID
            client_secret: OAuth2 client secret
            refresh_token: OAuth2 refresh token
            customer_id: Google Ads customer ID (10 digits, no hyphens)
            api_version: API version (default: v16)
        """
        ...

    def execute_query(
        self,
        query: str,
        customer_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Execute GAQL (Google Ads Query Language) query.

        Args:
            query: GAQL query string
            customer_id: Override default customer ID

        Returns:
            List[Dict]: Query results as dictionaries

        Example:
            query = '''
                SELECT campaign.id, campaign.name, metrics.cost_micros
                FROM campaign
                WHERE segments.date BETWEEN '2024-01-01' AND '2024-01-31'
            '''
            results = adapter.execute_query(query)
        """
        ...

    def get_account_hierarchy(self) -> List[Dict[str, Any]]:
        """Get account hierarchy (MCC > accounts > campaigns).

        Returns:
            List[Dict]: Hierarchical account structure
        """
        ...
```

---

## Platform Processors

### LinkedInProcessor

**Location**: `social/platforms/linkedin/processor.py`

Data processor for LinkedIn with chainable methods.

```python
class LinkedInProcessor:
    """LinkedIn data processor with fluent interface."""

    def __init__(self, df: pd.DataFrame):
        """Initialize with DataFrame.

        Args:
            df: Input DataFrame
        """
        self.df = df.copy()

    def extract_id_from_urn(self, columns: List[str]) -> 'LinkedInProcessor':
        """Extract numeric ID from LinkedIn URNs.

        Args:
            columns: Columns containing URN strings

        Example:
            "urn:li:sponsoredCampaign:123456" → "123456"

        Returns:
            Self for chaining
        """
        ...

    def add_company(self, company: str) -> 'LinkedInProcessor':
        """Add company column.

        Args:
            company: Company name/code

        Returns:
            Self for chaining
        """
        ...

    def de_emojify(self, columns: List[str]) -> 'LinkedInProcessor':
        """Remove emojis from text columns.

        Args:
            columns: Columns to clean

        Returns:
            Self for chaining
        """
        ...

    def get_df(self) -> pd.DataFrame:
        """Get processed DataFrame.

        Returns:
            pd.DataFrame: Processed data
        """
        return self.df
```

**Usage Example**:
```python
processor = LinkedInProcessor(raw_df)
processed_df = (
    processor
    .extract_id_from_urn(['campaign_id', 'account_id'])
    .add_company('ESP')
    .de_emojify(['campaign_name'])
    .get_df()
)
```

### GoogleProcessor

**Location**: `social/platforms/google/processor.py`

Data processor for Google Ads with micros conversion.

```python
class GoogleProcessor:
    """Google Ads data processor."""

    def __init__(self, df: pd.DataFrame):
        """Initialize with DataFrame."""
        self.df = df.copy()

    def convert_costs(self, columns: List[str]) -> 'GoogleProcessor':
        """Convert micros to currency (divide by 1,000,000).

        Args:
            columns: Cost columns in micros

        Example:
            5000000 micros → 5.00 EUR

        Returns:
            Self for chaining

        Note:
            CRITICAL: Google Ads returns all costs in micros
        """
        ...

    def flatten_segments(self) -> 'GoogleProcessor':
        """Flatten nested segment fields.

        Returns:
            Self for chaining
        """
        ...
```

### FacebookProcessor

**Location**: `social/platforms/facebook/processor.py`

Data processor for Facebook with nested breakdowns handling.

```python
class FacebookProcessor:
    """Facebook data processor with nested array handling."""

    def __init__(self, df: pd.DataFrame):
        """Initialize with DataFrame."""
        self.df = df.copy()

    def extract_nested_actions(
        self,
        action_col: str = 'actions',
        prefix: str = 'action_'
    ) -> 'FacebookProcessor':
        """Flatten actions array to columns.

        Args:
            action_col: Column containing actions array
            prefix: Prefix for output columns

        Example:
            Input: actions = [{"action_type": "link_click", "value": 50}, ...]
            Output: action_link_click = 50, action_page_engagement = 120, ...

        Returns:
            Self for chaining
        """
        ...

    def extract_nested_action_values(
        self,
        action_col: str = 'action_values',
        prefix: str = 'value_'
    ) -> 'FacebookProcessor':
        """Flatten action_values array to columns.

        Returns:
            Self for chaining
        """
        ...
```

---

## Platform Pipelines

All platform pipelines implement the `SocialAdsPipeline` protocol.

### LinkedInPipeline

**Location**: `social/platforms/linkedin/pipeline.py`

```python
class LinkedInPipeline:
    """LinkedIn Ads ETL pipeline."""

    def __init__(
        self,
        token_provider: TokenProvider,
        data_sink: DataSink,
        config: PipelineConfig
    ):
        """Initialize pipeline with injected dependencies.

        Args:
            token_provider: Token provider implementation
            data_sink: Data sink implementation
            config: Pipeline configuration
        """
        ...

    def run(self, start_date: str, end_date: str) -> PipelineResult:
        """Execute pipeline for date range.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            PipelineResult: Execution result with metrics

        Process:
            1. Get access token from token_provider
            2. Fetch campaigns from LinkedIn API
            3. Fetch insights for each campaign
            4. Process data (extract URNs, clean, transform)
            5. Write to data_sink
            6. Return result with metrics
        """
        ...
```

**Usage Example**:
```python
from social.platforms.linkedin.pipeline import LinkedInPipeline
from social.infrastructure.file_token_provider import FileTokenProvider
from social.infrastructure.vertica_sink import VerticaSink
from social.core.config import PipelineConfig

# Setup dependencies
token_provider = FileTokenProvider(tokens_file="tokens.json")
data_sink = VerticaSink(host="vertica.example.com", ...)
config = PipelineConfig.from_yaml("config_linkedin_ads.yml")

# Create and run pipeline
pipeline = LinkedInPipeline(token_provider, data_sink, config)
result = pipeline.run(start_date="2024-01-01", end_date="2024-01-31")

print(f"Success: {result.success}")
print(f"Rows processed: {result.rows_processed}")
print(f"Duration: {result.duration_seconds}s")
```

---

## Orchestrator

### SocialAdsOrchestrator

**Location**: `social/orchestrator/orchestrator.py`

Main orchestrator for coordinating all platforms.

```python
class SocialAdsOrchestrator:
    """Orchestrator for coordinating all social ads platforms."""

    def __init__(self, config_path: str = "orchestrator_config.yml"):
        """Initialize orchestrator.

        Args:
            config_path: Path to orchestrator configuration YAML
        """
        ...

    def run_all_platforms(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> OrchestratorResult:
        """Run all enabled platforms in dependency order.

        Args:
            start_date: Override start date (default: from config)
            end_date: Override end date (default: from config)

        Returns:
            OrchestratorResult: Aggregate results

        Process:
            1. Load configuration
            2. Build dependency graph
            3. Perform topological sort
            4. Execute platforms in parallel groups
            5. Track metrics and status
            6. Generate final report
        """
        ...

    def run_platform(
        self,
        platform_name: str,
        start_date: str,
        end_date: str
    ) -> PlatformExecutionResult:
        """Run single platform.

        Args:
            platform_name: Platform name ('microsoft', 'linkedin', etc.)
            start_date: Start date
            end_date: End date

        Returns:
            PlatformExecutionResult: Platform-specific result
        """
        ...
```

**Configuration Example** (`orchestrator_config.yml`):
```yaml
orchestrator:
  parallel_execution: true
  max_parallel: 2
  continue_on_failure: true
  default_date_range:
    start_date: "2024-01-01"
    end_date: "2024-01-31"

platforms:
  - name: microsoft
    enabled: true
    priority: 1
    timeout: 1800
    retry:
      max_attempts: 2
      backoff_seconds: 60

  - name: linkedin
    enabled: true
    priority: 2
    depends_on: []

  - name: facebook
    enabled: true
    priority: 2
    depends_on: []

  - name: google
    enabled: true
    priority: 3
    depends_on: [microsoft]

parallel_groups:
  - [microsoft, linkedin]  # Group 1: run together
  - [facebook, google]      # Group 2: run after Group 1
```

**Usage Example**:
```python
from social.orchestrator.orchestrator import SocialAdsOrchestrator

orchestrator = SocialAdsOrchestrator("orchestrator_config.yml")
result = orchestrator.run_all_platforms()

print(f"Total platforms: {result.total_platforms}")
print(f"Successful: {result.successful_platforms}")
print(f"Failed: {result.failed_platforms}")
print(f"Total duration: {result.total_duration_seconds}s")

for platform_result in result.platform_results:
    print(f"\n{platform_result.platform_name}:")
    print(f"  Status: {platform_result.status}")
    print(f"  Rows: {platform_result.rows_processed}")
    print(f"  Duration: {platform_result.duration_seconds}s")
```

---

## Infrastructure

### VerticaSink

**Location**: `social/infrastructure/vertica_sink.py`

```python
class VerticaSink:
    """Vertica database sink implementation."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        schema: str = "public"
    ):
        """Initialize Vertica connection."""
        ...

    def write(
        self,
        df: pd.DataFrame,
        table: str,
        if_exists: str = "append"
    ) -> None:
        """Write DataFrame to Vertica table.

        Args:
            df: Data to write
            table: Target table name
            if_exists: 'append', 'replace', or 'fail'
        """
        ...
```

### TableStorageSink

**Location**: `social/infrastructure/table_storage_sink.py`

```python
class TableStorageSink:
    """Azure Table Storage sink implementation."""

    def __init__(
        self,
        connection_string: str,
        table_name: str
    ):
        """Initialize Table Storage connection."""
        ...

    def write(
        self,
        df: pd.DataFrame,
        table: str,
        partition_key_col: str = "platform",
        row_key_col: str = "id"
    ) -> None:
        """Write DataFrame to Table Storage.

        Args:
            df: Data to write
            table: Table name
            partition_key_col: Column to use as PartitionKey
            row_key_col: Column to use as RowKey
        """
        ...
```

### FileTokenProvider

**Location**: `social/infrastructure/file_token_provider.py`

```python
class FileTokenProvider:
    """Token provider that reads from JSON file."""

    def __init__(self, tokens_file: str = "tokens.json"):
        """Initialize file-based token provider.

        Args:
            tokens_file: Path to JSON file with tokens

        File Format:
            {
                "microsoft": "token_xxx",
                "linkedin": "token_yyy",
                "facebook": "token_zzz",
                "google": "token_www"
            }
        """
        ...

    def get_token(self, platform: str) -> str:
        """Get token for platform."""
        ...

    def refresh_token(self, platform: str) -> str:
        """Refresh token (reload from file)."""
        ...
```

---

## Utilities

### Processing Utilities

**Location**: `social/utils/processing.py`

```python
def deEmojify(text: str) -> str:
    """Remove emojis from text.

    Args:
        text: Input text with potential emojis

    Returns:
        str: Text without emojis
    """
    ...

def fix_id_type(id_value: Any) -> str:
    """Convert ID to string, handling various types.

    Args:
        id_value: ID value (int, float, str, None)

    Returns:
        str: String representation
    """
    ...
```

### URN Utilities

**Location**: `social/utils/urn_utils.py`

```python
def extract_id_from_urn(urn: str) -> str:
    """Extract numeric ID from LinkedIn URN.

    Args:
        urn: URN string (e.g., "urn:li:sponsoredCampaign:123456")

    Returns:
        str: Numeric ID ("123456")

    Example:
        >>> extract_id_from_urn("urn:li:sponsoredCampaign:123456")
        "123456"
    """
    ...
```

### Date Utilities

**Location**: `social/utils/date_utils.py`

```python
def convert_unix_to_datetime(unix_timestamp: int) -> str:
    """Convert Unix timestamp to ISO datetime string.

    Args:
        unix_timestamp: Unix timestamp (seconds since epoch)

    Returns:
        str: ISO 8601 datetime string

    Example:
        >>> convert_unix_to_datetime(1640995200)
        "2022-01-01T00:00:00Z"
    """
    ...

def get_date_range(
    start_date: str,
    end_date: str,
    format: str = "%Y-%m-%d"
) -> List[str]:
    """Generate list of dates in range.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        format: Output date format

    Returns:
        List[str]: List of date strings
    """
    ...
```

---

## Error Handling

All platform adapters raise specific exception types:

```python
# Base exceptions
class PlatformError(Exception):
    """Base exception for platform errors."""
    pass

class TokenError(PlatformError):
    """Token retrieval/refresh failed."""
    pass

class APIError(PlatformError):
    """API call failed."""
    pass

class WriteError(PlatformError):
    """Data write operation failed."""
    pass

# Platform-specific exceptions
class LinkedInAPIError(APIError):
    """LinkedIn API error."""
    pass

class FacebookAPIError(APIError):
    """Facebook API error."""
    pass

class GoogleAdsError(APIError):
    """Google Ads API error."""
    pass

class MicrosoftAdsError(APIError):
    """Microsoft Ads API error."""
    pass
```

---

## Type Definitions

### PipelineResult

```python
@dataclass
class PipelineResult:
    """Result of pipeline execution."""

    platform: str
    success: bool
    rows_processed: int
    duration_seconds: float
    start_time: datetime
    end_time: datetime
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
```

### OrchestratorResult

```python
@dataclass
class OrchestratorResult:
    """Result of orchestrator execution."""

    total_platforms: int
    successful_platforms: int
    failed_platforms: int
    total_duration_seconds: float
    platform_results: List[PipelineResult]
    execution_order: List[str]
    parallel_groups: List[List[str]]
```

---

## Best Practices

### 1. Always Use Dependency Injection

```python
# ✅ GOOD: Dependencies injected
pipeline = LinkedInPipeline(
    token_provider=FileTokenProvider("tokens.json"),
    data_sink=VerticaSink(...),
    config=config
)

# ❌ BAD: Dependencies created internally
pipeline = LinkedInPipeline()  # Creates dependencies inside
```

### 2. Use Protocols for Type Hints

```python
# ✅ GOOD: Depend on protocols
def run_pipeline(
    pipeline: SocialAdsPipeline,  # Protocol
    token_provider: TokenProvider  # Protocol
) -> PipelineResult:
    ...

# ❌ BAD: Depend on concrete classes
def run_pipeline(
    pipeline: LinkedInPipeline,  # Concrete class
    token_provider: FileTokenProvider  # Concrete class
) -> PipelineResult:
    ...
```

### 3. Chain Processor Methods

```python
# ✅ GOOD: Fluent chaining
processed_df = (
    LinkedInProcessor(raw_df)
    .extract_id_from_urn(['campaign_id'])
    .add_company('ESP')
    .de_emojify(['name'])
    .get_df()
)

# ❌ BAD: Procedural style
processor = LinkedInProcessor(raw_df)
processor.extract_id_from_urn(['campaign_id'])
df = processor.get_df()
processor2 = LinkedInProcessor(df)
processor2.add_company('ESP')
df = processor2.get_df()
```

### 4. Handle Errors Gracefully

```python
# ✅ GOOD: Specific exception handling
try:
    result = pipeline.run(start_date, end_date)
except TokenError as e:
    logger.error(f"Token error: {e}")
    # Retry token refresh
except APIError as e:
    logger.error(f"API error: {e}")
    # Retry with backoff
except WriteError as e:
    logger.error(f"Write error: {e}")
    # Save to fallback storage

# ❌ BAD: Catch-all exception
try:
    result = pipeline.run(start_date, end_date)
except Exception as e:
    logger.error(f"Error: {e}")
    # Can't determine appropriate recovery action
```

---

## Migration from Old Architecture

### Old Adapter → New Adapter

```python
# ❌ OLD (DEPRECATED)
from social.adapters.linkedin_adapter import LinkedInAdsAdapter
adapter = LinkedInAdsAdapter(access_token, account_id)
campaigns = adapter.get_campaigns()

# ✅ NEW
from social.platforms.linkedin.adapter import LinkedInAdapter
adapter = LinkedInAdapter(access_token)
campaigns = adapter.get_campaigns(account_id)
```

### Old Strategy → New Utility

```python
# ❌ OLD (DEPRECATED)
from social.processing.strategies import DeEmojifyStrategy
strategy = DeEmojifyStrategy(columns=['name'])
df_processed = strategy.apply(df)

# ✅ NEW
from social.utils.processing import deEmojify
df['name'] = df['name'].apply(deEmojify)
```

### Old Pipeline → New Pipeline

```python
# ❌ OLD (DEPRECATED)
from social.adapters.linkedin_adapter import LinkedInAdsAdapter
from social.processing.factory import ProcessingStrategyFactory
from social.processing.pipeline import DataProcessingPipeline

adapter = LinkedInAdsAdapter(token, account_id)
raw_df = adapter.fetch_data()
strategies = ProcessingStrategyFactory.create_linkedin_strategies()
pipeline = DataProcessingPipeline(strategies)
processed_df = pipeline.run(raw_df)

# ✅ NEW
from social.platforms.linkedin.pipeline import LinkedInPipeline
from social.infrastructure.file_token_provider import FileTokenProvider
from social.infrastructure.vertica_sink import VerticaSink

token_provider = FileTokenProvider("tokens.json")
data_sink = VerticaSink(...)
config = PipelineConfig.from_yaml("config_linkedin_ads.yml")

pipeline = LinkedInPipeline(token_provider, data_sink, config)
result = pipeline.run(start_date, end_date)
```

---

## See Also

- [README.md](../README.md) - Project overview
- [USAGE_GUIDE.md](USAGE_GUIDE.md) - Platform-by-platform usage guide
- [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) - Deployment procedures
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Common issues and solutions

---

**Questions or Issues?**

Contact: Alessandro Benelli (alessandro.benelli@esprinet.com)
