# Social Ads Orchestrator

The **Social Ads Orchestrator** is a unified coordinator for all 4 platform ETL pipelines:
- Microsoft Ads (BingAds SDK)
- LinkedIn Ads (REST API)
- Facebook Ads (Graph API SDK)
- Google Ads (gRPC + Protobuf)

## Overview

The orchestrator provides centralized coordination while keeping each platform pipeline completely independent. There is no shared base class or inheritance - each platform maintains its own implementation and the orchestrator coordinates them via protocol-based interfaces.

### Key Features

- **Platform Independence**: Each platform pipeline runs as-is without modification
- **Dependency Management**: Topological sorting ensures correct execution order
- **Parallel Execution**: Run independent platforms concurrently
- **Retry Logic**: Exponential backoff for transient failures
- **Comprehensive Monitoring**: Track status, timing, and metrics for each platform
- **Flexible Configuration**: YAML-based configuration for all settings
- **Multiple Export Formats**: JSON and CSV reports

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Social Ads Orchestrator                    │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Config     │  │   Registry   │  │  Scheduler   │     │
│  │  Management  │  │   Platform   │  │ Dependencies │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│  ┌──────────────┐  ┌──────────────┐                       │
│  │   Monitor    │  │ Orchestrator │                       │
│  │   Tracking   │  │ Coordinator  │                       │
│  └──────────────┘  └──────────────┘                       │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
┌───────▼────────┐  ┌───────▼────────┐  ┌─────▼──────┐
│   Microsoft    │  │    LinkedIn    │  │  Facebook  │
│   Pipeline     │  │    Pipeline    │  │  Pipeline  │
└────────────────┘  └────────────────┘  └────────────┘
        │
┌───────▼────────┐
│     Google     │
│    Pipeline    │
└────────────────┘
```

## Components

### 1. Configuration Management (`config.py`)

Manages orchestrator and platform configurations with full type safety.

**Key Classes:**
- `OrchestratorConfig`: Main orchestrator settings
- `PlatformConfig`: Individual platform configuration
- `RetryConfig`: Retry policy settings

**Example:**
```python
from pathlib import Path
from social.orchestrator import load_orchestrator_config

# Load configuration
config = load_orchestrator_config(Path("orchestrator_config.yml"))

# Access platform config
microsoft_config = config.get_platform("microsoft")
print(f"Timeout: {microsoft_config.timeout}s")
```

### 2. Platform Registry (`registry.py`)

Manages platform pipeline registration and instantiation.

**Key Features:**
- Factory pattern for pipeline creation
- Pre-registered support for all 4 platforms
- Dynamic configuration loading

**Example:**
```python
from social.orchestrator import create_default_registry

# Create registry with all platforms
registry = create_default_registry()

# List available platforms
platforms = registry.list_platforms()
print(f"Registered: {platforms}")

# Instantiate a pipeline
pipeline = registry.get_pipeline("linkedin", token_provider, data_sink)
```

### 3. Scheduler (`scheduler.py`)

Handles execution scheduling and dependency resolution.

**Key Features:**
- Topological sorting for dependencies
- Parallel execution group management
- Circular dependency detection

**Example:**
```python
from social.orchestrator import PlatformScheduler

scheduler = PlatformScheduler()

# Schedule platforms
execution_groups = scheduler.schedule_platforms(platforms)

# Check if platform can execute
can_run = scheduler.can_execute("linkedin", completed_platforms)
```

### 4. Execution Monitor (`monitor.py`)

Tracks execution status and generates reports.

**Key Features:**
- Real-time status tracking
- Duration and performance metrics
- Export to JSON/CSV

**Example:**
```python
from social.orchestrator import ExecutionMonitor

monitor = ExecutionMonitor()

# Track execution
monitor.start_platform("linkedin")
monitor.complete_platform("linkedin", rows_processed=5000)

# Get summary
summary = monitor.get_summary()
print(f"Success rate: {summary.success_rate}%")

# Export report
monitor.export_report("json", Path("report.json"))
```

### 5. Main Orchestrator (`orchestrator.py`)

Coordinates all platform executions.

**Key Features:**
- Sequential and parallel execution
- Retry logic with exponential backoff
- Continue-on-failure support
- Comprehensive error handling

**Example:**
```python
from social.orchestrator import SocialAdsOrchestrator

orchestrator = SocialAdsOrchestrator(
    config=config,
    registry=registry,
    token_provider=token_provider,
    data_sink=data_sink
)

# Run all platforms
result = orchestrator.run_all_platforms()

if result.success:
    print(f"Processed {result.total_rows_processed:,} rows")
else:
    print(f"Failed platforms: {result.failed_platforms}")
```

## Configuration

### orchestrator_config.yml

The main configuration file defines:
- Global orchestrator settings
- Individual platform configurations
- Parallel execution groups

```yaml
orchestrator:
  parallel_execution: true
  max_parallel: 2
  continue_on_failure: true
  global_timeout: 7200

platforms:
  - name: microsoft
    enabled: true
    priority: 1
    timeout: 1800
    retry:
      max_attempts: 2
      backoff_seconds: 60
    dependencies: []

  - name: linkedin
    enabled: true
    priority: 2
    timeout: 1800
    retry:
      max_attempts: 2
      backoff_seconds: 60
    dependencies: []

parallel_groups:
  - [microsoft, google]
  - [linkedin, facebook]
```

### Environment Variables

#### Data Sink
```bash
STORAGE_TYPE=vertica  # or azure_table or none

# Vertica
VERTICA_HOST=localhost
VERTICA_PORT=5433
VERTICA_DATABASE=analytics
VERTICA_USER=dbuser
VERTICA_PASSWORD=dbpass

# Azure
AZURE_STORAGE_CONNECTION_STRING="..."
AZURE_TABLE_NAME=socialads
```

#### Platform Credentials
```bash
# Microsoft Ads
MICROSOFT_CLIENT_ID=your_client_id
MICROSOFT_CLIENT_SECRET=your_client_secret
MICROSOFT_TENANT_ID=your_tenant_id
MICROSOFT_DEVELOPER_TOKEN=your_dev_token
MICROSOFT_REFRESH_TOKEN=your_refresh_token

# LinkedIn Ads
LINKEDIN_CLIENT_ID=your_client_id
LINKEDIN_CLIENT_SECRET=your_client_secret
LINKEDIN_ACCESS_TOKEN=your_access_token

# Facebook Ads
FACEBOOK_APP_ID=your_app_id
FACEBOOK_APP_SECRET=your_app_secret
FACEBOOK_ACCESS_TOKEN=your_access_token
FACEBOOK_AD_ACCOUNT_IDS=act_123,act_456

# Google Ads
GOOGLE_CONFIG_FILE=/path/to/google-ads.yaml
GOOGLE_MANAGER_CUSTOMER_ID=9474097201
```

#### Optional Settings
```bash
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR
LOG_TO_FILE=true
EXPORT_REPORT=true
REPORT_FORMAT=json  # or csv
REPORT_PATH=execution_report.json
```

## Usage

### Running the Orchestrator

#### From Command Line
```bash
# Set environment variables
export STORAGE_TYPE=vertica
export VERTICA_HOST=localhost
# ... set other variables ...

# Run orchestrator
python social/orchestrator/run_orchestrator.py
```

#### From Python
```python
from pathlib import Path
from social.orchestrator import (
    load_orchestrator_config,
    create_default_registry,
    SocialAdsOrchestrator
)

# Load config
config = load_orchestrator_config(Path("orchestrator_config.yml"))

# Setup token provider and data sink
token_provider = setup_token_provider()
data_sink = setup_data_sink()

# Create orchestrator
registry = create_default_registry()
orchestrator = SocialAdsOrchestrator(
    config=config,
    registry=registry,
    token_provider=token_provider,
    data_sink=data_sink
)

# Run all platforms
result = orchestrator.run_all_platforms()

# Export report
orchestrator.export_report("json", Path("report.json"))
```

### Running Single Platform
```python
# Run just one platform
success = orchestrator.run_platform("linkedin")
```

### Execution Modes

#### Sequential Execution
All platforms run one after another:
```yaml
orchestrator:
  parallel_execution: false
```

#### Parallel Execution
Independent platforms run concurrently:
```yaml
orchestrator:
  parallel_execution: true
  max_parallel: 2

parallel_groups:
  - [microsoft, google]
  - [linkedin, facebook]
```

#### Stop on Failure
Stop if any platform fails:
```yaml
orchestrator:
  continue_on_failure: false
```

## Monitoring & Reporting

### Real-time Monitoring

The orchestrator provides real-time logging:
```
2026-01-22 10:00:00 | INFO     | Starting Social Ads Orchestrator
2026-01-22 10:00:01 | INFO     | Executing Group 1/2: microsoft, google
2026-01-22 10:15:30 | SUCCESS  | Platform 'microsoft' completed: 15,234 rows
2026-01-22 10:16:45 | SUCCESS  | Platform 'google' completed: 23,456 rows
```

### Execution Summary

At the end of execution, a summary is printed:
```
============================================================
Execution Summary
============================================================
Total Platforms:    4
Completed:          3
Failed:             1
Skipped:            0
Success Rate:       75.0%
Total Rows:         45,690
Total Duration:     1847.23s
============================================================
```

### Execution Reports

Export detailed reports in JSON or CSV format:

**JSON Report:**
```json
{
  "summary": {
    "total_platforms": 4,
    "completed": 3,
    "failed": 1,
    "success_rate": 75.0,
    "total_rows_processed": 45690,
    "total_duration_seconds": 1847.23
  },
  "platforms": [
    {
      "platform_name": "microsoft",
      "status": "completed",
      "duration_seconds": 930.45,
      "rows_processed": 15234,
      "tables_processed": 5,
      "retry_count": 0,
      "error_message": null
    }
  ]
}
```

## Deployment

### Docker Container

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

ENV PYTHONPATH=/app
CMD ["python", "social/orchestrator/run_orchestrator.py"]
```

### Azure Container App

```bash
# Build and push image
az acr build --registry myregistry --image social-orchestrator:latest .

# Create container app
az containerapp create \
  --name social-orchestrator \
  --resource-group mygroup \
  --environment myenv \
  --image myregistry.azurecr.io/social-orchestrator:latest \
  --env-vars \
    STORAGE_TYPE=vertica \
    VERTICA_HOST=myhost \
    # ... other env vars ...
```

### Kubernetes

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: social-orchestrator
spec:
  schedule: "0 2 * * *"  # Daily at 2am
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: orchestrator
            image: myregistry/social-orchestrator:latest
            env:
            - name: STORAGE_TYPE
              value: "vertica"
            - name: VERTICA_HOST
              valueFrom:
                secretKeyRef:
                  name: db-credentials
                  key: host
```

## Error Handling

### Retry Logic

Platforms are automatically retried on failure with exponential backoff:
```yaml
retry:
  max_attempts: 3
  backoff_seconds: 60
  backoff_multiplier: 2.0
  max_backoff_seconds: 600
```

Backoff calculation:
- Attempt 1: 60s
- Attempt 2: 120s (60 × 2)
- Attempt 3: 240s (120 × 2)

### Failure Modes

1. **Single Platform Failure** (continue_on_failure=true):
   - Failed platform is marked as failed
   - Other platforms continue execution
   - Exit code: 2 (partial success)

2. **Single Platform Failure** (continue_on_failure=false):
   - Orchestrator stops immediately
   - Remaining platforms are skipped
   - Exit code: 4 (orchestrator error)

3. **All Platforms Failed**:
   - All platforms attempted and failed
   - Exit code: 3 (all failed)

4. **Configuration Error**:
   - Invalid configuration
   - Exit code: 1 (config error)

## Troubleshooting

### Platform Not Executing

**Check:**
1. Platform enabled in config: `enabled: true`
2. Dependencies satisfied
3. Credentials configured
4. Platform registered in registry

### Slow Execution

**Optimize:**
1. Enable parallel execution
2. Increase `max_parallel`
3. Adjust parallel groups
4. Increase platform timeouts

### Memory Issues

**Solutions:**
1. Reduce `max_parallel`
2. Run platforms sequentially
3. Increase container memory limits
4. Process tables in batches

### Authentication Failures

**Verify:**
1. Environment variables set correctly
2. Tokens not expired
3. Credentials file path correct
4. Refresh tokens valid

## Best Practices

1. **Configuration**
   - Version control orchestrator_config.yml
   - Use environment-specific configs
   - Document dependency reasons

2. **Monitoring**
   - Always export execution reports
   - Monitor execution duration trends
   - Alert on failure rate thresholds

3. **Scheduling**
   - Schedule during off-peak hours
   - Allow sufficient timeout buffers
   - Consider data volume patterns

4. **Security**
   - Store credentials in secrets manager
   - Rotate tokens regularly
   - Use least-privilege access

5. **Testing**
   - Test with single platform first
   - Validate parallel groups
   - Test failure scenarios

## Exit Codes

| Code | Meaning | Description |
|------|---------|-------------|
| 0 | Success | All platforms completed successfully |
| 1 | Configuration Error | Invalid configuration or missing settings |
| 2 | Partial Failure | Some platforms succeeded, some failed |
| 3 | All Failed | All platforms failed execution |
| 4 | Orchestrator Error | Internal orchestrator error |
| 130 | Interrupted | User interrupted execution (Ctrl+C) |

## Support

For issues or questions:
1. Check logs for detailed error messages
2. Review configuration syntax
3. Verify environment variables
4. Check individual platform documentation
5. Export execution report for analysis

## Version History

### 1.0.0 (2026-01-22)
- Initial release
- Support for all 4 platforms
- Parallel execution
- Retry logic
- Comprehensive monitoring
- JSON/CSV reporting
