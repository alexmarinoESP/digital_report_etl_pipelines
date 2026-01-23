# Social Ads Platform - Deployment Guide

**Version**: 2.0 (Post-Refactoring 2026)
**Last Updated**: 22 January 2026

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Local Development](#local-development)
4. [Docker Build](#docker-build)
5. [Azure Container Apps Deployment](#azure-container-apps-deployment)
6. [Configuration Management](#configuration-management)
7. [Monitoring & Logging](#monitoring--logging)
8. [Scaling & Performance](#scaling--performance)

---

## Overview

The Social Ads Platform is designed for deployment on **Azure Container Apps** with:
- **Multi-stage Docker builds** (optimized images)
- **Environment-based configuration** (dev, staging, production)
- **Scheduled execution** (Azure Container Apps Jobs)
- **Centralized monitoring** (Azure Application Insights + Loguru)

### Deployment Targets

| Environment | Azure Subscription | Resource Group | Container Registry |
|-------------|-------------------|----------------|-------------------|
| Development | Dev Subscription | rg-socialads-dev | acrsocialadsdev |
| Staging | Test Subscription | rg-socialads-staging | acrsocialadsstaging |
| Production | Prod Subscription | rg-socialads-prod | acrsocialadsprod |

---

## Prerequisites

### Required Tools

```bash
# Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
az --version

# Docker
docker --version

# UV (Python package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh
uv --version
```

### Azure Resources

**Required Azure Resources**:
1. **Azure Container Registry** (ACR) - Store Docker images
2. **Azure Container Apps Environment** - Host containers
3. **Azure Vertica/PostgreSQL** - Database
4. **Azure Key Vault** - Store secrets
5. **Azure Application Insights** - Monitoring
6. **Azure Log Analytics Workspace** - Logging

**Create Resources**:
```bash
# Set variables
SUBSCRIPTION_ID="your-subscription-id"
RESOURCE_GROUP="rg-socialads-prod"
LOCATION="westeurope"
ACR_NAME="acrsocialadsprod"
CONTAINERAPPS_ENV="env-socialads-prod"

# Login
az login
az account set --subscription $SUBSCRIPTION_ID

# Create resource group
az group create \
  --name $RESOURCE_GROUP \
  --location $LOCATION

# Create Container Registry
az acr create \
  --resource-group $RESOURCE_GROUP \
  --name $ACR_NAME \
  --sku Standard \
  --admin-enabled true

# Create Container Apps Environment
az containerapp env create \
  --name $CONTAINERAPPS_ENV \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION

# Create Key Vault
az keyvault create \
  --name kv-socialads-prod \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION

# Create Application Insights
az monitor app-insights component create \
  --app ai-socialads-prod \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --application-type web
```

---

## Local Development

### Setup

```bash
# Clone repository
git clone https://gitlabds.esprinet.com/datascience/digital_report_etl_pipelines.git
cd digital_report_etl_pipelines

# Install dependencies
uv sync --extra social

# Configure environment
cp .env.example .env
# Edit .env with your credentials
```

### Run Locally

```bash
# Activate virtual environment
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

# Run orchestrator
uv run python -m social.orchestrator.run_orchestrator

# Run single platform
uv run python -m social.platforms.linkedin.pipeline
```

### Run with Docker Locally

```bash
# Build image
docker build -f social/Dockerfile -t social-ads:local .

# Run orchestrator
docker run --env-file .env social-ads:local \
  python -m social.orchestrator.run_orchestrator

# Run single platform
docker run --env-file .env social-ads:local \
  python -m social.platforms.linkedin.pipeline
```

---

## Docker Build

### Dockerfile Structure

**File**: `social/Dockerfile`

```dockerfile
# ==========================================
# Stage 1: Builder - Install dependencies
# ==========================================
FROM python:3.11-slim AS builder

# Install UV
RUN pip install uv

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies to virtual environment
RUN uv sync --extra social --no-dev

# ==========================================
# Stage 2: Runtime - Minimal image
# ==========================================
FROM python:3.11-slim

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy source code
COPY shared/ /app/shared/
COPY social/ /app/social/

# Set Python path
ENV PYTHONPATH=/app
ENV PATH="/app/.venv/bin:$PATH"

# Default command (can be overridden)
CMD ["python", "-m", "social.orchestrator.run_orchestrator"]
```

### Build Commands

**Build for Local Testing**:
```bash
docker build -f social/Dockerfile -t social-ads:local .
```

**Build for Azure Container Registry**:
```bash
# Set ACR name
ACR_NAME="acrsocialadsprod"

# Login to ACR
az acr login --name $ACR_NAME

# Build and push
docker build -f social/Dockerfile \
  -t $ACR_NAME.azurecr.io/social-ads:latest \
  -t $ACR_NAME.azurecr.io/social-ads:$(date +%Y%m%d-%H%M%S) \
  .

docker push $ACR_NAME.azurecr.io/social-ads:latest
docker push $ACR_NAME.azurecr.io/social-ads:$(date +%Y%m%d-%H%M%S)
```

**Build Directly in ACR** (Recommended):
```bash
az acr build \
  --registry $ACR_NAME \
  --image social-ads:latest \
  --image social-ads:$(date +%Y%m%d-%H%M%S) \
  --file social/Dockerfile \
  .
```

### Multi-Platform Build

```bash
# Build for both AMD64 and ARM64
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f social/Dockerfile \
  -t $ACR_NAME.azurecr.io/social-ads:latest \
  --push \
  .
```

---

## Azure Container Apps Deployment

### Deployment Options

1. **Container App** - Long-running service (REST API, web app)
2. **Container App Job** - Scheduled execution (ETL pipelines) ← **Recommended for Social Ads**

### Deploy as Container App Job

**Create Job (Orchestrator)**:
```bash
# Set variables
RESOURCE_GROUP="rg-socialads-prod"
CONTAINERAPPS_ENV="env-socialads-prod"
ACR_NAME="acrsocialadsprod"
JOB_NAME="job-socialads-orchestrator"

# Get ACR credentials
ACR_USERNAME=$(az acr credential show --name $ACR_NAME --query username -o tsv)
ACR_PASSWORD=$(az acr credential show --name $ACR_NAME --query passwords[0].value -o tsv)

# Create job
az containerapp job create \
  --name $JOB_NAME \
  --resource-group $RESOURCE_GROUP \
  --environment $CONTAINERAPPS_ENV \
  --trigger-type Schedule \
  --cron-expression "0 2 * * *" \
  --image $ACR_NAME.azurecr.io/social-ads:latest \
  --registry-server $ACR_NAME.azurecr.io \
  --registry-username $ACR_USERNAME \
  --registry-password $ACR_PASSWORD \
  --cpu 2 \
  --memory 4Gi \
  --command "python" "-m" "social.orchestrator.run_orchestrator" \
  --env-vars \
    VERTICA_HOST=secretref:vertica-host \
    VERTICA_PORT=secretref:vertica-port \
    VERTICA_DATABASE=secretref:vertica-database \
    VERTICA_USER=secretref:vertica-user \
    VERTICA_PASSWORD=secretref:vertica-password \
    MICROSOFT_DEVELOPER_TOKEN=secretref:microsoft-token \
    LINKEDIN_ACCESS_TOKEN=secretref:linkedin-token \
    FACEBOOK_ACCESS_TOKEN=secretref:facebook-token \
    GOOGLE_ADS_DEVELOPER_TOKEN=secretref:google-token
```

**Create Jobs for Individual Platforms**:
```bash
# LinkedIn
az containerapp job create \
  --name job-socialads-linkedin \
  --resource-group $RESOURCE_GROUP \
  --environment $CONTAINERAPPS_ENV \
  --trigger-type Schedule \
  --cron-expression "0 3 * * *" \
  --image $ACR_NAME.azurecr.io/social-ads:latest \
  --cpu 1 \
  --memory 2Gi \
  --command "python" "-m" "social.platforms.linkedin.pipeline"

# Facebook
az containerapp job create \
  --name job-socialads-facebook \
  --resource-group $RESOURCE_GROUP \
  --environment $CONTAINERAPPS_ENV \
  --trigger-type Schedule \
  --cron-expression "0 3 * * *" \
  --image $ACR_NAME.azurecr.io/social-ads:latest \
  --cpu 1 \
  --memory 2Gi \
  --command "python" "-m" "social.platforms.facebook.pipeline"

# Google
az containerapp job create \
  --name job-socialads-google \
  --resource-group $RESOURCE_GROUP \
  --environment $CONTAINERAPPS_ENV \
  --trigger-type Schedule \
  --cron-expression "0 4 * * *" \
  --image $ACR_NAME.azurecr.io/social-ads:latest \
  --cpu 1 \
  --memory 2Gi \
  --command "python" "-m" "social.platforms.google.pipeline"

# Microsoft
az containerapp job create \
  --name job-socialads-microsoft \
  --resource-group $RESOURCE_GROUP \
  --environment $CONTAINERAPPS_ENV \
  --trigger-type Schedule \
  --cron-expression "0 4 * * *" \
  --image $ACR_NAME.azurecr.io/social-ads:latest \
  --cpu 1 \
  --memory 2Gi \
  --command "python" "-m" "social.platforms.microsoft.pipeline"
```

### Manual Job Execution

```bash
# Execute orchestrator job manually
az containerapp job start \
  --name job-socialads-orchestrator \
  --resource-group $RESOURCE_GROUP

# Execute LinkedIn job manually
az containerapp job start \
  --name job-socialads-linkedin \
  --resource-group $RESOURCE_GROUP
```

### Update Deployment

**Update Image**:
```bash
az containerapp job update \
  --name job-socialads-orchestrator \
  --resource-group $RESOURCE_GROUP \
  --image $ACR_NAME.azurecr.io/social-ads:20260122-143000
```

**Update Environment Variables**:
```bash
az containerapp job update \
  --name job-socialads-orchestrator \
  --resource-group $RESOURCE_GROUP \
  --set-env-vars \
    NEW_VAR=new_value
```

---

## Configuration Management

### Azure Key Vault Integration

**Store Secrets in Key Vault**:
```bash
KEY_VAULT_NAME="kv-socialads-prod"

# Store database credentials
az keyvault secret set --vault-name $KEY_VAULT_NAME \
  --name vertica-host --value "vertica.example.com"
az keyvault secret set --vault-name $KEY_VAULT_NAME \
  --name vertica-password --value "password123"

# Store API tokens
az keyvault secret set --vault-name $KEY_VAULT_NAME \
  --name linkedin-token --value "Bearer xxx..."
az keyvault secret set --vault-name $KEY_VAULT_NAME \
  --name facebook-token --value "Bearer yyy..."
az keyvault secret set --vault-name $KEY_VAULT_NAME \
  --name google-token --value "Bearer zzz..."
```

**Grant Container App Access to Key Vault**:
```bash
# Get Container App identity
IDENTITY_ID=$(az containerapp job show \
  --name job-socialads-orchestrator \
  --resource-group $RESOURCE_GROUP \
  --query identity.principalId -o tsv)

# Grant access
az keyvault set-policy \
  --name $KEY_VAULT_NAME \
  --object-id $IDENTITY_ID \
  --secret-permissions get list
```

**Reference Secrets in Container App**:
```bash
az containerapp job update \
  --name job-socialads-orchestrator \
  --resource-group $RESOURCE_GROUP \
  --set-env-vars \
    VERTICA_PASSWORD=secretref:vertica-password \
    LINKEDIN_ACCESS_TOKEN=secretref:linkedin-token
```

### Configuration Files

**Upload Configuration to Azure File Share**:
```bash
# Create storage account
az storage account create \
  --name stsocialadsprod \
  --resource-group $RESOURCE_GROUP \
  --location westeurope \
  --sku Standard_LRS

# Create file share
az storage share create \
  --name config \
  --account-name stsocialadsprod

# Upload configuration files
az storage file upload \
  --share-name config \
  --source social/orchestrator/orchestrator_config.yml \
  --path orchestrator_config.yml \
  --account-name stsocialadsprod

az storage file upload \
  --share-name config \
  --source social/platforms/linkedin/config_linkedin_ads.yml \
  --path config_linkedin_ads.yml \
  --account-name stsocialadsprod
```

**Mount File Share in Container App**:
```bash
az containerapp job update \
  --name job-socialads-orchestrator \
  --resource-group $RESOURCE_GROUP \
  --azure-file-volume-name config \
  --azure-file-volume-share config \
  --azure-file-volume-account-name stsocialadsprod \
  --azure-file-volume-account-key $(az storage account keys list \
    --account-name stsocialadsprod \
    --query [0].value -o tsv) \
  --azure-file-volume-mount /app/config
```

---

## Monitoring & Logging

### Application Insights Integration

**Enable Application Insights**:
```bash
# Get instrumentation key
INSTRUMENTATION_KEY=$(az monitor app-insights component show \
  --app ai-socialads-prod \
  --resource-group $RESOURCE_GROUP \
  --query instrumentationKey -o tsv)

# Add to Container App
az containerapp job update \
  --name job-socialads-orchestrator \
  --resource-group $RESOURCE_GROUP \
  --set-env-vars \
    APPLICATIONINSIGHTS_INSTRUMENTATION_KEY=$INSTRUMENTATION_KEY
```

**Add Application Insights to Code**:
```python
# social/orchestrator/run_orchestrator.py
import os
from opencensus.ext.azure.log_exporter import AzureLogHandler
from loguru import logger

# Setup Application Insights
if instrumentation_key := os.getenv("APPLICATIONINSIGHTS_INSTRUMENTATION_KEY"):
    from opencensus.ext.azure import metrics_exporter
    exporter = metrics_exporter.new_metrics_exporter(
        instrumentation_key=instrumentation_key
    )

    # Log to Application Insights
    logger.add(
        AzureLogHandler(instrumentation_key=instrumentation_key),
        format="{time} {level} {message}",
        level="INFO"
    )
```

### Log Analytics Queries

**Query Job Execution Logs**:
```kusto
ContainerAppConsoleLogs_CL
| where ContainerAppName_s == "job-socialads-orchestrator"
| where TimeGenerated > ago(24h)
| project TimeGenerated, Log_s
| order by TimeGenerated desc
```

**Query Performance Metrics**:
```kusto
customMetrics
| where name in ("rows_processed", "duration_seconds")
| where timestamp > ago(7d)
| summarize avg(value), max(value), min(value) by name, bin(timestamp, 1d)
```

**Query Errors**:
```kusto
traces
| where severityLevel >= 3  // Warning and above
| where timestamp > ago(24h)
| project timestamp, severityLevel, message
| order by timestamp desc
```

### Alerts

**Create Alert for Job Failure**:
```bash
az monitor metrics alert create \
  --name alert-orchestrator-failure \
  --resource-group $RESOURCE_GROUP \
  --scopes "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.App/jobs/job-socialads-orchestrator" \
  --condition "count failedRuns > 0" \
  --window-size 5m \
  --evaluation-frequency 5m \
  --action email-admin
```

---

## Scaling & Performance

### Resource Sizing

| Platform | CPU | Memory | Avg Duration | Peak Duration |
|----------|-----|--------|--------------|---------------|
| Microsoft | 1 core | 2 GB | 25 min | 35 min |
| LinkedIn | 1 core | 2 GB | 15 min | 20 min |
| Facebook | 1 core | 2 GB | 18 min | 25 min |
| Google | 1 core | 2 GB | 20 min | 30 min |
| **Orchestrator** | 2 cores | 4 GB | 65 min | 90 min |

### Parallelism Configuration

**orchestrator_config.yml**:
```yaml
orchestrator:
  parallel_execution: true
  max_parallel: 2  # Number of platforms to run simultaneously

parallel_groups:
  - [microsoft, linkedin]  # Group 1
  - [facebook, google]      # Group 2
```

**Performance Impact**:
- `max_parallel: 1` (sequential): ~118 minutes
- `max_parallel: 2` (2 groups): ~65 minutes (45% faster)
- `max_parallel: 4` (all parallel): ~35 minutes (70% faster, but higher resource usage)

### Retry Configuration

```yaml
platforms:
  - name: linkedin
    retry:
      max_attempts: 3
      backoff_seconds: 60  # Initial backoff
      backoff_multiplier: 2  # Exponential: 60s, 120s, 240s
```

### Timeout Configuration

```bash
az containerapp job update \
  --name job-socialads-orchestrator \
  --resource-group $RESOURCE_GROUP \
  --timeout 7200  # 2 hours
```

---

## CI/CD Pipeline

### GitHub Actions Workflow

**File**: `.github/workflows/deploy-social-ads.yml`

```yaml
name: Deploy Social Ads Platform

on:
  push:
    branches:
      - main
    paths:
      - 'social/**'
      - 'shared/**'

env:
  ACR_NAME: acrsocialadsprod
  RESOURCE_GROUP: rg-socialads-prod

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Login to Azure
        uses: azure/login@v1
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}

      - name: Build and push image
        run: |
          az acr build \
            --registry ${{ env.ACR_NAME }} \
            --image social-ads:${{ github.sha }} \
            --image social-ads:latest \
            --file social/Dockerfile \
            .

      - name: Update Container App Job
        run: |
          az containerapp job update \
            --name job-socialads-orchestrator \
            --resource-group ${{ env.RESOURCE_GROUP }} \
            --image ${{ env.ACR_NAME }}.azurecr.io/social-ads:${{ github.sha }}

      - name: Run health check
        run: |
          az containerapp job start \
            --name job-socialads-orchestrator \
            --resource-group ${{ env.RESOURCE_GROUP }}
```

### GitLab CI Pipeline

**File**: `.gitlab-ci.yml`

```yaml
stages:
  - build
  - deploy

variables:
  ACR_NAME: acrsocialadsprod
  RESOURCE_GROUP: rg-socialads-prod

build:
  stage: build
  image: mcr.microsoft.com/azure-cli
  script:
    - az login --service-principal -u $AZURE_CLIENT_ID -p $AZURE_CLIENT_SECRET --tenant $AZURE_TENANT_ID
    - az acr build --registry $ACR_NAME --image social-ads:$CI_COMMIT_SHA --image social-ads:latest --file social/Dockerfile .
  only:
    changes:
      - social/**
      - shared/**

deploy:
  stage: deploy
  image: mcr.microsoft.com/azure-cli
  script:
    - az login --service-principal -u $AZURE_CLIENT_ID -p $AZURE_CLIENT_SECRET --tenant $AZURE_TENANT_ID
    - az containerapp job update --name job-socialads-orchestrator --resource-group $RESOURCE_GROUP --image $ACR_NAME.azurecr.io/social-ads:$CI_COMMIT_SHA
  only:
    - main
```

---

## Rollback Procedure

**Rollback to Previous Image**:
```bash
# List available images
az acr repository show-tags \
  --name $ACR_NAME \
  --repository social-ads \
  --orderby time_desc

# Rollback to specific version
az containerapp job update \
  --name job-socialads-orchestrator \
  --resource-group $RESOURCE_GROUP \
  --image $ACR_NAME.azurecr.io/social-ads:20260121-120000
```

---

## Security Best Practices

1. **Use Managed Identity**: Enable system-assigned or user-assigned managed identity
2. **Store Secrets in Key Vault**: Never hardcode credentials
3. **Network Isolation**: Use Virtual Network integration for Container Apps
4. **Image Scanning**: Enable Defender for Containers on ACR
5. **Least Privilege**: Grant minimal RBAC permissions
6. **Rotate Tokens**: Implement token rotation for API credentials

---

## Troubleshooting Deployment

### Issue: Job Fails to Start

```bash
# Check job status
az containerapp job show \
  --name job-socialads-orchestrator \
  --resource-group $RESOURCE_GROUP

# View execution history
az containerapp job execution list \
  --name job-socialads-orchestrator \
  --resource-group $RESOURCE_GROUP

# View logs
az containerapp job logs show \
  --name job-socialads-orchestrator \
  --resource-group $RESOURCE_GROUP
```

### Issue: Image Pull Failure

```bash
# Verify ACR credentials
az acr credential show --name $ACR_NAME

# Test image pull
docker pull $ACR_NAME.azurecr.io/social-ads:latest
```

### Issue: Environment Variables Not Set

```bash
# List environment variables
az containerapp job show \
  --name job-socialads-orchestrator \
  --resource-group $RESOURCE_GROUP \
  --query properties.template.containers[0].env
```

---

## Next Steps

- [USAGE_GUIDE.md](USAGE_GUIDE.md) - Platform usage examples
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Common issues and solutions
- [API_REFERENCE.md](API_REFERENCE.md) - API documentation

---

**Questions or Issues?**

Contact: Alessandro Benelli (alessandro.benelli@esprinet.com)
