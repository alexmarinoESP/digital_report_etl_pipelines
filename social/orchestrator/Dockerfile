# Multi-stage build for Social Ads Orchestrator
# Unified container for all 4 platforms (Microsoft, LinkedIn, Facebook, Google)

# Stage 1: Builder - Install dependencies
FROM python:3.12-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements for all platforms
COPY requirements-orchestrator.txt /tmp/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements-orchestrator.txt

# Stage 2: Runtime - Minimal final image
FROM python:3.12-slim

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash appuser && \
    mkdir -p /app/data /app/logs /app/reports && \
    chown -R appuser:appuser /app

# Set working directory
WORKDIR /app

# Copy application code
COPY --chown=appuser:appuser social/ /app/social/

# Set Python path
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Entry point - orchestrator runs all platforms
CMD ["python", "social/orchestrator/run_orchestrator.py"]

# Labels for metadata
LABEL maintainer="Data Science Team"
LABEL version="1.0.0"
LABEL description="Social Ads Unified Orchestrator for Azure Container Apps"
LABEL platforms="microsoft,linkedin,facebook,google"
