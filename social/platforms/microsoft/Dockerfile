# Multi-stage build for Microsoft Ads ETL Container
# Optimized for Azure Container Apps with minimal size using uv

# Stage 1: Builder - Install dependencies with uv
FROM python:3.12-slim AS builder

# Install build dependencies and uv
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set working directory
WORKDIR /app

# Copy dependency files (for layer caching)
COPY pyproject.toml uv.lock README.md ./

# Install dependencies with uv (social extras)
RUN uv sync --frozen --no-dev --extra social

# Stage 2: Runtime - Minimal final image
FROM python:3.12-slim

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy uv binary and virtual environment from builder
COPY --from=builder /usr/local/bin/uv /usr/local/bin/uv
COPY --from=builder /app/.venv /app/.venv

# Set PATH for virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash appuser && \
    mkdir -p /app/data && \
    chown -R appuser:appuser /app

# Set working directory
WORKDIR /app

# Copy application code
COPY --chown=appuser:appuser social/ /app/social/
COPY --chown=appuser:appuser shared/ /app/shared/

# Copy Microsoft Ads tokens file
COPY --chown=appuser:appuser social/platforms/microsoft/tokens.json /app/tokens.json

# Set Python path
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Set default token file location
ENV TOKEN_FILE=/app/tokens.json

# Switch to non-root user
USER appuser

# Health check (optional, for debugging)
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Entry point
CMD ["python", "social/platforms/microsoft/run_microsoft.py"]

# Labels for metadata
LABEL maintainer="Data Science Team"
LABEL version="1.0.0"
LABEL description="Microsoft Ads ETL Pipeline for Azure Container Apps"
