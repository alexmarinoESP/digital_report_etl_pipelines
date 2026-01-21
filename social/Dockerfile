# =============================================================================
# Dockerfile for social
# ETL pipeline for social media ads data (Facebook, Google, LinkedIn)
# =============================================================================

FROM python:3.10-slim

LABEL maintainer="alex.marino@esprinet.com"
LABEL description="ETL pipeline for social media ads data"

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # PostgreSQL client dependencies
    libpq-dev \
    # Build dependencies for some packages
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv for dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy dependency files first (for better Docker layer caching)
COPY pyproject.toml uv.lock* ./

# Create virtual environment and install dependencies
RUN uv venv && uv sync --extra social --frozen --no-cache

# Add venv to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy shared module
COPY shared/ ./shared/

# Copy social module
COPY social/ ./social/

# Create data directory
RUN mkdir -p /app/data

CMD ["python", "-m", "social.scripts"]
