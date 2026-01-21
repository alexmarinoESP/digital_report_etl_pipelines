# =============================================================================
# Dockerfile for newsletter
# Renders newsletter HTML to images for digital reports
# =============================================================================

FROM python:3.10-slim

LABEL maintainer="alex.marino@esprinet.com"
LABEL description="ETL pipeline for newsletter rendering"

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    # WeasyPrint dependencies
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libgdk-pixbuf2.0-0 \
    libffi-dev \
    shared-mime-info \
    # Oracle client dependencies
    libaio1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv for dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy dependency files first (for better Docker layer caching)
COPY pyproject.toml uv.lock* ./

# Create virtual environment and install dependencies
RUN uv venv && uv sync --extra newsletter --frozen --no-cache

# Add venv to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy shared module
COPY shared/ ./shared/

# Copy newsletter module
COPY newsletter/ ./newsletter/

# Create data directory
RUN mkdir -p /app/data

# Default: run full pipeline
# Override with docker run args for specific options:
#   --companies it es vvit
#   --sources mapp dynamics
#   --years-behind 2
CMD ["python", "-m", "newsletter.scripts.run_pipeline"]
