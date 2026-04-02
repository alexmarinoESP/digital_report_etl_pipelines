"""
FastAPI application for image upload service.
Entry point for the upload-images container app.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.loguru import LoguruIntegration
import os

from social.upload_images.api import router


# Initialize Sentry (opzionale)
SENTRY_DSN = os.getenv("SENTRY_DSN")
if SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        integrations=[
            FastApiIntegration(),
            LoguruIntegration(),
        ],
        traces_sample_rate=0.1,
        environment=os.getenv("ENVIRONMENT", "production"),
    )
    logger.info("Sentry initialized")


# Create FastAPI app
app = FastAPI(
    title="Upload Images Service",
    description="Image upload service for social media campaigns (migrated from MinIO to Azure Blob Storage)",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)


# CORS middleware (configurare secondo necessità)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TODO: Configurare con domini specifici in produzione
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Include router
app.include_router(router, tags=["upload"])


@app.on_event("startup")
async def startup_event():
    """Application startup event."""
    logger.info("Upload Images Service starting up...")
    logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'production')}")


@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown event."""
    logger.info("Upload Images Service shutting down...")


@app.get("/", tags=["root"])
async def root():
    """Root endpoint."""
    return {
        "service": "upload-images",
        "version": "2.0.0",
        "status": "running"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
