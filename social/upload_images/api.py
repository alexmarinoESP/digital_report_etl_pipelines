"""
FastAPI endpoint for image upload.
Maintains exact same API contract as reportdigital-api for CRM compatibility.
Migrated from MinIO to Azure Blob Storage.
"""

import io
import logging
from fastapi import APIRouter, File, UploadFile, HTTPException, Depends
from pydantic import BaseModel
from loguru import logger
from sentry_sdk import start_transaction, capture_message, set_context
from social.upload_images.azure_storage import AzureBlobImageStorage


# Response models (manteniamo compatibilità con API esistente)
class MessageUploadFile(BaseModel):
    message: str
    status_code: int


router = APIRouter()


# Dependency per autenticazione (da implementare secondo le tue esigenze)
async def validate_request() -> bool:
    """
    Validate API request.
    TODO: Implementare autenticazione secondo le necessità del progetto.
    """
    # Per ora accetta tutte le richieste
    return True


def set_sentry_context(**kwargs):
    """Set Sentry context for error tracking."""
    return set_context("upload_context", {
        "Campaign_ID": kwargs.get('campaign_id', None),
        "Error Type": kwargs.get('error_type', None),
        "Error": kwargs.get('error', None)
    })


@router.post(
    '/uploadfile',
    responses={
        200: {"model": MessageUploadFile},
        204: {},
        400: {"model": MessageUploadFile},
        401: {"model": MessageUploadFile},
        500: {"model": MessageUploadFile}
    },
    name="UploadFile"
)
async def upload_file(
    activity_code: str,
    my_file: UploadFile = File(...),
    authenticated: bool = Depends(validate_request)
):
    """
    Upload image file to Azure Blob Storage.

    Maintains exact same behavior as MinIO version:
    - Campaign code extracted from activity_code (first part before '.')
    - Progressive numbering for duplicate activity codes
    - File stored in: report-digital/{campaign_code}/{activity_code}.{ext}

    Args:
        activity_code: Activity code in format "CODE.something" or "CODE"
        my_file: Uploaded file
        authenticated: Authentication validation result

    Returns:
        Success message or error

    Example:
        POST /uploadfile?activity_code=CAMP001.ACT001
        Body: multipart/form-data with image file
    """
    # Clean activity code (rimuovi apici come nel codice originale)
    activity_code_clean = activity_code.replace('\"', '').replace('\'', '')

    # Extract campaign code (prima parte prima del punto)
    campaign_code = activity_code_clean.split('.')[0]

    logger.info(f"Upload request - Campaign: {campaign_code}, Activity: {activity_code_clean}")

    try:
        # Initialize Azure Blob Storage client
        storage = AzureBlobImageStorage(container_name="report-digital")

        # Get existing images in campaign
        list_bucket_object, image_in_bucket = storage.list_images_in_campaign(campaign_code)

        # Determine file name (con logica progressive numbering)
        file_extension = my_file.filename.split('.')[-1]

        if activity_code_clean in image_in_bucket:
            # Activity già esiste, calcola numero progressivo
            next_number = storage.get_next_progressive_number(
                campaign_code,
                activity_code_clean,
                list_bucket_object
            )
            file_name = f"{activity_code_clean}_{next_number}.{file_extension}"
            logger.info(f"Duplicate activity detected, using progressive number: {next_number}")
        else:
            # Prima immagine per questa activity
            file_name = f"{activity_code_clean}.{file_extension}"

        # Read file content
        file_content = await my_file.read()

        # Determine content type
        content_type = "image/png" if file_extension.lower() == "png" else "image/jpeg"

        # Upload to Azure Blob Storage
        full_path = storage.upload_image(
            file_content=file_content,
            campaign_code=campaign_code,
            file_name=file_name,
            content_type=content_type
        )

        logger.info(f"Successfully uploaded: {full_path}")

        return {
            "message": f"File uploaded successfully: {full_path}",
            "status_code": 200
        }

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Exception in Upload Image: {error_msg}")

        # Log to Sentry
        with start_transaction(name='Upload Image'):
            set_sentry_context(
                campaign_id=campaign_code,
                error_type='Image Upload',
                error=error_msg
            )
            capture_message('Exception in Upload Image')

        raise HTTPException(
            status_code=500,
            detail=error_msg
        )


@router.get('/health', name="HealthCheck")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "upload-images"}
