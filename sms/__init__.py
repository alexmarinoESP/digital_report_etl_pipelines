"""
SMS Campaign ETL Pipeline.

This module provides ETL functionality for SMS campaigns sent via MAPP platform.
Extracts SMS data, enriches with Bitly link statistics, and persists to database.

Main components:
- Domain: Models and interfaces
- Adapters: External system integrations (MAPP, Bitly, Database)
- Services: Business logic
- Pipeline: Orchestration

Usage:
    from sms.pipeline import PipelineFactory
    from infrastructure.db_connection import get_vertica_connection

    # Create connection
    conn = get_vertica_connection()

    # Create and run pipeline
    pipeline = PipelineFactory.create_default(conn)
    result = pipeline.run()

    print(result)
"""

from sms.pipeline import SMSPipeline, PipelineFactory, PipelineResult
from sms.domain.models import Company, SMSCampaign, BitlyLink

__all__ = [
    "SMSPipeline",
    "PipelineFactory",
    "PipelineResult",
    "Company",
    "SMSCampaign",
    "BitlyLink",
]

__version__ = "1.0.0"
