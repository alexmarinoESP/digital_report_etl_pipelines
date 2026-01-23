"""
SMS Pipeline - Single Campaign Test Script.

Test script to verify the complete SMS pipeline flow with a single campaign.
Simulates finding a campaign from Vertica, then processes it through the full pipeline:
1. Fetch statistics from MAPP API
2. Fetch SMS text from MAPP API
3. Extract Bitly links from text
4. Fetch click statistics from Bitly API
5. Save campaign and links to database

Usage:
    python -m sms.test_single_campaign
"""

import sys
from pathlib import Path

from loguru import logger

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sms.domain.models import Company, SMSCampaign
from sms.adapters.mapp_sms_adapter import MappSMSAdapter
from sms.adapters.bitly_adapter import BitlyAdapter
from sms.adapters.repository_adapter import SMSRepositoryAdapter
from sms.services.link_utils import extract_bitly_links
from shared.connection.vertica import VerticaConnection


# =============================================================================
# TEST PARAMETERS - SET THESE VALUES
# =============================================================================

# Campaign details from MAPP (simulating Vertica query result)
MESSAGE_ID = 4201469668  # MAPP message ID
ACTIVITY_ID = "C012420.001"  # External message ID / Activity code
COMPANY = Company.IT  # IT, ES, or VVIT

# =============================================================================


def configure_logging():
    """Configure loguru logger for test output."""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="DEBUG",
    )


def test_campaign_processing(
    message_id: int,
    activity_id: str,
    company: Company,
) -> bool:
    """
    Test processing a single SMS campaign through the complete pipeline.

    Args:
        message_id: MAPP message ID
        activity_id: External message ID (activity code)
        company: Company enum

    Returns:
        True if successful, False otherwise
    """
    logger.info("=" * 70)
    logger.info("SMS Campaign Test - Single Campaign Processing")
    logger.info("=" * 70)
    logger.info(f"MESSAGE_ID: {message_id}")
    logger.info(f"ACTIVITY_ID: {activity_id}")
    logger.info(f"COMPANY: {company.name}")
    logger.info("=" * 70)

    try:
        # =====================================================================
        # Step 0: Connect to database
        # =====================================================================
        logger.info("\n[Step 0] Connecting to Vertica database...")
        vertica_conn = VerticaConnection()
        connection = vertica_conn.connect()
        logger.success("✓ Database connection established")

        # =====================================================================
        # Step 1: Initialize adapters
        # =====================================================================
        logger.info("\n[Step 1] Initializing API adapters...")
        mapp_client = MappSMSAdapter(company=company)
        bitly_client = BitlyAdapter()
        repository = SMSRepositoryAdapter(connection=connection)
        logger.success("✓ Adapters initialized")
        logger.debug(f"  - MAPP URL: {mapp_client.base_url}")
        logger.debug(f"  - Bitly URL: {bitly_client.base_url}")

        # =====================================================================
        # Step 2: Check if campaign already exists
        # =====================================================================
        logger.info("\n[Step 2] Checking if campaign already exists in database...")
        campaign_exists = repository.campaign_exists(message_id)
        if campaign_exists:
            logger.warning(f"⚠ Campaign {message_id} already exists in database")
            logger.info("  Will update link click counts only (UPSERT mode)")
        else:
            logger.success("✓ Campaign does not exist, will create new")

        # =====================================================================
        # Step 3: Fetch SMS statistics from MAPP
        # =====================================================================
        logger.info("\n[Step 3] Fetching SMS statistics from MAPP API...")
        stats = mapp_client.get_sms_statistics(message_id)
        logger.success("✓ Statistics retrieved")
        logger.debug(f"  - Sent: {stats['sent_count']}")
        logger.debug(f"  - Delivered: {stats['delivered_count']}")
        logger.debug(f"  - Bounced: {stats['bounced_count']}")
        logger.debug(f"  - Acceptance Rate: {stats['acceptance_rate']}%")
        logger.debug(f"  - Sendout Date: {stats['sendout_date']}")

        # =====================================================================
        # Step 4: Fetch SMS content from MAPP
        # =====================================================================
        logger.info("\n[Step 4] Fetching SMS content from MAPP API...")
        content = mapp_client.get_sms_content(message_id)
        logger.success("✓ Content retrieved")
        logger.debug(f"  - Campaign Name: {content['campaign_name']}")
        logger.debug(f"  - SMS Text: {content['sms_text'][:100]}...")

        # =====================================================================
        # Step 5: Extract Bitly links from SMS text
        # =====================================================================
        logger.info("\n[Step 5] Extracting Bitly links from SMS text...")
        sms_text = content['sms_text']
        bitly_links = extract_bitly_links(
            sms_text=sms_text,
            message_id=message_id,
            activity_id=activity_id,
        )
        logger.success(f"✓ Found {len(bitly_links)} Bitly link(s)")
        for link in bitly_links:
            logger.debug(f"  - {link.bitly_short_url}")

        # =====================================================================
        # Step 6: Enrich Bitly links with click statistics
        # =====================================================================
        if bitly_links:
            logger.info("\n[Step 6] Fetching click statistics from Bitly API...")
            enriched_links = []
            for link in bitly_links:
                try:
                    enriched = bitly_client.enrich_link(link)
                    enriched_links.append(enriched)
                    logger.debug(f"  - {enriched.bitly_short_url}: {enriched.total_clicks} clicks")
                except Exception as e:
                    logger.warning(f"  ⚠ Failed to enrich {link.bitly_short_url}: {e}")
                    enriched_links.append(link)  # Keep original without enrichment

            bitly_links = enriched_links
            logger.success(f"✓ Enriched {len(bitly_links)} link(s)")
        else:
            logger.info("\n[Step 6] No Bitly links to enrich")

        # =====================================================================
        # Step 7: Create SMSCampaign object
        # =====================================================================
        logger.info("\n[Step 7] Creating SMSCampaign object...")
        campaign = SMSCampaign(
            message_id=message_id,
            activity_id=activity_id,
            campaign_name=content['campaign_name'],
            company_id=company.company_id,
            sms_text=sms_text,
            sendout_date=stats['sendout_date'],
            sent_count=stats['sent_count'],
            delivered_count=stats['delivered_count'],
            bounced_count=stats['bounced_count'],
            acceptance_rate=stats['acceptance_rate'],
            bitly_links=bitly_links,
        )
        logger.success("✓ Campaign object created")

        # =====================================================================
        # Step 8: Save to database
        # =====================================================================
        logger.info("\n[Step 8] Saving to database...")

        if campaign_exists:
            # Campaign exists - only upsert links (update clicks)
            logger.info("  Campaign exists, updating link clicks only...")
            campaigns_saved = 0
        else:
            # New campaign - save campaign first
            campaigns_saved = repository.save_campaigns([campaign])
            logger.success(f"✓ Saved {campaigns_saved} campaign(s)")

        # Upsert links (insert new or update clicks for existing)
        if bitly_links:
            links_upserted = repository.upsert_links(bitly_links)
            logger.success(f"✓ Upserted {links_upserted} link(s)")
        else:
            logger.info("  No links to save")

        # =====================================================================
        # Step 9: Verify saved data
        # =====================================================================
        logger.info("\n[Step 9] Verifying saved data...")
        exists_after = repository.campaign_exists(message_id)
        if exists_after:
            logger.success("✓ Campaign successfully saved to database")
        else:
            logger.error("✗ Campaign not found in database after save!")
            return False

        # Close connection
        connection.close()
        logger.info("\nDatabase connection closed")

        # =====================================================================
        # Summary
        # =====================================================================
        logger.info("\n" + "=" * 70)
        logger.info("TEST COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info(f"Campaign: {campaign.campaign_name}")
        logger.info(f"Message ID: {message_id}")
        logger.info(f"Activity ID: {activity_id}")
        logger.info(f"SMS Text: {sms_text[:80]}...")
        logger.info(f"Statistics: {stats['sent_count']} sent, {stats['delivered_count']} delivered")
        logger.info(f"Bitly Links: {len(bitly_links)} links with total clicks")
        logger.info("=" * 70)

        return True

    except Exception as e:
        logger.exception(f"Test failed with error: {e}")
        return False


def main() -> int:
    """Main entry point."""
    configure_logging()

    # Validate parameters
    if MESSAGE_ID is None:
        logger.error("ERROR: MESSAGE_ID is not set!")
        logger.info("Please edit this script and set MESSAGE_ID at the top")
        return 1

    if ACTIVITY_ID is None:
        logger.error("ERROR: ACTIVITY_ID is not set!")
        logger.info("Please edit this script and set ACTIVITY_ID at the top")
        return 1

    # Run test
    success = test_campaign_processing(
        message_id=MESSAGE_ID,
        activity_id=ACTIVITY_ID,
        company=COMPANY,
    )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
