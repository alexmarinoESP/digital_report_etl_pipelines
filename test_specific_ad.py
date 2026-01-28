"""Test Google Ads API for specific ad_id 791614485563"""
import sys
sys.path.insert(0, '.')

from social.platforms.google.http_client import GoogleHTTPClient
from social.infrastructure.file_token_provider import FileBasedTokenProvider
from loguru import logger
import pandas as pd

# Initialize
token_provider = FileBasedTokenProvider(platform="google")
http_client = GoogleHTTPClient(token_provider=token_provider)

logger.info("=" * 80)
logger.info("TEST 1: Query WITH segments.date (daily breakdown)")
logger.info("=" * 80)

query_daily = """
    SELECT 
        ad_group_ad.ad.id,
        ad_group.id,
        campaign.id,
        segments.date,
        metrics.clicks,
        metrics.impressions,
        metrics.cost_micros,
        metrics.conversions
    FROM ad_group_ad
    WHERE ad_group_ad.ad.id = 791614485563
    AND segments.date DURING LAST_30_DAYS
    ORDER BY segments.date DESC
"""

results_daily = list(http_client.search(query_daily))
logger.info(f"Total rows returned: {len(results_daily)}")

if results_daily:
    # Show first 5 and last 5 rows
    logger.info("\nFirst 5 rows:")
    for i, row in enumerate(results_daily[:5]):
        logger.info(f"  Row {i+1}: date={row.get('segments.date')}, clicks={row.get('metrics.clicks')}, impressions={row.get('metrics.impressions')}")
    
    if len(results_daily) > 10:
        logger.info("\n...")
        logger.info("\nLast 5 rows:")
        for i, row in enumerate(results_daily[-5:]):
            logger.info(f"  Row {len(results_daily)-4+i}: date={row.get('segments.date')}, clicks={row.get('metrics.clicks')}, impressions={row.get('metrics.impressions')}")
    
    # Calculate totals
    df_daily = pd.DataFrame(results_daily)
    total_clicks = df_daily['metrics.clicks'].sum()
    total_impressions = df_daily['metrics.impressions'].sum()
    total_cost = df_daily['metrics.cost_micros'].sum()
    
    logger.info(f"\n{'='*80}")
    logger.info(f"DAILY DATA TOTALS (last 30 days, {len(results_daily)} days):")
    logger.info(f"  Total Clicks: {total_clicks}")
    logger.info(f"  Total Impressions: {total_impressions}")
    logger.info(f"  Total Cost (micros): {total_cost}")
    logger.info(f"{'='*80}")

logger.info("\n\n")
logger.info("=" * 80)
logger.info("TEST 2: Query WITHOUT segments.date (lifetime aggregated)")
logger.info("=" * 80)

query_lifetime = """
    SELECT 
        ad_group_ad.ad.id,
        ad_group.id,
        campaign.id,
        metrics.clicks,
        metrics.impressions,
        metrics.cost_micros,
        metrics.conversions,
        metrics.average_cpc,
        metrics.average_cpm,
        metrics.ctr
    FROM ad_group_ad
    WHERE ad_group_ad.ad.id = 791614485563
"""

results_lifetime = list(http_client.search(query_lifetime))
logger.info(f"Total rows returned: {len(results_lifetime)}")

if results_lifetime:
    row = results_lifetime[0]
    logger.info(f"\n{'='*80}")
    logger.info(f"LIFETIME AGGREGATED DATA (ALL TIME):")
    logger.info(f"  Ad ID: {row.get('adgroupad.ad.id')}")
    logger.info(f"  Total Clicks: {row.get('metrics.clicks')}")
    logger.info(f"  Total Impressions: {row.get('metrics.impressions')}")
    logger.info(f"  Total Cost (micros): {row.get('metrics.cost_micros')}")
    logger.info(f"  Total Conversions: {row.get('metrics.conversions')}")
    logger.info(f"  Average CPC: {row.get('metrics.average_cpc')}")
    logger.info(f"  Average CPM: {row.get('metrics.average_cpm')}")
    logger.info(f"  CTR: {row.get('metrics.ctr')}")
    logger.info(f"{'='*80}")

logger.info("\n\n")
logger.info("=" * 80)
logger.info("COMPARISON")
logger.info("=" * 80)
if results_daily and results_lifetime:
    daily_clicks = sum(r.get('metrics.clicks', 0) for r in results_daily)
    lifetime_clicks = results_lifetime[0].get('metrics.clicks', 0)
    
    daily_impressions = sum(r.get('metrics.impressions', 0) for r in results_daily)
    lifetime_impressions = results_lifetime[0].get('metrics.impressions', 0)
    
    logger.info(f"Daily sum (last 30 days): {daily_clicks} clicks, {daily_impressions} impressions")
    logger.info(f"Lifetime total (all time): {lifetime_clicks} clicks, {lifetime_impressions} impressions")
    logger.info(f"\nDifference (lifetime - last 30 days):")
    logger.info(f"  Clicks: {lifetime_clicks - daily_clicks} ({((lifetime_clicks - daily_clicks) / lifetime_clicks * 100):.1f}% of total)")
    logger.info(f"  Impressions: {lifetime_impressions - daily_impressions} ({((lifetime_impressions - daily_impressions) / lifetime_impressions * 100):.1f}% of total)")
    logger.info(f"\n✓ This shows the query WITHOUT segments.date gives you ALL historical data")
    logger.info(f"✓ The query WITH segments.date only gives you the filtered period")
