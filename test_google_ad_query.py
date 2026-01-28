"""Test Google Ads API query for specific ad_id"""
import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from google.ads.googleads.client import GoogleAdsClient
from loguru import logger
import pandas as pd

# Initialize client directly
logger.info("Initializing Google Ads client...")
client = GoogleAdsClient.load_from_storage("social/config/google_ads.yaml")
customer_id = "6177409913"  # Your customer ID

# Query 1: With segments.date (daily breakdown) - last 30 days
logger.info("\n=== QUERY 1: With segments.date (daily data - last 30 days) ===")
query_daily = """
    SELECT 
        metrics.clicks, 
        metrics.impressions, 
        metrics.cost_micros,
        ad_group_ad.ad.id, 
        segments.date
    FROM ad_group_ad
    WHERE ad_group_ad.ad.id = 791614485563
    AND segments.date DURING LAST_30_DAYS
"""

ga_service = client.get_service("GoogleAdsService")
stream = ga_service.search_stream(customer_id=customer_id, query=query_daily)

rows_daily = []
for batch in stream:
    for row in batch.results:
        rows_daily.append({
            'ad_id': row.ad_group_ad.ad.id,
            'date': str(row.segments.date),
            'clicks': row.metrics.clicks,
            'impressions': row.metrics.impressions,
            'cost_micros': row.metrics.cost_micros,
        })

df_daily = pd.DataFrame(rows_daily)
logger.info(f"Daily data rows: {len(df_daily)}")
if not df_daily.empty:
    logger.info(f"\nFirst 10 rows:\n{df_daily.head(10).to_string()}")
    logger.info(f"\nLast 10 rows:\n{df_daily.tail(10).to_string()}")
    logger.info(f"\nTotal clicks (sum of {len(df_daily)} days): {df_daily['clicks'].sum()}")
    logger.info(f"Total impressions (sum of {len(df_daily)} days): {df_daily['impressions'].sum()}")
    logger.info(f"Total cost_micros (sum of {len(df_daily)} days): {df_daily['cost_micros'].sum()}")

# Query 2: Without segments.date (aggregated lifetime)
logger.info("\n\n=== QUERY 2: Without segments.date (aggregated lifetime ALL TIME) ===")
query_lifetime = """
    SELECT 
        metrics.clicks, 
        metrics.impressions, 
        metrics.cost_micros,
        ad_group_ad.ad.id
    FROM ad_group_ad
    WHERE ad_group_ad.ad.id = 791614485563
"""

stream = ga_service.search_stream(customer_id=customer_id, query=query_lifetime)

rows_lifetime = []
for batch in stream:
    for row in batch.results:
        rows_lifetime.append({
            'ad_id': row.ad_group_ad.ad.id,
            'clicks': row.metrics.clicks,
            'impressions': row.metrics.impressions,
            'cost_micros': row.metrics.cost_micros,
        })

df_lifetime = pd.DataFrame(rows_lifetime)
logger.info(f"Lifetime data rows: {len(df_lifetime)}")
if not df_lifetime.empty:
    logger.info(f"\nLifetime aggregated data:\n{df_lifetime.to_string()}")

logger.info("\n\n=== COMPARISON ===")
if not df_daily.empty and not df_lifetime.empty:
    daily_clicks = df_daily['clicks'].sum()
    lifetime_clicks = df_lifetime['clicks'].iloc[0]
    daily_impressions = df_daily['impressions'].sum()
    lifetime_impressions = df_lifetime['impressions'].iloc[0]
    
    logger.info(f"Daily sum (last 30 days): {daily_clicks} clicks, {daily_impressions} impressions")
    logger.info(f"Lifetime total (all time): {lifetime_clicks} clicks, {lifetime_impressions} impressions")
    logger.info(f"\n✓ Lifetime should be >= Daily sum (it includes all historical data)")
    logger.info(f"  Difference: {lifetime_clicks - daily_clicks} clicks, {lifetime_impressions - daily_impressions} impressions")
