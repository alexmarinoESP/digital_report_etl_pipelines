"""Test script to run only fb_ads_campaign table."""
import os
os.environ['TEST_MODE'] = 'true'
os.environ['VERTICA_HOST'] = '10.128.6.48'
os.environ['VERTICA_PORT'] = '5433'
os.environ['VERTICA_DATABASE'] = 'DWPRD'
os.environ['VERTICA_USER'] = 'bi_alex'
os.environ['VERTICA_PASSWORD'] = 'Temporary1234!'
os.environ['STORAGE_TYPE'] = 'vertica'
os.environ['LOG_LEVEL'] = 'INFO'

from social.platforms.facebook.pipeline import FacebookPipeline
from social.infrastructure.database import VerticaDataSink
from social.core.config import DatabaseConfig
from loguru import logger

logger.info("Starting Facebook Campaign test...")

# Initialize data sink
db_config = DatabaseConfig(
    host='10.128.6.48',
    port=5433,
    database='DWPRD',
    user='bi_alex',
    password='Temporary1234!',
    schema='GoogleAnalytics'
)

data_sink = VerticaDataSink(config=db_config, test_mode=True)

# Initialize pipeline
pipeline = FacebookPipeline(
    config_path='social/platforms/facebook/config_facebook_ads.yml',
    credentials_path='social/platforms/facebook/credentials.yml',
    data_sink=data_sink
)

# Run ONLY fb_ads_campaign table
logger.info("=" * 60)
logger.info("Testing fb_ads_campaign table")
logger.info("=" * 60)

try:
    df = pipeline.run(table_name='fb_ads_campaign')
    logger.success(f"✓ fb_ads_campaign completed successfully! Rows: {len(df)}")

    # Show sample data
    if not df.empty:
        logger.info(f"\nColumns: {list(df.columns)}")
        logger.info(f"\nSample data (first 3 rows):")
        print(df.head(3).to_string())

        # Check created_time dtype
        logger.info(f"\ncreated_time dtype: {df['created_time'].dtype}")
        logger.info(f"created_time sample: {df['created_time'].head(3).tolist()}")

except Exception as e:
    logger.error(f"✗ fb_ads_campaign failed: {e}")
    import traceback
    traceback.print_exc()

finally:
    data_sink.close()
    logger.info("Test completed")
