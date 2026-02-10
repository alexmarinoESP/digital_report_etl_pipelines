"""Truncate fb_ads_campaign_TEST table to remove duplicates."""
import os
os.environ['VERTICA_HOST'] = '10.128.6.48'
os.environ['VERTICA_PORT'] = '5433'
os.environ['VERTICA_DATABASE'] = 'DWPRD'
os.environ['VERTICA_USER'] = 'bi_alex'
os.environ['VERTICA_PASSWORD'] = 'Temporary1234!'
os.environ['STORAGE_TYPE'] = 'vertica'

from shared.connection.vertica import VerticaConnection

conn = VerticaConnection(
    host='10.128.6.48',
    port=5433,
    database='DWPRD',
    user='bi_alex',
    password='Temporary1234!'
).connect()

cursor = conn.cursor()

print("Truncating fb_ads_campaign_TEST...")
cursor.execute("TRUNCATE TABLE GoogleAnalytics.fb_ads_campaign_TEST")
cursor.execute("COMMIT")
print("✓ Table truncated successfully")

cursor.close()
conn.close()
