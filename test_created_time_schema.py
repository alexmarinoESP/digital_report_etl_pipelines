"""Test script to check created_time column type in production."""
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

# Query schema for fb_ads_campaign
cursor.execute("""
SELECT column_name, data_type, is_nullable
FROM v_catalog.columns
WHERE table_schema = 'GoogleAnalytics'
AND table_name = 'fb_ads_campaign'
ORDER BY ordinal_position
""")

print("\nfb_ads_campaign schema:")
print("-" * 60)
for row in cursor.fetchall():
    print(f"{row[0]:<30} {row[1]:<20} NULL={row[2]}")

# Also check some sample data
cursor.execute("""
SELECT campaign_id, created_time, typeof(created_time)
FROM GoogleAnalytics.fb_ads_campaign
LIMIT 3
""")

print("\n\nSample data:")
print("-" * 60)
for row in cursor.fetchall():
    print(f"campaign_id={row[0]}, created_time={row[1]}, type={row[2]}")

cursor.close()
conn.close()
