"""Check production table schema and sample data."""
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

# Check production schema
print("\nPRODUCTION fb_ads_campaign schema:")
print("=" * 80)
cursor.execute("""
SELECT column_name, data_type, is_nullable
FROM v_catalog.columns
WHERE table_schema = 'GoogleAnalytics'
AND table_name = 'fb_ads_campaign'
ORDER BY ordinal_position
""")

for row in cursor.fetchall():
    print(f"{row[0]:<30} {row[1]:<25} NULL={row[2]}")

# Check sample data for created_time format
print("\n\nSample created_time values from PRODUCTION:")
print("=" * 80)
cursor.execute("""
SELECT campaign_id, created_time, typeof(created_time)
FROM GoogleAnalytics.fb_ads_campaign
ORDER BY campaign_id
LIMIT 5
""")

for row in cursor.fetchall():
    print(f"campaign_id={row[0]}, created_time={row[1]}, type={row[2]}")

# Check for duplicates in production
print("\n\nCheck for duplicates in PRODUCTION:")
print("=" * 80)
cursor.execute("""
SELECT campaign_id, COUNT(*) as cnt,
       MIN(created_time) as min_time,
       MAX(created_time) as max_time
FROM GoogleAnalytics.fb_ads_campaign
GROUP BY campaign_id
HAVING COUNT(*) > 1
LIMIT 5
""")

result = cursor.fetchall()
if result:
    print(f"Found {len(result)} campaigns with duplicates:")
    for row in result:
        print(f"  campaign_id={row[0]}, count={row[1]}, min={row[2]}, max={row[3]}")
else:
    print("✓ No duplicates found in production (campaign_id is unique)")

# Check TEST table
print("\n\nTEST fb_ads_campaign_TEST current data:")
print("=" * 80)
cursor.execute("""
SELECT campaign_id, created_time, load_date
FROM GoogleAnalytics.fb_ads_campaign_TEST
ORDER BY campaign_id, created_time
LIMIT 10
""")

for row in cursor.fetchall():
    print(f"campaign_id={row[0]}, created_time={row[1]}, load_date={row[2]}")

# Check for duplicates in TEST
cursor.execute("""
SELECT campaign_id, COUNT(*) as cnt
FROM GoogleAnalytics.fb_ads_campaign_TEST
GROUP BY campaign_id
HAVING COUNT(*) > 1
""")

dup_count = cursor.fetchall()
if dup_count:
    print(f"\n⚠ Found {len(dup_count)} campaigns with duplicates in TEST table")
else:
    print(f"\n✓ No duplicates in TEST table")

cursor.close()
conn.close()
