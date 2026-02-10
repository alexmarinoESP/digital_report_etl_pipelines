#!/usr/bin/env python3
"""Check CTR values in Microsoft tables."""

import os
import vertica_python

# Connect to DB
conn_info = {
    'host': os.getenv('VERTICA_HOST'),
    'port': int(os.getenv('VERTICA_PORT', '5433')),
    'database': os.getenv('VERTICA_DATABASE'),
    'user': os.getenv('VERTICA_USER'),
    'password': os.getenv('VERTICA_PASSWORD'),
}

with vertica_python.connect(**conn_info) as conn:
    cursor = conn.cursor()

    # Query test table
    print("=" * 60)
    print("TEST TABLE (microsoft_ads_data_test)")
    print("=" * 60)
    cursor.execute('SELECT AdId, Clicks, Impressions, Ctr FROM GoogleAnalytics.microsoft_ads_data_test LIMIT 5;')
    rows = cursor.fetchall()
    print(f"{'AdId':<15} {'Clicks':<10} {'Impressions':<15} {'Ctr':<10}")
    print("-" * 60)
    for row in rows:
        print(f"{row[0]:<15} {row[1]:<10} {row[2]:<15} {row[3]:<10}")

    # Check production table
    print("\n" + "=" * 60)
    print("PRODUCTION TABLE (microsoft_ads_data)")
    print("=" * 60)
    cursor.execute('SELECT AdId, Clicks, Impressions, Ctr FROM GoogleAnalytics.microsoft_ads_data LIMIT 5;')
    rows_prod = cursor.fetchall()
    print(f"{'AdId':<15} {'Clicks':<10} {'Impressions':<15} {'Ctr':<10}")
    print("-" * 60)
    for row in rows_prod:
        print(f"{row[0]:<15} {row[1]:<10} {row[2]:<15} {row[3]:<10}")
