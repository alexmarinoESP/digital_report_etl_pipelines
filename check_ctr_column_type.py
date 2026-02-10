#!/usr/bin/env python3
"""Check CTR column type in Microsoft tables."""

import os
import vertica_python

conn_info = {
    'host': os.getenv('VERTICA_HOST'),
    'port': int(os.getenv('VERTICA_PORT', '5433')),
    'database': os.getenv('VERTICA_DATABASE'),
    'user': os.getenv('VERTICA_USER'),
    'password': os.getenv('VERTICA_PASSWORD'),
}

with vertica_python.connect(**conn_info) as conn:
    cursor = conn.cursor()

    # Check column types for both tables
    for table in ['microsoft_ads_data', 'microsoft_ads_data_test']:
        print("=" * 60)
        print(f"Table: {table}")
        print("=" * 60)
        cursor.execute(f"""
            SELECT column_name, data_type, numeric_precision, numeric_scale
            FROM v_catalog.columns
            WHERE table_schema='GoogleAnalytics'
            AND table_name='{table}'
            AND column_name IN ('Ctr', 'AverageCpc', 'AverageCpm', 'Spend', 'Conversions')
            ORDER BY column_name;
        """)

        rows = cursor.fetchall()
        print(f"{'Column':<20} {'Type':<20} {'Precision':<12} {'Scale'}")
        print("-" * 60)
        for row in rows:
            print(f"{row[0]:<20} {row[1]:<20} {str(row[2]):<12} {row[3]}")
        print()

    # Check actual CTR values
    print("=" * 60)
    print("Actual CTR values in database")
    print("=" * 60)

    for table in ['microsoft_ads_data', 'microsoft_ads_data_test']:
        cursor.execute(f"""
            SELECT Ctr, Clicks, Impressions, CAST(Clicks AS FLOAT) / NULLIF(Impressions, 0) as CTR_calculated
            FROM GoogleAnalytics.{table}
            LIMIT 3;
        """)
        rows = cursor.fetchall()
        print(f"\n{table}:")
        print(f"{'Ctr (stored)':<15} {'Clicks':<10} {'Impressions':<15} {'CTR (calculated)'}")
        print("-" * 60)
        for row in rows:
            print(f"{row[0]:<15} {row[1]:<10} {row[2]:<15} {row[3]}")
