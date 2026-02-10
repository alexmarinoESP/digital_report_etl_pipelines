#!/usr/bin/env python3
"""Fix CTR column precision in Microsoft Ads tables."""

import os
import vertica_python

conn_info = {
    'host': os.getenv('VERTICA_HOST'),
    'port': int(os.getenv('VERTICA_PORT', '5433')),
    'database': os.getenv('VERTICA_DATABASE'),
    'user': os.getenv('VERTICA_USER'),
    'password': os.getenv('VERTICA_PASSWORD'),
}

tables = ['microsoft_ads_data', 'microsoft_ads_data_test']

print("Fixing CTR column precision to NUMERIC(10,4)...\n")

with vertica_python.connect(**conn_info) as conn:
    cursor = conn.cursor()

    for table in tables:
        print(f"Processing table: {table}")

        try:
            # Check current definition
            cursor.execute(f"""
                SELECT data_type, numeric_precision, numeric_scale
                FROM v_catalog.columns
                WHERE table_schema='GoogleAnalytics'
                AND table_name='{table}'
                AND column_name='Ctr';
            """)
            result = cursor.fetchone()

            if result:
                data_type, precision, scale = result
                print(f"  Current: {data_type}({precision},{scale})")

                if scale != 4:
                    # Alter column
                    cursor.execute(f"""
                        ALTER TABLE GoogleAnalytics.{table}
                        ALTER COLUMN Ctr SET DATA TYPE NUMERIC(10,4);
                    """)
                    conn.commit()
                    print(f"  ✓ Changed to NUMERIC(10,4)")
                else:
                    print(f"  ✓ Already NUMERIC(10,4)")
            else:
                print(f"  ✗ Column Ctr not found")

        except Exception as e:
            print(f"  ✗ Error: {e}")

        print()

print("Done!")
