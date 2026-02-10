#!/usr/bin/env python3
"""Check which Microsoft tables exist in Vertica."""

import os
import vertica_python

# Connect to Vertica
conn_info = {
    'host': os.getenv('VERTICA_HOST'),
    'port': int(os.getenv('VERTICA_PORT', '5433')),
    'database': os.getenv('VERTICA_DATABASE'),
    'user': os.getenv('VERTICA_USER'),
    'password': os.getenv('VERTICA_PASSWORD'),
}

print("Connecting to Vertica...")
with vertica_python.connect(**conn_info) as conn:
    cursor = conn.cursor()

    # Query for Microsoft tables
    print("=" * 60)
    print("Tabelle Microsoft in GoogleAnalytics schema:")
    print("=" * 60)

    cursor.execute(
        "SELECT table_name FROM v_catalog.tables "
        "WHERE table_schema='GoogleAnalytics' AND table_name LIKE '%microsoft%' "
        "ORDER BY table_name;"
    )

    result = cursor.fetchall()

    if result:
        for row in result:
            print(f"  • {row[0]}")
        print(f"\nTotale: {len(result)} tabella/e")
    else:
        print("  Nessuna tabella Microsoft trovata")

    print("=" * 60)

    # Also check structure of existing table(s)
    if result:
        print("\nStruttura della prima tabella:")
        print("=" * 60)
        table_name = result[0][0]
        cursor.execute(
            f"SELECT column_name, data_type FROM v_catalog.columns "
            f"WHERE table_schema='GoogleAnalytics' AND table_name='{table_name}' "
            f"ORDER BY ordinal_position;"
        )
        columns = cursor.fetchall()
        for col_name, col_type in columns:
            print(f"  {col_name:30} {col_type}")
