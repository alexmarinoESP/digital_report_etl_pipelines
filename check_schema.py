import vertica_python
import os

# Configurazione connessione
conn_info = {
    'host': '10.128.6.48',
    'port': 5433,
    'database': 'DWPRD',
    'user': 'bi_alex',
    'password': 'Temporary1234!'
}

try:
    conn = vertica_python.connect(**conn_info)
    cur = conn.cursor()

    # Lista tutte le tabelle google_ads
    print("=" * 80)
    print("TABELLE GOOGLE ADS DISPONIBILI")
    print("=" * 80)
    cur.execute("""
        SELECT table_name
        FROM tables
        WHERE table_schema = 'GoogleAnalytics'
        AND table_name LIKE 'google_ads_%'
        ORDER BY table_name
    """)
    tables = [row[0] for row in cur.fetchall()]
    for t in tables:
        print(f"  - {t}")

    # Confronta schema per ogni coppia prod/test
    table_pairs = []
    for table in tables:
        if table.endswith('_TEST'):
            prod_table = table.replace('_TEST', '')
            if prod_table in tables:
                table_pairs.append((prod_table, table))

    print("\n" + "=" * 80)
    print("CONFRONTO SCHEMA TABELLE PROD vs TEST")
    print("=" * 80)

    for prod_table, test_table in table_pairs:
        print(f"\n{'=' * 80}")
        print(f"Tabella: {prod_table} vs {test_table}")
        print(f"{'=' * 80}")

        # Schema tabella PROD
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM columns
            WHERE table_schema = 'GoogleAnalytics'
            AND table_name = %s
            ORDER BY ordinal_position
        """, (prod_table,))
        prod_columns = {row[0]: {'type': row[1], 'nullable': row[2], 'default': row[3]} for row in cur.fetchall()}

        # Schema tabella TEST
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM columns
            WHERE table_schema = 'GoogleAnalytics'
            AND table_name = %s
            ORDER BY ordinal_position
        """, (test_table,))
        test_columns = {row[0]: {'type': row[1], 'nullable': row[2], 'default': row[3]} for row in cur.fetchall()}

        # Confronta
        prod_only = set(prod_columns.keys()) - set(test_columns.keys())
        test_only = set(test_columns.keys()) - set(prod_columns.keys())
        common = set(prod_columns.keys()) & set(test_columns.keys())

        print(f"\nColonne PROD: {len(prod_columns)}")
        for col in sorted(prod_columns.keys()):
            info = prod_columns[col]
            print(f"  - {col:30} {info['type']:20} nullable={info['nullable']}")

        print(f"\nColonne TEST: {len(test_columns)}")
        for col in sorted(test_columns.keys()):
            info = test_columns[col]
            print(f"  - {col:30} {info['type']:20} nullable={info['nullable']}")

        if prod_only:
            print(f"\n⚠️  Colonne solo in PROD:")
            for col in sorted(prod_only):
                info = prod_columns[col]
                print(f"  - {col:30} {info['type']:20}")

        if test_only:
            print(f"\n⚠️  Colonne solo in TEST:")
            for col in sorted(test_only):
                info = test_columns[col]
                print(f"  - {col:30} {info['type']:20}")

        # Controlla differenze nei tipi
        type_diffs = []
        for col in common:
            if prod_columns[col]['type'] != test_columns[col]['type']:
                type_diffs.append((col, prod_columns[col]['type'], test_columns[col]['type']))

        if type_diffs:
            print(f"\n⚠️  Differenze di tipo:")
            for col, prod_type, test_type in type_diffs:
                print(f"  - {col}: PROD={prod_type} vs TEST={test_type}")

        if not prod_only and not test_only and not type_diffs:
            print("\n✓ Schema identico")

    conn.close()

except Exception as e:
    print(f"Errore: {e}")
    import traceback
    traceback.print_exc()
