"""
Cerca l'ID 801739206679 (e simili) nelle tabelle Google Ads su Vertica.
Cosi' verifichiamo se la pipeline ETL ha gia' portato i dati di quell'attivita'.

Usage:
    python check_id_vertica.py [TARGET_ID]

Default TARGET_ID: 801739206679
"""

from __future__ import annotations

import os
import sys
from typing import Iterable

import vertica_python


VERTICA_CFG = {
    "host": os.getenv("VERTICA_HOST", "vertica13.esprinet.com"),
    "port": int(os.getenv("VERTICA_PORT", "5433")),
    "user": os.getenv("VERTICA_USER", "ESPDM"),
    "password": os.getenv("VERTICA_PASSWORD", "Esprinet01"),
    "database": os.getenv("VERTICA_DATABASE", "Esprinet"),
    "tlsmode": "disable",
    "connection_timeout": 30,
}
SCHEMA = os.getenv("VERTICA_SCHEMA", "GoogleAnalytics")


# (table, columns_to_match)
TABLES = [
    ("google_ads_ad_creatives", ["ad_id", "adgroup_id"]),
    ("google_ads_campaign", ["id"]),
    ("google_ads_campaign_source", ["id"]),
    ("google_ads_report", ["campaign_id", "adgroup_id", "ad_id"]),
    ("google_ads_report_source", ["campaign_id", "adgroup_id", "ad_id"]),
    ("google_ads_placement", ["id"]),
    ("google_ads_audience", ["id"]),
    ("google_ads_cost_by_device", ["ad_id"]),
    ("google_ads_account", ["id"]),
    ("google_ads_violation", ["ad_id"]),
]


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_existing_columns(cur, schema: str, table: str) -> set[str]:
    cur.execute(
        """
        SELECT lower(column_name)
        FROM v_catalog.columns
        WHERE lower(table_schema) = lower(%s)
          AND lower(table_name) = lower(%s)
        """,
        (schema, table),
    )
    return {row[0] for row in cur.fetchall()}


def table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM v_catalog.tables
        WHERE lower(table_schema)=lower(%s) AND lower(table_name)=lower(%s)
        LIMIT 1
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def search_in_table(cur, schema: str, table: str, candidate_cols: Iterable[str], target: str) -> None:
    fq = f"{quote_ident(schema)}.{quote_ident(table)}"
    if not table_exists(cur, schema, table):
        print(f"  [skip] {schema}.{table} not found")
        return

    cols = get_existing_columns(cur, schema, table)
    matchable = [c for c in candidate_cols if c.lower() in cols]
    if not matchable:
        print(f"  [skip] {schema}.{table}: nessuna colonna utile (cerco {candidate_cols}, presenti {sorted(cols)[:8]}...)")
        return

    where = " OR ".join(f"CAST({quote_ident(c)} AS VARCHAR) = %s" for c in matchable)
    sql = f"SELECT COUNT(*) FROM {fq} WHERE {where}"
    cur.execute(sql, tuple([str(target)] * len(matchable)))
    n = cur.fetchone()[0]

    print(f"  [{table}] match cols={matchable} -> {n} righe")
    if n > 0:
        select_cols = ["*"]
        cur.execute(f"SELECT {', '.join(select_cols)} FROM {fq} WHERE {where} LIMIT 5",
                    tuple([str(target)] * len(matchable)))
        rows = cur.fetchall()
        col_names = [d.name for d in cur.description]
        for r in rows:
            row = dict(zip(col_names, r))
            keep = {k: v for k, v in row.items() if v not in (None, "", 0) or k in matchable}
            print(f"     -> {keep}")


def main() -> int:
    target = sys.argv[1] if len(sys.argv) > 1 else "801739206679"
    print(f"[INFO] Vertica: {VERTICA_CFG['host']}:{VERTICA_CFG['port']} db={VERTICA_CFG['database']} user={VERTICA_CFG['user']}")
    print(f"[INFO] Schema: {SCHEMA}")
    print(f"[INFO] Target ID: {target}")
    print()

    with vertica_python.connect(**VERTICA_CFG) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT table_name
            FROM v_catalog.tables
            WHERE lower(table_schema) = lower(%s)
              AND lower(table_name) LIKE 'google_ads_%%'
            ORDER BY table_name
            """,
            (SCHEMA,),
        )
        actual_tables = [r[0] for r in cur.fetchall()]
        print(f"[INFO] Tabelle google_ads_* in {SCHEMA}: {actual_tables}")
        print()

        for t, cols in TABLES:
            print(f"--- {t} ---")
            search_in_table(cur, SCHEMA, t, cols, target)

        for t in actual_tables:
            already = {x[0] for x in TABLES}
            if t.lower() in {x.lower() for x in already}:
                continue
            print(f"--- {t} (extra) ---")
            search_in_table(cur, SCHEMA, t, ["id", "ad_id", "adgroup_id", "campaign_id"], target)

    return 0


if __name__ == "__main__":
    sys.exit(main())
