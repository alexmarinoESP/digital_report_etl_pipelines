#!/usr/bin/env python3
"""Diagnostic verification for google_ads_audience load.

Goal: prove or disprove the "only 1 audience per ad_group" bug.

What this script does:
  1. Schema check on google_ads_audience and _TEST.
  2. Row counts: total, distinct ad_group ids, distinct display_name.
  3. Distribution of audiences per ad_group_id (the smoking gun).
  4. Number of ad_group with > 1 audience.
  5. Top 5 ad_group with the most audiences (drill-down example).
  6. Last load_date.
  7. Side-by-side TEST vs PROD comparison so you can see the fix at work.

Usage (from project root, .env already populated):

    python -m social.platforms.google.test_audience_db
or
    .venv\\Scripts\\python.exe social\\platforms\\google\\test_audience_db.py

It is read-only. Safe to run before and after the loader to compare states.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Iterable, List, Tuple

from dotenv import load_dotenv
from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from shared.connection.vertica import VerticaConnection


def setup_logging() -> None:
    logger.remove()
    logger.add(
        sys.stdout,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <7}</level> | {message}",
    )


def fetch_all(cur, sql: str, params: Iterable = ()) -> Tuple[List[str], List[Tuple]]:
    cur.execute(sql, tuple(params))
    cols = [d[0] for d in cur.description] if cur.description else []
    return cols, cur.fetchall()


def print_table(label: str, cols: List[str], rows: List[Tuple]) -> None:
    logger.info(f"-- {label}")
    if not rows:
        logger.info("   (no rows)")
        return
    if cols:
        logger.info("   " + " | ".join(cols))
    for r in rows:
        logger.info("   " + " | ".join("" if v is None else str(v) for v in r))


def inspect_table(cur, table: str) -> None:
    logger.info("=" * 80)
    logger.info(f"GoogleAnalytics.{table}")
    logger.info("=" * 80)

    # Existence check first; missing tables are normal in TEST until first run.
    cur.execute(
        "SELECT 1 FROM v_catalog.tables "
        "WHERE table_schema='GoogleAnalytics' AND table_name=%s",
        (table,),
    )
    if not cur.fetchone():
        logger.warning(f"Table GoogleAnalytics.{table} does not exist (skipping).")
        return

    # 1) Schema
    cols, rows = fetch_all(
        cur,
        "SELECT column_name, data_type FROM v_catalog.columns "
        "WHERE table_schema='GoogleAnalytics' AND table_name=%s "
        "ORDER BY ordinal_position",
        (table,),
    )
    print_table("schema", cols, rows)

    # 2) Headline counts
    cols, rows = fetch_all(
        cur,
        f"SELECT COUNT(*) AS rows, COUNT(DISTINCT id) AS distinct_ad_groups, "
        f"COUNT(DISTINCT display_name) AS distinct_audience_names "
        f"FROM GoogleAnalytics.{table}",
    )
    print_table("conteggi", cols, rows)

    # 3) Distribution audiences-per-adgroup (the bug fingerprint)
    cols, rows = fetch_all(
        cur,
        f"SELECT n_audience, COUNT(*) AS n_adgroups FROM ("
        f"  SELECT id, COUNT(*) AS n_audience FROM GoogleAnalytics.{table} GROUP BY id"
        f") s GROUP BY n_audience ORDER BY n_audience",
    )
    print_table("audience per ad_group (distribuzione)", cols, rows)

    # 4) Smoking gun: how many ad_group have > 1 audience?
    cols, rows = fetch_all(
        cur,
        f"SELECT COUNT(*) AS adgroups_with_multiple_audiences FROM ("
        f"  SELECT id FROM GoogleAnalytics.{table} GROUP BY id HAVING COUNT(*) > 1"
        f") s",
    )
    print_table("ad_group con > 1 audience (atteso > 0 dopo la fix)", cols, rows)

    # 5) Top 5 ad_group con piu audience (drill-down)
    cols, rows = fetch_all(
        cur,
        f"SELECT id, COUNT(*) AS n_audience FROM GoogleAnalytics.{table} "
        f"GROUP BY id ORDER BY n_audience DESC LIMIT 5",
    )
    print_table("top 5 ad_group per #audience", cols, rows)

    # 6) Drill-down sul primo ad_group multi-audience (se esiste)
    if rows and rows[0][1] > 1:
        top_id = rows[0][0]
        cols2, rows2 = fetch_all(
            cur,
            f"SELECT id, display_name, customer_id_google, load_date "
            f"FROM GoogleAnalytics.{table} WHERE id = %s ORDER BY display_name",
            (top_id,),
        )
        print_table(f"audience dell'ad_group {top_id}", cols2, rows2)

    # 7) Latest load_date
    cols, rows = fetch_all(
        cur,
        f"SELECT MAX(load_date) AS last_load FROM GoogleAnalytics.{table}",
    )
    print_table("ultimo load_date", cols, rows)


def diff_test_vs_prod(cur) -> None:
    """Quick side-by-side comparison so you can see the fix at work."""
    logger.info("=" * 80)
    logger.info("DIFF TEST vs PROD")
    logger.info("=" * 80)

    def _row(table: str) -> Tuple:
        cur.execute(
            "SELECT 1 FROM v_catalog.tables "
            "WHERE table_schema='GoogleAnalytics' AND table_name=%s",
            (table,),
        )
        if not cur.fetchone():
            return ("MISSING", "MISSING", "MISSING", "MISSING", "MISSING")
        # Two queries: Vertica refuses subqueries in SELECT with aggregates.
        cur.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT id), COUNT(DISTINCT display_name), "
            f"MAX(load_date) FROM GoogleAnalytics.{table}"
        )
        rows, ad_groups, names, last_load = cur.fetchone()
        cur.execute(
            f"SELECT COUNT(*) FROM ("
            f"  SELECT id FROM GoogleAnalytics.{table} GROUP BY id HAVING COUNT(*) > 1"
            f") s"
        )
        (multi,) = cur.fetchone()
        return (rows, ad_groups, names, multi, last_load)

    headers = ["table", "rows", "ad_groups", "names", "ad_groups_with>1", "last_load"]
    logger.info("   " + " | ".join(f"{h:>22}" for h in headers))
    for table in ("google_ads_audience", "google_ads_audience_TEST"):
        r = _row(table)
        cells = [table] + [str(v) for v in r]
        logger.info("   " + " | ".join(f"{c:>22}" for c in cells))


def main() -> int:
    setup_logging()

    required = ["VERTICA_HOST", "VERTICA_DATABASE", "VERTICA_USER", "VERTICA_PASSWORD"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        logger.error(f"Missing env vars: {missing}")
        return 2

    conn = VerticaConnection(
        host=os.environ["VERTICA_HOST"],
        port=int(os.environ.get("VERTICA_PORT", "5433")),
        database=os.environ["VERTICA_DATABASE"],
        user=os.environ["VERTICA_USER"],
        password=os.environ["VERTICA_PASSWORD"],
    ).connect()
    cur = conn.cursor()
    try:
        # PROD first (so you see the broken baseline)
        inspect_table(cur, "google_ads_audience")
        # TEST (where the loader run after the fix should land)
        inspect_table(cur, "google_ads_audience_TEST")
        # Side-by-side
        diff_test_vs_prod(cur)
    finally:
        conn.close()

    logger.info("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
