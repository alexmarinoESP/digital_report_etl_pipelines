"""
Script per verificare i dati nelle tabelle di test.

Questo script:
1. Si connette a Vertica
2. Controlla le tabelle di test (_test suffix)
3. Mostra conteggi, date, esempi di dati
4. Verifica integrità dei dati

Usage:
    python -m social.verify_test_data

    # Oppure solo una tabella:
    python -m social.verify_test_data --table linkedin_campaigns_test
"""

import os
import sys
import argparse
from typing import Optional

import vertica_python
from loguru import logger
import pandas as pd

# Setup logging
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>",
    level="INFO"
)


def get_connection():
    """Create Vertica connection."""
    return vertica_python.connect(
        host=os.getenv("VERTICA_HOST"),
        port=int(os.getenv("VERTICA_PORT", "5433")),
        database=os.getenv("VERTICA_DATABASE"),
        user=os.getenv("VERTICA_USER"),
        password=os.getenv("VERTICA_PASSWORD")
    )


def check_table(conn, schema: str, table: str):
    """Check test table data."""
    logger.info(f"\n{'='*80}")
    logger.info(f"📊 Table: {schema}.{table}")
    logger.info("="*80)

    cursor = conn.cursor()

    try:
        # Check if table exists
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM v_catalog.tables
            WHERE table_schema = '{schema}'
              AND table_name = '{table}'
        """)
        exists = cursor.fetchone()[0] > 0

        if not exists:
            logger.warning(f"⚠️  Table {schema}.{table} does not exist")
            return

        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        row_count = cursor.fetchone()[0]
        logger.info(f"📈 Total Rows: {row_count:,}")

        if row_count == 0:
            logger.warning("⚠️  Table is empty")
            return

        # Get date range
        cursor.execute(f"""
            SELECT
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM {schema}.{table}
            WHERE date IS NOT NULL
        """)
        date_range = cursor.fetchone()
        if date_range[0]:
            logger.info(f"📅 Date Range: {date_range[0]} to {date_range[1]}")

        # Get record counts by date
        cursor.execute(f"""
            SELECT
                date,
                COUNT(*) as records
            FROM {schema}.{table}
            GROUP BY date
            ORDER BY date DESC
            LIMIT 10
        """)
        daily_counts = cursor.fetchall()

        if daily_counts:
            logger.info("\n📊 Records by Date (last 10 days):")
            for date, count in daily_counts:
                logger.info(f"   {date}: {count:>6,} records")

        # Get sample data
        cursor.execute(f"""
            SELECT *
            FROM {schema}.{table}
            ORDER BY date DESC
            LIMIT 3
        """)

        columns = [desc[0] for desc in cursor.description]
        sample_data = cursor.fetchall()

        if sample_data:
            logger.info("\n📋 Sample Data (3 most recent records):")
            df = pd.DataFrame(sample_data, columns=columns)
            # Show only first 10 columns to fit in terminal
            display_df = df.iloc[:, :min(10, len(df.columns))]
            logger.info("\n" + display_df.to_string(index=False))

            if len(df.columns) > 10:
                logger.info(f"\n   ... and {len(df.columns) - 10} more columns")

        # Check for NULL values in key columns
        key_columns = ['campaign_id', 'campaign_name', 'impressions', 'clicks', 'spend']
        null_checks = []

        for col in key_columns:
            try:
                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM {schema}.{table}
                    WHERE {col} IS NULL
                """)
                null_count = cursor.fetchone()[0]
                if null_count > 0:
                    null_checks.append(f"{col}: {null_count} NULLs")
            except:
                pass  # Column might not exist

        if null_checks:
            logger.warning("\n⚠️  NULL Values Found:")
            for check in null_checks:
                logger.warning(f"   {check}")

        logger.info("="*80)

    except Exception as e:
        logger.error(f"❌ Error checking table {table}: {e}")
    finally:
        cursor.close()


def verify_all_tables():
    """Verify all test tables."""
    schema = os.getenv("VERTICA_SCHEMA", "social_ads")

    test_tables = [
        "linkedin_campaigns_test",
        "linkedin_ad_groups_test",
        "facebook_campaigns_test",
        "facebook_ad_sets_test",
        "google_campaigns_test",
        "google_ad_groups_test",
        "microsoft_campaigns_test",
        "microsoft_ad_groups_test"
    ]

    logger.info("="*80)
    logger.info("🔍 VERIFYING TEST TABLES")
    logger.info("="*80)
    logger.info(f"Schema: {schema}")
    logger.info(f"Tables to check: {len(test_tables)}")
    logger.info("="*80)

    conn = get_connection()

    try:
        for table in test_tables:
            check_table(conn, schema, table)

        # Summary
        logger.info("\n" + "="*80)
        logger.info("✅ VERIFICATION COMPLETE")
        logger.info("="*80)

    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")
        sys.exit(1)
    finally:
        conn.close()


def verify_single_table(table_name: str):
    """Verify single test table."""
    schema = os.getenv("VERTICA_SCHEMA", "social_ads")

    logger.info("="*80)
    logger.info(f"🔍 VERIFYING TABLE: {table_name}")
    logger.info("="*80)

    conn = get_connection()

    try:
        check_table(conn, schema, table_name)

        logger.info("\n" + "="*80)
        logger.info("✅ VERIFICATION COMPLETE")
        logger.info("="*80)

    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")
        sys.exit(1)
    finally:
        conn.close()


def cleanup_test_tables():
    """Delete all test tables (DANGEROUS - use with caution)."""
    schema = os.getenv("VERTICA_SCHEMA", "social_ads")

    logger.warning("="*80)
    logger.warning("⚠️  CLEANUP TEST TABLES")
    logger.warning("="*80)
    logger.warning("This will DELETE all data in test tables!")
    logger.warning("Are you sure? Type 'yes' to confirm:")

    confirmation = input().strip().lower()

    if confirmation != "yes":
        logger.info("Cleanup cancelled.")
        return

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Get all test tables
        cursor.execute(f"""
            SELECT table_name
            FROM v_catalog.tables
            WHERE table_schema = '{schema}'
              AND table_name LIKE '%_test'
        """)

        test_tables = [row[0] for row in cursor.fetchall()]

        if not test_tables:
            logger.info("No test tables found.")
            return

        logger.info(f"Found {len(test_tables)} test tables:")
        for table in test_tables:
            logger.info(f"  - {table}")

        logger.info("\nDeleting tables...")

        for table in test_tables:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
                logger.info(f"✅ Deleted: {table}")
            except Exception as e:
                logger.error(f"❌ Failed to delete {table}: {e}")

        conn.commit()

        logger.info("\n" + "="*80)
        logger.info("✅ CLEANUP COMPLETE")
        logger.info("="*80)

    except Exception as e:
        logger.error(f"❌ Cleanup failed: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


def main():
    """Main verification runner."""
    parser = argparse.ArgumentParser(description="Verify test tables data")
    parser.add_argument(
        "--table",
        type=str,
        help="Specific table to verify (e.g., linkedin_campaigns_test)"
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete all test tables (DANGEROUS)"
    )

    args = parser.parse_args()

    if args.cleanup:
        cleanup_test_tables()
    elif args.table:
        verify_single_table(args.table)
    else:
        verify_all_tables()


if __name__ == "__main__":
    main()
