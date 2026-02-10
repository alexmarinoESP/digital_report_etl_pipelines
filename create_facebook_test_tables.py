"""
Script to drop old Facebook _TEST tables and create new ones with updated schema.

Changes:
- Removes row_loaded_date (TIMESTAMP) and last_updated_date (TIMESTAMP)
- Adds load_date (DATE)
"""

import vertica_python
import os

# Connection parameters
conn_info = {
    'host': os.getenv('VERTICA_HOST', '10.128.6.48'),
    'port': int(os.getenv('VERTICA_PORT', 5433)),
    'database': os.getenv('VERTICA_DATABASE', 'DWPRD'),
    'user': os.getenv('VERTICA_USER', 'bi_alex'),
    'password': os.getenv('VERTICA_PASSWORD', 'Temporary1234!'),
}

# SQL statements
tables_ddl = {
    'fb_ads_campaign_TEST': """
        CREATE TABLE IF NOT EXISTS GoogleAnalytics.fb_ads_campaign_TEST (
            campaign_id VARCHAR(255) NOT NULL,
            status VARCHAR(50),
            configured_status VARCHAR(50),
            effective_status VARCHAR(50),
            created_time TIMESTAMP,
            objective VARCHAR(100),
            load_date DATE,
            PRIMARY KEY (campaign_id)
        );
    """,

    'fb_ads_ad_set_TEST': """
        CREATE TABLE IF NOT EXISTS GoogleAnalytics.fb_ads_ad_set_TEST (
            id VARCHAR(255) NOT NULL,
            campaign_id VARCHAR(255),
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            destination_type VARCHAR(100),
            load_date DATE,
            PRIMARY KEY (id)
        );
    """,

    'fb_ads_insight_TEST': """
        CREATE TABLE IF NOT EXISTS GoogleAnalytics.fb_ads_insight_TEST (
            ad_id VARCHAR(255) NOT NULL,
            account_id VARCHAR(255),
            campaign_id VARCHAR(255),
            adset_id VARCHAR(255),
            ad_name VARCHAR(500),
            spend DECIMAL(18,2),
            impressions INTEGER,
            reach INTEGER,
            inline_link_clicks INTEGER,
            inline_link_click_ctr DECIMAL(10,6),
            clicks INTEGER,
            ctr DECIMAL(10,6),
            cpc DECIMAL(18,6),
            cpm DECIMAL(18,6),
            load_date DATE,
            PRIMARY KEY (ad_id)
        );
    """,

    'fb_ads_insight_actions_TEST': """
        CREATE TABLE IF NOT EXISTS GoogleAnalytics.fb_ads_insight_actions_TEST (
            ad_id VARCHAR(255) NOT NULL,
            action_type VARCHAR(255) NOT NULL,
            value DECIMAL(18,2),
            conversion_id VARCHAR(255),
            load_date DATE,
            PRIMARY KEY (ad_id, action_type)
        );
    """,

    'fb_ads_audience_adset_TEST': """
        CREATE TABLE IF NOT EXISTS GoogleAnalytics.fb_ads_audience_adset_TEST (
            audience_id VARCHAR(255) NOT NULL,
            adset_id VARCHAR(255) NOT NULL,
            campaign_id VARCHAR(255),
            name VARCHAR(500),
            load_date DATE,
            PRIMARY KEY (audience_id, adset_id)
        );
    """,

    'fb_ads_custom_conversion_TEST': """
        CREATE TABLE IF NOT EXISTS GoogleAnalytics.fb_ads_custom_conversion_TEST (
            conversion_id VARCHAR(255) NOT NULL,
            custom_event_type VARCHAR(100),
            rule TEXT,
            pixel_id VARCHAR(255),
            event_type VARCHAR(255),
            load_date DATE,
            PRIMARY KEY (conversion_id)
        );
    """
}

def main():
    print("Connecting to Vertica...")
    conn = vertica_python.connect(**conn_info)
    cursor = conn.cursor()

    try:
        # Drop old tables
        print("\n[1/2] Dropping old _TEST tables...")
        for table_name in tables_ddl.keys():
            drop_sql = f"DROP TABLE IF EXISTS GoogleAnalytics.{table_name} CASCADE;"
            print(f"  - Dropping {table_name}...")
            cursor.execute(drop_sql)
            conn.commit()
            print(f"    ✓ Dropped {table_name}")

        # Create new tables
        print("\n[2/2] Creating new _TEST tables with updated schema...")
        for table_name, create_sql in tables_ddl.items():
            print(f"  - Creating {table_name}...")
            cursor.execute(create_sql)
            conn.commit()
            print(f"    ✓ Created {table_name}")

        print("\n✅ All Facebook _TEST tables have been recreated successfully!")
        print("\nNew schema changes:")
        print("  - Removed: row_loaded_date (TIMESTAMP)")
        print("  - Removed: last_updated_date (TIMESTAMP)")
        print("  - Added: load_date (DATE)")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()
        print("\nConnection closed.")

if __name__ == "__main__":
    main()
