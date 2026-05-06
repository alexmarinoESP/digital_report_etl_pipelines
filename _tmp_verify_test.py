"""Verifica più mirata: cosa è successo davvero al run delle 14:36."""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
sys.path.insert(0, str(ROOT))

from shared.connection.vertica import VerticaConnection

conn = VerticaConnection(
    host=os.environ["VERTICA_HOST"],
    port=int(os.environ.get("VERTICA_PORT", "5433")),
    database=os.environ["VERTICA_DATABASE"],
    user=os.environ["VERTICA_USER"],
    password=os.environ["VERTICA_PASSWORD"],
).connect()
cur = conn.cursor()


def q(label, sql, params=()):
    print(f"\n=== {label} ===")
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description] if cur.description else []
    if cols:
        print("  " + " | ".join(cols))
    for r in cur.fetchall():
        print("  " + " | ".join(str(x) for x in r))


# 1) È tabella o view?
q("È TABELLA in v_catalog.tables?",
  "SELECT table_name FROM v_catalog.tables "
  "WHERE table_schema='GoogleAnalytics' AND table_name LIKE 'linkedin_ads_campaign_audience%'")

q("È VIEW in v_catalog.views?",
  "SELECT table_name FROM v_catalog.views "
  "WHERE table_schema='GoogleAnalytics' AND table_name LIKE 'linkedin_ads_campaign_audience%'")

# 2) Cosa c'è nella _TEST_source?
q("Conteggio _TEST_source",
  "SELECT COUNT(*) FROM GoogleAnalytics.linkedin_ads_campaign_audience_TEST_source")

q("Distribuzione audience/campagna in _TEST_source",
  "SELECT n_audience, COUNT(*) AS n_campagne FROM ("
  "  SELECT id, COUNT(*) AS n_audience "
  "  FROM GoogleAnalytics.linkedin_ads_campaign_audience_TEST_source "
  "  GROUP BY id) s "
  "GROUP BY n_audience ORDER BY n_audience")

q("594515453 in _TEST_source",
  "SELECT COUNT(*) FROM GoogleAnalytics.linkedin_ads_campaign_audience_TEST_source WHERE id='594515453'")

q("594515453 audience nello _TEST_source",
  "SELECT id, audience_id FROM GoogleAnalytics.linkedin_ads_campaign_audience_TEST_source WHERE id='594515453' ORDER BY audience_id")

# 3) Controlla constraints/PK sulla _TEST e _TEST_source
q("Constraints su _TEST",
  "SELECT constraint_name, constraint_type, column_name FROM v_catalog.constraint_columns "
  "WHERE table_schema='GoogleAnalytics' AND table_name='linkedin_ads_campaign_audience_TEST' ORDER BY constraint_name")

q("Constraints su _TEST_source",
  "SELECT constraint_name, constraint_type, column_name FROM v_catalog.constraint_columns "
  "WHERE table_schema='GoogleAnalytics' AND table_name='linkedin_ads_campaign_audience_TEST_source' ORDER BY constraint_name")

# 4) Stato attuale TARGET _TEST
q("Conteggi _TEST adesso",
  "SELECT COUNT(*), COUNT(DISTINCT id), MAX(load_date) FROM GoogleAnalytics.linkedin_ads_campaign_audience_TEST")

q("594515453 in _TEST adesso",
  "SELECT id, audience_id, load_date FROM GoogleAnalytics.linkedin_ads_campaign_audience_TEST WHERE id='594515453' ORDER BY audience_id")

conn.close()
