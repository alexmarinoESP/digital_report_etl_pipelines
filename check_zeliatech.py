"""
Cerca tutti i riferimenti a Zeliatech nel tenant Esprinet Google Ads e
verifica se l'ID 801739206679 e' una sua entita'.
"""
from __future__ import annotations
import sys
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict

CONFIG = "social/platforms/google/google-ads-9474097201.yml"
API_VERSION = "v23"
TARGET_ID = sys.argv[1] if len(sys.argv) > 1 else "801739206679"


def main() -> int:
    client = GoogleAdsClient.load_from_storage(path=CONFIG, version=API_VERSION)
    svc = client.get_service("GoogleAdsService")

    print("=" * 90)
    print("Step 1: tutti i customer_client che contengono 'zelia' nel nome")
    print("=" * 90)
    q = """
        SELECT
          customer_client.id,
          customer_client.descriptive_name,
          customer_client.status,
          customer_client.manager,
          customer_client.level
        FROM customer_client
        WHERE customer_client.descriptive_name LIKE '%elia%'
    """
    matches: list[dict] = []
    for seed in ("9474097201", "4619434319"):
        try:
            resp = svc.search(customer_id=seed, query=q)
            for row in resp:
                cc = row.customer_client
                m = {
                    "id": str(cc.id),
                    "name": cc.descriptive_name,
                    "status": cc.status.name,
                    "manager": bool(cc.manager),
                    "level": cc.level,
                    "via": seed,
                }
                if "zelia" in (cc.descriptive_name or "").lower():
                    matches.append(m)
                    print(f"  HIT via MCC {seed}: {m}")
        except Exception as e:
            print(f"  [warn] seed {seed}: {str(e).splitlines()[0][:120]}")

    if not matches:
        print("  Nessun match")
        return 1

    zelia_ids = sorted({m["id"] for m in matches})
    print(f"\nAccount Zeliatech trovati: {zelia_ids}")

    print()
    print("=" * 90)
    print(f"Step 2: cerco l'ID {TARGET_ID} dentro l'account Zeliatech")
    print("=" * 90)

    queries = {
        "ad_id": f"""
            SELECT campaign.id, campaign.name, ad_group.id, ad_group.name,
                   ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.status
            FROM ad_group_ad
            WHERE ad_group_ad.ad.id = {TARGET_ID}
        """,
        "ad_group_id": f"""
            SELECT campaign.id, campaign.name, ad_group.id, ad_group.name, ad_group.status
            FROM ad_group
            WHERE ad_group.id = {TARGET_ID}
        """,
        "campaign_id": f"""
            SELECT campaign.id, campaign.name, campaign.status, campaign.serving_status,
                   campaign.start_date, campaign.end_date
            FROM campaign
            WHERE campaign.id = {TARGET_ID}
        """,
    }

    for zid in zelia_ids:
        print(f"\n--- Account {zid} ---")
        for kind, q in queries.items():
            try:
                resp = svc.search(customer_id=zid, query=q)
                rows = [MessageToDict(r._pb) for r in resp]
                print(f"  [{kind}] -> {len(rows)} righe")
                for r in rows[:5]:
                    print(f"     {r}")
            except Exception as e:
                print(f"  [warn] {kind}: {str(e).splitlines()[0][:150]}")

    print()
    print("=" * 90)
    print(f"Step 3: ultime campagne attive in Zeliatech (per contesto)")
    print("=" * 90)
    last_q = """
        SELECT campaign.id, campaign.name, campaign.status,
               campaign.start_date, campaign.end_date
        FROM campaign
        WHERE campaign.status IN ('ENABLED', 'PAUSED')
        ORDER BY campaign.start_date DESC
        LIMIT 25
    """
    for zid in zelia_ids:
        print(f"\n--- Campagne in account {zid} ---")
        try:
            resp = svc.search(customer_id=zid, query=last_q)
            for row in resp:
                c = row.campaign
                print(f"  {c.id}  {c.status.name:<10}  {c.start_date} -> {c.end_date}  {c.name}")
        except Exception as e:
            print(f"  [warn] {str(e).splitlines()[0][:150]}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
