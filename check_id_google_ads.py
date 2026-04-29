"""
Diagnostic script: cerca un ID nelle entita' Google Ads (ad_id, ad_group_id,
campaign_id) su tutti gli account sotto MCC 9474097201.

Usage:
    python check_id_google_ads.py [TARGET_ID] [--config PATH] [--login-cid 9474097201]

Default TARGET_ID: 801739206679
Default config path: prova ENV GOOGLE_ADS_CONFIG_FILE, poi
    ./social/platforms/google/google-ads-9474097201.yml
    OneDrive/.../invoice-ads/.../google-ads.yaml

Note:
- il file YAML deve contenere refresh_token VALIDO (non scaduto/revocato)
- login_customer_id va impostato a 9474097201 (MCC) per accedere a tutti
  i sub-account
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List

from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict


DEFAULT_CONFIG_CANDIDATES = [
    Path("social/platforms/google/google-ads-9474097201.yml"),
    Path("social/platforms/google/google-ads.yaml"),
    Path(
        r"c:\Users\Alessandro.Benelli\OneDrive - Esprinet\Alessandro\PROGETTI"
        r"\invoice-ads\src\invoice_ads\resources\google-ads.yaml"
    ),
]
MANAGER_CUSTOMER_ID = "9474097201"
API_VERSION = "v23"
LOOKBACK_DAYS = 365


def resolve_config(cli_path: str | None) -> Path:
    if cli_path:
        p = Path(cli_path)
        if not p.exists():
            raise FileNotFoundError(f"Config not found: {p}")
        return p
    env_path = os.getenv("GOOGLE_ADS_CONFIG_FILE")
    if env_path and Path(env_path).exists():
        return Path(env_path)
    for c in DEFAULT_CONFIG_CANDIDATES:
        if c.exists():
            return c
    raise FileNotFoundError(
        "Nessun file google-ads.yaml trovato. Passare --config PATH "
        "oppure settare la variabile env GOOGLE_ADS_CONFIG_FILE."
    )


def get_all_accounts(client: GoogleAdsClient) -> List[Dict[str, Any]]:
    """Restituisce la lista di tutti gli account customer (non manager) sotto MCC."""
    customer_service = client.get_service("CustomerService")
    google_ads_service = client.get_service("GoogleAdsService")

    accessible = customer_service.list_accessible_customers()
    seed_ids = [
        google_ads_service.parse_customer_path(rn)["customer_id"]
        for rn in accessible.resource_names
    ]
    print(f"[INFO] Seed accessible customers: {seed_ids}")

    hierarchy_query = """
        SELECT
          customer_client.client_customer,
          customer_client.level,
          customer_client.manager,
          customer_client.descriptive_name,
          customer_client.id,
          customer_client.status
        FROM customer_client
    """

    all_customers: Dict[str, Dict[str, Any]] = {}
    processed_managers: set[str] = set()

    def query_manager(manager_id: str) -> None:
        if manager_id in processed_managers:
            return
        processed_managers.add(manager_id)
        try:
            resp = google_ads_service.search(
                customer_id=str(manager_id), query=hierarchy_query
            )
            for row in resp:
                cc = row.customer_client
                cid = str(cc.id)
                if not cc.manager:
                    all_customers[cid] = {
                        "id": cid,
                        "name": cc.descriptive_name,
                        "status": cc.status.name if hasattr(cc.status, "name") else str(cc.status),
                        "via_manager": manager_id,
                    }
                else:
                    query_manager(cid)
        except Exception as e:
            print(f"[WARN] Errore querying manager {manager_id}: {e}")

    for sid in seed_ids:
        query_manager(sid)

    return list(all_customers.values())


def search_id_in_account(
    client: GoogleAdsClient,
    customer_id: str,
    target_id: str,
    start_date: str,
    end_date: str,
) -> Dict[str, List[Dict[str, Any]]]:
    """Cerca target_id come ad_id, ad_group_id, campaign_id nell'account."""
    google_ads_service = client.get_service("GoogleAdsService")
    findings: Dict[str, List[Dict[str, Any]]] = {
        "ad_id": [],
        "ad_group_id": [],
        "campaign_id": [],
    }

    queries = {
        "ad_id": f"""
            SELECT
              ad_group_ad.ad.id,
              ad_group_ad.ad.name,
              ad_group_ad.ad.type,
              ad_group_ad.status,
              ad_group.id,
              ad_group.name,
              campaign.id,
              campaign.name,
              campaign.status,
              customer.id,
              customer.descriptive_name
            FROM ad_group_ad
            WHERE ad_group_ad.ad.id = {target_id}
        """,
        "ad_group_id": f"""
            SELECT
              ad_group.id,
              ad_group.name,
              ad_group.status,
              campaign.id,
              campaign.name,
              campaign.status,
              customer.id,
              customer.descriptive_name
            FROM ad_group
            WHERE ad_group.id = {target_id}
        """,
        "campaign_id": f"""
            SELECT
              campaign.id,
              campaign.name,
              campaign.status,
              campaign.serving_status,
              campaign.start_date,
              campaign.end_date,
              customer.id,
              customer.descriptive_name
            FROM campaign
            WHERE campaign.id = {target_id}
        """,
    }

    for kind, q in queries.items():
        try:
            resp = google_ads_service.search(customer_id=customer_id, query=q)
            for row in resp:
                findings[kind].append(
                    MessageToDict(row._pb, preserving_proto_field_name=False)
                )
        except Exception as e:
            msg = str(e).split("\n")[0]
            if "PERMISSION_DENIED" in msg or "USER_PERMISSION_DENIED" in msg:
                continue
            print(f"  [warn] {kind} on {customer_id}: {msg[:160]}")

    metrics_query = f"""
        SELECT
          ad_group_ad.ad.id,
          ad_group.id,
          campaign.id,
          campaign.name,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros,
          segments.date,
          customer.id,
          customer.descriptive_name
        FROM ad_group_ad
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
          AND (
            ad_group_ad.ad.id = {target_id}
            OR ad_group.id = {target_id}
            OR campaign.id = {target_id}
          )
    """
    try:
        resp = google_ads_service.search(customer_id=customer_id, query=metrics_query)
        rows = []
        for row in resp:
            rows.append(MessageToDict(row._pb, preserving_proto_field_name=False))
        if rows:
            findings["metrics"] = rows
    except Exception as e:
        msg = str(e).split("\n")[0]
        if "PERMISSION_DENIED" not in msg and "USER_PERMISSION_DENIED" not in msg:
            print(f"  [warn] metrics on {customer_id}: {msg[:160]}")

    return findings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("target_id", nargs="?", default="801739206679")
    parser.add_argument("--config", default=None, help="path to google-ads.yaml")
    parser.add_argument("--login-cid", default=MANAGER_CUSTOMER_ID,
                        help="login_customer_id (MCC). Default: 9474097201")
    parser.add_argument("--days", type=int, default=LOOKBACK_DAYS,
                        help="lookback window for metrics query")
    args = parser.parse_args()

    target_id = args.target_id
    end = date.today()
    start = end - timedelta(days=args.days)

    config_path = resolve_config(args.config)
    print(f"[INFO] Target ID: {target_id}")
    print(f"[INFO] Date range for metrics: {start} -> {end}")
    print(f"[INFO] Loading client from {config_path}")

    client = GoogleAdsClient.load_from_storage(
        path=str(config_path), version=API_VERSION
    )
    if args.login_cid and not client.login_customer_id:
        client.login_customer_id = args.login_cid
    print(f"[INFO] Client OK. Manager (login_customer_id): {client.login_customer_id}")

    accounts = get_all_accounts(client)
    print(f"[INFO] Found {len(accounts)} customer accounts under MCC")

    total_hits = 0
    summary: List[Dict[str, Any]] = []

    for i, acc in enumerate(accounts, 1):
        cid = acc["id"]
        name = acc["name"]
        print(f"\n[{i}/{len(accounts)}] Account {cid} - {name} (status={acc['status']})")
        try:
            findings = search_id_in_account(
                client, cid, target_id, start.isoformat(), end.isoformat()
            )
        except Exception as e:
            print(f"  [error] {e}")
            continue

        hits = {k: len(v) for k, v in findings.items() if v}
        if hits:
            total_hits += sum(hits.values())
            print(f"  >>> HIT: {hits}")
            summary.append({"account": cid, "name": name, "hits": hits, "data": findings})
            for kind, rows in findings.items():
                for r in rows[:3]:
                    print(f"    [{kind}] {r}")
                if len(rows) > 3:
                    print(f"    ... e altri {len(rows) - 3} record di tipo {kind}")
        else:
            print("  no match")

    print("\n" + "=" * 80)
    print(f"FINAL SUMMARY for ID {target_id}")
    print("=" * 80)
    if not summary:
        print(f"Nessun match trovato in nessun account per ID {target_id}.")
    else:
        for s in summary:
            print(f"- Account {s['account']} ({s['name']}): {s['hits']}")
    print(f"\nTotale match: {total_hits}")
    return 0 if summary else 1


if __name__ == "__main__":
    sys.exit(main())
