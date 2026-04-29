"""
Dump completo di tutti gli account Google Ads sotto i MCC Esprinet.
Usa entrambi i config (9474097201 e 4619434319) per coprire l'intera gerarchia.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from google.ads.googleads.client import GoogleAdsClient


CONFIGS = [
    ("9474097201", Path("social/platforms/google/google-ads-9474097201.yml")),
    ("4619434319", Path("social/platforms/google/google-ads-4619434319.yml")),
]
API_VERSION = "v23"


def dump_hierarchy(seed_id: str, config_path: Path, mcc_label: str) -> list[dict[str, Any]]:
    client = GoogleAdsClient.load_from_storage(path=str(config_path), version=API_VERSION)
    google_ads_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          customer_client.client_customer,
          customer_client.level,
          customer_client.manager,
          customer_client.descriptive_name,
          customer_client.id,
          customer_client.status,
          customer_client.currency_code,
          customer_client.time_zone
        FROM customer_client
    """

    seen: dict[str, dict[str, Any]] = {}
    queue: list[str] = [seed_id]
    processed_managers: set[str] = set()

    while queue:
        manager_id = queue.pop(0)
        if manager_id in processed_managers:
            continue
        processed_managers.add(manager_id)

        try:
            resp = google_ads_service.search(customer_id=manager_id, query=query)
        except Exception as e:
            print(f"  [warn] cannot query {manager_id}: {str(e).splitlines()[0][:150]}")
            continue

        for row in resp:
            cc = row.customer_client
            cid = str(cc.id)
            entry = {
                "mcc": mcc_label,
                "id": cid,
                "name": cc.descriptive_name,
                "status": cc.status.name if hasattr(cc.status, "name") else str(cc.status),
                "level": cc.level,
                "manager": bool(cc.manager),
                "currency": cc.currency_code,
                "tz": cc.time_zone,
                "via_mcc": manager_id,
            }
            seen[cid] = entry
            if cc.manager and cid != seed_id:
                queue.append(cid)

    return list(seen.values())


def main() -> int:
    all_entries: dict[str, dict[str, Any]] = {}
    for label, cfg in CONFIGS:
        print(f"\n=== Dumping hierarchy via MCC {label} ({cfg.name}) ===")
        entries = dump_hierarchy(label, cfg, label)
        print(f"  found {len(entries)} accounts")
        for e in entries:
            existing = all_entries.get(e["id"])
            if existing is None:
                all_entries[e["id"]] = e
            else:
                existing["mcc"] = existing["mcc"] + "," + e["mcc"]

    rows = sorted(all_entries.values(), key=lambda r: (r["status"], r["name"] or ""))

    print()
    print("=" * 130)
    print(f"TOTALE: {len(rows)} account unici")
    print("=" * 130)

    by_status: dict[str, int] = {}
    for r in rows:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    print(f"Per status: {by_status}")
    n_manager = sum(1 for r in rows if r["manager"])
    print(f"Manager accounts: {n_manager}  |  Customer accounts: {len(rows) - n_manager}")
    print()

    print(f"{'ID':<14} {'Status':<10} {'Mgr':<4} {'Lvl':<4} {'MCC':<22} {'Curr':<5} {'Name'}")
    print("-" * 130)
    for r in rows:
        mgr = "Y" if r["manager"] else ""
        name = (r["name"] or "")[:60]
        print(f"{r['id']:<14} {r['status']:<10} {mgr:<4} {r['level']:<4} {r['mcc']:<22} {r['currency'] or '':<5} {name}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
