"""
Diagnostica accesso al manager Zeliatech (760-454-3417) e al sub-account
Zeliatech (974-732-4854) usando i refresh token attualmente in uso dal
progetto.

Strategia:
  1. Stampa list_accessible_customers() con ognuno dei due config esistenti
     (sono lo stesso refresh_token, ma proviamo entrambi i login_customer_id).
  2. Per ogni MCC noto, scorre customer_client e cerca id 7604543417 oppure
     9747324854 nella gerarchia.
  3. Tenta accesso diretto con login_customer_id = 7604543417 (il manager
     Zeliatech). Funziona solo se l'utente del refresh token e' linkato
     a quel manager.
  4. Tenta query basilare sull'account 9747324854 con ognuno dei MCC come
     login_customer_id.

Output utile per capire se serve:
  - linkare Zeliatech MCC sotto Esprinet MCC, oppure
  - generare un nuovo refresh token con un utente Google diverso (es.
    quello che gestisce Zeliatech).
"""
from __future__ import annotations

import sys
from pathlib import Path

from google.ads.googleads.client import GoogleAdsClient

API_VERSION = "v23"

CONFIGS = [
    Path("social/platforms/google/google-ads-9474097201.yml"),
    Path("social/platforms/google/google-ads-4619434319.yml"),
]

ZELIA_MANAGER_ID = "7604543417"   # 760-454-3417
ZELIA_ACCOUNT_ID = "9747324854"   # 974-732-4854

KNOWN_MCC_IDS = ["9474097201", "4619434319"]


def banner(s: str) -> None:
    print()
    print("=" * 100)
    print(s)
    print("=" * 100)


def step1_list_accessible(cfg: Path) -> list[str]:
    banner(f"[1] list_accessible_customers via config {cfg.name}")
    client = GoogleAdsClient.load_from_storage(path=str(cfg), version=API_VERSION)
    cs = client.get_service("CustomerService")
    gas = client.get_service("GoogleAdsService")
    try:
        resp = cs.list_accessible_customers()
    except Exception as e:
        print(f"  ERROR: {str(e).splitlines()[0][:200]}")
        return []
    ids = [gas.parse_customer_path(rn)["customer_id"] for rn in resp.resource_names]
    print(f"  refresh-token vede {len(ids)} customer accessibili (top-level):")
    for cid in ids:
        marker = ""
        if cid == ZELIA_MANAGER_ID:
            marker = "  <-- MANAGER ZELIATECH"
        elif cid == ZELIA_ACCOUNT_ID:
            marker = "  <-- ACCOUNT ZELIATECH"
        print(f"    - {cid}{marker}")
    return ids


def step2_search_in_hierarchy(cfg: Path, seed_mcc: str) -> None:
    banner(f"[2] customer_client sotto MCC {seed_mcc} (config {cfg.name})")
    client = GoogleAdsClient.load_from_storage(path=str(cfg), version=API_VERSION)
    svc = client.get_service("GoogleAdsService")
    q = """
        SELECT
          customer_client.id,
          customer_client.descriptive_name,
          customer_client.manager,
          customer_client.level,
          customer_client.status
        FROM customer_client
    """
    try:
        resp = svc.search(customer_id=seed_mcc, query=q)
    except Exception as e:
        print(f"  ERROR: {str(e).splitlines()[0][:200]}")
        return
    found_mgr = False
    found_acct = False
    total = 0
    for row in resp:
        cc = row.customer_client
        cid = str(cc.id)
        total += 1
        if cid == ZELIA_MANAGER_ID:
            found_mgr = True
            print(f"  HIT manager Zeliatech: {cid}  name={cc.descriptive_name!r}  "
                  f"manager={bool(cc.manager)}  level={cc.level}  status={cc.status.name}")
        if cid == ZELIA_ACCOUNT_ID:
            found_acct = True
            print(f"  HIT account Zeliatech: {cid}  name={cc.descriptive_name!r}  "
                  f"manager={bool(cc.manager)}  level={cc.level}  status={cc.status.name}")
    print(f"  totale customer_client letti: {total}")
    if not found_mgr:
        print(f"  manager {ZELIA_MANAGER_ID} NON presente nella gerarchia di MCC {seed_mcc}")
    if not found_acct:
        print(f"  account {ZELIA_ACCOUNT_ID} NON presente nella gerarchia di MCC {seed_mcc}")


def step3_direct_login_as_zelia_mcc(cfg: Path) -> None:
    banner(f"[3] Tentativo login_customer_id={ZELIA_MANAGER_ID} (forziamo MCC Zeliatech)")
    client = GoogleAdsClient.load_from_storage(path=str(cfg), version=API_VERSION)
    client.login_customer_id = ZELIA_MANAGER_ID  # override
    svc = client.get_service("GoogleAdsService")
    q = """
        SELECT
          customer_client.id,
          customer_client.descriptive_name,
          customer_client.manager,
          customer_client.level,
          customer_client.status
        FROM customer_client
    """
    try:
        resp = svc.search(customer_id=ZELIA_MANAGER_ID, query=q)
        rows = list(resp)
        print(f"  OK: viste {len(rows)} entita' sotto MCC Zeliatech")
        for row in rows[:20]:
            cc = row.customer_client
            print(f"    - {cc.id}  name={cc.descriptive_name!r}  manager={bool(cc.manager)}  "
                  f"lvl={cc.level}  status={cc.status.name}")
    except Exception as e:
        first = str(e).splitlines()[0][:240]
        print(f"  ERROR: {first}")
        # error code interessanti
        if "USER_PERMISSION_DENIED" in first:
            print("  -> il refresh token NON ha permessi su questo MCC.")
        elif "NOT_ADS_USER" in first:
            print("  -> il login_customer_id non e' un utente Ads valido per questo refresh token.")
        elif "CUSTOMER_NOT_ENABLED" in first:
            print("  -> account esiste ma non e' attivo / non collegato.")
        elif "NOT_FOUND" in first:
            print("  -> il customer non esiste o il refresh token non ha visibilita'.")


def step4_query_zelia_account(cfg: Path) -> None:
    banner(f"[4] Tentativo query metadati su account {ZELIA_ACCOUNT_ID} via config {cfg.name}")
    for login_cid in KNOWN_MCC_IDS + [ZELIA_MANAGER_ID, None]:
        client = GoogleAdsClient.load_from_storage(path=str(cfg), version=API_VERSION)
        if login_cid is not None:
            client.login_customer_id = login_cid
        else:
            client.login_customer_id = ""
        svc = client.get_service("GoogleAdsService")
        q = """
            SELECT customer.id, customer.descriptive_name, customer.currency_code,
                   customer.time_zone, customer.manager
            FROM customer
        """
        try:
            resp = svc.search(customer_id=ZELIA_ACCOUNT_ID, query=q)
            rows = list(resp)
            print(f"  login={login_cid!r:>15} -> OK, righe={len(rows)}")
            for row in rows:
                c = row.customer
                print(f"     id={c.id} name={c.descriptive_name!r} cur={c.currency_code} "
                      f"tz={c.time_zone} manager={bool(c.manager)}")
        except Exception as e:
            print(f"  login={login_cid!r:>15} -> ERR: {str(e).splitlines()[0][:200]}")


def main() -> int:
    print(f"Manager Zeliatech atteso: {ZELIA_MANAGER_ID} (760-454-3417)")
    print(f"Account  Zeliatech atteso: {ZELIA_ACCOUNT_ID} (974-732-4854)")

    for cfg in CONFIGS:
        if not cfg.exists():
            print(f"\n[skip] {cfg} non esiste")
            continue

        accessible = step1_list_accessible(cfg)

        # search dentro la gerarchia di ogni MCC noto e di tutti gli accessibili top-level
        seeds = sorted(set(KNOWN_MCC_IDS + accessible))
        for seed in seeds:
            step2_search_in_hierarchy(cfg, seed)

        step3_direct_login_as_zelia_mcc(cfg)
        step4_query_zelia_account(cfg)

    return 0


if __name__ == "__main__":
    sys.exit(main())
