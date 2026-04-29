"""
Genera un refresh_token Google Ads per l'utente che ha accesso al manager
Zeliatech (760-454-3417).

Riusa client_id / client_secret / developer_token gia' presenti in
social/platforms/google/google-ads-9474097201.yml (progetto OAuth Esprinet).
L'unica cosa che cambia rispetto agli altri config e' l'utente Google che
fa login: deve essere digital.esprinet@gmail.com, che ha accesso al
manager Zeliatech.

Cosa fa:
  1. Avvia un mini web server locale su una porta libera.
  2. Apre il browser su accounts.google.com con scope adwords.
  3. Tu fai login con digital.esprinet@gmail.com e dai consenso.
  4. Google reindirizza a http://localhost:<porta>/?code=... e lo script
     scambia il code per un refresh_token, che viene stampato.
  5. Lo script propone di salvarlo direttamente in
     social/platforms/google/google-ads-7604543417.yml.

Prerequisiti:
  - L'OAuth client 88104010282-... deve avere "http://localhost" tra le
    Authorized redirect URIs (Desktop app va bene di default).
  - L'app OAuth deve essere "In production" oppure
    digital.esprinet@gmail.com deve essere nei Test users.

Usage:
    .venv/Scripts/python generate_zeliatech_refresh_token.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml
from google_auth_oauthlib.flow import InstalledAppFlow

SOURCE_CONFIG = Path("social/platforms/google/google-ads-9474097201.yml")
TARGET_CONFIG = Path("social/platforms/google/google-ads-7604543417.yml")
ZELIA_LOGIN_CID = "7604543417"
SCOPES = ["https://www.googleapis.com/auth/adwords"]


def main() -> int:
    if not SOURCE_CONFIG.exists():
        print(f"[ERR] {SOURCE_CONFIG} non trovato.")
        return 1

    with SOURCE_CONFIG.open("r", encoding="utf-8") as fh:
        src = yaml.safe_load(fh)

    client_id = src["client_id"]
    client_secret = src["client_secret"]
    developer_token = src["developer_token"]

    print("=" * 80)
    print("Generazione refresh_token Google Ads per il manager Zeliatech")
    print("=" * 80)
    print(f"  client_id        : {client_id}")
    print(f"  developer_token  : {developer_token}")
    print(f"  scope            : {SCOPES[0]}")
    print(f"  login_customer_id: {ZELIA_LOGIN_CID}  (760-454-3417)")
    print()
    print("Si aprira' una finestra del browser. Fai login con")
    print("    digital.esprinet@gmail.com")
    print("e accetta i permessi richiesti.")
    print()

    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }

    flow = InstalledAppFlow.from_client_config(client_config, scopes=SCOPES)
    # port=0 -> il SO assegna una porta libera, evita conflitti.
    creds = flow.run_local_server(
        port=0,
        prompt="consent",       # forza emissione di refresh_token anche se gia' autorizzato
        access_type="offline",
        open_browser=True,
    )

    if not creds.refresh_token:
        print("[ERR] Nessun refresh_token ricevuto. Riprova con prompt=consent.")
        return 2

    print()
    print("OK -- refresh_token ottenuto.")
    print(f"   account loggato : {getattr(creds, 'id_token', None) or '(sconosciuto)'}")
    print(f"   refresh_token   : {creds.refresh_token[:25]}...{creds.refresh_token[-6:]}")
    print()

    new_yaml = {
        "developer_token": developer_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": creds.refresh_token,
        "scopes": SCOPES[0],
        "use_proto_plus": True,
        "login_customer_id": int(ZELIA_LOGIN_CID),
    }

    print(f"Salvo il config in: {TARGET_CONFIG}")
    TARGET_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    with TARGET_CONFIG.open("w", encoding="utf-8") as fh:
        for k, v in new_yaml.items():
            fh.write(f"{k}: {v}\n")
    print(f"Salvato {TARGET_CONFIG}")
    print()
    print("Prossimo passo:")
    print(f"  .venv/Scripts/python -c \"from google.ads.googleads.client import "
          f"GoogleAdsClient; "
          f"c=GoogleAdsClient.load_from_storage(path='{TARGET_CONFIG.as_posix()}', "
          f"version='v23'); "
          f"r=c.get_service('GoogleAdsService').search(customer_id='{ZELIA_LOGIN_CID}', "
          f"query='SELECT customer_client.id, customer_client.descriptive_name FROM customer_client'); "
          f"[print(row.customer_client.id, row.customer_client.descriptive_name) for row in r]\"")
    return 0


if __name__ == "__main__":
    sys.exit(main())
