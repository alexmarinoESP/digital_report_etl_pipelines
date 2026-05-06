#!/usr/bin/env python3
"""Diagnostic test for LinkedIn audiences API call.

Goals:
1. Verify that get_audiences() actually returns data for each account.
2. Surface raw HTTP details (final URL, status, paging, sample element) so we
   can see WHY data may be missing.
3. Try a couple of fallback variants when the response is empty
   (e.g. without 'count', with explicit pagination, plain accounts param).
4. Compare API count vs current rows in Vertica
   GoogleAnalytics.linkedin_ads_audience to spot stale/empty loads.

Usage (from project root, with .env loaded):
    python -m social.platforms.linkedin.test_audience_api
or
    python social/platforms/linkedin/test_audience_api.py
"""

from __future__ import annotations

import json
import os
import sys
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except Exception:
    pass

from social.platforms.linkedin.adapter import LinkedInAdapter
from social.platforms.linkedin.constants import COMPANY_ACCOUNT_MAP
from social.platforms.linkedin.vertica_token_provider import VerticaTokenProvider
from social.core.constants import LINKEDIN_API_VERSION


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
def setup_logging(level: str = "DEBUG") -> None:
    logger.remove()
    logger.add(
        sys.stdout,
        format=(
            "<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | "
            "<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        ),
        level=level,
    )


def get_token_provider() -> VerticaTokenProvider:
    host = os.getenv("VERTICA_HOST")
    port = int(os.getenv("VERTICA_PORT", "5433"))
    database = os.getenv("VERTICA_DATABASE")
    user = os.getenv("VERTICA_USER")
    password = os.getenv("VERTICA_PASSWORD")

    missing = [k for k, v in {
        "VERTICA_HOST": host,
        "VERTICA_DATABASE": database,
        "VERTICA_USER": user,
        "VERTICA_PASSWORD": password,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing env vars: {missing}")

    logger.info(f"Connecting to Vertica {host}:{port}/{database} as {user}")
    return VerticaTokenProvider(
        host=host, port=port, database=database, user=user, password=password
    )


# ---------------------------------------------------------------------------
# Raw call (so we can inspect URL / status / response shape)
# ---------------------------------------------------------------------------
def call_audiences_raw(
    adapter: LinkedInAdapter,
    account_id: str,
    *,
    count: Optional[str] = "400",
    start: Optional[int] = None,
    extra_params: Optional[Dict[str, str]] = None,
    extra_no_encoded: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Call /rest/adSegments?q=accounts&accounts=...

    Returns a dict with diagnostic info:
        {
            "url": final URL after no-encoded substitutions (best-effort),
            "elements": [...],
            "paging": {...},
            "raw_keys": [...],          # top-level keys in response
            "error": None | str
        }
    """
    url = "https://api.linkedin.com/rest/adSegments"
    account_urn = f"urn:li:sponsoredAccount:{account_id}"
    encoded = urllib.parse.quote(account_urn)
    accounts_param = f"List({encoded})"

    params: Dict[str, Any] = {"q": "accounts"}
    if count is not None:
        params["count"] = count
    if start is not None:
        params["start"] = str(start)
    if extra_params:
        params.update(extra_params)

    no_encoded = {"accounts": accounts_param}
    if extra_no_encoded:
        no_encoded.update(extra_no_encoded)

    # Build a representation of the URL we expect to hit
    qs = urllib.parse.urlencode(params)
    final_url = f"{url}?{qs}&accounts={accounts_param}"

    try:
        response = adapter.http_client.get(
            url=url, params=params, no_encoded_params=no_encoded
        )
    except Exception as e:
        return {
            "url": final_url,
            "elements": [],
            "paging": {},
            "raw_keys": [],
            "error": f"{type(e).__name__}: {e}",
        }

    return {
        "url": final_url,
        "elements": response.get("elements", []),
        "paging": response.get("paging", {}),
        "raw_keys": list(response.keys()),
        "error": None,
    }


def page_through_audiences(
    adapter: LinkedInAdapter, account_id: str, page_size: int = 400
) -> Dict[str, Any]:
    """Iterate using start/count pagination until paging returns no more rows."""
    all_elements: List[Dict[str, Any]] = []
    start = 0
    last_paging: Dict[str, Any] = {}
    pages = 0
    error: Optional[str] = None

    while True:
        result = call_audiences_raw(
            adapter, account_id, count=str(page_size), start=start
        )
        if result["error"]:
            error = result["error"]
            break

        elements = result["elements"]
        last_paging = result["paging"]
        pages += 1
        if not elements:
            break

        all_elements.extend(elements)

        total = last_paging.get("total")
        if total is not None and len(all_elements) >= total:
            break
        if len(elements) < page_size:
            break
        start += page_size
        # safety net
        if pages > 50:
            logger.warning("Stopping pagination at 50 pages (safety)")
            break

    return {
        "elements": all_elements,
        "paging": last_paging,
        "pages": pages,
        "error": error,
    }


# ---------------------------------------------------------------------------
# Vertica state (how stale is linkedin_ads_audience?)
# ---------------------------------------------------------------------------
def check_vertica_state(token_provider: VerticaTokenProvider) -> None:
    logger.info("=" * 80)
    logger.info("Vertica state for GoogleAnalytics.linkedin_ads_audience")
    logger.info("=" * 80)

    queries = [
        ("Total rows", "SELECT COUNT(*) FROM GoogleAnalytics.linkedin_ads_audience"),
        (
            "Rows per account",
            "SELECT account, COUNT(*) AS n "
            "FROM GoogleAnalytics.linkedin_ads_audience GROUP BY account ORDER BY account",
        ),
        (
            "Last load date",
            "SELECT MAX(row_loaded_date) FROM GoogleAnalytics.linkedin_ads_audience",
        ),
        (
            "Rows per load date (last 10)",
            "SELECT row_loaded_date::date AS d, COUNT(*) AS n "
            "FROM GoogleAnalytics.linkedin_ads_audience "
            "GROUP BY 1 ORDER BY 1 DESC LIMIT 10",
        ),
    ]

    conn = None
    cur = None
    try:
        conn = token_provider._connection.connect()
        cur = conn.cursor()
        for label, q in queries:
            try:
                cur.execute(q)
                rows = cur.fetchall()
                logger.info(f"-- {label}")
                for r in rows:
                    logger.info(f"   {r}")
            except Exception as e:
                logger.warning(f"   {label}: query failed: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# ---------------------------------------------------------------------------
# Per-account test
# ---------------------------------------------------------------------------
def test_account(adapter: LinkedInAdapter, account_id: str) -> Dict[str, Any]:
    logger.info("=" * 80)
    logger.info(f"Account {account_id}")
    logger.info("=" * 80)

    summary: Dict[str, Any] = {"account_id": account_id}

    # Pass 1: same call as in production
    logger.info("[1] Production-equivalent call (q=accounts, count=400)")
    res = call_audiences_raw(adapter, account_id, count="400")
    logger.info(f"   final URL : {res['url']}")
    logger.info(f"   raw keys  : {res['raw_keys']}")
    logger.info(f"   paging    : {res['paging']}")
    logger.info(f"   elements  : {len(res['elements'])}")
    if res["error"]:
        logger.error(f"   error     : {res['error']}")
    elif res["elements"]:
        sample = res["elements"][0]
        logger.info(
            f"   sample id : {sample.get('id')}  "
            f"name={sample.get('name')!r}  status={sample.get('status')!r}"
        )
        logger.debug(f"   sample raw: {json.dumps(sample, default=str)[:500]}")
    summary["production_call"] = {
        "elements": len(res["elements"]),
        "paging": res["paging"],
        "error": res["error"],
    }

    # Pass 2: full pagination (in case >400 rows or LinkedIn returns chunks)
    logger.info("[2] Walking pagination start/count")
    pag = page_through_audiences(adapter, account_id, page_size=400)
    logger.info(
        f"   total elements after paging: {len(pag['elements'])} "
        f"in {pag['pages']} page(s); paging={pag['paging']}"
    )
    if pag["error"]:
        logger.error(f"   error: {pag['error']}")
    summary["paginated_call"] = {
        "elements": len(pag["elements"]),
        "pages": pag["pages"],
        "paging": pag["paging"],
        "error": pag["error"],
    }

    # Pass 3: only when the production call returned 0 (try a couple of
    # variants to surface the most likely root causes)
    if not res["error"] and not res["elements"]:
        logger.warning("[3] Production call returned 0 rows; trying fallbacks")

        # 3a: drop count
        res_a = call_audiences_raw(adapter, account_id, count=None)
        logger.info(
            f"   3a no-count            -> elements={len(res_a['elements'])} "
            f"paging={res_a['paging']} err={res_a['error']}"
        )

        # 3b: include all states explicitly (LinkedIn sometimes hides ARCHIVED)
        res_b = call_audiences_raw(
            adapter, account_id, count="400",
            extra_no_encoded={
                "search": "(status:(values:List(READY,DRAFT,ARCHIVED,PROCESSING)))",
            },
        )
        logger.info(
            f"   3b with status filter  -> elements={len(res_b['elements'])} "
            f"paging={res_b['paging']} err={res_b['error']}"
        )

        # 3c: through public adapter method (sanity check; should match 1)
        try:
            via_adapter = adapter.get_audiences(account_id)
            logger.info(f"   3c adapter.get_audiences() -> {len(via_adapter)} rows")
        except Exception as e:
            logger.error(f"   3c adapter.get_audiences() failed: {e}")

    return summary


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    setup_logging(os.getenv("LOG_LEVEL", "DEBUG"))
    logger.info(f"LinkedIn API version (header): {LINKEDIN_API_VERSION}")
    logger.info(f"Test started at {datetime.now().isoformat(timespec='seconds')}")

    try:
        token_provider = get_token_provider()
    except Exception as e:
        logger.error(f"Token provider setup failed: {e}")
        return 2

    # 1. DB state first (so we know what the loader has been writing)
    try:
        check_vertica_state(token_provider)
    except Exception as e:
        logger.warning(f"Could not inspect Vertica state: {e}")

    # 2. API calls per account
    adapter = LinkedInAdapter(token_provider=token_provider)
    summaries: List[Dict[str, Any]] = []
    try:
        for account_id in COMPANY_ACCOUNT_MAP.keys():
            try:
                summaries.append(test_account(adapter, account_id))
            except Exception as e:
                logger.exception(f"Account {account_id} crashed: {e}")
                summaries.append({"account_id": account_id, "error": str(e)})
    finally:
        adapter.close()

    # 3. Summary table
    logger.info("=" * 80)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 80)
    logger.info(f"{'account':<14} {'prod':>6} {'paged':>7} {'paging.total':>14}  notes")
    for s in summaries:
        prod = s.get("production_call", {})
        pag = s.get("paginated_call", {})
        total = pag.get("paging", {}).get("total", "-")
        notes = []
        if s.get("error"):
            notes.append(f"crash:{s['error']}")
        if prod.get("error"):
            notes.append(f"prod-err:{prod['error']}")
        if pag.get("error"):
            notes.append(f"page-err:{pag['error']}")
        logger.info(
            f"{s.get('account_id', '-'):<14} "
            f"{prod.get('elements', '-'):>6} "
            f"{pag.get('elements', '-'):>7} "
            f"{str(total):>14}  {' | '.join(notes)}"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
