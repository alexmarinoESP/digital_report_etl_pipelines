"""Google Ads Pipeline Entry Point.

This script is the main entry point for running the Google Ads ETL pipeline
in Azure Container Apps or other containerized environments.

Key Features:
- Environment variable configuration
- Multiple storage backend support (Vertica, Azure Blob)
- Comprehensive error handling and exit codes
- Detailed logging
- Production-ready for containerized deployment

Exit Codes:
- 0: Success
- 1: Configuration error
- 2: Authentication error
- 3: Pipeline execution error
- 4: Data sink error

Environment Variables:
- GOOGLE_ADS_CONFIG_FILES: Comma- or semicolon-separated list of google-ads.yaml
  config files. Each tenant runs sequentially in the same process. Optional —
  if absent, falls back to GOOGLE_ADS_CONFIG_FILE (single tenant).
- GOOGLE_ADS_CONFIG_FILE: (legacy / single-tenant) Path to one google-ads.yaml.
- GOOGLE_MANAGER_CUSTOMER_ID: (single-tenant only) Manager account ID (MCC).
  Ignored when GOOGLE_ADS_CONFIG_FILES is set: the MCC is read from each
  config's `login_customer_id`.
- GOOGLE_API_VERSION: Google Ads API version (default: v18)
- STORAGE_TYPE: Storage backend (vertica, azure, or none)
- VERTICA_HOST, VERTICA_PORT, VERTICA_DATABASE, VERTICA_USER, VERTICA_PASSWORD
- AZURE_STORAGE_CONNECTION_STRING, AZURE_CONTAINER_NAME
"""

import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

from loguru import logger

from social.core.exceptions import (
    AuthenticationError,
    ConfigurationError,
    PipelineError,
)
from social.infrastructure.file_token_provider import FileBasedTokenProvider
from social.platforms.google.constants import API_VERSION
from social.platforms.google.pipeline import GooglePipeline, load_config


class _GoogleNoopTokenProvider:
    """Inert TokenProvider used as a placeholder for Google Ads.

    Google Ads picks credentials directly from google-ads.yaml; the adapter
    only requires *something* matching the TokenProvider Protocol shape.
    None of these methods is called at runtime for Google.
    """

    def get_access_token(self) -> str:  # pragma: no cover
        raise NotImplementedError("Google Ads uses google-ads.yaml, not a token provider")

    def get_refresh_token(self) -> str:  # pragma: no cover
        raise NotImplementedError("Google Ads uses google-ads.yaml, not a token provider")

    def refresh_access_token(self) -> str:  # pragma: no cover
        raise NotImplementedError("Google Ads uses google-ads.yaml, not a token provider")

    def get_token_expiry(self):  # pragma: no cover
        from datetime import datetime, timedelta, timezone
        return datetime.now(timezone.utc) + timedelta(days=365)


def setup_logging() -> None:
    """Configure logging for the pipeline."""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO",
    )
    logger.info("Logging configured")


def get_config_path() -> Path:
    """
    Get path to googleads_config.yml configuration file.

    Returns:
        Path to config file

    Raises:
        ConfigurationError: If config file not found
    """
    # Try multiple locations
    possible_paths = [
        Path("social/platforms/google/googleads_config.yml"),
        Path(__file__).parent / "googleads_config.yml",
        Path("/app/social/platforms/google/googleads_config.yml"),  # Container path
    ]

    for path in possible_paths:
        if path.exists():
            logger.info(f"Found config file: {path}")
            return path

    raise ConfigurationError(
        f"Configuration file not found. Tried: {[str(p) for p in possible_paths]}"
    )


def get_google_ads_config_file() -> str:
    """
    Get path to google-ads.yaml credentials file from environment (single-tenant
    legacy path).

    Returns:
        Path to google-ads.yaml file

    Raises:
        ConfigurationError: If config file not specified or not found
    """
    config_file = os.getenv("GOOGLE_ADS_CONFIG_FILE")

    if not config_file:
        # Try default locations
        default_paths = [
            "social/platforms/google/google-ads-9474097201.yml",
            Path(__file__).parent / "google-ads-9474097201.yml",
        ]

        for path in default_paths:
            path_obj = Path(path)
            if path_obj.exists():
                logger.info(f"Using default Google Ads config: {path_obj}")
                return str(path_obj)

        raise ConfigurationError(
            "GOOGLE_ADS_CONFIG_FILE environment variable not set and no default config found"
        )

    config_path = Path(config_file)
    if not config_path.exists():
        raise ConfigurationError(f"Google Ads config file not found: {config_file}")

    logger.info(f"Using Google Ads config file: {config_file}")
    return config_file


def get_google_ads_tenants() -> List[Tuple[str, str]]:
    """
    Resolve the list of (config_file, manager_customer_id) tenants to run.

    Resolution order:
      1. GOOGLE_ADS_CONFIG_FILES (comma- or semicolon-separated list of paths)
         -> one tenant per path; manager_customer_id is read from each yaml's
         `login_customer_id`.
      2. GOOGLE_ADS_CONFIG_FILE + GOOGLE_MANAGER_CUSTOMER_ID (legacy single
         tenant). manager_customer_id falls back to `login_customer_id` from
         the yaml if env var is not set.

    Returns:
        Non-empty list of (config_file, manager_customer_id) pairs.

    Raises:
        ConfigurationError: If no tenants can be resolved.
    """
    multi = os.getenv("GOOGLE_ADS_CONFIG_FILES")
    if multi:
        raw_paths = [p.strip() for p in multi.replace(";", ",").split(",") if p.strip()]
        if not raw_paths:
            raise ConfigurationError("GOOGLE_ADS_CONFIG_FILES is set but empty")
        tenants: List[Tuple[str, str]] = []
        for raw in raw_paths:
            mcc = _read_login_customer_id(raw)
            tenants.append((raw, mcc))
        logger.info(f"Multi-tenant mode: {len(tenants)} tenant(s) resolved")
        for cfg, mcc in tenants:
            logger.info(f"  tenant MCC={mcc}  config={cfg}")
        return tenants

    # Single-tenant fallback (legacy)
    cfg = get_google_ads_config_file()
    mcc = os.getenv("GOOGLE_MANAGER_CUSTOMER_ID") or _read_login_customer_id(cfg)
    logger.info(f"Single-tenant mode: MCC={mcc}  config={cfg}")
    return [(cfg, mcc)]


def _read_login_customer_id(config_file: str) -> str:
    """Extract `login_customer_id` from a google-ads.yaml file."""
    path = Path(config_file)
    if not path.exists():
        raise ConfigurationError(f"Google Ads config file not found: {config_file}")
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except Exception as e:
        raise ConfigurationError(f"Failed to parse {config_file}: {e}") from e
    mcc = data.get("login_customer_id")
    if mcc in (None, ""):
        raise ConfigurationError(
            f"`login_customer_id` missing in {config_file}; cannot resolve MCC"
        )
    return str(mcc)


def get_data_sink() -> Optional[object]:
    """
    Initialize data sink based on STORAGE_TYPE environment variable.

    Returns:
        Data sink instance or None if storage not configured

    Raises:
        ConfigurationError: If storage configuration is invalid
    """
    storage_type = os.getenv("STORAGE_TYPE", "none").lower()

    if storage_type == "none":
        logger.warning("No storage backend configured (STORAGE_TYPE=none)")
        return None

    elif storage_type == "vertica":
        logger.info("Initializing Vertica storage backend")

        try:
            from social.infrastructure.database import VerticaDataSink
            from social.core.config import DatabaseConfig

            # Create database config from environment variables
            db_config = DatabaseConfig(
                host=os.getenv("VERTICA_HOST"),
                port=int(os.getenv("VERTICA_PORT", "5433")),
                database=os.getenv("VERTICA_DATABASE"),
                user=os.getenv("VERTICA_USER"),
                password=os.getenv("VERTICA_PASSWORD"),
            )

            # Check if running in test mode
            test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
            logger.info(f"Test mode: {test_mode}")

            return VerticaDataSink(config=db_config, test_mode=test_mode)
        except ImportError:
            raise ConfigurationError("Vertica dependencies not installed")
        except Exception as e:
            raise ConfigurationError(f"Failed to initialize Vertica: {str(e)}")

    elif storage_type == "azure":
        logger.info("Initializing Azure Blob Storage backend")

        try:
            from social.infrastructure.azure_blob_manager import AzureBlobManager

            return AzureBlobManager(
                connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
                container_name=os.getenv("AZURE_CONTAINER_NAME", "google-ads-data"),
            )
        except ImportError:
            raise ConfigurationError("Azure dependencies not installed")
        except Exception as e:
            raise ConfigurationError(f"Failed to initialize Azure: {str(e)}")

    else:
        raise ConfigurationError(f"Unknown storage type: {storage_type}")


def main() -> int:
    """
    Main entry point for Google Ads pipeline.

    Returns:
        Exit code (0 = success, non-zero = error)
    """
    # Setup logging
    setup_logging()
    logger.info("=" * 80)
    logger.info("Google Ads ETL Pipeline Starting")
    logger.info("=" * 80)

    # Initialize execution summary writer
    from datetime import datetime
    from shared.monitoring import ExecutionSummaryWriter

    summary_writer = ExecutionSummaryWriter(
        platform="google",
        storage_connection_string=os.getenv("SUMMARY_STORAGE_CONNECTION_STRING"),
    )

    pipeline_start = datetime.now()

    try:
        # Load configuration
        logger.info("Loading configuration...")
        config_path = get_config_path()
        config = load_config(config_path)

        # Resolve tenant list (one or many google-ads.yaml files)
        tenants = get_google_ads_tenants()
        api_version = os.getenv("GOOGLE_API_VERSION", API_VERSION)
        logger.info(f"API Version: {api_version}")
        logger.info(f"Tenants to process: {len(tenants)}")

        # Initialize token provider (not directly used by Google Ads, kept for
        # protocol compatibility). Google Ads gets its OAuth from the
        # google-ads.yaml file passed to the Google Ads client; this provider
        # is only here so the GoogleAdapter constructor signature is satisfied.
        # FileBasedTokenProvider does eager validation on init and demands
        # legacy single-tenant env vars (GOOGLE_ADS_CONFIG_FILE etc.) which
        # are not needed in multi-tenant mode — fall back to a minimal stub
        # that satisfies the TokenProvider Protocol without performing any I/O.
        try:
            token_provider = FileBasedTokenProvider(
                platform="google",
                credentials_file=None,
            )
        except Exception as e:
            logger.warning(
                f"FileBasedTokenProvider unavailable for google "
                f"(legacy single-tenant env vars missing): {e}. "
                f"Using inert stub — Google Ads auth comes from yaml."
            )
            token_provider = _GoogleNoopTokenProvider()

        # Initialize data sink
        logger.info("Initializing data sink...")
        data_sink = get_data_sink()

        if data_sink:
            logger.success("Data sink initialized successfully")
        else:
            logger.warning("Running without data sink (data will not be persisted)")

        # Per-tenant accumulators
        start_time = datetime.now()
        all_stats: Dict[str, Dict[str, int]] = {}
        all_errors: Dict[str, str] = {}
        per_tenant_summary: list = []

        for tenant_idx, (google_config_file, manager_customer_id) in enumerate(tenants, 1):
            logger.info("=" * 80)
            logger.info(
                f"[tenant {tenant_idx}/{len(tenants)}] MCC={manager_customer_id} "
                f"config={google_config_file}"
            )
            logger.info("=" * 80)

            try:
                pipeline = GooglePipeline(
                    config=config,
                    token_provider=token_provider,
                    google_config_file=google_config_file,
                    manager_customer_id=manager_customer_id,
                    api_version=api_version,
                    data_sink=data_sink,
                    # In multi-tenant runs, prevent each tenant from wiping
                    # other tenants' rows in TRUNCATE-style tables. Order
                    # matters: first matching column in the DF is used.
                    scoped_replace_columns=(
                        ["customer_id_google", "id"]
                        if len(tenants) > 1 else None
                    ),
                )
            except Exception as e:
                logger.error(f"[tenant {manager_customer_id}] pipeline init failed: {e}")
                all_errors[f"_tenant_{manager_customer_id}_init"] = str(e)
                continue

            try:
                tenant_stats, tenant_errors = pipeline.run_all_tables(
                    load_to_sink=(data_sink is not None)
                )
            finally:
                pipeline.close()

            # Namespace stats/errors per tenant so we don't collide across MCCs.
            # When data_sink is None (dry-run), pipeline.run() returns stats=None;
            # coerce to {} so the summary writer can iterate safely.
            for tname, st in tenant_stats.items():
                all_stats[f"{manager_customer_id}::{tname}"] = st or {}
            for tname, msg in tenant_errors.items():
                all_errors[f"{manager_customer_id}::{tname}"] = msg

            tenant_ok = sum(1 for n in tenant_stats if n not in tenant_errors)
            tenant_total = len(tenant_stats)
            per_tenant_summary.append({
                "manager_customer_id": manager_customer_id,
                "tables_successful": tenant_ok,
                "tables_total": tenant_total,
                "tables_failed": len(tenant_errors),
            })
            logger.info(
                f"[tenant {manager_customer_id}] done: "
                f"{tenant_ok}/{tenant_total} tables ok"
            )

        end_time = datetime.now()

        # Analyze results based on error tracking
        tables_succeeded_stats = {n: s for n, s in all_stats.items() if n not in all_errors}
        tables_failed = list(all_errors.keys())
        total = len(all_stats)

        logger.info("=" * 80)
        logger.info(
            f"Pipeline Execution Complete: "
            f"{len(tables_succeeded_stats)}/{total} (table x tenant) successful"
        )
        logger.info("=" * 80)

        # Write execution summary
        metadata = {
            "tenants": per_tenant_summary,
            "tenant_count": len(tenants),
            "api_version": api_version,
            "tables_successful": len(tables_succeeded_stats),
            "tables_total": total,
            "tables_failed": len(tables_failed),
        }

        if not tables_failed:
            logger.success("All tables processed successfully across all tenants")
            summary_writer.write_success(
                start_time=start_time,
                end_time=end_time,
                tables_stats=all_stats,
                exit_code=0,
                metadata=metadata,
            )
            return 0
        elif not tables_succeeded_stats:
            logger.error("All tables failed across all tenants")
            summary_writer.write_failure(
                start_time=start_time,
                end_time=end_time,
                error=f"All {len(tables_failed)} (table x tenant) failed to process",
                exit_code=3,
            )
            return 3
        else:
            logger.warning(
                f"Partial success: {len(tables_succeeded_stats)}/{total} (table x tenant) completed"
            )
            summary_writer.write_partial_success(
                start_time=start_time,
                end_time=end_time,
                tables_succeeded_stats=tables_succeeded_stats,
                tables_failed=tables_failed,
                errors=[{"table": name, "message": all_errors[name]} for name in tables_failed],
                exit_code=3,
                metadata=metadata,
            )
            return 3

    except ConfigurationError as e:
        logger.error(f"Configuration error: {str(e)}")
        summary_writer.write_failure(
            start_time=pipeline_start,
            end_time=datetime.now(),
            error=e,
            exit_code=1,
        )
        return 1

    except AuthenticationError as e:
        logger.error(f"Authentication error: {str(e)}")
        summary_writer.write_failure(
            start_time=pipeline_start,
            end_time=datetime.now(),
            error=e,
            exit_code=2,
        )
        return 2

    except PipelineError as e:
        logger.error(f"Pipeline execution error: {str(e)}")
        summary_writer.write_failure(
            start_time=pipeline_start,
            end_time=datetime.now(),
            error=e,
            exit_code=3,
        )
        return 3

    except Exception as e:
        logger.exception(f"Unexpected error: {str(e)}")
        summary_writer.write_failure(
            start_time=pipeline_start,
            end_time=datetime.now(),
            error=e,
            exit_code=4,
        )
        return 4


if __name__ == "__main__":
    sys.exit(main())
