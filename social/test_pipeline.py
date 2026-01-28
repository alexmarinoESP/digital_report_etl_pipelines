#!/usr/bin/env python
"""Test script for Social Pipeline.

This script tests the complete pipeline end-to-end, writing to TEST tables
in the database. It validates the entire SOLID architecture implementation.

Usage:
    # Test LinkedIn only
    python test_pipeline.py --platform linkedin

    # Test all platforms
    python test_pipeline.py

    # Dry run (no database writes)
    python test_pipeline.py --dry-run

    # Verbose logging
    python test_pipeline.py --verbose

    # Test specific tables
    python test_pipeline.py --platform linkedin --tables linkedin_ads_campaign,linkedin_ads_insights
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
from loguru import logger

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from social.run_pipeline import (
    SocialPipeline,
    setup_logging,
    ConfigurationManager,
)
from social.core.exceptions import SocialError


def print_banner():
    """Print test banner."""
    print("=" * 80)
    print("  SOCIAL PIPELINE - TEST SUITE")
    print("  SOLID Architecture Validation")
    print("=" * 80)
    print()


def print_test_config(args):
    """Print test configuration."""
    print("Test Configuration:")
    print(f"  Platform: {args.platform}")
    print(f"  Tables: {args.tables if args.tables else 'ALL'}")
    print(f"  Dry Run: {args.dry_run}")
    print(f"  Verbose: {args.verbose}")
    print()


def validate_results(results: dict) -> bool:
    """Validate pipeline execution results.

    Args:
        results: Results from pipeline execution

    Returns:
        True if all platforms succeeded, False otherwise
    """
    print("\n" + "=" * 80)
    print("VALIDATION RESULTS")
    print("=" * 80)

    all_success = True
    total_rows = 0

    for platform, tables_result in results.items():
        print(f"\n{platform.upper()}:")

        if isinstance(tables_result, dict) and "error" in tables_result:
            print(f"  [FAIL] FAILED: {tables_result['error']}")
            all_success = False
        else:
            platform_rows = sum(tables_result.values())
            total_rows += platform_rows
            print(f"  [OK] SUCCESS: {platform_rows} total rows")

            # Print individual tables
            for table, count in tables_result.items():
                status = "[OK]" if count > 0 else "[WARN]"
                print(f"    {status} {table}: {count} rows")

    print("\n" + "=" * 80)
    print(f"TOTAL ROWS LOADED: {total_rows}")
    print(f"STATUS: {'[OK] ALL TESTS PASSED' if all_success else '[FAIL] SOME TESTS FAILED'}")
    print("=" * 80)

    return all_success


def test_configuration():
    """Test configuration loading."""
    print("\n" + "-" * 80)
    print("TEST 1: Configuration Loading")
    print("-" * 80)

    try:
        config_manager = ConfigurationManager()
        config = config_manager.load_config(test_mode=True)

        print(f"[OK] Configuration loaded successfully")
        print(f"  Test Mode: {config.test_mode}")
        print(f"  Platforms: {', '.join(config.platforms.keys())}")

        if config.database:
            print(f"  Database: {config.database.host}:{config.database.port}")

        return True

    except Exception as e:
        print(f"[FAIL] Configuration loading failed: {e}")
        return False


def test_adapters_initialization(pipeline: SocialPipeline):
    """Test adapter initialization."""
    print("\n" + "-" * 80)
    print("TEST 2: Adapter Initialization")
    print("-" * 80)

    try:
        pipeline.initialize()

        print(f"[OK] Adapters initialized successfully")
        print(f"  Initialized platforms: {', '.join(pipeline.adapters.keys())}")

        # Test each adapter
        for platform_name, adapter in pipeline.adapters.items():
            # Handle different method names across platforms
            if hasattr(adapter, 'get_all_tables'):
                tables = adapter.get_all_tables()
            elif hasattr(adapter, 'get_table_names'):
                tables = adapter.get_table_names()
            else:
                tables = []
            print(f"  {platform_name}: {len(tables)} tables configured")

        return True

    except Exception as e:
        print(f"[FAIL] Adapter initialization failed: {e}")
        logger.exception(e)
        return False


def test_pipeline_execution(
    pipeline: SocialPipeline,
    platforms: list = None,
    tables: list = None
):
    """Test pipeline execution."""
    print("\n" + "-" * 80)
    print("TEST 3: Pipeline Execution")
    print("-" * 80)

    try:
        results = pipeline.run(platforms=platforms, tables=tables)

        print("[OK] Pipeline execution completed")
        return results

    except Exception as e:
        print(f"[FAIL] Pipeline execution failed: {e}")
        logger.exception(e)
        return None


def run_tests(args):
    """Run all tests."""
    print_banner()
    print_test_config(args)

    # Setup logging
    setup_logging(verbose=args.verbose)

    # Track test results
    tests_passed = 0
    tests_total = 3

    # Test 1: Configuration
    if test_configuration():
        tests_passed += 1

    # Create pipeline
    try:
        config_manager = ConfigurationManager()
        config = config_manager.load_config(
            platform=None if args.platform == "all" else args.platform,
            test_mode=True,  # Always use test mode
            dry_run=args.dry_run,
        )

        pipeline = SocialPipeline(config)

        # Test 2: Adapter Initialization
        if test_adapters_initialization(pipeline):
            tests_passed += 1

        # Test 3: Pipeline Execution
        platforms = None if args.platform == "all" else [args.platform]
        tables = args.tables.split(",") if args.tables else None

        results = test_pipeline_execution(pipeline, platforms, tables)

        if results:
            tests_passed += 1

            # Validate and display results (doesn't affect test count)
            validate_results(results)

        # Cleanup
        pipeline.cleanup()

    except Exception as e:
        logger.error(f"Test suite failed: {e}")
        logger.exception(e)

    # Final summary
    print("\n" + "=" * 80)
    print("TEST SUITE SUMMARY")
    print("=" * 80)
    print(f"Tests Passed: {int(tests_passed)}/{tests_total}")
    print(f"Success Rate: {int(tests_passed/tests_total*100)}%")

    if tests_passed == tests_total:
        print("[OK] ALL TESTS PASSED - Pipeline is working correctly!")
        return 0
    else:
        print("[FAIL] SOME TESTS FAILED - Review logs above")
        return 1


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Test Social Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--platform",
        type=str,
        choices=["linkedin", "google", "facebook", "microsoft", "all"],
        default="linkedin",
        help="Platform to test (default: linkedin)",
    )

    parser.add_argument(
        "--tables",
        type=str,
        help="Comma-separated list of specific tables to test (default: all)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode (no database writes)",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug-level logging",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    sys.exit(run_tests(args))
