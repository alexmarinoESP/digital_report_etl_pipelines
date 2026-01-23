"""
DEPRECATED: This module is deprecated as of Phase 6 cleanup (January 2026).

The old adapter architecture with base classes has been completely replaced
by platform-specific implementations in the new architecture:

  OLD (deprecated):
    - social.adapters.base.BaseAdsPlatformAdapter
    - social.adapters.linkedin_adapter.LinkedInAdsAdapter
    - social.adapters.google_adapter.GoogleAdsAdapter
    - social.adapters.facebook_adapter.FacebookAdsAdapter

  NEW (use these instead):
    - social.platforms.linkedin.adapter.LinkedInAdapter
    - social.platforms.google.adapter.GoogleAdapter
    - social.platforms.facebook.adapter.FacebookAdapter
    - social.platforms.microsoft.client.MicrosoftAdsClient

For orchestrated execution of all platforms, use:
    - social.orchestrator.run_orchestrator (recommended)

Migration completed: January 2026
Architecture: Protocol-based (no base classes), platform independence
"""

__all__ = []

# Keep module importable but empty to prevent breaking existing imports
# All functionality has been migrated to social/platforms/
