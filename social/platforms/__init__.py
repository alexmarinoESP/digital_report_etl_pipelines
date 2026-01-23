"""Social platforms module.

This module contains platform-specific implementations:
- microsoft: Microsoft Ads (Bing Ads SDK v13)
- linkedin: LinkedIn Ads (REST API v202601)
- facebook: Facebook Ads (Graph API SDK v19.0)
- google: Google Ads (gRPC + Protobuf)

Each platform is completely independent and self-contained.
Import from specific platform modules directly:

    from social.platforms.linkedin.pipeline import LinkedInPipeline
    from social.platforms.facebook.pipeline import FacebookPipeline
    from social.platforms.google.pipeline import GooglePipeline
    from social.platforms.microsoft.pipeline import MicrosoftPipeline
"""

__all__ = []
