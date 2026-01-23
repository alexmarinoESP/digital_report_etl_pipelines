"""
SQL Queries per SMS Pipeline.

Tutte le query SQL con JinjaSQL templates.
"""

from sms.sql.queries import (
    QUERY_SMS_CAMPAIGNS,
    CHECK_CAMPAIGN_EXISTS,
    INSERT_CAMPAIGN,
    INSERT_LINK,
)

__all__ = [
    "QUERY_SMS_CAMPAIGNS",
    "CHECK_CAMPAIGN_EXISTS",
    "INSERT_CAMPAIGN",
    "INSERT_LINK",
]
