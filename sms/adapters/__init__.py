"""
SMS Adapters Layer.

This module contains the infrastructure implementations of domain interfaces.
Adapters connect the domain to external systems (databases, APIs).

SQL queries are defined in sms.sql module for clean separation:
- Queries are domain knowledge, not infrastructure
- Documented with business context
- Reusable across different adapters
"""

from sms.adapters.mapp_sms_adapter import MappSMSAdapter
from sms.adapters.bitly_adapter import BitlyAdapter
from sms.adapters.repository_adapter import SMSRepositoryAdapter

__all__ = [
    "MappSMSAdapter",
    "BitlyAdapter",
    "SMSRepositoryAdapter",
]
