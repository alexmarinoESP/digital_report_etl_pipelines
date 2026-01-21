"""Newsletter API adapters."""

from newsletter.adapters.mapp_client import MappConnector
from newsletter.adapters.hcti_client import HctiClient

__all__ = ["MappConnector", "HctiClient"]
