"""Social repository module."""

from social.repository.social_repository import SocialRepository
from social.repository.operations import Insert, Update, Merge, Truncate, Delete

__all__ = ["SocialRepository", "Insert", "Update", "Merge", "Truncate", "Delete"]
