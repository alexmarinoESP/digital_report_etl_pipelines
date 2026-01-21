"""
Database operations for social data.
Strategy pattern implementation for Insert, Update, Merge, Truncate, Delete.
"""

from abc import ABC, abstractmethod

from social import SCHEMA
from social.repository.social_repository import SocialRepository


class Operation(ABC):
    """Abstract base class for database operations."""

    @abstractmethod
    def do_operation(self, **kwargs):
        pass


class DBContext:
    """Context for database operations (Strategy pattern)."""

    def __init__(self, op: Operation):
        self.operation = op

    def set_operation(self, op: Operation):
        self.operation = op

    def do_operation(self, **kwargs):
        self.operation.do_operation(**kwargs)


class Insert(Operation):
    """Insert operation."""

    def __init__(self):
        self.repository = SocialRepository()

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def do_operation(self, connect, data, cfg, table_name):
        with connect.connect_to_vertica().cursor() as cur:
            self.repository.write_to_db(
                table_name=table_name,
                schema=SCHEMA,
                data=data,
                cur=cur,
                params_query=cfg,
            )


class Update(Operation):
    """Update operation."""

    def __init__(self):
        self.repository = SocialRepository()

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def do_operation(self, connect, data, cfg, table_name):
        # Update logic would go here
        # For brevity, using simplified version
        pass


class Merge(Operation):
    """Merge operation."""

    def __init__(self):
        self.repository = SocialRepository()

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def do_operation(self, connect, data, cfg, table_name):
        # Merge logic would go here
        pass


class Truncate(Operation):
    """Truncate operation."""

    def __init__(self):
        self.repository = SocialRepository()

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def do_operation(self, connect, data, cfg, table_name):
        with connect.connect_to_vertica().cursor() as cur:
            self.repository.truncate_table(
                schema=SCHEMA, table_name=table_name, cur=cur
            )


class Delete(Operation):
    """Delete operation."""

    def __init__(self):
        self.repository = SocialRepository()

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def do_operation(self, connect, data, cfg, table_name):
        period = cfg.get("delete").get("period")
        delete_col = cfg.get("delete").get("delete_col", "row_loaded_date")
        with connect.connect_to_vertica().cursor() as cur:
            self.repository.delete(
                table_name=table_name,
                cur=cur,
                min_date=period,
                delete_col=delete_col,
            )


class Dispatch:
    """
    Dispatcher for database operations.
    Maps operation names to operation classes.
    """

    def __init__(self):
        self.methods = {
            "merge": Merge,
            "delete": Delete,
            "truncate": Truncate,
            "update": Update,
            "insert": Insert,
        }

    def dispatch(self, string):
        """Get operation class by name."""
        from loguru import logger
        import sys

        meth = self.methods.get(string, None)
        if meth is None:
            logger.error(
                "Method not available! You should add it to the constructor "
                "of the Dispatch class"
            )
            sys.exit(-1)
        else:
            return meth
