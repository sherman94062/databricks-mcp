"""
client/__init__.py - Factory: returns MockClient or RealClient based on config.
"""

from databricks_mcp.config import settings
from .base import DatabricksClientBase
from .mock import MockClient
from .real import RealClient


def get_client() -> DatabricksClientBase:
    if settings.mock:
        return MockClient()
    settings.validate_live()
    return RealClient(
        host=settings.host,
        token=settings.token,
        warehouse_id=settings.warehouse_id,
        default_catalog=settings.default_catalog,
    )
