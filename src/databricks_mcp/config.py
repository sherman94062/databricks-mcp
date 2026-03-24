"""
config.py - loads settings from environment variables / .env file.
All other modules import from here; nothing touches os.environ directly.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root (silently ignored if absent)
load_dotenv(Path(__file__).parent.parent.parent / ".env", override=False)


class Settings:
    """Central config object. Instantiated once at import time."""

    def __init__(self) -> None:
        self.mock: bool = os.getenv("DATABRICKS_MOCK", "true").lower() in ("1", "true", "yes")
        self.host: str = os.getenv("DATABRICKS_HOST", "")
        self.token: str = os.getenv("DATABRICKS_TOKEN", "")
        self.warehouse_id: str = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
        self.default_catalog: str = os.getenv("DATABRICKS_CATALOG", "main")
        self.default_schema: str = os.getenv("DATABRICKS_SCHEMA", "default")

    def validate_live(self) -> None:
        """Raise if live mode is requested but credentials are missing."""
        missing = [f for f in ("host", "token") if not getattr(self, f)]
        if missing:
            raise EnvironmentError(
                "DATABRICKS_MOCK=false but these env vars are not set: "
                + ", ".join(f"DATABRICKS_{m.upper()}" for m in missing)
            )


settings = Settings()
