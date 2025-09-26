import os
from typing import List


class Config:
    """Environment-driven configuration for the ETL app ie config comes from env vars, with sensible fallbacks"""

    VALID_LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

    def __init__(self):
        """Load configuration from environment variables with sensible defaults."""
        self.forces = self._parse_forces()
        self.database_url = self._get_database_url()
        self.log_level = self._get_log_level()

    def _parse_forces(self) -> List[str]:
        """split comma-separated list of forces, default = metropolitan"""
        forces_str = os.environ.get("FORCES", "metropolitan")
        return [force.strip() for force in forces_str.split(",")]

    def _get_database_url(self) -> str:
        """default to sqlite file if nothing set"""
        return os.environ.get("DATABASE_URL", "sqlite:///stopsearch.db")

    def _get_log_level(self) -> str:
        """grab log level, make sure it's valid"""
        log_level = os.environ.get("LOG_LEVEL", "INFO").upper()

        if log_level not in self.VALID_LOG_LEVELS:
            raise ValueError(f"Invalid log level '{log_level}'. Must be one of: {self.VALID_LOG_LEVELS}")

        return log_level