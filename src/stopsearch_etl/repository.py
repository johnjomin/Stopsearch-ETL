from abc import ABC, abstractmethod
from typing import List

from .domain import StopSearchRecord


class StopSearchRepository(ABC):
    """Abstract repository for persisting stop & search records."""
    # base repo interface for saving / fetching stop & search records

    @abstractmethod
    def save(self, record: StopSearchRecord) -> None:
        """Save a single record"""
        pass

    @abstractmethod
    def save_batch(self, records: List[StopSearchRecord]) -> int:
        """
        Save multiple at once

        Returns:
            Number of records actually saved (after deduplication)
        """
        pass

    @abstractmethod
    def find_by_force_and_month(self, force: str, year_month: str) -> List[StopSearchRecord]:
        """Find all records for a specific force and month"""
        pass