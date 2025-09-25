from abc import ABC, abstractmethod


# Custom error just for our Police API
class ApiError(Exception):
    """Raised when API requests fail or return unexpected data."""
    pass


class PoliceApiClient(ABC):
    """Abstract interface for fetching UK Police stop & search data."""

    @abstractmethod
    def fetch_stops(self, force: str, year_month: str) -> list[dict]:
        """
        Grab stop & search records for a given police force + month.

        Args:
            force: Police force uid (ie 'metropolitan')
            year_month: Year-month format

        Returns:
            List of stop & search record dictionaries from the API
        """
        pass